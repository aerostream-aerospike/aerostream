# How Aerospike Stores Data

A technical walkthrough of the Aerospike storage architecture, grounded in the
actual server source (Community Edition 8.x, the same tree AeroStream is built
on). Every term here maps to a real struct or constant in
`aerospike-server/as/` so you can follow the code.

The companion file [`index.html`](index.html) animates this same pipeline. Open
it in any browser.

---

## 1. The data model hierarchy

Aerospike organizes data in a fixed hierarchy. From the outside in:

```
cluster
  └─ node
       └─ namespace            (a storage + policy domain, e.g. "aerostream")
            └─ set             (an optional named grouping, like a table)
                 └─ record     (one row, addressed by a key)
                      └─ bin    (a named field, like a column)
                           └─ particle   (the typed value held by a bin)
```

| Concept     | Source                      | What it is                                                  |
|-------------|-----------------------------|-------------------------------------------------------------|
| `namespace` | `as_namespace` (datamodel.h)| The top-level container. Owns a storage engine, replication factor, retention policy, and a primary index. Roughly a "database." |
| `set`       | `as_set` / `set_id_bits`    | An optional label on records inside a namespace. Like a table, but schemaless. Stored as a 12-bit `set_id` in the index entry. |
| `record`    | `as_record` = `as_index`    | One addressable unit. Identified by a 20-byte **digest**. |
| `bin`       | `as_bin` (datamodel.h)      | A named value within a record. A record can have many bins. |
| `particle`  | `as_particle` + `as_particle_type` | The actual typed value: INTEGER, STRING, BLOB, DOUBLE, BOOL, LIST, MAP, GEOJSON, HLL, etc. |

Particle types (from `as_particle_type` in `datamodel.h`):

```
NULL=0  INTEGER=1  FLOAT=2  STRING=3  BLOB=4  VECTOR=16
BOOL=17  HLL=18  MAP=19  LIST=20  GEOJSON=23
```

A record is **schemaless**: two records in the same set can have completely
different bins. The schema lives in your application, not the database.

---

## 2. From key to digest to partition

When you write or read a record, you supply `(namespace, set, key)`. Aerospike
turns that into a fixed address in three steps.

### 2.1 The digest (20-byte RIPEMD-160)

The set name and your key are hashed into a 20-byte digest (`cf_digest`,
RIPEMD-160). This is the record's true identity inside the namespace. The
original key is only stored on the record if you ask for it (`key_stored` bit);
otherwise the digest is all the server keeps.

```c
typedef struct cf_digest_s { uint8_t digest[20]; } cf_digest;
```

### 2.2 The partition (one of 4096)

Aerospike statically divides every namespace into **4096 partitions**
(`AS_PARTITIONS`). The partition id is just the low 12 bits of the first 4 bytes
of the digest:

```c
#define AS_PARTITIONS      4096
#define AS_PARTITION_MASK  (AS_PARTITIONS - 1)   // 0xFFF

static inline uint32_t
as_partition_getid(const cf_digest* d) {
    return *(uint32_t*)d & AS_PARTITION_MASK;
}
```

Partitions are the unit of:
- **distribution** — each partition has a master node and `replication-factor - 1`
  replica nodes, assigned by a deterministic rebalance algorithm.
- **migration** — when nodes join or leave, whole partitions move between nodes.
- **reservation** — a transaction "reserves" a partition (`as_partition_reserve`)
  to pin its index tree while it reads or writes.

Because the partition is derived purely from the digest, any node in the cluster
can compute where a record lives without a lookup. The client does the same math
to route requests straight to the master node.

---

## 3. The primary index: where the record metadata lives

Every record has a 64-byte **index entry** (`as_index`, which is typedef'd to
`as_record`). The index is always in RAM, even when the bin data lives on SSD.
This is the heart of Aerospike's hybrid memory model: **index in memory, data
wherever you configure it.**

### 3.1 The as_index entry (64 bytes)

The most important fields (`as/include/base/index.h`):

```c
typedef struct as_index_s {
    uint16_t  rc;                    // reference count
    uint8_t   tree_id : 6;
    cf_digest keyd;                  // the 20-byte digest (record identity)

    uint64_t  left_h  : 40;          // red-black tree children (arena handles)
    uint64_t  right_h : 40;

    uint16_t  set_id_bits : 12;      // which set this record belongs to

    uint32_t  void_time : 30;        // expiration time (TTL) - 0 = never
    uint64_t  last_update_time : 40; // LUT, for conflict resolution
    uint64_t  generation : 16;       // bumped on every write

    // --- pointer into the storage layer ---
    uint64_t  rblock_id : 37;        // where the data starts on the device
    uint64_t  n_rblocks : 19;        // how many rblocks the record occupies
    uint64_t  file_id   : 7;         // which device/file (up to 128)
    uint64_t  key_stored : 1;

    uint8_t   repl_state : 2;
    uint8_t   tombstone  : 1;        // deleted-but-tracked marker
} as_index;
```

Note what is *in the index* versus *in storage*:
- **In the index (RAM):** identity (digest), set, TTL (`void_time`), version
  (`generation`, `last_update_time`), and a pointer to the data
  (`file_id` + `rblock_id` + `n_rblocks`).
- **In storage:** the actual bins and their particle values.

For a pure in-memory namespace the data lives in RAM too, but the index/data
split is the same shape.

### 3.2 Red-black trees, sprigs, and the arena

Each of the 4096 partitions owns a primary index tree (`as_index_tree`). To
avoid one giant lock and one giant tree, each tree is split into many **sprigs**
— independent red-black sub-trees, each with its own lock. The digest bits pick
which sprig a record lands in (`NUM_SPRIG_BITS = 28`, so up to hundreds of
millions of sprigs are addressable).

```
namespace
  └─ partition[0..4095]
       └─ as_index_tree
            └─ sprig[i]            (a lock-striped red-black sub-tree)
                 └─ as_index        (64-byte node, an arena handle away)
```

The 64-byte index entries are not malloc'd individually. They live in an
**arena** (`cf_arenax`): a set of large fixed pre-allocated **stages**, carved
into equal **elements**. An index node is addressed by a 40-bit
`cf_arenax_handle` (which stage + which slot), not a raw pointer. That is why
`left_h` / `right_h` above are handles, not pointers — it keeps each node small
and lets the index survive a warm restart by re-attaching the arena.

```
cf_arenax
  └─ stage[0..N]                  (big contiguous blocks)
       └─ element[0..M]           (one 64-byte as_index each)
```

---

## 4. The storage layer: rblocks, wblocks, and flat records

The index points at the data with `file_id + rblock_id + n_rblocks`. Here is
what those mean.

### 4.1 rblock — the addressing unit

Storage is addressed in **rblocks** (read blocks). The minimum increment is
`RBLOCK_SIZE` (16 bytes). A record's size on device is rounded up to a whole
number of rblocks, and `rblock_id` is just a byte offset shifted down:

```c
#define LOG_2_RBLOCK_SIZE  ...
rblock_id = offset >> LOG_2_RBLOCK_SIZE;   // OFFSET_TO_RBLOCK_ID
offset    = rblock_id << LOG_2_RBLOCK_SIZE;
```

37 bits of `rblock_id` × 16 bytes addresses a 2 TB device; 19 bits of
`n_rblocks` lets a single record span up to ~8 MB.

### 4.2 wblock — the write/erase unit

Storage is physically managed in **wblocks** (write blocks), sized by
`write-block-size` (commonly 1–8 MB). A wblock is the unit Aerospike writes and
later reclaims. Each wblock has a state:

```c
WBLOCK_STATE_FREE      0   // available
WBLOCK_STATE_RESERVED  1   // claimed for the current write buffer
WBLOCK_STATE_USED      2   // full of live + dead records
WBLOCK_STATE_DEFRAG    3   // being reclaimed
WBLOCK_STATE_EMPTYING  4   // draining
```

### 4.3 The on-device record: as_flat_record

A record is serialized to a packed **flat record** before it hits the device
(`as/include/storage/flat.h`):

```c
#define AS_FLAT_MAGIC 0x037AF201

typedef struct as_flat_record_s {
    uint32_t  magic;          // 0x037AF201 - sanity check on read
    uint32_t  n_rblocks : 19; // self-describing length
    uint32_t  has_bins  : 1;
    ...
    cf_digest keyd;           // the record's digest, repeated here
    // followed by: metadata (LUT, void_time, ...) then packed bins/particles
} as_flat_record;
```

The flat record carries its own digest and length so the storage layer can
validate it and the index can be rebuilt from the device on a cold start.

---

## 5. The write path

Putting it together, here is what happens when a record is written:

1. **Route.** Client computes the digest, derives the partition (`& 0xFFF`), and
   sends the write to that partition's **master node**.
2. **Reserve.** The server reserves the partition (`as_partition_reserve`) and
   locks the target **sprig**.
3. **Index lookup/insert.** It finds or creates the `as_index` node in the sprig
   (an arena element). Generation and `last_update_time` are bumped.
4. **Serialize.** The bins are packed into an `as_flat_record`.
5. **Streaming write.** The flat record is appended to the current in-memory
   **streaming write buffer** for a `RESERVED` wblock. When the buffer fills, the
   wblock is flushed to the device and a fresh one is reserved.
6. **Point the index.** `file_id`, `rblock_id`, and `n_rblocks` in the index
   entry are updated to point at the new location.
7. **Replicate.** The write is shipped to the replica node(s) per
   `replication-factor` before the client gets its ack (in the default commit
   level).
8. **Post-write cache.** Recently written wblocks stay in a post-write queue so
   immediate reads are served from RAM, not the device.

A key consequence: writes are **append-only**. Updating a record does not
overwrite the old bytes. It writes a *new* flat record elsewhere and re-points
the index. The old copy becomes dead weight in its wblock.

---

## 6. The read path

1. **Route + reserve** the partition (same as write).
2. **Index find.** Walk the sprig's red-black tree by digest to the `as_index`
   node. If not found, the record does not exist.
3. **Locate data.** Read `file_id + rblock_id + n_rblocks` from the index.
4. **Fetch.** For an in-memory namespace the bytes are already in RAM. For a
   device namespace, read the rblocks from the post-write cache if present, else
   from SSD.
5. **Unpack.** Validate the `as_flat_record` magic, then unpack the requested
   bins into `as_bin` structures (`as_flat_unpack_bins`) and turn particles back
   into typed values.

A point read is therefore: one in-RAM tree walk, then at most one device read.
That single-hop design is why p99 read latency stays low even on SSD.

---

## 7. Reclaiming space: defragmentation

Because writes are append-only, wblocks slowly fill with dead records (old
versions, deletes, expirations). The **defrag** subsystem walks `USED` wblocks,
measures how much is still live, and once a wblock falls below a fill threshold
it:

1. copies the surviving records out to the current write buffer,
2. re-points their index entries,
3. returns the now-empty wblock to the `FREE` pool.

There is no separate compaction service and no log-segment bookkeeping — defrag
is the single, continuous space-reclamation mechanism.

---

## 8. Expiration and eviction: the namespace supervisor (nsup)

Each record can carry a **TTL**, stored as `void_time` in the index (an absolute
expiration time, 0 means never). The **namespace supervisor** thread (`nsup`,
controlled by `nsup-period`) periodically sweeps the index and:

- **expires** records whose `void_time` has passed (removes the index entry; the
  data becomes dead and is later defragged), and
- **evicts** records early if the namespace is running low on space, oldest-TTL
  first.

This is exactly the mechanism AeroStream leans on for log retention: every
stream record is written with a TTL, and nsup ages them out with no compaction
process of its own. (It is also why the `aerostream` namespace must set
`nsup-period > 0` — Aerospike refuses to write a record with a TTL if the
supervisor that would expire it is disabled.)

---

## 9. The hybrid memory model in one sentence

> The **primary index always lives in RAM** (64 bytes per record in an arena of
> lock-striped red-black sprigs, one tree per 4096 partitions), while the
> **bin data lives wherever you configure the storage engine** — RAM, SSD, or
> persistent memory — addressed by `file_id + rblock_id`, written append-only in
> wblocks, reclaimed by defrag, and expired by nsup.

That separation is what lets Aerospike index billions of records in a modest
amount of RAM while serving the actual values from cheap, dense flash at
memory-like latency.

---

## 10. Where AeroStream plugs in

AeroStream stores each stream record as an ordinary Aerospike record in the
`aerostream` namespace:

- **key** = `"{stream}:{partition}:{offset}"` → hashed to a digest → routed to
  one of the 4096 partitions like any other record,
- **bins** = `payload` (BLOB particle), `ts` (INTEGER), `offset` (INTEGER),
- **TTL** = the stream's retention, expired by nsup,
- **reads** (the consumer push loop) go through the exact read path in §6:
  `as_partition_reserve` → index find by digest → `as_storage_rd_load_bins`.

So a stream is not a special storage structure. It is a naming convention plus a
small amount of in-memory offset state on top of the same record engine
described above.

---

### Glossary

| Term | Meaning |
|------|---------|
| namespace | top-level storage + policy domain |
| set | optional table-like grouping within a namespace |
| record | one addressable row (`as_index` / `as_record`) |
| bin | a named field within a record |
| particle | the typed value in a bin (INTEGER, BLOB, MAP, ...) |
| digest | 20-byte RIPEMD-160 of (set, key); the record's identity |
| partition | one of 4096 shards; unit of distribution/migration |
| primary index | in-RAM red-black trees mapping digest → record metadata |
| sprig | a lock-striped red-black sub-tree of a partition's index |
| arena (`cf_arenax`) | pre-allocated pool the 64-byte index nodes live in |
| as_index | the 64-byte per-record metadata entry |
| rblock | 16-byte storage addressing unit (`rblock_id`) |
| wblock | write/erase block (`write-block-size`), unit of flush + defrag |
| as_flat_record | the packed on-device serialization of a record |
| void_time | absolute expiration time (TTL) in the index |
| nsup | namespace supervisor: expires + evicts records |
| defrag | reclaims partially-dead wblocks |
| replication-factor | how many nodes hold each partition |
