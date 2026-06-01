# AeroStream Wire Protocol Specification

**Version:** 0.1.0-draft  
**Status:** RFC  
**Base:** Aerospike Wire Protocol v2 (port 3000)  
**Author:** AeroStream Project

---

## 1. Overview

I extended the Aerospike binary wire protocol with eight new message types
covering stream produce, consume, offset management, pub/sub, and replay.
All messages run over the existing port 3000 TCP connection, share the
existing 8-byte `as_proto` header, and go through the same TLS and
authentication pipeline as standard Aerospike messages.

No new ports. No new connection pools. No new firewall rules.

---

## 2. Existing Protocol Header (unchanged)

Every Aerospike message - including AeroStream messages - begins with the
standard 8-byte `as_proto` header defined in `as_proto.h`:

```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|  version (1)  |   type (1)    |                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+    size (6 bytes, big-endian) +
|                                                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

| Field   | Size | Description                                      |
|---------|------|--------------------------------------------------|
| version | 1B   | Protocol version. AeroStream uses `2` (unchanged)|
| type    | 1B   | Message type. AeroStream claims `10`-`17`        |
| size    | 6B   | Payload size in bytes, big-endian, follows header|

### 2.1 New Type Values

| Value | Constant                        | Direction        |
|-------|---------------------------------|------------------|
| `10`  | `AS_PROTO_TYPE_STREAM_PRODUCE`  | Client → Server  |
| `11`  | `AS_PROTO_TYPE_STREAM_PROD_ACK` | Server → Client  |
| `12`  | `AS_PROTO_TYPE_STREAM_CONSUME`  | Client → Server  |
| `13`  | `AS_PROTO_TYPE_STREAM_RECORD`   | Server → Client  |
| `14`  | `AS_PROTO_TYPE_STREAM_ACK`      | Client → Server  |
| `15`  | `AS_PROTO_TYPE_STREAM_SEEK`     | Client → Server  |
| `16`  | `AS_PROTO_TYPE_STREAM_SUB`      | Client → Server  |
| `17`  | `AS_PROTO_TYPE_STREAM_UNSUB`    | Client → Server  |

---

## 3. Common Structures

All multi-byte integers are **big-endian** (network byte order), consistent
with the existing Aerospike protocol.

### 3.1 Stream Header (present in every AeroStream message body)

```c
typedef struct __attribute__((packed)) {
    uint64_t correlation_id;   // client-generated request ID, echoed in response
    uint8_t  stream_name[64];  // null-terminated UTF-8, max 63 chars
} as_stream_hdr;
```

### 3.2 Record Header (present in PRODUCE and RECORD messages)

Packed size is **22 bytes** (8 + 8 + 2 + 4).

```c
typedef struct __attribute__((packed)) {
    int64_t  offset;           // -1 on produce (server assigns), set on deliver
    uint64_t timestamp_ns;     // Unix nanoseconds, set by server on commit
    uint16_t headers_count;    // number of key-value header pairs that follow
    uint32_t payload_size;     // byte length of the payload that follows headers
} as_stream_record_hdr;
```

### 3.3 Record Header Entry

```c
typedef struct __attribute__((packed)) {
    uint16_t key_size;         // byte length of key
    uint16_t val_size;         // byte length of value
    // followed by: uint8_t key[key_size], uint8_t val[val_size]
} as_stream_header_entry;
```

---

## 4. Message Definitions

---

### 4.1 STREAM_PRODUCE (type `10`) - Client → Server

Appends one record to a named stream. The server assigns the offset and
timestamp atomically using `cf_atomic64_incr` on the partition's offset
counter, then writes the record to the storage engine.

**Body layout:**

```
+------------------+ 0
| as_stream_hdr    | 72 bytes
+------------------+ 72
| partition_key    | 64 bytes  null-terminated, used for consistent-hash routing
+------------------+ 136
| ack_mode (1B)    |           0x00 = none, 0x01 = leader, 0x02 = all replicas
+------------------+ 137
| as_stream_record_hdr | 22 bytes  (offset field ignored by server on produce)
+------------------+ 159
| header entries   | variable  (headers_count × as_stream_header_entry + data)
+------------------+
| payload          | variable  (payload_size bytes)
+------------------+
```

**C struct:**

```c
typedef struct __attribute__((packed)) {
    as_stream_hdr      hdr;
    uint8_t            partition_key[64];
    uint8_t            ack_mode;
    as_stream_record_hdr rec_hdr;
    // followed by header entries then payload bytes
} as_stream_produce_msg;
```

**Server behavior:**

1. Hash `partition_key` → `partition_id = fnv32a(partition_key) % num_partitions`
2. Atomically increment offset counter: `offset = cf_atomic64_incr(&partition->offset_seq)`
3. Stamp `timestamp_ns = cf_clock_getabstime_ns()`
4. Write record: key = `"{stream_name}:{partition_id}:{offset}"`, bins = `{payload, ts, headers, offset}`
5. If `ack_mode > 0x00`, send `STREAM_PROD_ACK`

---

### 4.2 STREAM_PROD_ACK (type `11`) - Server → Client

Confirms a produce. Echoes `correlation_id` so the client can match the ack to
the original request in an async pipeline.

**Body layout:**

```
+---------------------+ 0
| correlation_id (8B) |
+---------------------+ 8
| offset (8B)         |  assigned offset, int64, big-endian
+---------------------+ 16
| partition_id (4B)   |  uint32
+---------------------+ 20
| timestamp_ns (8B)   |  uint64, Unix nanoseconds
+---------------------+ 28
| status (1B)         |  0x00 = ok, 0x01 = stream not found, 0x02 = storage err
+---------------------+ 29
```

**C struct:**

```c
typedef struct __attribute__((packed)) {
    uint64_t correlation_id;
    int64_t  offset;
    uint32_t partition_id;
    uint64_t timestamp_ns;
    uint8_t  status;
} as_stream_prod_ack_msg;
```

---

### 4.3 STREAM_CONSUME (type `12`) - Client → Server

Registers a consumer group session on one or all partitions of a stream. The
server enters a push loop, delivering `STREAM_RECORD` messages as records become
available. The session persists until the connection closes or an `UNSUB` is received.

**Body layout:**

```
+------------------+ 0
| as_stream_hdr    | 72 bytes
+------------------+ 72
| group_name[64]   | null-terminated consumer group identifier
+------------------+ 136
| partition_id (4B)| uint32, specific partition or 0xFFFFFFFF = all partitions
+------------------+ 140
| seek_type (1B)   | 0x00 = latest, 0x01 = earliest, 0x02 = offset, 0x03 = timestamp
+------------------+ 141
| seek_value (8B)  | int64 offset or uint64 timestamp_ns (per seek_type)
+------------------+ 149
| max_in_flight(4B)| uint32, max unacked records before server pauses (default: 10)
+------------------+ 153
```

**C struct:**

```c
typedef struct __attribute__((packed)) {
    as_stream_hdr hdr;
    uint8_t       group_name[64];
    uint32_t      partition_id;
    uint8_t       seek_type;
    int64_t       seek_value;
    uint32_t      max_in_flight;
} as_stream_consume_msg;
```

**Seek type constants:**

```c
#define AS_STREAM_SEEK_LATEST     0x00
#define AS_STREAM_SEEK_EARLIEST   0x01
#define AS_STREAM_SEEK_OFFSET     0x02
#define AS_STREAM_SEEK_TIMESTAMP  0x03
```

**Server behavior:**

- If `partition_id` is a specific value: registers a single-partition push session.
- If `partition_id == 0xFFFFFFFF`: registers a session across all partitions.
  Records from all partitions are **multiplexed onto the single connection**. Each
  delivered `STREAM_RECORD` carries its `partition_id` so the client can demux.
  `STREAM_ACK` messages must include the correct `partition_id` for each record.
- `max_in_flight` of `0` is treated as the default value of `10`. The limit applies
  per-connection across all multiplexed partitions.

---

### 4.4 STREAM_RECORD (type `13`) - Server → Client

Delivers a single record to a consumer. Sent by the server push loop initiated
by `STREAM_CONSUME`. The client must send `STREAM_ACK` to advance the group
offset and release the in-flight slot.

**Body layout:**

```
+----------------------+ 0
| correlation_id (8B)  |  echoes the CONSUME request's correlation_id
+----------------------+ 8
| partition_id (4B)    |
+----------------------+ 12
| as_stream_record_hdr | 22 bytes  (offset and timestamp_ns are set)
+----------------------+ 34
| header entries       | variable
+----------------------+
| payload              | variable
+----------------------+
```

**C struct:**

```c
typedef struct __attribute__((packed)) {
    uint64_t             correlation_id;
    uint32_t             partition_id;
    as_stream_record_hdr rec_hdr;
    // followed by header entries then payload bytes
} as_stream_record_msg;
```

---

### 4.5 STREAM_ACK (type `14`) - Client → Server

Commits a consumer group offset. The server atomically CAS-updates the group's
committed offset in the `consumer_offsets` set using `as_operate` with a
compare-and-swap operation, preventing double-commit races.

**Body layout:**

```
+------------------+ 0
| as_stream_hdr    | 72 bytes
+------------------+ 72
| group_name[64]   |
+------------------+ 136
| partition_id (4B)|
+------------------+ 140
| offset (8B)      | int64, offset being committed
+------------------+ 148
```

**C struct:**

```c
typedef struct __attribute__((packed)) {
    as_stream_hdr hdr;
    uint8_t       group_name[64];
    uint32_t      partition_id;
    int64_t       offset;
} as_stream_ack_msg;
```

**Server behavior:**

1. Read current committed offset from `consumer_offsets` set
2. If `offset == committed + 1` (or committed == -1 and offset == 0): update
3. Else: reject with status `0x03` (out-of-order ack)
4. No response sent on success (fire-and-forget unless client requests ack)

---

### 4.6 STREAM_SEEK (type `15`) - Client → Server

Resets a consumer group's position on a partition. Used for replay. The server
updates the group's offset in `consumer_offsets` and resumes delivery from the
new position.

**Body layout:**

```
+------------------+ 0
| as_stream_hdr    | 72 bytes
+------------------+ 72
| group_name[64]   |
+------------------+ 136
| partition_id (4B)|
+------------------+ 140
| seek_type (1B)   | same constants as STREAM_CONSUME
+------------------+ 141
| seek_value (8B)  |
+------------------+ 149
```

**C struct:**

```c
typedef struct __attribute__((packed)) {
    as_stream_hdr hdr;
    uint8_t       group_name[64];
    uint32_t      partition_id;
    uint8_t       seek_type;
    int64_t       seek_value;
} as_stream_seek_msg;
```

**Server behavior:**

1. Resolve `seek_value` to a concrete offset (see below).
2. Update the group's committed offset in `consumer_offsets` to the resolved position.
3. Restart the push loop from that offset.

**Timestamp seek resolution (`seek_type == AS_STREAM_SEEK_TIMESTAMP`):**

Timestamps are server-assigned at commit time (`cf_clock_getabstime_ns()`) and are
therefore monotonically increasing with offset within a partition. The target offset
is resolved via **binary search on log record keys**: probe
`"{stream}:{partition}:{offset}"` using bisection until the record where
`ts >= seek_value` and the preceding record's `ts < seek_value` is found. This is
O(log N) direct key reads with no secondary index scan.

---

### 4.7 STREAM_SUB (type `16`) - Client → Server

Registers an ephemeral pub/sub subscription. Unlike `STREAM_CONSUME`, no
offset is tracked - records are delivered in real time and lost if the client
is disconnected. The server adds the connection to an in-memory subscription
registry. Delivery uses the same `STREAM_RECORD` message format.

**Body layout:**

```
+------------------+ 0
| as_stream_hdr    | 72 bytes
+------------------+ 72
| topic[64]        | pub/sub topic name, null-terminated
+------------------+ 136
```

**C struct:**

```c
typedef struct __attribute__((packed)) {
    as_stream_hdr hdr;
    uint8_t       topic[64];
} as_stream_sub_msg;
```

---

### 4.8 STREAM_UNSUB (type `17`) - Client → Server

Removes a subscription or consume session. The server removes the connection
from all relevant registries and stops delivery.

**Body layout:**

```
+------------------+ 0
| as_stream_hdr    | 72 bytes  (stream_name or topic in hdr.stream_name)
+------------------+ 72
| unsub_type (1B)  | 0x00 = consume session, 0x01 = pub/sub subscription
+------------------+ 73
```

**C struct:**

```c
typedef struct __attribute__((packed)) {
    as_stream_hdr hdr;
    uint8_t       unsub_type;
} as_stream_unsub_msg;
```

---

## 5. Session State Machine

```
Client                              Server
  |                                   |
  |--- STREAM_CONSUME --------------->|  register session, seek to position
  |                                   |
  |<-- STREAM_RECORD (offset N) ------|  push loop begins
  |<-- STREAM_RECORD (offset N+1) ----|
  |<-- STREAM_RECORD (offset N+2) ----|  pauses if max_in_flight reached
  |                                   |
  |--- STREAM_ACK (offset N) -------->|  committed_offset advances, slot freed
  |--- STREAM_ACK (offset N+1) ------>|
  |                                   |
  |--- STREAM_SEEK (offset 0) ------->|  replay from beginning
  |                                   |
  |<-- STREAM_RECORD (offset 0) ------|  push loop restarts from 0
  |                                   |
  |--- STREAM_UNSUB ----------------->|  session torn down
  |                                   |
```

---

## 6. Error Status Codes

Returned in `STREAM_PROD_ACK` and as a one-byte body in error responses to
any stream message type.

| Code   | Constant                         | Meaning                        |
|--------|----------------------------------|--------------------------------|
| `0x00` | `AS_STREAM_OK`                   | Success                        |
| `0x01` | `AS_STREAM_ERR_NOT_FOUND`        | Stream does not exist          |
| `0x02` | `AS_STREAM_ERR_STORAGE`          | Aerospike storage write failed |
| `0x03` | `AS_STREAM_ERR_OOO_ACK`          | Out-of-order ack rejected      |
| `0x04` | `AS_STREAM_ERR_MAX_IN_FLIGHT`    | Consumer paused, in-flight full|
| `0x05` | `AS_STREAM_ERR_INVALID_SEEK`     | Seek position out of range     |
| `0x06` | `AS_STREAM_ERR_GROUP_NOT_FOUND`  | Consumer group does not exist  |
| `0x07` | `AS_STREAM_ERR_AUTH`             | Not authorized on this stream  |

---

## 7. Aerospike Storage Layout

I'm using a dedicated namespace `aerostream` with two sets:

### 7.1 Set: `log`

One record per stream message.

| Bin       | Type    | Description                              |
|-----------|---------|------------------------------------------|
| `payload` | BLOB    | Raw message payload bytes                |
| `ts`      | INTEGER | Commit timestamp, Unix nanoseconds       |
| `offset`  | INTEGER | Partition-scoped monotonic offset        |
| `hdrs`    | MAP     | Key-value header pairs                   |

Record key format: `"{stream_name}:{partition_id}:{offset}"`  
TTL: configured per-stream, drives retention without compaction.

**Offset counter reconstruction on restart:** In-memory partition offset counters
(`cf_atomic64`) are not persisted directly. On server restart, each counter is
reconstructed by querying the maximum `offset` bin value in the `log` set for that
(stream, partition) via a secondary index aggregation. This is a one-time O(1) query
per partition at module init. Produces on that partition are blocked until
reconstruction completes.

### 7.2 Set: `consumer_offsets`

One record per (group, stream, partition) triple.

| Bin         | Type    | Description                            |
|-------------|---------|----------------------------------------|
| `committed` | INTEGER | Last committed offset                  |
| `lag`       | INTEGER | `head_offset - committed`              |
| `updated_at`| INTEGER | Unix nanoseconds of last commit        |

Record key format: `"{group_name}:{stream_name}:{partition_id}"`

---

## 8. Server-Side Files Changed

| File                          | Change                                              |
|-------------------------------|-----------------------------------------------------|
| `as/include/base/proto.h`     | Add type constants `10`-`17` and struct definitions |
| `as/src/base/service.c`       | Add dispatch cases for types `10`-`17`              |
| `as/src/modules/aerostream/`  | New module directory (all AeroStream logic)         |

---

## 9. Upgrade Path to Approach 1 (full integration)

I designed this spec so that the eight new type values (`10`-`17`) can be
submitted as a minimal patch to `proto.h` and `service.c` with the bulk
of the implementation living in `modules/aerostream/` as a self-contained
module. My goal is a PR surface small enough for the Aerospike core team to
review in a single sitting.

