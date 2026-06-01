# AeroStream - Design Document

**Status:** RFC  
**Version:** 0.1.0  
**Repo:** [github.com/aerostream/aerostream](https://github.com/aerostream-aerospike/aerostream)

---

## Problem

Aerospike is one of the fastest durable key-value stores available. Latency at
p99 that most databases can't hit at p50. But today, if you want to build
event-driven systems on top of Aerospike data, you have two bad options:

1. Poll Aerospike records in a loop and diff them - wasteful, laggy, fragile.
2. Run a separate Kafka or Pulsar cluster alongside Aerospike - now you have
   two systems to operate, two failure domains, and a replication lag you can't
   eliminate.

The question I kept asking myself: **what if the message broker and the database
were the same process?**

---

## What I Built

A native Aerospike server module that adds durable stream semantics directly
to the Aerospike wire protocol. No separate broker process. No additional
network hop between storage and messaging. Stream records are Aerospike records.
Consumer group offsets are Aerospike records. Replay is a range scan.

I added eight new message types to the Aerospike binary protocol
(types `10`–`17`) handled by a self-contained module in
`as/src/modules/aerostream/`. The existing port 3000, TLS config, and
authentication pipeline are reused unchanged.

---

## Core Concepts

**Stream** - a named, partitioned, ordered log of records. Analogous to a
Kafka topic. Backed by an Aerospike set with key format
`{stream_name}:{partition_id}:{offset}`.

**Partition** - a shard of a stream. Records within a partition are strictly
ordered by offset. Partition assignment is deterministic:
`partition_id = fnv32a(partition_key) % num_partitions`.

**Offset** - a monotonically increasing integer scoped to a partition.
Assigned atomically by the server using `cf_atomic64_incr`. Survives restarts
by persisting the high-water mark as an Aerospike record.

**Consumer group** - a named set of consumers sharing a committed offset
position per partition. Multiple groups on the same stream read independently
without interfering. Group state lives in the `consumer_offsets` set.

**Replay** - seeking a consumer group to any past offset or timestamp and
re-consuming records from that point. Offset seeks are direct key lookups. Timestamp
seeks use binary search on log record keys, since timestamps are server-assigned
monotonically with offset, O(log N) key probes and no secondary index needed.

**Pub/Sub** - ephemeral fan-out with no offset tracking. An in-memory
subscription registry in the AeroStream module delivers records to all
connected subscribers in real time. No durability guarantee.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Aerospike Server Process                   │
│                                                              │
│  ┌──────────────┐    ┌──────────────────────────────────┐   │
│  │  service.c   │───▶│     AeroStream Module            │   │
│  │              │    │                                  │   │
│  │ type 10-17   │    │  as_stream_log.c    (append)     │   │
│  │ dispatch     │    │  as_stream_groups.c (offsets)    │   │
│  │              │    │  as_stream_replay.c (seek/scan)  │   │
│  └──────────────┘    │  as_stream_pubsub.c (fan-out)    │   │
│                      └────────────┬─────────────────────┘   │
│                                   │                          │
│                      ┌────────────▼─────────────────────┐   │
│                      │     Aerospike Storage Engine      │   │
│                      │  namespace: aerostream            │   │
│                      │  set: log             (records)   │   │
│                      │  set: consumer_offsets (groups)   │   │
│                      └──────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
         ▲                          ▲
         │                          │
   Producers                   Consumers
   (any AS client)             (any AS client)
```

---

## Key Design Decisions

### Why extend the wire protocol instead of using UDFs?

UDFs (Lua) add interpretation overhead on every call. More importantly, the
consume path requires a **persistent push session** - the server pushes records
to a connected client as they arrive. That cannot be expressed as a UDF call.
New protocol message types are the only way to support server-push delivery
natively.

### Why port 3000 instead of a new port?

Every existing Aerospike deployment has port 3000 open, TLS-terminated, and
load-balanced. A new port doubles the operational surface for zero architectural
gain. The eight-byte header has 254 unused type values. Claiming eight of them
is a pretty minimal footprint.

### Why `cf_atomic64_incr` for offsets instead of a sequence record?

A sequence record requires a read-modify-write with a lock. `cf_atomic64_incr`
on an in-memory counter is a single CPU instruction with no I/O. On restart,
the counter is reconstructed from the max offset in the `log` set - a one-time
O(1) lookup of the partition's high-water mark record.

### Why TTL for retention instead of a compaction process?

Aerospike's eviction engine already handles TTL-based expiry at the storage
level with no background process. A stream's retention policy maps directly to
a record TTL on write. No compaction thread, no log segment management, no
retention broker config.

### Consumer group offset CAS

The committed offset for each (group, stream, partition) triple is stored as
an integer bin in the `consumer_offsets` set. Commits use Aerospike's
`as_operate` with a compare-and-swap predicate:

```
if consumer_offsets[key].committed == expected_offset:
    consumer_offsets[key].committed = new_offset
```

This prevents two consumers in the same group from committing the same
partition concurrently - a split-brain scenario that could cause record
re-delivery or loss.

---

## What I'm Not Building (yet)

- **Transactions across streams** - records within a single partition are
  ordered and durable. Cross-stream transactions are out of scope.
- **Compacted streams** (Kafka log compaction equivalent) - maybe v2.
- **Schema registry** - payloads are opaque bytes. Schema is the application's
  problem, not mine.
- **Exactly-once delivery** - at-least-once with idempotent consumer logic.
  Exactly-once needs distributed transaction support, that's a v2 thing.

---

## File Layout

```
aerospike-server/
  as/include/base/
    proto.h                 ← +8 type constants, +10 struct definitions
  as/src/base/
    service.c               ← +8 dispatch cases (~40 lines)
  as/src/modules/aerostream/
    aerostream.h            ← public API
    as_stream_log.c         ← produce path, offset assignment, storage write
    as_stream_groups.c      ← consumer group state, offset CAS, lag tracking
    as_stream_replay.c      ← seek logic, binary search key probes, push restart
    as_stream_pubsub.c      ← subscription registry, fan-out, connection mgmt
    as_stream_config.c      ← per-stream config (partitions, TTL, ack mode)
    Makefile
    README.md
```

I kept changes to existing files minimal. The entire implementation lives in
`modules/aerostream/` so it's easy to review, test, and eventually upstream.

---

---

## Comparison

|                        | Kafka    | Redis Streams | AeroStream         |
|------------------------|----------|---------------|--------------------|
| Separate broker        | Yes      | No            | No                 |
| Durable by default     | Yes      | Optional (AOF)| Yes (Aerospike)    |
| Consumer groups        | Yes      | Yes           | Yes                |
| Replay                 | Yes      | Yes           | Yes                |
| Pub/Sub                | No       | Yes           | Yes                |
| Write latency (p99)    | ~5ms     | ~1ms          | ~0.5ms             |
| Protocol               | Custom   | RESP          | Aerospike binary   |
| Co-located with DB     | No       | Yes           | Yes                |

---

## Prior Art

- **Kafka** - the reference design for durable partitioned logs.
- **Redis Streams** - the closest analog: streams native to a KV store.
  AeroStream differs in that Aerospike's hybrid memory/SSD model makes it
  viable at much larger record volumes without memory pressure.
- **Aerospike XDR** - the existing post-commit hook I studied for the
  notification architecture.
- **RocksDB + Kafka** - what most teams actually run today when they need
  both a fast store and a durable log.
