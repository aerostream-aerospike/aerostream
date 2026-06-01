# aerostream

so here's the deal. i've been running aerospike for a while and it's genuinely great - stupid fast, scales well, the hybrid memory/SSD model is kind of a cheat code for latency-sensitive stuff. but the moment you need event-driven anything on top of it, you're stuck bolting on kafka or pulsar next to it and suddenly you have two systems to babysit, two failure domains, twice the ops overhead, and this annoying replication lag between them that you can never fully get rid of.

it was a pain in my ass.

i kept thinking - aerospike already has the storage engine, the replication, the partitioning model. the architecture is literally right there. why am i running a whole separate broker process just to get ordered logs and consumer groups? it felt wrong.

so i built aerostream. it's a c module that lives inside the aerospike server and adds stream primitives directly to the wire protocol. no separate process, no extra network hop, no second cluster to monitor. stream records are just aerospike records. consumer group offsets are aerospike records. replay is a range scan. the broker and the database are the same thing.

---

## what it does

- **durable logs** - produce records to a named, partitioned stream. offsets are assigned atomically server-side and survive restarts
- **consumer groups** - multiple independent groups consume the same stream, each tracking their own offset. commits use aerospike CAS so you can't double-commit in a split-brain scenario
- **replay** - seek a consumer group back to any offset or timestamp and re-consume from there. uses binary search on log record keys to find the start offset, no secondary index needed
- **pub/sub** - ephemeral fan-out for when you don't need durability. in-memory only, no offset tracking, records are gone if you disconnect

---

## how it works

8 new message types (10-17) on the existing port 3000. same TLS, same auth, same everything - just new type values in the 8-byte header that aerospike already puts on every message. the dispatch lives in `service.c` and routes to the aerostream module. all the actual logic is self-contained in `as/src/modules/aerostream/`.

the two core files that touch existing aerospike code are `as/include/base/proto.h` (adds the type constants and wire structs) and `as/src/base/service.c` (adds the dispatch cases). that's it. the diff on those two files is intentionally tiny.

storage layout:
- namespace `aerostream`, set `log` - one record per message, key is `{stream}:{partition}:{offset}`
- namespace `aerostream`, set `consumer_offsets` - one record per (group, stream, partition) triple
- TTL on log records = retention policy. no compaction needed, aerospike's eviction handles it

---

## repo layout

```
aerospike-server/               git submodule (aerospike server source)
  as/include/base/
    proto.h                     patched: type constants 10-17 + wire structs
  as/src/base/
    service.c                   patched: dispatch cases for types 10-17
  as/src/modules/aerostream/
    aerostream.h
    as_stream_log.c
    as_stream_groups.c
    as_stream_replay.c
    as_stream_pubsub.c
    as_stream_config.c
clients/go/
clients/java/
docs/
  DESIGN.md                     architecture writeup + design decisions
  PROTOCOL.md                   full wire protocol spec
```

---

## building

ubuntu 20.04/22.04 or RHEL 8/9 only. doesn't build on mac, sorry.

```bash
sudo apt-get install -y build-essential autoconf automake libtool \
  libssl-dev zlib1g-dev liblua5.4-dev python3 git pkg-config

git submodule update --init --recursive
cd aerospike-server && make
```

---

## status

this is an RFC / early prototype. the design doc and protocol spec are done (`docs/`), implementation is in progress. if you've ever been annoyed by the aerospike + kafka setup or have thoughts on the protocol design, open an issue or read through `docs/DESIGN.md`.
