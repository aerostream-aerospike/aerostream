# AeroStream Node.js examples

Runnable example scripts for the [Node.js client](../../clients/node/). Each
script requires the client by relative path (`../../clients/node`), so no
install step is needed — just run them with `node`.

Make sure the server is running first:

```bash
cd ../..                 # project root
./aerostream-ctl.sh start
```

Override the target with `AEROSTREAM_HOST` / `AEROSTREAM_PORT` env vars
(defaults: `127.0.0.1:3000`).

## Scripts

### `roundtrip.js` — self-contained smoke test

Produces N records to a fresh stream, consumes them back from the earliest
offset, and verifies the count. The fastest way to confirm everything works.

```bash
node roundtrip.js 10
# -> produced 10 records / consumed 10/10 records / OK: round-trip succeeded
```

### `produce.js` — producer

```bash
node produce.js [stream] [count]      # defaults: orders 5
```

Appends `count` records to `stream`, printing the assigned partition/offset for
each.

### `consume.js` — durable consumer group

```bash
node consume.js [stream] [group]      # defaults: orders workers
```

Joins the group, reads from the earliest offset, prints and ACKs each record.
Leave it running and produce in another terminal to watch records arrive.
Ctrl-C to stop.

### `subscribe.js` — ephemeral pub/sub

```bash
node subscribe.js [topic]             # default: orders
```

Subscribes to a topic (= stream name) and prints records as they're produced in
real time. Start this **before** producing — pub/sub has no replay.

## Two-terminal demo

Durable consumer group:

```bash
# terminal 1
node consume.js orders billing

# terminal 2
node produce.js orders 5
```

Live pub/sub:

```bash
# terminal 1
node subscribe.js events

# terminal 2 (after the subscriber is connected)
node produce.js events 5
```
