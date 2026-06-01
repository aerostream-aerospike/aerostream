# aerostream (Node.js client)

A standalone Node.js client for [AeroStream](../../README.md) — native stream
primitives for Aerospike (durable logs, consumer groups, replay, pub/sub).

It speaks the AeroStream wire protocol directly over a raw TCP/TLS socket on
port 3000. It does **not** depend on the native Aerospike Node addon — the
streaming/push paths need a dedicated long-lived connection that a pooled
client can't provide, and AeroStream needs no auth on Community Edition, so a
plain socket is the simplest correct fit.

## Install

It's a zero-dependency module. From your project:

```bash
npm install /path/to/aerostream/clients/node
# or reference it directly:
const { Producer, Consumer, Subscriber } = require('../clients/node');
```

## Quick start

### Produce

```js
const { Producer, ACK_MODE } = require('aerostream');

const producer = new Producer({ host: '127.0.0.1', port: 3000 });
await producer.connect();

const ack = await producer.produce({
  stream: 'orders',
  partitionKey: 'customer-42', // consistent-hash routing to a partition
  payload: JSON.stringify({ id: 1, total: 99.5 }),
  ackMode: ACK_MODE.LEADER,    // wait for the durable write
});
// ack = { offset: 0n, partitionId: 3, timestampNs: 1780...n, status: 0 }

await producer.close();
```

`ackMode: ACK_MODE.NONE` makes produce fire-and-forget (resolves once flushed,
no offset returned).

### Consume (durable, consumer groups)

```js
const { Consumer, SEEK, ALL_PARTITIONS } = require('aerostream');

const consumer = new Consumer({ host: '127.0.0.1', port: 3000 });

consumer.on('record', (rec) => {
  console.log(rec.partitionId, rec.offset, rec.payload.toString());
  rec.ack(); // advance the committed group offset
});

await consumer.connect();
await consumer.consume({
  stream: 'orders',
  group: 'billing',
  partition: ALL_PARTITIONS,
  seekType: SEEK.EARLIEST,   // LATEST | EARLIEST | OFFSET | TIMESTAMP
  maxInFlight: 10,
});
```

Replay by seeking the group:

```js
await consumer.seek({
  stream: 'orders', group: 'billing', partitionId: 3,
  seekType: SEEK.OFFSET, seekValue: 0,        // re-read from offset 0
});
// or SEEK.TIMESTAMP with seekValue = a Unix-nanoseconds BigInt
```

Pass `{ autoAck: true }` to the `Consumer` constructor to ack every record
automatically after the handler returns.

### Subscribe (ephemeral pub/sub)

```js
const { Subscriber } = require('aerostream');

const sub = new Subscriber({ host: '127.0.0.1', port: 3000 });

sub.on('message', (msg) => {
  console.log(msg.topic, msg.payload.toString());
});

await sub.connect();
await sub.subscribe({ topic: 'orders' }); // topic == stream name
```

No offsets, no durability — only records produced while subscribed are
delivered. Anything sent while disconnected is lost.

## Notes on types

- `offset` and `timestampNs` are returned as **BigInt** (timestamps in
  nanoseconds exceed `Number.MAX_SAFE_INTEGER`).
- `payload` is always a `Buffer`.
- `correlationId` is managed internally; you don't set it.

## TLS

```js
new Producer({ host, port: 4333, tls: { ca: fs.readFileSync('ca.pem') } });
```

`tls: true` uses default TLS options; an object is passed through to
`tls.connect`.

## API surface

| Class        | Methods                                              | Events     |
|--------------|------------------------------------------------------|------------|
| `Producer`   | `connect` `produce` `close`                          | `error`    |
| `Consumer`   | `connect` `consume` `ack` `seek` `unsubscribe` `close` | `record` `error` |
| `Subscriber` | `connect` `subscribe` `unsubscribe` `close`          | `message` `error` |

Protocol constants are exported too: `SEEK`, `ACK_MODE`, `STATUS`,
`ALL_PARTITIONS`, `TYPE`, plus the low-level `proto` codec.

See [`../../examples/node/`](../../examples/node/) for runnable scripts.
