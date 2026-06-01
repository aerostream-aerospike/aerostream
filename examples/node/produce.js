'use strict';

/*
 * Producer example: append N records to a stream and print each assigned offset.
 *
 *   node produce.js [stream] [count]
 *
 * Defaults: stream="orders", count=5
 */

const { Producer, ACK_MODE } = require('../../clients/node');

const HOST = process.env.AEROSTREAM_HOST || '127.0.0.1';
const PORT = Number(process.env.AEROSTREAM_PORT || 3000);

const stream = process.argv[2] || 'orders';
const count = Number(process.argv[3] || 5);

async function main() {
  const producer = new Producer({ host: HOST, port: PORT });
  producer.on('error', (err) => console.error('producer error:', err.message));

  await producer.connect();
  console.log(`connected to ${HOST}:${PORT}, producing ${count} record(s) to "${stream}"`);

  for (let i = 0; i < count; i++) {
    const payload = JSON.stringify({ seq: i, ts: Date.now(), msg: `hello-${i}` });
    const ack = await producer.produce({
      stream,
      partitionKey: `key-${i % 4}`, // spread across partitions
      payload,
      ackMode: ACK_MODE.LEADER,
    });
    console.log(
      `  produced #${i} -> partition ${ack.partitionId}, offset ${ack.offset}, ts ${ack.timestampNs}`
    );
  }

  await producer.close();
  console.log('done');
}

main().catch((err) => {
  console.error('fatal:', err);
  process.exit(1);
});
