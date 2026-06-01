'use strict';

/*
 * Consumer example: join a consumer group, read records from the earliest
 * offset, print and ACK each one.
 *
 *   node consume.js [stream] [group]
 *
 * Defaults: stream="orders", group="workers"
 * Run produce.js in another terminal to see records arrive.
 * Ctrl-C to stop.
 */

const { Consumer, SEEK, ALL_PARTITIONS } = require('../../clients/node');

const HOST = process.env.AEROSTREAM_HOST || '127.0.0.1';
const PORT = Number(process.env.AEROSTREAM_PORT || 3000);

const stream = process.argv[2] || 'orders';
const group = process.argv[3] || 'workers';

async function main() {
  const consumer = new Consumer({ host: HOST, port: PORT });
  consumer.on('error', (err) => console.error('consumer error:', err.message));

  consumer.on('record', (rec) => {
    let body;
    try {
      body = JSON.parse(rec.payload.toString('utf8'));
    } catch {
      body = rec.payload.toString('utf8');
    }
    console.log(
      `[p${rec.partitionId} @${rec.offset}] ts=${rec.timestampNs} ` +
        `payload=${JSON.stringify(body)}`
    );
    // Commit so the group advances and the in-flight slot is released.
    rec.ack().catch((err) => console.error('ack error:', err.message));
  });

  await consumer.connect();
  console.log(`connected to ${HOST}:${PORT}, consuming "${stream}" as group "${group}"`);

  await consumer.consume({
    stream,
    group,
    partition: ALL_PARTITIONS,
    seekType: SEEK.EARLIEST,
    maxInFlight: 10,
  });
  console.log('subscribed; waiting for records (Ctrl-C to stop) ...');

  process.on('SIGINT', async () => {
    console.log('\nstopping ...');
    await consumer.unsubscribe({ stream }).catch(() => {});
    await consumer.close();
    process.exit(0);
  });
}

main().catch((err) => {
  console.error('fatal:', err);
  process.exit(1);
});
