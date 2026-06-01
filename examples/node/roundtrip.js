'use strict';

/*
 * Self-contained end-to-end check: produce N records, consume them back from
 * the earliest offset, verify the count, then exit. Good for a quick smoke test
 * of the whole protocol path in one process.
 *
 *   node roundtrip.js [count]
 */

const { Producer, Consumer, ACK_MODE, SEEK, ALL_PARTITIONS } = require('../../clients/node');

const HOST = process.env.AEROSTREAM_HOST || '127.0.0.1';
const PORT = Number(process.env.AEROSTREAM_PORT || 3000);

const count = Number(process.argv[2] || 10);
const stream = `rt-${Date.now()}`; // fresh stream so offsets start at 0
const group = 'rt-group';

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function main() {
  console.log(`round-trip: stream="${stream}", count=${count}`);

  // ---- produce ----
  const producer = new Producer({ host: HOST, port: PORT });
  producer.on('error', (e) => console.error('producer error:', e.message));
  await producer.connect();

  const produced = [];
  for (let i = 0; i < count; i++) {
    const ack = await producer.produce({
      stream,
      partitionKey: `k${i % 3}`,
      payload: JSON.stringify({ seq: i }),
      ackMode: ACK_MODE.LEADER,
    });
    produced.push(ack);
  }
  console.log(`produced ${produced.length} records`);
  await producer.close();

  // ---- consume ----
  const consumer = new Consumer({ host: HOST, port: PORT });
  consumer.on('error', (e) => console.error('consumer error:', e.message));

  let received = 0;
  const seen = [];
  consumer.on('record', (rec) => {
    received++;
    seen.push({ p: rec.partitionId, o: Number(rec.offset) });
    rec.ack().catch((e) => console.error('ack error:', e.message));
  });

  await consumer.connect();
  await consumer.consume({
    stream,
    group,
    partition: ALL_PARTITIONS,
    seekType: SEEK.EARLIEST,
    maxInFlight: 100,
  });

  // Wait until we've drained all produced records (or time out).
  const deadline = Date.now() + 5000;
  while (received < count && Date.now() < deadline) {
    await sleep(50);
  }

  await consumer.close();

  console.log(`consumed ${received}/${count} records`);
  if (received === count) {
    console.log('OK: round-trip succeeded');
    process.exit(0);
  } else {
    console.error('FAIL: did not receive all records');
    console.error('seen:', seen);
    process.exit(1);
  }
}

main().catch((err) => {
  console.error('fatal:', err);
  process.exit(1);
});
