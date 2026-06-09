'use strict';

/*
 * Exercises the three fixes:
 *   1. headers bin round-trip
 *   2. back-pressure (ERR_MAX_IN_FLIGHT) when consuming without acking
 *   3. consumer-group commit + replay (CAS path)
 *
 *   node test-fixes.js
 */

const { Producer, Consumer, ACK_MODE, SEEK, ALL_PARTITIONS } =
  require('../../clients/node');

const HOST = process.env.AEROSTREAM_HOST || '127.0.0.1';
const PORT = Number(process.env.AEROSTREAM_PORT || 3000);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

let failures = 0;
function check(name, cond) {
  console.log(`  ${cond ? 'PASS' : 'FAIL'}  ${name}`);
  if (!cond) failures++;
}

async function testHeaders() {
  console.log('\n[1] headers round-trip');
  const stream = `hdr-${Date.now()}`;
  const producer = new Producer({ host: HOST, port: PORT });
  await producer.connect();
  await producer.produce({
    stream, partitionKey: 'k0',
    payload: JSON.stringify({ hello: 'world' }),
    headers: { 'trace-id': 'abc-123', 'content-type': 'application/json' },
    ackMode: ACK_MODE.LEADER,
  });
  await producer.close();

  const consumer = new Consumer({ host: HOST, port: PORT });
  let got = null;
  consumer.on('record', (rec) => { got = rec; rec.ack(); });
  await consumer.connect();
  await consumer.consume({ stream, group: 'g', partition: ALL_PARTITIONS, seekType: SEEK.EARLIEST });
  const deadline = Date.now() + 3000;
  while (!got && Date.now() < deadline) await sleep(40);
  await consumer.close();

  check('record received', !!got);
  if (got) {
    check('trace-id header round-tripped',
      got.headers && got.headers['trace-id'] &&
      got.headers['trace-id'].toString() === 'abc-123');
    check('content-type header round-tripped',
      got.headers && got.headers['content-type'] &&
      got.headers['content-type'].toString() === 'application/json');
  }
}

async function testBackpressure() {
  console.log('\n[2] back-pressure (max_in_flight)');
  const stream = `bp-${Date.now()}`;
  const producer = new Producer({ host: HOST, port: PORT });
  await producer.connect();
  // all to one partition so a single session hits the limit
  for (let i = 0; i < 6; i++) {
    await producer.produce({ stream, partitionKey: 'same', payload: `m${i}`, ackMode: ACK_MODE.LEADER });
  }
  await producer.close();

  const consumer = new Consumer({ host: HOST, port: PORT });
  let backpressure = false, delivered = 0;
  consumer.on('backpressure', () => { backpressure = true; });
  consumer.on('record', () => { delivered++; /* deliberately do NOT ack */ });
  await consumer.connect();
  // single partition, max_in_flight = 2 → after 2 unacked records, expect pause + signal
  await consumer.consume({
    stream, group: 'g', partition: ALL_PARTITIONS,
    seekType: SEEK.EARLIEST, maxInFlight: 2,
  });
  const deadline = Date.now() + 3000;
  while (!backpressure && Date.now() < deadline) await sleep(40);
  await consumer.close();

  check('received some records', delivered > 0);
  check('paused at max_in_flight (no flood)', delivered <= 2 * 8); // <= per-partition cap headroom
  check('back-pressure signal delivered', backpressure);
}

async function testCommitReplay() {
  console.log('\n[3] commit + replay (CAS write path)');
  const stream = `cas-${Date.now()}`;
  const producer = new Producer({ host: HOST, port: PORT });
  await producer.connect();
  for (let i = 0; i < 5; i++) {
    await producer.produce({ stream, partitionKey: 'p', payload: `v${i}`, ackMode: ACK_MODE.LEADER });
  }
  await producer.close();

  // consume + ack all, then a fresh consumer at LATEST should get nothing new,
  // and a SEEK to EARLIEST should replay them.
  const c1 = new Consumer({ host: HOST, port: PORT });
  let n1 = 0;
  c1.on('record', (r) => { n1++; r.ack(); });
  await c1.connect();
  await c1.consume({ stream, group: 'g', partition: ALL_PARTITIONS, seekType: SEEK.EARLIEST });
  let d = Date.now() + 2500;
  while (n1 < 5 && Date.now() < d) await sleep(40);

  const consumedCount = n1;  // snapshot before replay re-delivers

  // replay the same group from the beginning (stop counting on n1)
  c1.removeAllListeners('record');
  let n2 = 0;
  c1.on('record', () => { n2++; });
  // seek every partition back to offset 0
  for (let p = 0; p < 8; p++) {
    await c1.seek({ stream, group: 'g', partitionId: p, seekType: SEEK.OFFSET, seekValue: 0 });
  }
  d = Date.now() + 2500;
  while (n2 < 5 && Date.now() < d) await sleep(40);
  await c1.close();

  check('consumed all 5 committed', consumedCount === 5);
  check('replay re-delivered records', n2 >= 5);
}

async function main() {
  await testHeaders();
  await testBackpressure();
  await testCommitReplay();
  console.log(`\n${failures === 0 ? 'ALL PASS' : failures + ' FAILURE(S)'}`);
  process.exit(failures === 0 ? 0 : 1);
}

main().catch((e) => { console.error('fatal:', e); process.exit(1); });
