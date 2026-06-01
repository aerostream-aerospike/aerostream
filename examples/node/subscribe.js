'use strict';

/*
 * Subscriber example: ephemeral pub/sub. Subscribe to a topic (= stream name)
 * and print records as they are produced in real time. No offsets, no replay —
 * only records produced while subscribed are delivered.
 *
 *   node subscribe.js [topic]
 *
 * Default: topic="orders"
 * Run produce.js in another terminal AFTER this is subscribed.
 * Ctrl-C to stop.
 */

const { Subscriber } = require('../../clients/node');

const HOST = process.env.AEROSTREAM_HOST || '127.0.0.1';
const PORT = Number(process.env.AEROSTREAM_PORT || 3000);

const topic = process.argv[2] || 'orders';

async function main() {
  const sub = new Subscriber({ host: HOST, port: PORT });
  sub.on('error', (err) => console.error('subscriber error:', err.message));

  sub.on('message', (msg) => {
    let body;
    try {
      body = JSON.parse(msg.payload.toString('utf8'));
    } catch {
      body = msg.payload.toString('utf8');
    }
    console.log(
      `[topic=${msg.topic} p${msg.partitionId} @${msg.offset}] ` +
        `payload=${JSON.stringify(body)}`
    );
  });

  await sub.connect();
  console.log(`connected to ${HOST}:${PORT}, subscribing to topic "${topic}"`);

  await sub.subscribe({ topic });
  console.log('subscribed; waiting for live records (Ctrl-C to stop) ...');

  process.on('SIGINT', async () => {
    console.log('\nstopping ...');
    await sub.unsubscribe({ topic }).catch(() => {});
    await sub.close();
    process.exit(0);
  });
}

main().catch((err) => {
  console.error('fatal:', err);
  process.exit(1);
});
