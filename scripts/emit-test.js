/**
 * One-shot redis-emitter test (no Kafka producer).
 *
 * Usage:
 *   npm run emit:test
 *   npm run emit:test -- rider-001 '{"lat":30.04,"lon":31.23}'
 *
 * Prerequisites: Redis up, gateway running, Postman joined that userId and listening for "drivers".
 */
const { Emitter } = require('@socket.io/redis-emitter');
const Redis = require('ioredis');

async function main() {
  const userId = process.argv[2] || 'rider-001';
  const raw = process.argv[3] || '{"hello":true,"from":"emit:test"}';
  let payload;
  try {
    payload = JSON.parse(raw);
  } catch {
    payload = { text: raw };
  }

  const room = `user:${userId}`;
  const url = process.env.REDIS_URL || 'redis://localhost:6379';
  const redis = new Redis(url);

  await new Promise((resolve, reject) => {
    if (redis.status === 'ready') return resolve();
    redis.once('ready', resolve);
    redis.once('error', reject);
  });

  const io = new Emitter(redis);
  io.to(room).emit('drivers', payload);
  console.log(`emitted event=drivers room=${room} payload=${JSON.stringify(payload)}`);

  await redis.quit();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
