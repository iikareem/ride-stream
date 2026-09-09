/**
 * Smoke test: Redis PUBLISH to user:{id}.
 * Gateway must be running and the client must have joined that userId.
 *
 * Usage:
 *   npm run emit:test
 *   npm run emit:test -- rider-001 '{"lat":30.04,"lon":31.23}'
 *
 * Or in Redis Insight: PUBLISH user:rider-001 '{"hello":true}'
 */
const Redis = require('ioredis');

async function main() {
  const userId = process.argv[2] || 'rider-001';
  const raw = process.argv[3] || '{"hello":true,"from":"emit:test"}';
  let payload;
  try {
    JSON.parse(raw);
    payload = raw;
  } catch {
    payload = JSON.stringify({ text: raw });
  }

  const channel = `user:${userId}`;
  const url = process.env.REDIS_URL || 'redis://localhost:6379';
  const client = new Redis(url);

  const receivers = await client.publish(channel, payload);
  console.log(
    `PUBLISH channel=${channel} receivers=${receivers} payload=${payload}`,
  );

  await client.quit();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
