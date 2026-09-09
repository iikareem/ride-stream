export const redisConfig = {
  url: process.env.REDIS_URL ?? 'redis://localhost:6379',
  /** Redis GEO key holding rider positions (member = rider_id). */
  ridersGeoKey: process.env.RIDERS_GEO_KEY ?? 'riders:geo',
} as const;
