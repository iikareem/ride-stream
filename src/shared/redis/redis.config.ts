export const redisConfig = {
  url: process.env.REDIS_URL ?? 'redis://localhost:6379',
  /** Redis GEO key holding rider positions (member = rider_id). */
  ridersGeoKey: process.env.RIDERS_GEO_KEY ?? 'riders:geo',
  /** Radius (km) for GEOSEARCH around a driver GPS point. */
  nearbyRadiusKm: Number(process.env.NEARBY_RADIUS_KM ?? '2'),
} as const;

/** Business Pub/Sub channel for a user (Redis Insight can PUBLISH here). */
export function userChannel(userId: string): string {
  return `user:${userId}`;
}
