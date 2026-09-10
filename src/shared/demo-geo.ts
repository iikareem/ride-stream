/**
 * Shared demo pin so rider-001 and driver-001 stay inside NEARBY_RADIUS_KM
 * for local WebSocket / Pub/Sub smoke tests.
 */
export const DEMO_MEETUP = {
  latitude: 30.05,
  longitude: 31.3,
} as const;

/** ~100 m jitter — stays well under the default 2 km nearby radius. */
export const DEMO_MEETUP_NUDGE = 0.001;
