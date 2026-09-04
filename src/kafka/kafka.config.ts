export const kafkaConfig = {
  clientId: process.env.KAFKA_CLIENT_ID ?? 'ridestream',
  brokers: (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(','),
  gpsEventsTopic: process.env.GPS_EVENTS_TOPIC ?? 'gps-events',
  etaUpdatesTopic: process.env.ETA_UPDATES_TOPIC ?? 'eta-updates',
  etaGroupId: process.env.ETA_GROUP_ID ?? 'ridestream-eta',
  liveMapGroupId: process.env.LIVE_MAP_GROUP_ID ?? 'ridestream-live-map',
  schemaRegistryUrl:
    process.env.SCHEMA_REGISTRY_URL ?? 'http://localhost:8081',
  driverCount: Number(process.env.DRIVER_COUNT ?? '10'),
} as const;
