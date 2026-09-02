export const kafkaConfig = {
  clientId: process.env.KAFKA_CLIENT_ID ?? 'ridestream',
  brokers: (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(','),
  gpsEventsTopic: process.env.GPS_EVENTS_TOPIC ?? 'gps-events',
  driverCount: Number(process.env.DRIVER_COUNT ?? '10'),
} as const;
