/**
 * EtaUpdate Avro schema.
 * Subject (TopicNameStrategy): eta-updates-value
 */

export const ETA_UPDATE_SCHEMA_V1 = {
  type: 'record',
  name: 'EtaUpdate',
  namespace: 'com.ridestream',
  fields: [
    { name: 'driver_id', type: 'string' },
    { name: 'latitude', type: 'double' },
    { name: 'longitude', type: 'double' },
    { name: 'destination_lat', type: 'double' },
    { name: 'destination_lon', type: 'double' },
    { name: 'distance_km', type: 'float' },
    { name: 'speed_kmh', type: 'float' },
    { name: 'eta_seconds', type: 'int' },
    { name: 'timestamp', type: 'long' },
  ],
} as const;

export const ETA_UPDATE_VALUE_SUBJECT = 'eta-updates-value';
