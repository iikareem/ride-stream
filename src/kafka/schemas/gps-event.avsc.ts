/**
 * GPSEvent Avro schemas.
 * Subject (TopicNameStrategy): gps-events-value
 *
 * v1 — Phase 2 baseline fields
 * v2 — adds optional `heading` (degrees 0–360) with default null → BACKWARD compatible
 */

export const GPS_EVENT_SCHEMA_V1 = {
  type: 'record',
  name: 'GPSEvent',
  namespace: 'com.ridestream',
  fields: [
    { name: 'driver_id', type: 'string' },
    { name: 'latitude', type: 'double' },
    { name: 'longitude', type: 'double' },
    { name: 'speed_kmh', type: 'float' },
    { name: 'timestamp', type: 'long' },
    { name: 'status', type: 'string' },
  ],
} as const;

export const GPS_EVENT_SCHEMA_V2 = {
  type: 'record',
  name: 'GPSEvent',
  namespace: 'com.ridestream',
  fields: [
    { name: 'driver_id', type: 'string' },
    { name: 'latitude', type: 'double' },
    { name: 'longitude', type: 'double' },
    { name: 'speed_kmh', type: 'float' },
    { name: 'timestamp', type: 'long' },
    { name: 'status', type: 'string' },
    {
      name: 'heading',
      type: ['null', 'float'],
      default: null,
      doc: 'Compass heading in degrees 0–360; null when unknown',
    },
  ],
} as const;

export const GPS_EVENT_VALUE_SUBJECT = 'gps-events-value';
