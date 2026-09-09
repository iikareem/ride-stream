/**
 * Rider GPSEvent Avro schema.
 * Subject (TopicNameStrategy): gps-events-rider-value
 */

export const RIDER_GPS_EVENT_SCHEMA_V1 = {
  type: 'record',
  name: 'RiderGPSEvent',
  namespace: 'com.ridestream',
  fields: [
    { name: 'rider_id', type: 'string' },
    { name: 'latitude', type: 'double' },
    { name: 'longitude', type: 'double' },
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

export const RIDER_GPS_EVENT_VALUE_SUBJECT = 'gps-events-rider-value';
