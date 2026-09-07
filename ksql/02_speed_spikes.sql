-- Phase 4 Step 3 — speed spike anomalies → topic driver-anomalies
-- Requires Step 2: stream GPS_EVENTS must already exist.
--
-- Apply:
--   docker exec -it ridestream-ksqldb-cli ksql http://ksqldb-server:8088
--   RUN SCRIPT '/ksql/02_speed_spikes.sql';
--
-- Threshold 50: Nest producer emits roughly 5–60 km/h, so you can see hits locally.
-- Raise to 120 for a stricter “real” spike rule later.

CREATE STREAM IF NOT EXISTS speed_spikes
  WITH (
    KAFKA_TOPIC = 'driver-anomalies',
    VALUE_FORMAT = 'JSON',
    KEY_FORMAT = 'KAFKA'
  ) AS
SELECT
  DRIVER_ID,
  SPEED_KMH,
  'SPEED_SPIKE' AS TYPE,
  TIMESTAMP AS EVENT_TS
FROM gps_events
WHERE SPEED_KMH > 50
EMIT CHANGES;
