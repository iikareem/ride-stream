-- Phase 4 Step 5 — more anomaly types (simplified SQL)
-- Requires Step 2: stream GPS_EVENTS.
--
-- Apply:
--   docker exec -it ridestream-ksqldb-cli ksql http://ksqldb-server:8088
--   RUN SCRIPT '/ksql/04_freeze_teleport.sql';
--
-- These are learning heuristics. Nest producer only nudges ~0.002°, so true
-- teleports/freezes are rare unless you temporarily change the producer.
-- Thresholds are tuned to be teachable, not production-accurate.

--------------------------------------------------------------------------------
-- A) Latest position per driver (table) — used by teleport join
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS latest_gps AS
SELECT
  DRIVER_ID,
  LATEST_BY_OFFSET(LATITUDE) AS LATITUDE,
  LATEST_BY_OFFSET(LONGITUDE) AS LONGITUDE,
  LATEST_BY_OFFSET(TIMESTAMP) AS EVENT_TS
FROM gps_events_driver
GROUP BY DRIVER_ID
EMIT CHANGES;

--------------------------------------------------------------------------------
-- B) Teleport — large jump vs previous known position (stream ⋈ table)
--    GEO_DISTANCE is a built-in ksqlDB UDF (km).
--    Jump > 0.25 km between events ≈ “impossible” for our gentle nudge producer
--    when it fires; raise to 2–5 km for a stricter real-world rule.
--------------------------------------------------------------------------------
CREATE STREAM IF NOT EXISTS gps_teleports
  WITH (
    KAFKA_TOPIC = 'driver-anomalies-teleport',
    VALUE_FORMAT = 'JSON',
    KEY_FORMAT = 'KAFKA'
  ) AS
SELECT
  g.DRIVER_ID AS DRIVER_ID,
  g.LATITUDE AS LATITUDE,
  g.LONGITUDE AS LONGITUDE,
  t.LATITUDE AS PREV_LATITUDE,
  t.LONGITUDE AS PREV_LONGITUDE,
  GEO_DISTANCE(g.LATITUDE, g.LONGITUDE, t.LATITUDE, t.LONGITUDE, 'KM') AS JUMP_KM,
  'TELEPORT' AS TYPE,
  g.TIMESTAMP AS EVENT_TS
FROM gps_events_driver g
INNER JOIN latest_gps t WITHIN 1 HOURS ON g.DRIVER_ID = t.DRIVER_ID
WHERE GEO_DISTANCE(g.LATITUDE, g.LONGITUDE, t.LATITUDE, t.LONGITUDE, 'KM') > 0.25
  AND g.TIMESTAMP > t.EVENT_TS
EMIT CHANGES;

--------------------------------------------------------------------------------
-- C) GPS freeze — almost no movement over a 2-minute tumbling window
--    (max−min lat/lon tiny + several samples + low average speed)
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gps_freeze
  WITH (
    KAFKA_TOPIC = 'driver-anomalies-freeze',
    VALUE_FORMAT = 'JSON',
    KEY_FORMAT = 'KAFKA'
  ) AS
SELECT
  DRIVER_ID,
  AS_VALUE(DRIVER_ID) AS DRIVER_ID_VALUE,
  COUNT(*) AS EVENT_COUNT,
  (MAX(LATITUDE) - MIN(LATITUDE)) AS LAT_SPAN,
  (MAX(LONGITUDE) - MIN(LONGITUDE)) AS LON_SPAN,
  AVG(SPEED_KMH) AS AVG_SPEED,
  WINDOWSTART AS WINDOW_START,
  WINDOWEND AS WINDOW_END,
  'GPS_FREEZE' AS TYPE
FROM gps_events_driver
WINDOW TUMBLING (SIZE 2 MINUTES)
GROUP BY DRIVER_ID
HAVING COUNT(*) >= 4
  AND (MAX(LATITUDE) - MIN(LATITUDE)) < 0.00015
  AND (MAX(LONGITUDE) - MIN(LONGITUDE)) < 0.00015
  AND AVG(SPEED_KMH) < 8
EMIT CHANGES;
