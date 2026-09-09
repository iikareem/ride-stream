-- Phase 4 Step 4 — windowed spike counts (aggregates)
-- Requires Step 2: stream GPS_EVENTS.
-- Step 3 (speed_spikes) can keep running; this is a separate query.
--
-- Apply:
--   docker exec -it ridestream-ksqldb-cli ksql http://ksqldb-server:8088
--   RUN SCRIPT '/ksql/03_spike_windows.sql';
--
-- If tables already exist with the old shape, drop first:
--   DROP TABLE IF EXISTS spike_counts_tumbling DELETE TOPIC;
--   DROP TABLE IF EXISTS spike_counts_hopping DELETE TOPIC;
--   (recreate window topics via: docker compose up -d --force-recreate init-topics)
--   then RUN SCRIPT again.
--
-- AS_VALUE(DRIVER_ID) AS DRIVER_ID_VALUE → readable id in JSON
-- (cannot reuse alias DRIVER_ID twice on ksqlDB 7.9; key still uses DRIVER_ID via GROUP BY)

-- Tumbling: fixed 1-minute buckets, no overlap (0–60s, 60–120s, …)
CREATE TABLE IF NOT EXISTS spike_counts_tumbling
  WITH (
    KAFKA_TOPIC = 'driver-anomaly-windows',
    VALUE_FORMAT = 'JSON',
    KEY_FORMAT = 'KAFKA'
  ) AS
SELECT
  DRIVER_ID,
  AS_VALUE(DRIVER_ID) AS DRIVER_ID_VALUE,
  COUNT(*) AS SPIKE_COUNT,
  WINDOWSTART AS WINDOW_START,
  WINDOWEND AS WINDOW_END
FROM gps_events_driver
WINDOW TUMBLING (SIZE 1 MINUTE)
WHERE SPEED_KMH > 50
GROUP BY DRIVER_ID
HAVING COUNT(*) >= 2
EMIT CHANGES;

-- Hopping: 1-minute window that advances every 15s (windows overlap)
CREATE TABLE IF NOT EXISTS spike_counts_hopping
  WITH (
    KAFKA_TOPIC = 'driver-anomaly-windows-hop',
    VALUE_FORMAT = 'JSON',
    KEY_FORMAT = 'KAFKA'
  ) AS
SELECT
  DRIVER_ID,
  AS_VALUE(DRIVER_ID) AS DRIVER_ID_VALUE,
  COUNT(*) AS SPIKE_COUNT,
  WINDOWSTART AS WINDOW_START,
  WINDOWEND AS WINDOW_END
FROM gps_events_driver
WINDOW HOPPING (SIZE 1 MINUTE, ADVANCE BY 15 SECONDS)
WHERE SPEED_KMH > 50
GROUP BY DRIVER_ID
HAVING COUNT(*) >= 2
EMIT CHANGES;
