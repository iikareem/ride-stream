-- Phase 4 Step 2 — register Avro gps-events-driver as a ksqlDB stream.
-- Schema comes from Schema Registry subject gps-events-driver-value (Nest producer).
-- Message key is driver_id (UTF-8 bytes from KafkaJS) → KEY_FORMAT='KAFKA'.
--
-- Apply (once stack is up and Nest has registered the Avro schema at least once):
--   docker exec -it ridestream-ksqldb-cli ksql http://ksqldb-server:8088
--   RUN SCRIPT '/ksql/01_gps_stream.sql';
-- Or paste the statement below into the CLI.

CREATE STREAM IF NOT EXISTS gps_events_driver
  WITH (
    KAFKA_TOPIC = 'gps-events-driver',
    KEY_FORMAT = 'KAFKA',
    VALUE_FORMAT = 'AVRO'
  );
