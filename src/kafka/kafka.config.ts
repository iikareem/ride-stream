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

  /**
   * When true, consumers replay from the earliest offset (catch-up).
   * Latency numbers look huge until the backlog is drained — set false for live drills.
   */
  consumeFromBeginning: (process.env.CONSUME_FROM_BEGINNING ?? 'true') === 'true',

  /** Artificial sleep inside eachMessage — raise this to grow consumer lag on purpose. */
  processingDelayMs: Number(process.env.PROCESSING_DELAY_MS ?? '0'),

  // --- KafkaJS consumer tuning (rebalance / fetch latency) ---
  /** How long the broker waits for a heartbeat before kicking the member (ms). */
  sessionTimeoutMs: Number(process.env.SESSION_TIMEOUT_MS ?? '30000'),
  /** How often this consumer heartbeats to the group coordinator (ms). */
  heartbeatIntervalMs: Number(process.env.HEARTBEAT_INTERVAL_MS ?? '3000'),
  /** Max time allowed for a rebalance to finish (ms). */
  rebalanceTimeoutMs: Number(process.env.REBALANCE_TIMEOUT_MS ?? '60000'),
  /** Max time the broker holds a fetch when there is little data (ms). */
  maxWaitTimeInMs: Number(process.env.FETCH_MAX_WAIT_MS ?? '500'),
  /** Min bytes before a fetch returns (1 = return ASAP). */
  fetchMinBytes: Number(process.env.FETCH_MIN_BYTES ?? '1'),
} as const;
