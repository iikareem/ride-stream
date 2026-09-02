# RideStream

Real-time ride-sharing GPS pipeline on Apache Kafka — NestJS + KafkaJS.

Simulates drivers emitting location events, then processes them through consumer groups and a stateful anomaly detector, with schema evolution and Prometheus/Grafana observability planned across phases.

> CV one-liner: *Built a real-time GPS tracking pipeline simulating a ride-sharing backend using Apache Kafka and NestJS. Partitioned events by driver ID for strict ordering, implemented stateful stream processing for anomaly detection, and designed for exactly-once delivery with Schema Registry and Prometheus/Grafana observability.*

---

## Architecture (target)

```
Drivers (Producers)
      │
      ▼
 Kafka Broker (single instance for now)
      │
      ├──▶ ETA Calculator
      ├──▶ Live Map Updater
      └──▶ Anomaly Detector (stateful KafkaJS consumer)
                    │
                    ▼
             Prometheus + Grafana
```

Multi-broker clustering is deferred until the full pipeline works on one broker.

---

## Stack

| Layer | Tech |
| --- | --- |
| App | NestJS (Node.js / TypeScript) |
| Kafka client | KafkaJS |
| Broker | Apache Kafka (KRaft, single broker) |
| Schema | Confluent Schema Registry + Avro (Phase 2+) |
| Metrics | Prometheus + Grafana (Phase 5) |
| Infra | Docker Compose |

---

## Quick start (Phase 1)

**Prerequisites:** Docker Desktop, Node.js 20+

```bash
# 1. Install deps
npm install

# 2. Start Kafka + Schema Registry + Kafka UI
docker compose up -d

# 3. Confirm topic exists
docker compose logs init-topics

# 4. Produce simulated GPS events
npm run start:producer

# 5. In another terminal, print events
npm run start:consumer
```

- Kafka: `localhost:9092`
- Schema Registry: `http://localhost:8081`
- Kafka UI: `http://localhost:8080`

Stop the stack: `docker compose down`

---

## Phase checklist

### Phase 1 — Foundation (current)
- [x] Single-broker Kafka via Docker Compose
- [x] `gps-events` topic (6 partitions, keyed by `driver_id`)
- [x] GPS event producer (JSON, NestJS)
- [x] Plain consumer that prints events

### Phase 2 — Schema management
- [ ] Avro / Schema Registry serialization
- [ ] Backward-compatible schema evolution

### Phase 3 — Consumers
- [ ] ETA Calculator consumer group
- [ ] Live Map Updater consumer group
- [ ] Rebalance handling + latency tuning

### Phase 4 — Stream processing
- [ ] Anomaly Detector (hopping windows, stateful stores, EOS-oriented design)

### Phase 5 — Observability
- [ ] Metrics + Grafana + lag alerts

### Phase 6 — Fault tolerance
- [ ] Broker restart, slow consumer, duplicate injection tests

### Phase 7 — Cluster (future)
- [ ] 3-broker setup, replication, leader election drills

---

## Design notes

- **Partition by `driver_id`** — keeps per-driver GPS order.
- **JSON first** — unblock the pipeline; Avro lands in Phase 2.
- **NestJS workers** — separate entrypoints for producer and consumer keep Phase 1 simple; later phases add more Nest modules/services.
