# RideStream

Real-time GPS event pipeline for a simulated ride-sharing backend, built on Apache Kafka with NestJS and KafkaJS.

Drivers emit location updates. Kafka ingests them at scale, keyed by `driver_id` for per-driver ordering. Downstream consumer groups will compute ETAs, maintain live positions, and detect anomalies. Schema Registry, exactly-once-oriented processing, and Prometheus/Grafana observability are planned in later phases.

**Status:** Phase 1 (foundation) — single-broker Kafka, GPS producer, and a printable consumer group.

---

## Why this project

RideStream is a hands-on Kafka system designed around production concerns, not a hello-world broker demo:

- Partitioning and key strategy
- Consumer groups, rebalancing, and offset commits
- Schema evolution (Avro + Schema Registry)
- Stateful stream-style processing and anomaly detection
- Lag monitoring and fault-injection drills

Clustering (multi-broker) is deferred until the full single-broker pipeline works end to end.

---

## Architecture

### Target pipeline

```
Drivers (producers)
        │
        ▼
  Kafka broker (KRaft, single node for now)
        │
        ├──▶ ETA Calculator          (consumer group)
        ├──▶ Live Map Updater        (consumer group)
        └──▶ Anomaly Detector        (stateful processing)
                    │
                    ▼
             Prometheus + Grafana
```

### Phase 1 (current)

```
GPS producer  ──JSON──▶  gps-events (6 partitions)
                                │
                                ▼
                     gps-printer consumer group
                     (one or more members)
```

| Component | Role |
| --- | --- |
| Producer | Simulates N drivers; emits GPS events every 2–5s; message key = `driver_id` |
| Topic `gps-events` | 6 partitions, created by Compose `init-topics` |
| Consumer `ridestream-gps-printer` | Logs events; run multiple instances to observe rebalance |

---

## Tech stack

| Layer | Technology |
| --- | --- |
| Runtime | Node.js 20+, TypeScript |
| Application | NestJS (separate entrypoints per worker) |
| Kafka client | KafkaJS |
| Broker | Confluent Kafka 7.9 (KRaft, single broker) |
| Schema (Phase 2+) | Confluent Schema Registry + Avro |
| Metrics (Phase 5) | Prometheus + Grafana |
| Local infra | Docker Compose |
| Ops UI | Kafka UI (`localhost:8080`) |

---

## Repository layout

```
ride-stream/
├── docker-compose.yml          # Broker, Schema Registry, Kafka UI, topic init
├── docs/
│   └── kafka-learning-qa.md    # Study Q&A from building the pipeline
├── src/
│   ├── kafka/                  # Shared Kafka client, config, GPS event type
│   ├── producer/               # GPS simulator worker
│   ├── consumer/               # gps-printer consumer worker
│   ├── app.module.ts           # Default HTTP bootstrap (unused in Phase 1 workers)
│   └── main.ts
├── .env.example
└── package.json
```

Workers are isolated Nest processes (not one monolith handling produce + consume). That keeps consumer groups easy to scale and reason about.

---

## Prerequisites

- Docker Desktop (or compatible Docker engine)
- Node.js 20+
- npm

---

## Getting started

```bash
git clone https://github.com/iikareem/ride-stream.git
cd ride-stream
cp .env.example .env
npm install

# Start Kafka, Schema Registry, Kafka UI, and create gps-events
docker compose up -d
docker compose logs init-topics

# Terminal A — simulate drivers
npm run start:producer

# Terminal B — consume and print
npm run start:consumer
```

### Local endpoints

| Service | URL |
| --- | --- |
| Kafka bootstrap | `localhost:9092` |
| Schema Registry | http://localhost:8081 |
| Kafka UI | http://localhost:8080 |

Tear down (no volumes: data and offsets are wiped):

```bash
docker compose down
```

---

## Configuration

Copy `.env.example` to `.env`:

| Variable | Default | Description |
| --- | --- | --- |
| `KAFKA_BROKERS` | `localhost:9092` | Comma-separated bootstrap servers |
| `GPS_EVENTS_TOPIC` | `gps-events` | GPS topic name |
| `DRIVER_COUNT` | `10` | Simulated drivers in the producer |
| `KAFKA_CLIENT_ID` | `ridestream` | Kafka client id (group id prefix for printer: `ridestream-gps-printer`) |

Topic partition count (6) is set in `docker-compose.yml` under `init-topics`, not in the Nest app.

---

## npm scripts

| Script | Purpose |
| --- | --- |
| `npm run start:producer` | GPS event producer |
| `npm run start:producer:dev` | Producer with watch mode |
| `npm run start:consumer` | GPS printer consumer |
| `npm run start:consumer:dev` | Consumer with watch mode |
| `npm run build` | Compile TypeScript |
| `npm start` | Default Nest HTTP app (not used for Phase 1 pipeline) |

---

## Observing consumer rebalance

The printer consumer logs partition assignment on `GROUP_JOIN` / `REBALANCING`.

```bash
# Terminal 1
npm run start:consumer

# Terminal 2 — same groupId → Kafka splits partitions
npm run start:consumer
```

You should see lines like:

```text
[ridestream-gps-printer] rebalancing…
[ridestream-gps-printer] joined — member=… assignment: gps-events=[0, 2, 4]
```

Stop one process; the other takes the remaining partitions.

Also useful:

```bash
docker exec ridestream-broker kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group ridestream-gps-printer
```

Or open Kafka UI → Consumer Groups → `ridestream-gps-printer`.

---

## Event shape (Phase 1 JSON)

```json
{
  "driver_id": "driver-001",
  "latitude": 30.0444,
  "longitude": 31.2357,
  "speed_kmh": 42.5,
  "timestamp": 1710000000000,
  "status": "en_route"
}
```

Message key: `driver_id` (ordering per driver). Serialization moves to Avro in Phase 2.

---

## Roadmap

### Phase 1 — Foundation (done)

- [x] Single-broker Kafka via Docker Compose (KRaft)
- [x] `gps-events` topic (6 partitions)
- [x] GPS producer (JSON, keyed by `driver_id`)
- [x] Plain consumer group that prints events
- [x] Rebalance assignment logging

### Phase 2 — Schema management

- [ ] Avro serialization + Schema Registry
- [ ] Backward-compatible schema evolution

### Phase 3 — Consumers

- [ ] ETA Calculator consumer group
- [ ] Live Map Updater consumer group
- [ ] Latency tuning and rebalance behavior

### Phase 4 — Stream processing

- [ ] Anomaly detector (speed spikes, GPS freeze, teleport, route deviation)
- [ ] Hopping windows + stateful stores
- [ ] Exactly-once-oriented configuration

### Phase 5 — Observability

- [ ] JMX / Prometheus metrics
- [ ] Grafana dashboards
- [ ] Consumer lag alerts

### Phase 6 — Fault tolerance

- [ ] Broker restart and offset resume
- [ ] Slow consumer / lag growth
- [ ] Duplicate injection vs idempotent producer

### Phase 7 — Cluster (future)

- [ ] 3-broker cluster, replication, `min.insync.replicas`
- [ ] Broker failure and leader election drills

---

## Design decisions

| Decision | Rationale |
| --- | --- |
| Partition by `driver_id` | Preserves GPS order per driver; required for sane ETA / anomaly logic |
| 6 partitions | Enough parallelism to demo consumer-group scaling without over-provisioning locally |
| JSON in Phase 1 | Unblocks the pipeline; Avro comes once the path is proven |
| Separate Nest entrypoints | One process per worker; scale a group by running more members with the same `groupId` |
| Topics created in Compose | Explicit layout; `AUTO_CREATE_TOPICS` is disabled |
| No Docker volumes (yet) | Ephemeral local data; wipe clean with `compose down` |
| Single broker first | Learn the full pipeline before cluster failure modes |

---

## Learning notes

Companion study sheet (questions and answers from building Phase 1):

[`docs/kafka-learning-qa.md`](docs/kafka-learning-qa.md)

---

## Resume blurb

> Built a real-time GPS tracking pipeline simulating a ride-sharing backend using Apache Kafka and NestJS. Partitioned events by driver ID for strict ordering, added consumer-group processing with rebalance visibility, and scoped later phases for schema evolution, stateful anomaly detection, and Prometheus/Grafana observability.

---

## License

Private / unlicensed (`UNLICENSED`). Not published for reuse.
