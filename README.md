# RideStream

RideStream is a real-time GPS streaming pipeline that models the backend of a ride-sharing platform. Simulated drivers publish location events into Apache Kafka; NestJS workers consume them through independent consumer groups to derive ETAs, maintain live positions, and detect anomalies.

Events are keyed by `driver_id` for strict per-driver ordering. The serialization path is designed around **Avro** and Confluent Schema Registry so schemas can evolve safely under compatibility rules. Exactly-once-oriented processing and Prometheus/Grafana observability complete the operational story.

**Status:** Phase 2 — Avro serialization via Confluent Schema Registry; Phase 1 JSON path retired.

> **Learning project.** RideStream is a practical build for learning Apache Kafka, Avro, Schema Registry, consumer groups, and stream-processing concepts by implementing a realistic ride-sharing GPS pipeline.

---

## Scope

What this project exercises end to end:

- Partitioning strategy and message keys
- Consumer groups, rebalancing, and offset commits
- Avro serialization with Schema Registry and schema evolution
- Stateful processing for anomaly detection
- Consumer lag monitoring and fault-injection drills

The full pipeline is proven on one broker first. Multi-broker clustering follows once that path is solid.

---

## Architecture

### Target pipeline

```
Drivers (producers)
        │
        ▼
  Kafka broker (KRaft, single node for now)
        │
        ├──▶ ETA Calculator          → eta-updates
        ├──▶ Live Map Updater        → latest positions
        └──▶ Anomaly Detector        → driver-anomalies
                    │
                    ▼
             Prometheus + Grafana

  Capstone (Phase 8): Redis Pub/Sub → WebSocket live push to clients
```

### Phase 2 (current)

```
GPS producer ──Avro──▶ Schema Registry (register / fetch by id)
       │
       └──binary──▶ gps-events (6 partitions)
                          │
                          ▼
               gps-printer consumer group
               (decode Avro via Schema Registry)
```

| Component | Role |
| --- | --- |
| Producer | Simulates N drivers; encodes GPS events as Avro; key = `driver_id` |
| Subject `gps-events-value` | Schema Registry subject (v1 baseline, v2 adds optional `heading`) |
| Topic `gps-events` | 6 partitions, created by Compose `init-topics` |
| Consumer `ridestream-gps-printer` | Decodes Avro and logs fields; run multiple instances to observe rebalance |

Avro payloads are not compatible with leftover Phase 1 JSON messages. After upgrading, reset local Kafka once:

```bash
docker compose down && docker compose up -d
```

---

## Tech stack

| Layer | Technology |
| --- | --- |
| Runtime | Node.js 20+, TypeScript |
| Application | NestJS (separate entrypoints per worker) |
| Kafka client | KafkaJS |
| Broker | Confluent Kafka 7.9 (KRaft, single broker) |
| Schema | Confluent Schema Registry + Avro |
| Metrics (Phase 5) | Prometheus + Grafana |
| Read model (Phase 8) | Redis (latest state + Pub/Sub) |
| Live clients (Phase 8) | WebSocket push |
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
│   ├── kafka/                  # Kafka client, Schema Registry, Avro schemas
│   ├── producer/               # GPS simulator worker
│   ├── consumer/               # gps-printer consumer worker
│   ├── app.module.ts           # Default HTTP bootstrap (unused by workers)
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
| `SCHEMA_REGISTRY_URL` | `http://localhost:8081` | Confluent Schema Registry |
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

## Event shape (Avro)

Logical record (wire format is Confluent Avro binary with schema id):

```json
{
  "driver_id": "driver-001",
  "latitude": 30.0444,
  "longitude": 31.2357,
  "speed_kmh": 42.5,
  "timestamp": 1710000000000,
  "status": "en_route",
  "heading": 187.5
}
```

Message key: `driver_id` (ordering per driver).

Schemas live in [`src/kafka/schemas/gps-event.avsc.ts`](src/kafka/schemas/gps-event.avsc.ts):

- **v1** — baseline fields  
- **v2** — adds optional `heading` (`null` default) under Registry `BACKWARD` compatibility  

On startup the app registers v1 then v2 for subject `gps-events-value`, then produces with v2.

### Verify schema evolution

```bash
# List versions for the value subject
curl -s http://localhost:8081/subjects/gps-events-value/versions

# Inspect latest schema
curl -s http://localhost:8081/subjects/gps-events-value/versions/latest | jq .

# Compatibility level for the subject (Compose defaults the cluster to BACKWARD)
curl -s http://localhost:8081/config/gps-events-value | jq .
```

Or open Kafka UI → Schema Registry → `gps-events-value`.

---

## Roadmap

### Phase 1 — Foundation (done)

- [x] Single-broker Kafka via Docker Compose (KRaft)
- [x] `gps-events` topic (6 partitions)
- [x] GPS producer (JSON, keyed by `driver_id`)
- [x] Plain consumer group that prints events
- [x] Rebalance assignment logging

### Phase 2 — Schema management (done)

- [x] Avro serialization + Schema Registry
- [x] Backward-compatible schema evolution (`heading` optional in v2)

### Phase 3 — Consumers

- [ ] ETA Calculator consumer group → `eta-updates` topic
- [ ] Live Map Updater consumer group (in-memory latest positions)
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

### Phase 8 — Live clients (capstone)

Push ride state to the browser in real time with **Redis Pub/Sub + WebSockets**.

```text
Kafka consumers  →  Redis (SET latest + PUBLISH update)
                         │
                         ▼
              Nest subscribes (Pub/Sub)  →  WebSocket  →  live clients
```

- [ ] Redis in Docker Compose (keys for latest location/ETA + TTL)
- [ ] Consumers **SET** latest state and **PUBLISH** on change (Pub/Sub)
- [ ] Nest gateway subscribes to Redis and **pushes** over WebSocket
- [ ] Typed live messages: `driver.location`, `driver.eta`, optional `chat.message`
- [ ] Simple client UI that renders the live feed

Kafka = events. Redis Pub/Sub = notify. WebSocket = live push to the client.

---

## Design decisions

| Decision | Rationale |
| --- | --- |
| Partition by `driver_id` | Preserves GPS order per driver; required for sane ETA / anomaly logic |
| 6 partitions | Enough parallelism to practice consumer-group scaling without over-provisioning locally |
| JSON then Avro | Phase 1 proved the path with JSON; Phase 2 switched to Avro + Registry |
| Avro + BACKWARD | Optional fields with defaults (e.g. `heading`) let readers use new schemas on old data |
| Separate Nest entrypoints | One process per worker; scale a group by running more members with the same `groupId` |
| Topics created in Compose | Explicit layout; `AUTO_CREATE_TOPICS` is disabled |
| No Docker volumes (yet) | Ephemeral local data; wipe clean with `compose down` |
| Single broker first | Learn the full pipeline before cluster failure modes |
| Redis + Pub/Sub (Phase 8) | Latest state in keys; PUBLISH triggers live fan-out |
| WebSocket (Phase 8) | Push location / ETA / chat to clients in real time |

---

## Learning notes

Companion study sheet (questions and answers from building the pipeline):

[`docs/kafka-learning-qa.md`](docs/kafka-learning-qa.md)

---

## License

Private / unlicensed (`UNLICENSED`). Not published for reuse.
