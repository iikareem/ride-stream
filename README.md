# RideStream

RideStream is a real-time GPS streaming pipeline that models the backend of a ride-sharing platform. Simulated drivers publish location events into Apache Kafka; NestJS workers consume them through independent consumer groups to derive ETAs, maintain live positions, and detect anomalies.

Events are keyed by `driver_id` for strict per-driver ordering. The serialization path is designed around **Avro** and Confluent Schema Registry so schemas can evolve safely under compatibility rules. Exactly-once-oriented processing and Prometheus/Grafana observability complete the operational story.

**Status:** Phase 3c — Latency tuning and rebalance behavior (observe `latency_ms`, tune fetch/session knobs, drill rebalances).

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

### Phase 3 (current)

```
GPS producer ──Avro──▶ gps-events
                          │
          ┌───────────────┼───────────────┐
          ▼               ▼               
   gps-printer      ridestream-eta
   (log only)       (haversine ETA)
                          │
                          ▼
                     eta-updates (Avro)
                          │
                          ▼
                  ridestream-live-map
                  (in-memory Map: position + ETA)
```

| Component | Role |
| --- | --- |
| Producer | Simulates N drivers; Avro GPS to `gps-events` |
| `ridestream-gps-printer` | Decodes and logs GPS (independent group) |
| `ridestream-eta` | Decodes GPS, assigns a stable fake destination per driver, publishes Avro ETA to `eta-updates` |
| `ridestream-live-map` | Consumes `eta-updates`, upserts latest position + ETA per `driver_id` in memory (no HTTP yet) |
| Topics | `gps-events`, `eta-updates` (6 partitions each via `init-topics`) |

If Kafka was already running before `eta-updates` was added, recreate topics:

```bash
docker compose up -d --force-recreate init-topics
# or: docker compose down && docker compose up -d
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
│   ├── kafka/                  # Kafka client, Schema Registry, Avro schemas, rebalance helpers
│   ├── producer/               # GPS simulator worker
│   ├── consumer/               # gps-printer consumer worker
│   ├── eta/                    # ETA calculator worker
│   ├── live-map/               # Live map updater (in-memory latest positions)
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

# Start Kafka, Schema Registry, Kafka UI, and create topics
docker compose up -d
docker compose logs init-topics

# Terminal A — simulate drivers
npm run start:producer

# Terminal B — ETA calculator (gps-events → eta-updates)
npm run start:eta

# Optional — print raw GPS
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
| `ETA_UPDATES_TOPIC` | `eta-updates` | ETA output topic |
| `ETA_GROUP_ID` | `ridestream-eta` | ETA consumer group id |
| `LIVE_MAP_GROUP_ID` | `ridestream-live-map` | Live map consumer group id |
| `SCHEMA_REGISTRY_URL` | `http://localhost:8081` | Confluent Schema Registry |
| `DRIVER_COUNT` | `10` | Simulated drivers in the producer |
| `KAFKA_CLIENT_ID` | `ridestream` | Kafka client id (printer group: `ridestream-gps-printer`) |
| `CONSUME_FROM_BEGINNING` | `true` | Replay earliest offsets (`false` = live tail only) |
| `PROCESSING_DELAY_MS` | `0` | Artificial per-message sleep to grow lag |
| `SESSION_TIMEOUT_MS` | `30000` | Broker kicks member if no heartbeat in this window |
| `HEARTBEAT_INTERVAL_MS` | `3000` | How often the consumer heartbeats |
| `REBALANCE_TIMEOUT_MS` | `60000` | Max time allowed for a rebalance |
| `FETCH_MAX_WAIT_MS` | `500` | Broker may hold a fetch up to this long |
| `FETCH_MIN_BYTES` | `1` | Min bytes before a fetch returns (`1` = ASAP) |

Topic partition count (6) is set in `docker-compose.yml` under `init-topics`, not in the Nest app.

---

## npm scripts

| Script | Purpose |
| --- | --- |
| `npm run start:producer` | GPS event producer |
| `npm run start:producer:dev` | Producer with watch mode |
| `npm run start:consumer` | GPS printer consumer |
| `npm run start:consumer:dev` | Consumer with watch mode |
| `npm run start:eta` | ETA calculator (`gps-events` → `eta-updates`) |
| `npm run start:eta:dev` | ETA calculator with watch mode |
| `npm run start:live-map` | Live map updater (`eta-updates` → in-memory Map) |
| `npm run start:live-map:dev` | Live map updater with watch mode |
| `npm run build` | Compile TypeScript |
| `npm start` | Default Nest HTTP app (not used by pipeline workers) |

---

## Latency and rebalance (Phase 3c)

Every consumer log line includes `latency_ms` = `now - event.timestamp` (producer wall clock embedded in the Avro payload). That is **end-to-end processing delay**, not Kafka’s official consumer-lag metric (offset lag lives in Kafka UI / `kafka-consumer-groups`).

### Drill 1 — Live latency

```bash
# Prefer a clean live tail so latency_ms stays small
CONSUME_FROM_BEGINNING=false npm run start:eta
npm run start:producer
```

Expect `latency_ms` mostly in the tens–hundreds of ms when caught up. If you leave `CONSUME_FROM_BEGINNING=true` with a backlog, `latency_ms` will be huge until catch-up finishes — that is intentional teaching of lag.

### Drill 2 — Grow lag on purpose

```bash
PROCESSING_DELAY_MS=2000 CONSUME_FROM_BEGINNING=false npm run start:eta
npm run start:producer
```

Each message sleeps 2s → consumer cannot keep up → Kafka UI lag rises. Set delay back to `0` and watch lag drain.

### Drill 3 — Rebalance (same groupId)

Works for printer, ETA, or live-map — use the **same** script twice:

```bash
# Terminal 1
npm run start:eta

# Terminal 2 — same ridestream-eta group → Kafka splits the 6 partitions
npm run start:eta
```

You should see:

```text
[ridestream-eta] rebalancing — partitions being revoked/reassigned
[ridestream-eta] joined — member=… assignment: gps-events=[0, 2, 4]
```

Stop one process; the survivor rebalances and takes the rest. During rebalance, at-least-once delivery can mean a few **duplicate** processings (offsets not yet committed).

Also useful:

```bash
docker exec ridestream-broker kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group ridestream-eta
```

Or open Kafka UI → Consumer Groups.

### What the knobs mean

| Knob | Effect |
| --- | --- |
| `FETCH_MAX_WAIT_MS` / `FETCH_MIN_BYTES` | Fetch wait vs return-ASAP (micro-batching of pulls) |
| `SESSION_TIMEOUT_MS` / `HEARTBEAT_INTERVAL_MS` | How fast a dead member is detected → rebalance starts |
| `PROCESSING_DELAY_MS` | Simulates slow business logic → lag grows |
| `CONSUME_FROM_BEGINNING` | Replay history vs live-only |

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

- [x] ETA Calculator consumer group → `eta-updates` topic
- [x] Live Map Updater consumer group (`eta-updates` → in-memory latest + ETA)
- [x] Latency tuning and rebalance behavior (`latency_ms`, fetch/session knobs, drills)

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
