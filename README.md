# RideStream

RideStream is a real-time GPS streaming pipeline that models the backend of a ride-sharing platform. Simulated drivers publish location events into Apache Kafka; NestJS workers consume them through independent consumer groups to derive ETAs. **Stream processing for anomalies uses ksqlDB** (SQL on Kafka), not Nest business logic.

Events are keyed by `driver_id` for strict per-driver ordering. The serialization path is designed around **Avro** and Confluent Schema Registry so schemas can evolve safely under compatibility rules. ksqlDB can run with exactly-once processing guarantees; Prometheus/Grafana observability complete the operational story.

**Status:** Phase 7 — Live clients (Redis GEO + Pub/Sub + WebSocket + live feed UI). Typed event names still open; Phase 8 cluster is next.

> **Learning project.** RideStream is a practical build for learning Apache Kafka, Avro, Schema Registry, consumer groups, and stream-processing concepts (Nest workers + ksqlDB) by implementing a realistic ride-sharing GPS pipeline.

---

## Scope

What this project exercises end to end:

- Partitioning strategy and message keys
- Consumer groups, rebalancing, and offset commits
- Avro serialization with Schema Registry and schema evolution
- Stateful / windowed stream processing with **ksqlDB** (anomalies)
- Consumer lag monitoring and fault-injection drills

The full pipeline is proven on one broker first. Multi-broker clustering follows once that path is solid.

---

## Architecture

### Target pipeline

```
Drivers (Nest producers)
        │
        ▼
  Kafka broker (KRaft, single node for now)
        │
        ├──▶ Nest ETA Calculator     → eta-updates
        └──▶ ksqlDB                  → driver-anomalies
                    │
                    ▼
             Prometheus + Grafana (Phase 5)

  Capstone (Phase 7): Redis Pub/Sub → WebSocket live push to clients
```

### Why ksqlDB (Phase 4)

| Approach | Role in RideStream |
| --- | --- |
| **Nest + KafkaJS** | Produce GPS, ETA, optional print/consume of anomalies |
| **ksqlDB** | Continuous SQL on topics: filters, tumbling/hopping windows, aggregates → `driver-anomalies` |
| Kafka Streams / Flink | Not used here; noted as heavier JVM alternatives for production |

Nest does **not** embed ksqlDB. ksqlDB runs as its own Docker service, reads/writes Kafka topics; Nest only talks to topics (and optionally the ksqlDB REST API for admin).

### Phase 3 (done) + Phase 4 (next)

```
GPS producer ──Avro──▶ gps-events-driver
                          │
          ┌───────────────┼───────────────┬────────────────┐
          ▼               ▼               ▼                
   ridestream-nearby  ridestream-eta      ksqlDB
   (GEOSEARCH+PUBLISH) (haversine ETA)   (SQL anomalies)
                          │               │
                          ▼               ▼
                     eta-updates    driver-anomalies
```

| Component | Role |
| --- | --- |
| Producer | Simulates N drivers; Avro GPS to `gps-events-driver` |
| `ridestream-nearby` | GEOSEARCH riders around driver → Redis `PUBLISH user:{riderId}` |
| `ridestream-eta` | Decodes GPS, **transactional** publish of Avro ETA to `eta-updates` (EOS: produce + offset commit) |
| **ksqlDB** (Phase 4) | Docker service; SQL streams/tables on `gps-events-driver` → `driver-anomalies` (optional EOS) |
| Topics | `gps-events-driver`, `gps-events-rider`, `eta-updates`, `driver-anomalies`, `driver-anomaly-windows`, `driver-anomaly-windows-hop`, `driver-anomalies-teleport`, `driver-anomalies-freeze` |

### Phase 4 Step 1 — ksqlDB infra

ksqlDB is up; anomaly SQL comes in later steps.

```bash
docker compose up -d
docker compose ps
# Recreate topics if the stack predates driver-anomalies:
docker compose up -d --force-recreate init-topics

# Health
curl -s http://localhost:8088/info

# Interactive CLI (SHOW TOPICS; then Ctrl+D to exit)
docker exec -it ridestream-ksqldb-cli ksql http://ksqldb-server:8088
```

| Endpoint | URL |
| --- | --- |
| ksqlDB REST | `http://localhost:8088` |
| Kafka UI | `http://localhost:8080` |
| Schema Registry | `http://localhost:8081` |

SQL files live under [`ksql/`](ksql/) (mounted into the CLI container at `/ksql`).

### Phase 4 Step 2 — `CREATE STREAM gps_events`

Registers the existing Avro topic so ksqlDB can query it. Nest must have produced at least once (so subject `gps-events-driver-value` exists in Schema Registry).

```bash
# 1) Infra + producer (registers Avro schema)
docker compose up -d
npm run start:producer

# 2) Apply stream definition
docker exec -it ridestream-ksqldb-cli ksql http://ksqldb-server:8088
```

In the CLI:

```sql
RUN SCRIPT '/ksql/01_gps_stream.sql';

SHOW STREAMS;
DESCRIBE gps_events;

SET 'auto.offset.reset' = 'earliest';
SELECT DRIVER_ID, LATITUDE, LONGITUDE, SPEED_KMH, STATUS
FROM gps_events
EMIT CHANGES
LIMIT 5;
```

Expect rows while the Nest producer is running. Stop the push query with `Ctrl+C`. Statement source: [`ksql/01_gps_stream.sql`](ksql/01_gps_stream.sql).

### Phase 4 Step 3 — speed spikes → `driver-anomalies`

Persistent query: keep reading `gps_events`, filter high speed, write JSON to topic `driver-anomalies`.

```bash
npm run start:producer
docker exec -it ridestream-ksqldb-cli ksql http://ksqldb-server:8088
```

```sql
-- if Step 2 not applied yet:
RUN SCRIPT '/ksql/01_gps_stream.sql';

RUN SCRIPT '/ksql/02_speed_spikes.sql';

SHOW STREAMS;
SHOW QUERIES;

SET 'auto.offset.reset' = 'earliest';
PRINT 'driver-anomalies' FROM BEGINNING;
-- or: SELECT * FROM speed_spikes EMIT CHANGES LIMIT 10;
```

Threshold in the SQL is **`SPEED_KMH > 50`** so the current Nest producer (speeds ~5–60) can generate hits. Raise it to `120` later for a stricter rule.

Source: [`ksql/02_speed_spikes.sql`](ksql/02_speed_spikes.sql).

### Phase 4 Step 4 — windowed spike counts

Step 3 emits **every** fast GPS point. Step 4 **counts** how many fast points a driver had in a time window, and only emits when the count is high enough (`HAVING COUNT(*) >= 2`).

| Window | Meaning | Output topic |
| --- | --- | --- |
| **Tumbling** `SIZE 1 MINUTE` | Non-overlapping 1-minute buckets | `driver-anomaly-windows` |
| **Hopping** `SIZE 1 MINUTE, ADVANCE BY 15 SECONDS` | Overlapping windows (slide every 15s) | `driver-anomaly-windows-hop` |

These are **`CREATE TABLE … AS SELECT`** (aggregates), not streams — each key holds the latest count for that window.

```bash
docker compose up -d --force-recreate init-topics   # creates the two window topics
npm run start:producer
docker exec -it ridestream-ksqldb-cli ksql http://ksqldb-server:8088
```

```sql
-- recreate (old tables lack DRIVER_ID in the value)
DROP TABLE IF EXISTS spike_counts_tumbling DELETE TOPIC;
DROP TABLE IF EXISTS spike_counts_hopping DELETE TOPIC;
-- then: docker compose up -d --force-recreate init-topics

RUN SCRIPT '/ksql/03_spike_windows.sql';

SHOW TABLES;
SHOW QUERIES;

PRINT 'driver-anomaly-windows' FROM BEGINNING;
-- PRINT 'driver-anomaly-windows-hop' FROM BEGINNING;
```

JSON value includes `DRIVER_ID_VALUE` (ksqlDB 7.9 cannot alias both key and value as `DRIVER_ID`). The Kafka key may still look binary for windows.

Source: [`ksql/03_spike_windows.sql`](ksql/03_spike_windows.sql).

### Phase 4 Step 5 — freeze + teleport (SQL heuristics)

More anomaly types **in ksqlDB**, still no Nest detection logic.

| Query | Pattern | Output |
| --- | --- | --- |
| `latest_gps` | Table: last lat/lon/ts per driver | changelog (internal / derived) |
| `gps_teleports` | Stream ⋈ table: `GEO_DISTANCE` to previous point > 0.25 km | `driver-anomalies-teleport` |
| `gps_freeze` | 2-min tumbling: almost no lat/lon span + low avg speed | `driver-anomalies-freeze` |

```bash
docker compose up -d --force-recreate init-topics
npm run start:producer
docker exec -it ridestream-ksqldb-cli ksql http://ksqldb-server:8088
```

```sql
RUN SCRIPT '/ksql/04_freeze_teleport.sql';
SHOW STREAMS;
SHOW TABLES;
SHOW QUERIES;

PRINT 'driver-anomalies-teleport' FROM BEGINNING;
PRINT 'driver-anomalies-freeze' FROM BEGINNING;
```

**Demo note:** the Nest producer only nudges ~0.002°. Real teleports/freezes may be rare until you temporarily inject jumps or stuck coordinates in the producer.

#### What SQL fits vs what belongs in Nest later

| Fits ksqlDB well | Harder / better in Nest (or Flink) later |
| --- | --- |
| Speed threshold, window counts | Route deviation (needs trip polyline / map match) |
| Jump vs last point (`GEO_DISTANCE`) | Multi-signal ML / scoring |
| “Barely moved for N minutes” freeze heuristic | Exact consecutive-sample physics with custom state machines |
| Filters + tumbling/hopping aggregates | Rich per-driver state beyond SQL joins |

Source: [`ksql/04_freeze_teleport.sql`](ksql/04_freeze_teleport.sql).

### Phase 4 Step 6 — exactly-once (EOS) on ksqlDB

Persistent queries use Kafka Streams transactions:

```yaml
# docker-compose.yml → ksqldb-server
KSQL_KSQL_STREAMS_PROCESSING_GUARANTEE: exactly_once_v2
```

```bash
docker compose up -d --force-recreate ksqldb-server ksqldb-cli
curl -s http://localhost:8088/info
```

**Important:** EOS applies to **new** persistent queries after restart. Queries created earlier keep their old guarantee — drop and re-`RUN SCRIPT` if you want them on EOS too.

Optional per-session override before creating a query:

```sql
SET 'processing.guarantee' = 'exactly_once_v2';
```

EOS here = ksqlDB **read → process → write** as one transactional unit. Nest consumers of `driver-anomalies*` are still typically **at-least-once** unless you add idempotency.

Notes: [`ksql/05_eos.md`](ksql/05_eos.md). Broker already has `transaction.state.log.*` for a single-node cluster.

---

## Tech stack

| Layer | Technology |
| --- | --- |
| Runtime | Node.js 20+, TypeScript |
| Application | NestJS (separate entrypoints per worker) |
| Kafka client | KafkaJS |
| Broker | Confluent Kafka 7.9 (KRaft, single broker) |
| Schema | Confluent Schema Registry + Avro |
| Stream SQL (Phase 4) | **ksqlDB** (Docker; continuous queries on Kafka topics) |
| Metrics (Phase 5) | kafka-exporter + Prometheus + Grafana |
| Read model (Phase 7) | Redis (latest state + Pub/Sub) |
| Live clients (Phase 7) | WebSocket push |
| Local infra | Docker Compose |
| Ops UI | Kafka UI (`localhost:8080`) |
| Redis Insight | `localhost:5540` (preconfigured; host is Compose service `redis`, not `127.0.0.1`) |
| ksqlDB UI/REST | `localhost:8088` |
| Prometheus | `localhost:9090` |
| Grafana | `localhost:3000` (admin / admin) |
| kafka-exporter | `localhost:9308/metrics` |

---

## Repository layout

```
ride-stream/
├── docker-compose.yml          # Broker, Schema Registry, Kafka UI, Redis, Redis Insight, ksqlDB, monitoring
├── monitoring/                 # Phase 5: Prometheus + Grafana + lag alerts
│   ├── prometheus.yml
│   ├── alerts.yml
│   └── grafana/
├── docs/
│   └── kafka-learning-qa.md    # Study Q&A from building the pipeline
├── client/                     # Phase 7: live feed UI (served by gateway)
├── ksql/                       # Phase 4: ksqlDB statements (streams, anomaly queries)
├── src/
│   ├── drivers/
│   │   ├── producer/           # Driver GPS simulator → gps-events-driver
│   │   └── consumer/
│   │       ├── printer/        # GPS printer
│   │       ├── nearby/         # Driver GPS → GEOSEARCH → PUBLISH user:{riderId}
│   │       └── eta/            # ETA calculator → eta-updates
│   ├── riders/
│   │   ├── producer/           # Rider GPS simulator → gps-events-rider
│   │   └── consumer/
│   │       └── geo/            # Rider GPS → Redis GEOADD
│   ├── gateway/                # Socket.IO + Redis Pub/Sub (+ static UI)
│   └── shared/
│       ├── kafka/              # Kafka client, Schema Registry, Avro schemas
│       └── redis/              # Redis client (GEO + Pub/Sub helpers)
├── .env.example
└── package.json
```

Nest workers are isolated processes under `drivers/`, `riders/`, and `gateway/`. **ksqlDB** and **Prometheus/Grafana** are separate Compose services — no Nest metrics code required for Phase 5.

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

# Terminal B — ETA calculator (gps-events-driver → eta-updates)
npm run start:eta

# Terminal C — nearby fan-out (GEOSEARCH riders → PUBLISH user:{riderId})
npm run start:nearby
```

### Local endpoints

| Service | URL |
| --- | --- |
| Kafka bootstrap | `localhost:9092` |
| Schema Registry | http://localhost:8081 |
| Kafka UI | http://localhost:8080 |
| Redis | `localhost:6379` |
| Redis Insight | http://localhost:5540 |

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
| `GPS_EVENTS_DRIVER_TOPIC` | `gps-events-driver` | Driver GPS topic name |
| `GPS_EVENTS_RIDER_TOPIC` | `gps-events-rider` | Rider GPS topic name |
| `ETA_UPDATES_TOPIC` | `eta-updates` | ETA output topic |
| `ETA_GROUP_ID` | `ridestream-eta` | ETA consumer group id |
| `ETA_TRANSACTIONAL_ID` | `ridestream-eta-producer` | ETA EOS transactional.id (one live ETA instance) |
| `SCHEMA_REGISTRY_URL` | `http://localhost:8081` | Confluent Schema Registry |
| `DRIVER_COUNT` | `10` | Simulated drivers in the producer |
| `RIDER_COUNT` | `10` | Simulated riders in the rider producer |
| `RIDER_GEO_GROUP_ID` | `ridestream-rider-geo` | Rider GEO consumer group id |
| `NEARBY_GROUP_ID` | `ridestream-nearby` | Nearby fan-out consumer group id |
| `NEARBY_RADIUS_KM` | `2` | GEOSEARCH radius around driver (km) |
| `REDIS_URL` | `redis://localhost:6379` | Redis connection URL |
| `RIDERS_GEO_KEY` | `riders:geo` | Redis GEO key for rider positions |
| `GATEWAY_PORT` | `3001` | Socket.IO gateway HTTP port |
| `KAFKA_CLIENT_ID` | `ridestream` | Kafka client id |
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
| `npm run start:producer` | Driver GPS event producer (`gps-events-driver`) |
| `npm run start:producer:dev` | Driver producer with watch mode |
| `npm run start:rider-producer` | Rider GPS event producer (`gps-events-rider`) |
| `npm run start:rider-producer:dev` | Rider producer with watch mode |
| `npm run start:rider-geo` | Rider GEO consumer (`gps-events-rider` → Redis GEO) |
| `npm run start:rider-geo:dev` | Rider GEO consumer with watch mode |
| `npm run start:gateway` | WebSocket gateway + live feed UI on `GATEWAY_PORT` |
| `npm run start:gateway:dev` | Gateway with watch mode |
| `npm run emit:test` | Redis `PUBLISH user:{id}` → gateway → Socket.IO `drivers` (no Kafka) |
| `npm run start:nearby` | Nearby fan-out (`gps-events-driver` → GEOSEARCH → `PUBLISH user:{riderId}`) |
| `npm run start:nearby:dev` | Nearby fan-out with watch mode |
| `npm run start:eta` | ETA calculator (`gps-events-driver` → `eta-updates`) |
| `npm run start:eta:dev` | ETA calculator with watch mode |
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

Works for nearby or ETA — use the **same** script twice:

```bash
# Terminal 1
npm run start:eta

# Terminal 2 — same ridestream-eta group → Kafka splits the 6 partitions
npm run start:eta
```

You should see:

```text
[ridestream-eta] rebalancing — partitions being revoked/reassigned
[ridestream-eta] joined — member=… assignment: gps-events-driver=[0, 2, 4]
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

Schemas live in [`src/shared/kafka/schemas/gps-event.avsc.ts`](src/shared/kafka/schemas/gps-event.avsc.ts):

- **v1** — baseline fields  
- **v2** — adds optional `heading` (`null` default) under Registry `BACKWARD` compatibility  

On startup the app registers v1 then v2 for subject `gps-events-driver-value`, then produces with v2.

### Verify schema evolution

```bash
# List versions for the value subject
curl -s http://localhost:8081/subjects/gps-events-driver-value/versions

# Inspect latest schema
curl -s http://localhost:8081/subjects/gps-events-driver-value/versions/latest | jq .

# Compatibility level for the subject (Compose defaults the cluster to BACKWARD)
curl -s http://localhost:8081/config/gps-events-driver-value | jq .
```

Or open Kafka UI → Schema Registry → `gps-events-driver-value`.

---

## Roadmap

### Phase 1 — Foundation (done)

- [x] Single-broker Kafka via Docker Compose (KRaft)
- [x] `gps-events-driver` topic (6 partitions)
- [x] GPS producer (JSON, keyed by `driver_id`)
- [x] Plain consumer group that prints events
- [x] Rebalance assignment logging

### Phase 2 — Schema management (done)

- [x] Avro serialization + Schema Registry
- [x] Backward-compatible schema evolution (`heading` optional in v2)

### Phase 3 — Consumers

- [x] ETA Calculator consumer group → `eta-updates` topic
- [x] Latency tuning and rebalance behavior (`latency_ms`, fetch/session knobs, drills)

### Phase 4 — Stream processing (**ksqlDB**)

Nest keeps ETA. Anomalies move to **ksqlDB** continuous SQL on `gps-events-driver`.

- [x] Add ksqlDB server (+ CLI) to Docker Compose; wire Schema Registry; create `driver-anomalies` topic
- [x] `ksql/` statements: `CREATE STREAM` over Avro `gps-events-driver`
- [x] Anomaly queries → `driver-anomalies` topic (speed spikes first; then freeze / teleport where SQL fits)
- [x] Windowed aggregates (tumbling / hopping) for spike counts
- [x] Freeze + teleport heuristics (`04_freeze_teleport.sql`)
- [x] `processing.guarantee = exactly_once_v2` (EOS) on ksqlDB
- [x] Nest anomaly printer — **skipped** (verify with Kafka UI / `PRINT 'driver-anomalies'`)

### Phase 5 — Observability (done)

Infra only: **kafka-exporter** scrapes consumer lag via the Kafka protocol; **Prometheus** pulls every 15s; **Grafana** dashboards + Prometheus alert rules. Broker still exposes JMX on `9101` for optional JVM tooling; lag alerts use the exporter (no Nest code).

```bash
docker compose up -d
open http://localhost:3000          # Grafana — admin / admin
# Dashboard: RideStream → RideStream Kafka
open http://localhost:9090          # Prometheus
open http://localhost:9090/alerts   # lag alert rules
curl -s http://localhost:9308/metrics | grep kafka_consumergroup_lag
```

**Lag drill:** slow ETA and watch Grafana climb:

```bash
PROCESSING_DELAY_MS=2000 CONSUME_FROM_BEGINNING=false npm run start:eta
npm run start:producer
```

Alert rules (see [`monitoring/alerts.yml`](monitoring/alerts.yml)): warn if lag `> 50` for 1m; critical if `> 500` for 2m.

- [x] Kafka metrics into Prometheus (via kafka-exporter; broker JMX on `9101`)
- [x] Grafana dashboards (provisioned RideStream Kafka)
- [x] Consumer lag alerts (Prometheus rules)

### Phase 6 — Fault tolerance (done)

- [x] Broker restart and offset resume
- [x] Slow consumer / lag growth
- [x] Duplicate injection vs idempotent producer (idempotent Nest producer enabled; drill still optional)

### Phase 7 — Live clients (capstone)

Push ride state to the browser in real time with **Redis Pub/Sub + WebSockets**.

```text
Kafka consumers  →  Redis (SET latest + PUBLISH update)
                         │
                         ▼
              Nest subscribes (Pub/Sub)  →  WebSocket  →  live clients
```

- [x] Rider GPS topic + producer (`gps-events-rider`, Avro `rider_id`)
- [x] Redis in Docker Compose (GEO for riders)
- [x] Consumer **GEOADD** riders from `gps-events-rider`
- [x] Nest WebSocket gateway — connect + join `user:{userId}` + Redis Pub/Sub `SUBSCRIBE` / `PUBLISH`
- [x] Fan-out driver updates via GEOSEARCH + Redis `PUBLISH user:{riderId}`
- [ ] Typed live messages: `driver.location`, `driver.eta`, optional `chat.message`
- [x] Simple client UI that renders the live feed (`client/`, served at `http://localhost:3001/`)

**Live feed demo**

```bash
# Redis + Kafka stack up, then:
npm run start:gateway
# open http://localhost:3001/ → Connect & join as rider-001

# Smoke without the full pipeline:
npm run emit:test -- rider-001 '{"driver_id":"driver-001","latitude":30.04,"longitude":31.23,"speed_kmh":42,"status":"available"}'

# Or full path: rider-producer + rider-geo + producer + nearby → UI updates
```

### Phase 8 — Cluster (future)

- [ ] 3-broker cluster, replication, `min.insync.replicas`
- [ ] Broker failure and leader election drills

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
| Idempotent Nest GPS producer | KafkaJS `idempotent: true` → PID + sequence numbers; retries don’t duplicate |
| Transactional Nest ETA | `transactional.id` + `send` + `sendOffsets` + `commit` |
| **ksqlDB for anomalies (Phase 4)** | SQL stream processing on Kafka; Nest stays TypeScript workers for ETA |
| Not Kafka Streams / Flink here | Heavier JVM apps; overkill for this learning repo — document as production alternatives |
| Topics created in Compose | Explicit layout; `AUTO_CREATE_TOPICS` is disabled |
| No Docker volumes (yet) | Ephemeral local data; wipe clean with `compose down` |
| Single broker first | Learn the full pipeline before cluster failure modes |
| Redis + Pub/Sub (Phase 7) | Latest state in keys; PUBLISH triggers live fan-out |
| WebSocket (Phase 7) | Push location / ETA / chat to clients in real time |

---

## Learning notes

Companion study sheet (questions and answers from building the pipeline):

[`docs/kafka-learning-qa.md`](docs/kafka-learning-qa.md)

Plain-language walkthrough of the WebSocket gateway and Redis Pub/Sub:

[`docs/redis-pubsub-gateway.md`](docs/redis-pubsub-gateway.md)

---

## License

Private / unlicensed (`UNLICENSED`). Not published for reuse.
