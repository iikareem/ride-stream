# RideStream — Kafka Learning Q&A

Questions from building this project, plus advanced ones for later review.
Answers match how **this** repo works (NestJS + KafkaJS + Docker Compose).

---

## A. Docker Compose & topics

### Q1. Why does `docker-compose.yml` look so heavy?

Kafka in **KRaft mode** (no ZooKeeper) needs many env vars just to boot. Most are boilerplate:

- **Listeners** — one address for containers (`29092`), one for your Mac (`localhost:9092`)
- **Replication = 1** — single broker, don’t copy to 3 nodes
- **Auto-create topics off** — topics are created on purpose

Day to day you only care about: `localhost:9092`.

**Services in this compose:**

| Service | Role |
| --- | --- |
| `broker` | The actual Kafka server |
| `schema-registry` | Phase 2 (Avro) — unused in Phase 1 |
| `kafka-ui` | Browser UI at `:8080` |
| `init-topics` | One-shot script that creates `gps-events` |

---

### Q2. What does `init-topics` do?

Kafka won’t create your topic automatically here. `init-topics`:

1. Waits until the broker is healthy  
2. Creates `gps-events` with **6 partitions** (if missing)  
3. Lists topics and **exits**

It is **not** another Kafka server — just a setup script in a container.

```bash
docker compose up -d
docker compose logs init-topics
```

---

### Q3. Aren’t topics and partitions created at the app level?

Two different ideas:

| Concept | Who decides? |
| --- | --- |
| Topic exists + **how many** partitions | Admin / infra (Compose, UI, or Admin API) |
| **Which** partition a message goes to | Producer — via message **key** |

The Nest app does **not** create the topic in Phase 1. It sends with `key: driver.id`; Kafka hashes that into one of the existing partitions.

---

### Q4. Can topics be created in UI or code instead of Compose?

Yes — all valid:

1. Kafka UI  
2. App Admin API on startup  
3. CLI / Compose (`init-topics`)

Compose is used here for **automatic, repeatable** local setup — not because it’s the only way.

---

### Q5. Are Docker volumes used?

**No.** Data lives inside the container. `docker compose down` wipes topics, messages, and consumer offsets. Fine for learning; add a volume later if you want persistence.

---

## B. Keys, partitions, and drivers

### Q6. We only have 6 partitions — what if there are thousands of drivers?

**6 partitions ≠ 6 drivers.**

Thousands of drivers hash into 6 buckets:

`hash(driver_id) % 6`

Many drivers share a partition. Partitions = **parallelism**, not “one per driver.”

---

### Q7. Isn’t the partition key unique per user/driver?

- **Key** (`driver_id`) — unique per driver  
- **Partition** — shared bucket (you only have 6)

Same key → always same partition → **order preserved per driver**.  
Different keys → can share a partition.

Like checkout lanes: many customers, few lanes; same customer always uses the same lane.

---

### Q8. Why was partition 4 empty while others had data?

With ~10 driver keys into 6 partitions, hash distribution can leave **some partitions unused**. Normal with a small key space. More drivers usually fills more partitions; hashes still aren’t perfectly even.

---

## C. Producers, Nest entrypoints, offsets (producer side)

### Q9. Does `nest start` run the producer and ingest data?

**No.** `npm start` / `nest start` boots the default HTTP app (`src/main.ts`).

Producer:

```bash
npm run start:producer
```

Consumer:

```bash
npm run start:consumer
```

Separate Nest processes on purpose.

---

### Q10. Do I send the offset with each message?

**No.** The producer sends topic + key + value. The **broker assigns** the offset when the message is written.

- Producer → write  
- Broker → assign offset  
- Consumer → track / commit offset (bookmark)

---

## D. Consumers & consumer groups

### Q11. Is the current consumer a single consumer with no group, reading all partitions?

It **uses a consumer group**: `ridestream-gps-printer`.

Right now that group has **one member**, so that member gets **all** partitions. Still a group — just size 1.

---

### Q12. If `onModuleInit` runs the Kafka consumer, does it monopolize the Nest app?

If you `await consumer.run()` in init, that process’s main job becomes Kafka.

- **This repo:** producer / consumer / API are **separate processes** → OK  
- **One Nest app doing API + heavy Kafka + other work:** the consumer can starve other work on the same event loop  

Prefer **isolated processes** for Kafka-heavy work. Worker threads can offload CPU, but Kafka consumers usually belong in their **own process**.

---

### Q13. So for mixed workloads, isolate with a process or worker?

Yes. Prefer:

1. **Separate process** (best for Kafka — what this project does)  
2. Worker threads — mainly for CPU work inside one process  

Right now each command can run alone.

---

### Q14. How do we maintain different consumer groups for topics?

**Rule:** each logical service → its own **`groupId`**. Topics in shared config.

| Service | Topic(s) | groupId |
| --- | --- | --- |
| GPS printer | `gps-events` | `ridestream-gps-printer` |
| ETA | `gps-events` → `eta-updates` | `ridestream-eta` |
| Live map | `eta-updates` | `ridestream-live-map` |
| Anomaly | `gps-events` → `driver-anomalies` | `ridestream-anomaly` |

Same topic + **different** groups = each service gets a full independent stream (own offsets).  
Same topic + **same** group = replicas sharing partitions for scale.

---

### Q15. Different consumers = different commands, each knowing its topic?

Yes on commands and “knows its topic(s).”

Isolation is by **`groupId`**, not always by different topics. Printer and ETA both read `gps-events` with different groups; live-map reads the derived `eta-updates` stream.

---

### Q16. Is a consumer group subscribed to only one topic? Can groups map to different topics?

A group is **not** locked to one topic. Kafka allows:

- one group → one or many topics  
- many groups → same topic  
- different groups → different topics  

In *this* project we keep it simple: one service → one group → usually one main topic.

---

### Q17. How do I get different groups, and how do members of the same group share partitions?

**Different groups (independent readers):** different Nest entry + different `groupId`.

**Same group (share load):** run the **same** command twice with the **same** `groupId` → rebalance → split partitions.

Max useful members ≈ number of partitions (6 here).

---

### Q18. If I run `start:consumer` twice, does Kafka rebalance and split partitions roughly in half?

**Yes**, if both use the same `groupId`. Often ~3 and ~3 (not always exact). Each message goes to **only one** member of that group.

Different group ids → no split; both get all partitions.

---

### Q19. If I stop a consumer and start it again, from beginning or continue?

**Usually continue** from the last **committed** offset for that `groupId`.

`fromBeginning: true` only applies when that group has **no** committed offset yet (first run, or new group id, or Kafka wiped with no volumes).

---

### Q20. How does Kafka know it’s the “same” consumer after restart?

It does **not** track your PID or laptop. Identity = the **`groupId` string**.

Same `groupId` → same offset bookmarks.  
New `groupId` → brand-new reader.

---

## E. Commits & acknowledgements

### Q21. There’s no offset/ack code — who handles that?

**KafkaJS auto-commit** (default).

Flow:

1. Join group, get partitions  
2. Fetch messages  
3. Run `eachMessage`  
4. On success, mark offset ready  
5. Periodically commit to `__consumer_offsets`  
6. Restart → resume from bookmark  

Manual commit is for when “done” must mean “my DB/API side effect finished.”

---

### Q22. Batch or one-by-one? If processing fails, how does it not commit?

- **Wire:** fetch arrives as a **batch**  
- **`eachMessage`:** you see **one** message at a time  
- **On success:** that offset marked ready  
- **On throw:** that message not marked done → retry; shouldn’t skip past it  

**At-least-once caveat:** work succeeds → crash **before** commit → message can be delivered again (duplicates). Use idempotent writes or stronger EOS later if needed.

---

### Q23. Is auto-commit what most projects use? Is manual rare?

Auto-commit is the **library default** and fine for simple / idempotent handlers.

Manual commit is **common in serious pipelines** with important side effects.  
Start auto; go manual when commit must follow successful business work.

---

### Q24. So: batch in, one visible at a time, commit one-by-one?

Almost:

- Visible one-by-one  
- Tracked ready one-by-one after success  
- **Flushed** to Kafka on interval/threshold (not necessarily a network round-trip after every single message)

---

### Q24b. How can I see rebalancing and which partitions each consumer got?

Three ways:

**1. Terminal logs (added to the GPS printer)** — start two consumers:

```bash
# terminal A
npm run start:consumer

# terminal B
npm run start:consumer
```

Look for:
- `rebalancing…`
- `joined — member=… assignment: gps-events=[0, 2, 4]` (example)

Stop one (Ctrl+C) → the other rebalances and takes more partitions.

**2. Kafka UI** — http://localhost:8080 → Consumer Groups → `ridestream-gps-printer` → members + partitions.

**3. CLI:**

```bash
docker exec ridestream-broker kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group ridestream-gps-printer
```

Shows each member and which partitions it owns, plus lag.

---

## F. Advanced questions (for later in the project)

Use these as you hit Phases 2–6. Fill answers as you learn.

### Q25. What is the difference between at-least-once, at-most-once, and exactly-once?

*(Fill in during EOS / Phase 4–6.)*

---

### Q26. Why partition by `driver_id` instead of round-robin?

*(Ordering per driver; round-robin can reorder the same driver’s GPS points.)*

**Short answer:** keying keeps one driver’s events on one partition so order is preserved.

---

### Q27. What happens on a consumer group rebalance? Can messages be processed twice?

*(Revoke partitions → maybe commit → assign new partitions. Yes, rebalance + at-least-once can cause duplicates.)*

---

### Q28. `auto.offset.reset` / `fromBeginning` vs committed offsets — when does each apply?

*(Only when no commit exists for that group-topic-partition.)*

---

### Q29. Why use Schema Registry + Avro instead of JSON forever?

Binary Avro payloads are smaller than JSON. The Registry stores schemas by id; each message carries the id in a Confluent wire header. Producers and consumers agree on shape without embedding the full schema every time. Compatibility modes (BACKWARD here) block unsafe changes.

In RideStream: subject `gps-events-value`, schemas in `src/kafka/schemas/gps-event.avsc.ts`, client `@kafkajs/confluent-schema-registry`.

---

### Q30. What is consumer lag and why alert on it?

*(How far behind the consumer is vs the log end. High lag = slow consumer / incident.)*

---

### Q31. Hopping vs tumbling windows in stream processing — why hopping for speed anomalies?

*(Hopping overlaps → less chance to miss a spike at a window boundary.)*

---

### Q32. Idempotent producer vs transactional producer vs Streams EOS — what’s the difference?

*(Idempotent: no dup writes from one producer. Transactions / EOS: atomic consume-process-produce across partitions.)*

---

### Q33. If partition count is 6, can I run 10 consumers in the same group usefully?

*(No — max active members = partition count; extras idle until a member leaves.)*

---

### Q34. Same group, two different topics — how are partitions assigned?

*(Group members share all subscribed topic-partitions together as one assignment set.)*

---

### Q35. How would you make `eachMessage` safe under duplicates (idempotent consumer)?

*(Dedupe by event id / store last processed offset per key / upsert by driver_id + timestamp.)*

---

## G. Phase 2 — Avro + Schema Registry

### Q36. What does the Confluent Avro wire format look like?

Magic byte `0` + 4-byte big-endian schema id + Avro binary payload. KafkaJS Schema Registry `encode` / `decode` handle that for you.

---

### Q37. What is BACKWARD compatibility?

A **new** schema can read data written with the **previous** schema. Adding an optional field with a default (like `heading: null`) is BACKWARD-safe. Removing a required field or adding a required field without a default is not.

Compose sets `SCHEMA_REGISTRY_SCHEMA_COMPATIBILITY_LEVEL: BACKWARD`.

---

### Q38. How did RideStream evolve GPSEvent?

1. Register **v1** (baseline fields) under `gps-events-value`  
2. Register **v2** (adds optional `heading`) — Registry accepts it under BACKWARD  
3. Producer encodes with **v2** id; consumer `decode` uses the id in each message  

Verify:

```bash
curl -s http://localhost:8081/subjects/gps-events-value/versions
curl -s http://localhost:8081/subjects/gps-events-value/versions/latest | jq .
```

---

### Q39. Why wipe Kafka when switching from JSON to Avro?

Avro binary is not JSON. Old Phase 1 messages on `gps-events` will fail decode. No volumes → `docker compose down && docker compose up -d` recreates an empty topic.

---

### Q40. Who registers the schema — producer or Registry UI?

Either works. RideStream registers on app startup via `SchemaRegistryService.ensureSchemasRegistered()`. You can also paste schemas in Kafka UI. The subject name should stay `gps-events-value` (TopicNameStrategy for values).

---

### Q41. Why register both v1 and v2? What does that simulate?

Schema Registry keeps a **version history** per subject. Registering v1 then v2 simulates real time:

| Time | Production story |
| --- | --- |
| Day 1 | Ship GPS events with v1 (no `heading`) |
| Later | Product wants compass heading → register v2 (optional `heading`) |
| Transition | Old and new clients may both exist for a while |

Each Kafka message still uses **one** schema id. We produce with **v2**. Missing `heading` on old (v1) data becomes **`null`** via the field default — not “the whole event is undefined.”

---

### Q42. What other compatibility types are worth knowing?

| Mode | Meaning | Typical upgrade order |
| --- | --- | --- |
| **BACKWARD** (RideStream) | New schema can read **old** data | Consumers first, then producers |
| **FORWARD** | Old schema can read **new** data | Producers first; old consumers tolerate new fields |
| **FULL** | Both BACKWARD + FORWARD | Strictest; hardest to evolve |
| **NONE** | No checks | Avoid unless intentional |

Also: `*_TRANSITIVE` modes require compatibility against **all** older versions, not only the previous one.

---

### Q43. If we only encode with v2, why register v1 at all?

**You don’t need v1 for the app to run today** — only v2 is required on the wire.

v1 is registered first as a **learning/demo of evolution**: Registry shows versions `[1, 2]` and proves BACKWARD accepted the change. If we registered only v2, producer/consumer would work the same, but the UI would show a single version with no visible history.

---

### Q44. Where does Schema Registry store schemas? Is it a database?

Yes in spirit: a **catalog of schemas**. In Confluent’s stack it is usually **not** Postgres/MySQL. Schemas live in a Kafka topic (typically `_schemas`). Schema Registry is an HTTP API in front of that topic.

In RideStream Compose there are **no volumes**, so Registry state dies with `docker compose down`. Next `up` is empty; the app re-registers v1/v2 on startup.

---

### Q45. Is Schema Registry format-agnostic (Protobuf, anything)?

**Partly.** Confluent Schema Registry supports:

- Avro  
- Protobuf  
- JSON Schema  

Not arbitrary custom binary. Among those three it is format-flexible; it is still aimed at **Kafka event** schemas (Confluent wire format with schema id).

---

### Q46. Is Schema Registry only for Kafka producers/consumers, not gRPC?

**For practical use: yes.** It belongs in the Kafka ecosystem (producers, consumers, Streams, Connect).

gRPC service-to-service uses `.proto` files and stubs (or Buf), **not** Confluent Schema Registry. You *could* call the Registry HTTP API from any app, but that is not the normal design for request/response APIs.

---

### Q47. How do services share schemas for binary S2S communication (not Kafka)?

Same idea (one source of truth for shape), different tools:

| Approach | Typical for |
| --- | --- |
| Shared `.proto` / schema Git repo + versioned packages | gRPC, many companies |
| Buf Schema Registry (or similar API registries) | Protobuf at scale, breaking-change checks |
| Generated client SDKs from schemas | Polyglot microservices |
| OpenAPI + codegen | REST/JSON |

Rule of thumb:

- **Kafka events** → Confluent Schema Registry  
- **Service calls (gRPC/REST)** → protos/OpenAPI in repo or Buf-style registry  

---

## Phase 2 review letter

You finished Phase 2 with a working mental model, not only working code.

You can explain that Schema Registry is a shared catalog so Kafka producers and consumers agree on Avro shape by schema id, instead of shipping JSON forever. You know RideStream registers subject `gps-events-value`, stores schemas via the Registry (backed by Kafka’s `_schemas` topic in Confluent), and that your Docker setup is ephemeral without volumes.

You understand schema evolution: v2 adds optional `heading` with default `null` under BACKWARD compatibility so a new reader can still decode old data. You also know registering v1 then v2 is a demo of history — the producer only encodes with v2 — and you can name FORWARD, FULL, and TRANSITIVE as related modes.

You drew a clear boundary for interviews and design: Confluent Schema Registry is for Kafka event contracts (Avro/Protobuf/JSON Schema). Service-to-service gRPC shares schemas through `.proto` repos or Buf, not through this Registry. That distinction is the main conceptual win of Phase 2.

Next learning target when you are ready: Phase 3 — separate consumer groups (ETA, live map) on the same Avro `gps-events` stream.

---

## H. Phase 3a — ETA Calculator

### Q48. Why a separate consumer group for ETA instead of extending the printer?

Different **jobs** need different **offsets**. The printer and ETA calculator both read `gps-events` but must not share a group id. Same group would split partitions between them and each would miss half the drivers. Separate groups (`ridestream-gps-printer` vs `ridestream-eta`) each get the full stream.

---

### Q49. Why publish ETA to `eta-updates` instead of only logging?

So ETA becomes its own **event stream**. Other services (live UI, notifications, Phase 8 WebSocket) can subscribe later without changing the ETA worker. No database required for learning — Kafka carries the derived result.

---

### Q50. How does RideStream get a destination without a trip DB?

Learning shortcut: the ETA worker assigns a **stable fake destination per `driver_id`** (hash → Cairo point, kept in an in-memory `Map`). Formula: haversine distance / `max(speed_kmh, 5)` → `eta_seconds`.

In production, destination would come from a trip message/body or a Redis/DB lookup when the trip is assigned.

---

## I. Phase 3b — Live Map Updater

### Q51. Why consume `eta-updates` instead of `gps-events`?

`eta-updates` already carries **position + ETA** (lat/lon, speed, destination, `eta_seconds`). The live map is a read model for “where is the driver and when do they arrive,” so one topic covers both. Live-map does not need to recompute ETA or duplicate GPS consumption.

Kafka still keeps the raw GPS history on `gps-events`; live-map only needs the latest enriched state.

---

### Q52. Why keep it in memory instead of publishing another topic?

Live map is a **read model** (current state), not a new event stream. The updater collapses `eta-updates` into “latest per `driver_id`.” Later Phase 8 moves this `Map` into Redis for multi-process / WebSocket sharing. No HTTP endpoint in this phase — logs prove the upsert works.

---

### Q53. Does live-map need ETA running?

Yes for fresh data. Live-map only sees what ETA publishes to `eta-updates`. Start producer → ETA → live-map.

---

## J. Phase 3c — Latency and rebalance

### Q54. What does `latency_ms` in the logs mean?

`latency_ms = Date.now() - event.timestamp`. The producer stamps each GPS/ETA payload with wall-clock time; consumers subtract that when they finish handling the message. It answers: “how old is this event when I processed it?”

It is **not** the same as Kafka consumer lag (how many offsets behind the log end). Offset lag is what Kafka UI and `kafka-consumer-groups --describe` show.

---

### Q55. Why can `latency_ms` be huge at startup?

With `CONSUME_FROM_BEGINNING=true`, the group replays old messages. Their timestamps are minutes/hours ago → huge `latency_ms` until the consumer catches up. For a live drill, set `CONSUME_FROM_BEGINNING=false` so you only read new messages.

---

### Q56. What does `PROCESSING_DELAY_MS` teach?

It fakes slow business logic (DB call, heavy CPU). If each message takes 2s and the producer is faster, **offset lag grows**. Clear the delay and lag drains. That is the same pattern you alert on in production.

---

### Q57. What happens in a rebalance?

Same `groupId`, more/fewer members → coordinator revokes and reassigns partitions. Logs show `REBALANCING` then `GROUP_JOIN` with the new assignment. Processing pauses briefly; with auto-commit / at-least-once you may process the same offset twice around the boundary.

`SESSION_TIMEOUT_MS` / `HEARTBEAT_INTERVAL_MS` control how quickly a crashed member is noticed (too low = flaky rebalances; too high = slow failover).

---

## Quick command cheat sheet

```bash
# Infra
docker compose up -d
docker compose logs init-topics
docker compose down          # wipes data (no volumes)

# After JSON → Avro cutover, or after adding eta-updates:
docker compose down && docker compose up -d
# or only recreate topic init:
docker compose up -d --force-recreate init-topics

# Apps
npm run start:producer
npm run start:eta            # gps-events → eta-updates
npm run start:live-map       # eta-updates → in-memory Map
npm run start:consumer       # optional GPS printer

# Latency / lag drills
CONSUME_FROM_BEGINNING=false npm run start:eta
PROCESSING_DELAY_MS=2000 CONSUME_FROM_BEGINNING=false npm run start:eta

# Rebalance drill — two terminals, same group
npm run start:eta
npm run start:eta

# Lag describe
docker exec ridestream-broker kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group ridestream-eta

# Schema Registry
curl -s http://localhost:8081/subjects
curl -s http://localhost:8081/subjects/gps-events-value/versions
curl -s http://localhost:8081/subjects/eta-updates-value/versions

# UI
open http://localhost:8080
```

---

*Last updated after Phase 3c latency / rebalance (Q54–Q57). Add new Q&As as you go.*
