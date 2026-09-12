> Part of [RideStream](../RideStream.md).

# Kafka From First Principles

### A mental model of the RideStream pipeline

> Each section explains the _problem_ Kafka is solving, then how this system solves it.

---

## Table of Contents

1. [Why Kafka Exists — The Core Problem](#1-why-kafka-exists--the-core-problem)
2. [Topics and Partitions](#2-topics-and-partitions)
3. [Producers](#3-producers)
4. [Consumers and Consumer Groups](#4-consumers-and-consumer-groups)
5. [Offsets](#5-offsets)
6. [Brokers](#6-brokers)
7. [Schema Management and Avro](#7-schema-management-and-avro)
8. [Consumer Tuning](#8-consumer-tuning)
9. [Rebalancing](#9-rebalancing)
10. [ksqlDB](#10-ksqldb)
11. [Windowing](#11-windowing)
12. [State Stores](#12-state-stores)
13. [Exactly-Once Semantics (EOS)](#13-exactly-once-semantics-eos)
14. [Observability — Consumer Lag](#14-observability--consumer-lag)
15. [Fault Tolerance](#15-fault-tolerance)
16. [Mental Model Summary](#16-mental-model-summary)

---

## 1. Why Kafka Exists — The Core Problem

### The problem without Kafka

Imagine 500 drivers each emitting a GPS ping every 3 seconds. Three separate systems need that data: the ETA calculator, the nearby-rider worker, and the anomaly detector.

The naive approach: the producer calls each system directly (HTTP, gRPC, etc.).

```
Driver Producer ──HTTP──▶ ETA Calculator
                ──HTTP──▶ Nearby Worker
                ──HTTP──▶ Anomaly Detector (ksqlDB)
```

This creates **tight coupling**. If the anomaly detector is slow or crashes, the producer is blocked or has to implement retries. If you add a fourth consumer next month, you change the producer. The producer now has to care about the health of every downstream system.

### What Kafka does instead

Kafka is a **distributed commit log**. Producers write events to Kafka. Consumers read from Kafka independently. The producer has exactly one job: get the event into Kafka reliably.

```
Driver Producer ──▶ Kafka ──▶ ETA Calculator          (reads at its own pace)
                         ──▶ Nearby Worker            (reads at its own pace)
                         ──▶ Anomaly Detector (ksqlDB) (reads at its own pace)
```

Kafka decouples the write side from the read side. The producer doesn't know or care who consumes the data, or when.

### The secondary benefit: replay

Because Kafka stores events durably (not just in memory), a consumer can re-read old events. If the anomaly detector crashes and restarts, it picks up exactly where it left off. If you deploy a new service next month, it can read from the beginning of the topic and backfill its state. This is not possible with a direct queue that deletes messages on delivery.

---

## 2. Topics and Partitions

### The problem

You have 500 drivers each emitting events at ~20 events/minute. That's 10,000 events/minute. A single sequential log is a bottleneck — one writer, one reader. You need parallelism.

But you also need ordering: GPS events from **driver A** must be processed in the order they were emitted, or you'll calculate wrong ETAs and miss anomalies. You can't just shuffle all events randomly across parallel workers.

### Topics

A **topic** is a named stream of events. Think of it as a category. RideStream has:

- `gps-events-driver` — raw GPS pings from drivers, keyed by `driver_id`
- `gps-events-rider` — raw GPS pings from riders, keyed by `rider_id`
- `eta-updates` — ETA calculations published downstream
- `driver-anomalies` and the other anomaly topics — speed, windows, teleport, freeze

### Partitions

A topic is split into **partitions** — ordered, immutable sub-logs. Each partition is an independent sequence. Events in a single partition are strictly ordered. Events across partitions have no ordering guarantee.

```
Topic: gps-events-driver (6 partitions)

Partition 0: [event1][event7]...    ← driver_id hashes to 0
Partition 1: [event2][event8]...    ← driver_id hashes to 1
Partition 2: [event3][event9]...    ← driver_id hashes to 2
Partition 3: [event4][event10]...   ← driver_id hashes to 3
Partition 4: [event5][event11]...   ← driver_id hashes to 4
Partition 5: [event6][event12]...   ← driver_id hashes to 5
```

### Partition key — why `driver_id`

When a producer sends an event, it can supply a **key**. Kafka hashes the key to determine which partition the event lands in. Using `driver_id` as the key means: all events from driver A always go to the same partition, in order. The ETA calculator reading that partition sees driver A's GPS history in sequence — which is the only way to compute a correct ETA or detect a "GPS freeze."

Without a key, Kafka round-robins events across partitions. Driver A's events could land on partitions 0, 2, and 3 in random order. Ordering is gone.

### How many partitions?

Partitions are also the unit of **parallelism for consumers** — one consumer per partition maximum. RideStream uses 6 partitions on every application topic, so one consumer group can have at most 6 members doing real work. A seventh sits idle. More partitions would mean more parallelism, and more overhead. Six is the number this cluster was built with, not a guess to tune later.

---

## 3. Producers

### The problem

Your GPS producer is writing 10,000 events/minute. Two failure modes matter:

1. **Duplicate events** — the producer sends an event, doesn't get an acknowledgment (network blip), retries, and now Kafka has it twice. The anomaly detector sees a driver "teleport" because the same GPS coordinate was processed twice in rapid succession.
    
2. **Data loss** — the producer sends an event and Kafka acknowledges it before writing to disk. Broker restarts. Event is gone.
    

### Acknowledgment levels (`acks`)

The `acks` config controls when Kafka tells the producer "I got it."

|`acks` value|Meaning|Risk|
|---|---|---|
|`0`|Don't wait for any acknowledgment|Highest throughput, data loss possible|
|`1`|Wait for the leader broker to write it|Data loss if the leader crashes before replication|
|`all`|Wait for all in-sync replicas to write it|Safest|

RideStream uses `acks=all` on a 3-broker cluster: replication factor 3, `min.insync.replicas=2`. A write succeeds only when at least two in-sync replicas have it. One broker can die and writes still succeed. See [kafka cluster](kafka%20cluster.md).

### Idempotent producers

Setting `enable.idempotence=true` tells Kafka to deduplicate retries from the same producer. Each message gets a **sequence number** per partition. If Kafka sees the same sequence number twice from the same producer, it discards the duplicate. This is how RideStream prevents false "teleport" anomalies from network retries.

Idempotence is scoped to a single producer session. If the producer restarts, it gets a new producer ID and deduplication resets.

### Transactional producers

For atomic writes across multiple partitions or topics, producers use **transactions** with a `transactional.id`. This matters for EOS (covered in section 13). A transactional producer wraps a batch of writes in a begin/commit boundary. Consumers configured to read only committed data skip any in-flight or aborted transactions.

---

## 4. Consumers and Consumer Groups

### The problem

You have three systems that need driver GPS events: ETA calculator, nearby worker, anomaly detector. They are independent — they have different processing speeds, different failure modes, and different logic. If they shared one consumer, the slowest one would bottleneck all of them. A fourth group, rider GEO, reads `gps-events-rider` instead. It does not compete for the driver stream.

### Consumer groups

A **consumer group** is a set of consumers that cooperate to consume a topic, where each partition is assigned to exactly one consumer in the group at a time.

```
gps-events-driver (6 partitions)

ridestream-eta        one member today → P0 … P5 → eta-updates
ridestream-nearby     one member today → P0 … P5 → Redis Pub/Sub
ksqlDB group(s)       → P0 … P5 → anomaly topics
```

Each group tracks its own position in each partition independently. Group A being slow doesn't affect Group B — they read from the same log but maintain separate cursors. This is fundamentally different from a traditional queue where a message is deleted once any consumer reads it.

### Scaling consumers

You can scale a consumer group by adding more consumer instances. Kafka redistributes partitions across them. But adding more consumers than partitions means the extras sit idle — a partition can only have one active consumer per group at a time.

---

## 5. Offsets

### The problem

A consumer reads event #472, processes it, then crashes. When it restarts, how does it know where to continue? How does it avoid re-processing everything from the beginning? How does it avoid skipping event #473?

### What offsets are

An **offset** is an integer that identifies a message's position within a partition. Messages within a partition are numbered sequentially: 0, 1, 2, 3…

Kafka stores, per consumer group per partition, the **committed offset** — the position up to which the consumer has declared "I'm done with this."

```
Partition 0:  [0][1][2][3][4][5][6]...
                              ↑
                    committed offset = 5
                    (consumer has processed up to and including #5)
                    (next fetch starts at #6)
```

### Auto-commit vs manual commit

|Mode|Behavior|Risk|
|---|---|---|
|Auto-commit|Kafka commits the offset periodically (every 5 seconds by default)|Consumer can crash between auto-commits and reprocess the last 5 seconds of events|
|Manual commit|Consumer commits explicitly after processing|You control exactly when "done" is recorded; slightly more code|

RideStream's nearby and rider-GEO workers can live with auto-commit — a duplicate GEOADD overwrites the same rider, and a duplicate nearby publish is a repeated live hint. The ETA worker cannot. It turns auto-commit off and commits the GPS offset inside the same transaction that writes `eta-updates`. ksqlDB's anomaly queries commit internally under `exactly_once_v2`.

### `auto.offset.reset`

When a consumer group reads a topic for the first time (no committed offset exists), this config controls where to start:

- `earliest` — start from the oldest retained message
- `latest` — start from new messages only (ignore history)

---

## 6. Brokers

### The problem

Where do events actually live? Who coordinates which consumer gets which partition?

### What a broker is

A **broker** is a Kafka server process. It stores partitions on disk, handles producer writes, and serves consumer reads. RideStream runs three combined broker/controller nodes in KRaft mode (no ZooKeeper): host ports `9092`, `9093`, and `9094`. Each application topic has six partitions and three replicas, with `min.insync.replicas=2`.

### The log on disk

Each partition is physically stored as a **segmented append-only log** on the broker's filesystem. Kafka doesn't use a database internally. It writes sequentially to disk (fast) and uses the OS page cache aggressively. This is why Kafka can sustain high throughput on commodity hardware.

### Retention

Kafka keeps events for a configurable retention period (default: 7 days) or until the partition reaches a size limit. After that, old segments are deleted. Consumers that fall too far behind (beyond the retention window) can no longer read those events — they'd have to reset to `earliest` (which now means the oldest _available_ event, not the original beginning).

### The controller

In KRaft, controller quorum is the brokers themselves. If the active controller dies, another broker is elected. One broker down: the remaining two keep quorum, leaders are re-elected where needed, and writes continue because two in-sync replicas still meet `min.insync.replicas=2`. Two brokers down: quorum and the minimum ISR are both gone, and writes stop. Unclean leader election is off — Kafka will go unavailable rather than promote a replica that missed acknowledged records. The broker-failure drills are in [kafka cluster](kafka%20cluster.md).

---

## 7. Schema Management and Avro

### The problem

RideStream has three consumer groups. Next month you need to add a `heading` field to GPS events (the direction the driver is facing). You deploy the change to the producer. Now:

- The ETA calculator (not yet updated) receives an event it doesn't know how to deserialize — it crashes.
- Or worse: it silently ignores the field and produces wrong ETAs.

Without schema management, every schema change is a coordination nightmare: all consumers must be updated simultaneously.

### What Schema Registry does

Confluent Schema Registry is a separate service that stores versioned schemas. Producers register a schema before (or when) they first use it. The schema gets a numeric ID.

When the producer serializes an event with Avro, it prepends the schema ID (4 bytes) to the binary payload. When a consumer deserializes the event, it reads that schema ID, fetches the schema from the Registry (cached after first fetch), and deserializes against it.

```
Producer                          Consumer
  │                                  │
  ├─ register schema → Registry       │
  │                      ↓           │
  ├─ serialize: [schema_id][avro_bytes] → Kafka → Consumer reads schema_id
  │                                  ↓
  │                       Registry.getSchema(id) → deserialize
```

### Avro vs JSON

||JSON|Avro|
|---|---|---|
|Format|Text (human-readable)|Binary (compact)|
|Schema enforcement|None|Required|
|Payload size|Large|~3–5× smaller|
|Schema evolution|Manual, error-prone|Built-in compatibility rules|

For GPS events at 10,000/minute, the size difference compounds. Avro also catches mismatches at serialization time, not silently at runtime.

### Compatibility modes

When you register a new schema version, the Registry enforces a compatibility rule:

|Mode|Rule|
|---|---|
|`BACKWARD`|New schema can read data written by the old schema (default)|
|`FORWARD`|Old schema can read data written by the new schema|
|`FULL`|Both directions|

Adding an **optional field with a default value** is backward-compatible: old consumers skip the unknown field; new consumers see the default for old events. Removing a required field or changing a type is not backward-compatible — the Registry rejects it.

This is how RideStream demonstrates schema evolution: `heading` is added as an optional float with a default of `null` (`["null", "float"]`). `0.0` would look like "facing north." `null` means unknown. A new reader can still decode old events; the missing field becomes `null`. Details are in [Avro and Schema Registry](../02%20Avro/Avro%20and%20Schema%20Registry.md).

---

## 8. Consumer Tuning

### The problem

The ETA calculator needs low latency — riders want ETAs updated in near-real-time. But Kafka consumers batch their fetches by default, waiting for either a minimum amount of data or a timeout before returning. Default settings are tuned for throughput, not latency.

### Key configs

**`fetch.min.bytes`** — the minimum bytes Kafka waits to accumulate before returning a fetch response to the consumer.

- Default: `1` byte (return as soon as anything is available)
- Higher values → more throughput, more latency
- For ETA calculator: keep at `1` to minimize wait time

**`fetch.max.wait.ms`** — if `fetch.min.bytes` isn't satisfied, Kafka waits at most this long before responding anyway.

- Default: `500ms`
- For low-latency consumer: reduce to `50–100ms`

**`max.poll.records`** — maximum number of records returned per `poll()` call.

- Default: `500`
- If processing one batch takes longer than `max.poll.interval.ms`, Kafka thinks the consumer is dead and triggers a rebalance
- Tune this down if per-event processing is heavy

**`max.poll.interval.ms`** — the maximum time between `poll()` calls before the consumer is considered failed.

- Default: `5 minutes`
- If your processing logic is slow, increase this; don't let it expire silently

### The latency vs throughput trade-off

Reducing `fetch.min.bytes` and `fetch.max.wait.ms` makes the consumer more responsive but increases the number of fetch requests to the broker. This is fine at RideStream's scale. At massive scale (millions of events/sec), you'd batch more aggressively.

---

## 9. Rebalancing

### The problem

Kafka must assign each partition to exactly one consumer in a group at any moment. When the group membership changes — a consumer joins, crashes, or is added — partitions must be redistributed. During this redistribution, consumption pauses. If not handled carefully, in-flight events can be processed twice.

### When rebalances happen

- A new consumer instance starts and joins the group
- A consumer instance crashes or stops
- A consumer fails to poll within `max.poll.interval.ms` (Kafka assumes it's dead)
- You add partitions to a topic

### What happens during a rebalance

1. The **group coordinator** (a broker) detects the membership change
2. All consumers in the group are told to stop consuming and revoke their partitions
3. The coordinator re-assigns partitions across the current live members
4. Consumers resume from their committed offsets on their new partition assignments

This pause is the **stop-the-world rebalance** problem. For RideStream's nearby worker, a rebalance means a brief gap in live driver pushes. Acceptable here. For a financial system, even a 2-second pause matters.

### `ConsumerRebalanceListener`

You can hook into the rebalance lifecycle:

- `onPartitionsRevoked(partitions)` — called before partitions are taken away; the right place to **commit offsets** so you don't re-process events
- `onPartitionsAssigned(partitions)` — called after new partitions are assigned; the right place to **initialize local state** for the new partitions

RideStream does not keep a driver-position map inside the nearby worker. Rider positions live in Redis, and last-known driver position for teleport detection lives in the ksqlDB table `latest_gps`. The listener still matters if a consumer holds local state: commit before the partition is taken away, or the next owner replays from a stale bookmark.

---

## 10. ksqlDB

### The problem

The anomaly detector needs to:

1. Remember each driver's last known position
2. Count speed spikes inside a time window, not treat one fast ping as proof
3. Notice when a driver barely moves across several events

A plain Nest consumer can filter `speed_kmh > 50` and log it. It cannot, without extra work, keep per-driver state across restarts, window that state, and avoid double-counting if it crashes mid-window.

RideStream does not run a Java Kafka Streams app for this. It runs ksqlDB. A persistent ksqlDB query is a Kafka Streams job you write in SQL: it consumes a topic, keeps changelog-backed state, and produces to another topic. The queries themselves are in [ksqlDB Streams, Windows, and Anomalies](../03%20ksqlDB/ksqlDB%20Streams%2C%20Windows%2C%20and%20Anomalies.md).

```
gps-events-driver
      │
      ▼
  ksqlDB
      │
      ├─ filter: speed > 50 → driver-anomalies
      ├─ tumbling / hopping counts → window topics
      ├─ join to latest_gps table → driver-anomalies-teleport
      └─ 2-minute low-movement window → driver-anomalies-freeze
```

Kafka Streams the library (DSL, Processor API, RocksDB) is what ksqlDB runs underneath. You don't write that topology in this repo. Route deviation and map-matching still don't fit this SQL. Those want trip state a service can hold.

---

## 11. Windowing

### The problem

A single GPS event can't tell you a driver is genuinely speeding. One fast ping could be noise. You want to ask: "Did this driver spike more than once inside a minute?" That's a **time-window aggregation**.

### Tumbling windows

Fixed-size, non-overlapping time buckets.

```
|── 0:00–1:00 ──|── 1:00–2:00 ──|── 2:00–3:00 ──|
```

A speed spike at 0:59 and 1:01 falls into two different windows. Neither window alone might breach the threshold, even though the driver was speeding across both. Edge events are missed.

### Hopping windows

Fixed-size windows that **advance by a smaller step** (they overlap).

```
Window 1: 0:00–1:00
Window 2: 0:15–1:15
Window 3: 0:30–1:30
```

RideStream's hopping count is a 1-minute window that advances every 15 seconds. A spike near a bucket edge sits in more than one window, so it is harder to miss than with tumbling alone. Cost: more windows to compute. The tumbling count (non-overlapping 1-minute buckets) is a separate query, so both shapes are visible. Topic for hopping: `driver-anomaly-windows-hop`. The threshold is `SPEED_KMH > 50` and at least two spikes in the window — tuned so the local simulator can actually fire, not a production speed limit.

### Session windows

Windows that close after a **gap of inactivity**. If no event arrives from a driver for 30 seconds, the session closes. The next event starts a new session.

RideStream does not use a session window for freeze. Freeze is a 2-minute tumbling window: at least four events, latitude and longitude spans under `0.00015`, average speed under 8 km/h. The window closes because time advanced and enough points landed inside it, not because a timer fired on silence.

### Time semantics: event time vs processing time

**Event time** — the timestamp embedded in the GPS event itself (`timestamp` field in the schema). Reflects when the driver actually was at that position.

**Processing time** — the time the event arrives at the processor (ksqlDB, here).

These diverge in practice: a mobile app might buffer GPS events offline and upload them in bulk 2 minutes later. Event time is usually what you want for windowing — you're asking "what did this driver do in the last 60 seconds of _driving_", not "what events did we receive in the last 60 seconds."

ksqlDB, like Kafka Streams under it, can hold a window open for late events via a grace period. RideStream's queries use the default; the simulator does not delay events on purpose.

---

## 12. State Stores

### The problem

To detect a teleport, you need the driver's previous point. A stateless filter can't do that. You need memory, per driver, that survives a restart.

### What state stores are

In this repo that memory is the ksqlDB table `latest_gps`: last lat/lon per `DRIVER_ID`. Each new GPS event joins that table. If `GEO_DISTANCE` is over 0.25 km and the event is newer than the stored point, the teleport stream emits. Under the hood that table is a Kafka Streams state store (RocksDB by default), but the SQL is the interface. You don't open a store from Nest.

```
latest_gps  =  last lat/lon per DRIVER_ID
new GPS event  INNER JOIN  latest_gps
  if jump > 0.25 km and event is newer → driver-anomalies-teleport
```

### Changelog topics

Every write to that table is also **mirrored to a Kafka changelog topic**. If the ksqlDB query crashes and restarts, or its partition is reassigned, it replays the changelog and rebuilds `latest_gps`. You don't lose last-known position on crash.

This is the key advantage over a plain consumer with an in-memory HashMap: the state survives failures automatically.

### State store and partition alignment

Each state-store partition sits with the input partition it processes. The task that reads `gps-events-driver` partition 1 also owns `latest_gps` for the drivers hashed to partition 1. Driver A's events and driver A's last position stay together. No cross-partition lookup.

---

## 13. Exactly-Once Semantics (EOS)

### The problem

The anomaly detector reads a GPS event and writes an anomaly alert to `driver-anomalies`. These are two separate operations. Failures can happen between them:

1. Read event → process → **crash** → restart → re-read event → write alert **twice** → driver incorrectly suspended twice
2. Read event → process → write alert → **crash before committing offset** → restart → re-read event → write alert **twice** → same problem

**At-least-once** semantics (Kafka's default) means: you'll never lose a message, but you may process it more than once. That's mostly fine for rider `GEOADD` — the same rider id overwrites the point. It's **not fine** for a duplicate anomaly, or for an ETA written twice because the GPS offset was committed separately from the result.

### How EOS works here

ksqlDB's persistent queries run with `processing.guarantee=exactly_once_v2`. That is Kafka Streams EOS under the SQL. Each processing iteration is a **transaction**:

1. Read from input topic (committed offsets are tracked transactionally)
2. Apply processing logic (write to state store)
3. Write output to output topic
4. Commit the input offsets and output messages **atomically**

If any step fails, the entire transaction is aborted. The output is not visible to downstream consumers until committed. On restart, the query resumes from the last committed transaction boundary — no duplicates. Existing queries keep the guarantee they were created with; turning the setting on does not rewrite old ones.

### The latency cost

EOS has a latency overhead: Kafka must flush transactions to disk and coordinate with the broker before a batch is considered committed. For these anomaly queries that cost is accepted so a crash does not double-apply the count.

The ETA worker pays the same kind of cost on purpose: one transaction sends `eta-updates` and the next GPS offset together. Nearby and rider-GEO stay at-least-once. They don't wrap Redis in a Kafka transaction — and they can't. That boundary is in [Kafka Exactly-Once Processing & Idempotent Producer](Kafka%20Exactly-Once%20Processing%20%26%20Idempotent%20Producer.md).

### Idempotent vs transactional

||Idempotent Producer|Transactional Producer|
|---|---|---|
|Scope|Single producer, deduplication of retries|Atomic multi-topic writes with consumer offset commits|
|Config|`enable.idempotence=true`|`transactional.id=<id>` + idempotence|
|Use case|GPS producers: no duplicate appends from a retry|ETA worker, and ksqlDB queries: output and offset commit together|

---

## 14. Observability — Consumer Lag

### The problem

Throughput metrics ("we processed 10,000 events/second") tell you how fast you're going. They don't tell you if you're **falling behind**. If producers are writing 11,000 events/second and a consumer is only reading 9,000/second, the backlog grows by 2,000/second. In 10 minutes, the consumer is 1.2 million events behind. ETAs become stale. Anomaly detection is 20 minutes late.

### Consumer lag

**Consumer lag** = (latest offset on partition) − (consumer group's committed offset on that partition)

A lag of 0 means the consumer is caught up. A lag of 50,000 means 50,000 unprocessed events are queued. Lag, not throughput, is the health signal that matters.

```
Partition 0:  latest offset = 10,000
              ETA Calculator committed offset = 9,950
              Lag = 50   ← healthy

              Anomaly Detector committed offset = 5,000
              Lag = 5,000 ← warning: falling behind
```

### Why lag matters more than throughput

A consumer processing 9,000 events/second looks healthy in isolation. But if production rate is 9,500/second, lag is growing at 500/second. Throughput metrics would show green. Lag would show the problem.

### RideStream observability setup

No Nest metrics code. **kafka-exporter** talks to the brokers and exposes `kafka_consumergroup_lag`. Prometheus scrapes that. Grafana reads Prometheus.

- Warning: lag summed by consumer group and topic > 50 for 1 minute
- Critical: the same sum > 500 for 2 minutes

The thresholds are low because the local producer is slow on purpose. A one-second spike does not page; the condition has to stay true for the `for` duration. Lag is not the same number as `latency_ms` in the worker logs. That split is in [Consumer Lag and Monitoring](../06%20Observability/Consumer%20Lag%20and%20Monitoring.md).

---

## 15. Fault Tolerance

### Scenario 1: One broker dies

**What happens:** That broker's partition leaders are gone. Producers and consumers refresh metadata and talk to the new leaders. The other two brokers still have controller quorum and, while replicas were in sync, two ISR members. Writes continue. They do not if a second broker is also down.

**Recovery:** The restarted broker fetches what it missed from the current leaders and rejoins the ISR only after it catches up. Consumers resume from their last **committed offset**.

**Key lesson:** The committed offset is the consumer's bookmark. If offsets are committed frequently, recovery loses little. If the last commit was 10,000 events ago (e.g., auto-commit had just missed the window), those 10,000 events are reprocessed. This is at-least-once behavior — acceptable for RideStream's non-anomaly consumers.

### Scenario 2: Slow consumer (anomaly detector)

**What happens:** The anomaly detector's processing logic becomes slow (e.g., an expensive route-deviation calculation). It can't keep up with the producer rate. Lag grows.

**Impact:** The Prometheus alert fires. But — and this is the critical design point — the ETA worker and the nearby worker are **unaffected** if the slow one is ksqlDB, and the reverse is also true. Separate groups, separate offsets. They read the same `gps-events-driver` topic at their own pace.

**Key lesson:** Consumer group isolation. Kafka doesn't slow down for the slowest consumer.

### Scenario 3: Producer sends duplicate events

**What happens:** Network blip causes the producer to retry an event already successfully written. Without idempotence, Kafka stores the duplicate.

**Recovery:** With `enable.idempotence=true`, Kafka's broker-side deduplication discards the duplicate based on the producer's sequence number. The anomaly detector never sees it.

### Scenario 4: Schema change

**What happens:** The producer starts emitting a v2 schema with the new `heading` field. An older consumer is still running the v1 read path.

**Recovery:** BACKWARD means a new reader can still decode old events. Missing `heading` becomes `null` from the field default, not a crash. Registry rejects the version if the new field is required and has no default.

---

## 16. Mental Model Summary

|Concept|The problem it solves|RideStream usage|
|---|---|---|
|Topic|Named stream of events|`gps-events-driver`, `gps-events-rider`, `eta-updates`, anomaly topics|
|Partition|Parallelism without losing per-key ordering|6 partitions; key is `driver_id` or `rider_id`|
|Producer key|Route related events to the same partition|Same driver stays ordered on one partition|
|Consumer group|Multiple independent readers of the same topic|`ridestream-eta`, `ridestream-nearby`, ksqlDB; rider GEO on its own topic|
|Offset|Consumer's position in a partition|Nearby/rider-GEO auto-commit; ETA commits inside the transaction|
|Idempotent producer|Deduplicate retries caused by network failures|GPS producers|
|Schema Registry|Safe schema evolution without breaking consumers|Optional `heading`, default `null`|
|Avro|Compact, schema-enforced serialization|Driver GPS, rider GPS, ETA|
|Consumer lag|True health indicator (not throughput)|Warning > 50 for 1m, critical > 500 for 2m|
|ksqlDB|Stateful stream processing without a custom consumer|Spikes, windows, teleport, freeze|
|Table / state|Per-key memory that survives crashes|`latest_gps`|
|Hopping window|Overlap so a boundary spike is not missed|1-minute window, advance 15 seconds|
|EOS|Output and input offset commit together|ksqlDB `exactly_once_v2`; ETA Kafka transaction|
|Redis|Current position and live push, not a log|GEOADD / GEOSEARCH, then Pub/Sub to the gateway|

---
