---
tags:
  - ride-stream
  - ksqldb
  - kafka
  - windows
---

# ksqlDB Streams, Windows, and Anomalies

> Part of [RideStream](../RideStream.md). Needs [Avro and Schema Registry](../02%20Avro/Avro%20and%20Schema%20Registry.md) (the GPS stream is Avro) and [Kafka Exactly-Once Processing & Idempotent Producer](../01%20Kafka/Kafka%20Exactly-Once%20Processing%20%26%20Idempotent%20Producer.md) (what `exactly_once_v2` does and does not cover).

Anomaly detection here is ksqlDB: SQL queries that are persistent Kafka Streams jobs. You do not write a Java topology in this repo.

Start the driver producer once before registering the stream, so Schema Registry already has `gps-events-driver-value`.

---

## Why not another Nest consumer

A Nest consumer can filter `speed_kmh > 50` and log it. That is not the hard part.

The hard part is:

- remember each driver's last position across restarts
- count spikes inside a time bucket
- not double-count if the query crashes mid-window

ksqlDB already has streams, tables, windows, and changelog-backed state. I write the rule in SQL. I do not build the store.

Route deviation, map-matching, and multi-signal scoring stay hard in SQL. Those want trip state a service can hold. ksqlDB is the right tool for filters, windows, and "join this event to the latest row for this key."

---

## Stream vs table

| | Stream | Table |
| --- | --- | --- |
| What a row is | An event that happened | The current value of a key |
| Example here | `gps_events_driver`, `speed_spikes`, `gps_teleports` | `latest_gps`, `spike_counts_tumbling`, `gps_freeze` |
| Changelog | Append | Each key (and window) is updated |

`gps_events_driver` is a stream over `gps-events-driver`. Key format `KAFKA` because the key is the raw id bytes from KafkaJS. Value format `AVRO` because the payload is the Registry schema.

A windowed count is a table. The row for `(driver-001, 10:00–10:01)` gets updated as more spikes land in that bucket. It is not a new event for every GPS ping.

---

## What each query does

Thresholds are tuned for the local simulator (gentle nudges, speeds mostly 5–60 km/h). They are teaching numbers, not production rules.

### Speed spike — stream filter

`SPEED_KMH > 50` → topic `driver-anomalies`, type `SPEED_SPIKE`.

One input event, at most one output event. No state.

### Tumbling window — non-overlapping buckets

```text
|── 0:00–1:00 ──|── 1:00–2:00 ──|── 2:00–3:00 ──|
```

Count spikes per `DRIVER_ID` in a fixed 1-minute bucket. Emit when the count is at least 2. Topic: `driver-anomaly-windows`.

A spike at 0:59 and another at 1:01 fall in different buckets. Neither bucket may cross the threshold, even though the driver was fast across the boundary.

### Hopping window — overlapping buckets

```text
Window 1: 0:00–1:00
Window 2: 0:15–1:15
Window 3: 0:30–1:30
```

Same 1-minute size, advances every 15 seconds. A spike near a boundary sits in more than one window, so it is harder to miss. Cost: more windows to compute. Topic: `driver-anomaly-windows-hop`.

Hopping is there so a spike on a bucket edge is harder to miss. Tumbling is a separate query, so both shapes show up in Kafka UI.

### Teleport — stream joined to a table

`latest_gps` is a table: last lat/lon per `DRIVER_ID`, via `LATEST_BY_OFFSET`.

Each new GPS event joins that table (`WITHIN 1 HOURS` on `DRIVER_ID`). If `GEO_DISTANCE` is over 0.25 km and the new event is newer than the stored one, emit `TELEPORT` to `driver-anomalies-teleport`.

This is the "remember the previous point" problem. The table is the memory. The join is the comparison. I do not keep a `Map` in Nest for this path.

### GPS freeze — almost no movement

2-minute tumbling window. Emit `GPS_FREEZE` when all of these hold:

- at least 4 events
- lat span and lon span each under `0.00015`
- average speed under 8 km/h

Topic: `driver-anomalies-freeze`.

This is a tumbling window with a span check, not a session window. The window closes because time advanced and enough points landed inside it, not because a timer fired on silence.

---

## Exactly-once, and the boundary

Compose sets `KSQL_KSQL_STREAMS_PROCESSING_GUARANTEE=exactly_once_v2`. That wraps each persistent query's consume → update state → produce in a Kafka transaction. A crash should not double-apply that query's output the at-least-once way.

It applies to **new** persistent queries after a ksqlDB restart. An existing query keeps the guarantee it was created with. Drop and recreate it if you turned EOS on later.

For the next statement in a CLI session only:

```sql
SET 'processing.guarantee' = 'exactly_once_v2';
```

Check with `SHOW QUERIES`. The brokers already have the transaction state topics.

Nest consumers of `driver-anomalies*` are still at-least-once unless they dedupe. EOS stops at the Kafka boundary. See [Kafka Exactly-Once Processing & Idempotent Producer](../01%20Kafka/Kafka%20Exactly-Once%20Processing%20%26%20Idempotent%20Producer.md).

GPS producers are idempotent only. The ETA worker is the Nest place that uses a `transactional.id`. ksqlDB's EOS is separate from that.

---

## Stream processing vs a normal consumer

High rate alone is not a reason to use ksqlDB.

| I need | Use |
| --- | --- |
| Filter, publish, write Redis, one event at a time | Consumer group (nearby, rider-geo, ETA) |
| Buffer N events or T milliseconds, then one flush | Same consumer, with a buffer or `eachBatch` |
| Windowed aggregates, late data, state that survives restart | ksqlDB or Kafka Streams |

A plain consumer's in-memory buffer dies with the process. Each member only sees its own partitions. A count across all drivers needs a shared store or a stream processor.

---

## One-liners

| Question | Answer |
| --- | --- |
| Stream or table? | Event that happened vs current value of a key |
| Why hopping? | Overlap, so a spike on a bucket edge is not missed |
| Where does teleport memory live? | `latest_gps` table, joined within 1 hour |
| Does EOS cover the Nest reader of anomaly topics? | No |
| Why these thresholds? | So the local simulator can actually trigger them |
