---
aliases:
  - RideTrack
  - Ride stream
tags:
  - ride-stream
  - moc
---

# RideStream

What I learned building the live location pipeline. One note per idea. The code is [github.com/iikareem/ride-stream](https://github.com/iikareem/ride-stream). This folder is the reference.

Open the notes in order. Later notes assume the earlier ones.

## The system, in one pass

```text
driver GPS ──Avro──▶ gps-events-driver ──┬── ETA worker ──txn──▶ eta-updates
                                         ├── nearby worker ──GEOSEARCH──▶ Redis Pub/Sub ──▶ gateway ──▶ browser
                                         └── ksqlDB ──▶ anomaly topics

rider GPS ──Avro──▶ gps-events-rider ──▶ rider-geo worker ──GEOADD──▶ riders:geo
```

Kafka keeps the durable log. Redis keeps the current map and the live push. The gateway only holds sockets.

| Piece | What it owns | Guarantee |
| --- | --- | --- |
| GPS producers | Write location events, keyed by id | Idempotent produce |
| ETA worker | Distance and ETA, write `eta-updates` | Kafka transaction (consume + produce) |
| ksqlDB | Speed, freeze, teleport | `exactly_once_v2` inside Kafka |
| Rider GEO worker | Current rider position | At-least-once into Redis |
| Nearby worker | Who is close, then `PUBLISH` | At-least-once; Pub/Sub itself does not replay |
| Gateway | Rooms and delivery | Live only. Missed while disconnected is gone |

## What I can explain, in the order I learned it

### 1. Kafka

- [Kafka From First Principles](01%20Kafka/Kafka%20From%20First%20Principles.md) — why a log, topics, partitions, keys, groups, offsets, and how this pipeline uses them.
- [kafka cluster](01%20Kafka/kafka%20cluster.md) — three brokers, replication, ISR, `min.insync.replicas`, leader election.
- [Kafka Exactly-Once Processing & Idempotent Producer](01%20Kafka/Kafka%20Exactly-Once%20Processing%20%26%20Idempotent%20Producer.md) — PID and seq, transactions, and where exactly-once stops.

### 2. Avro

- [Avro and Schema Registry](02%20Avro/Avro%20and%20Schema%20Registry.md) — wire format, subjects, BACKWARD, and the `heading` change.

### 3. Stream SQL

- [ksqlDB Streams, Windows, and Anomalies](03%20ksqlDB/ksqlDB%20Streams%2C%20Windows%2C%20and%20Anomalies.md) — stream vs table, tumbling vs hopping, teleport join, freeze window.

### 4. Location

- [Geohash — Complete Guide](04%20Redis/Geohash%20%E2%80%94%20Complete%20Guide.md) — why a 2D point becomes one sorted number, and how `GEOSEARCH` stays fast.
- [Redis in RideStream](04%20Redis/Redis%20in%20RideStream.md) — GEOADD, Pub/Sub, the second connection, refcount.

### 5. Live delivery

- [WebSocket Gateway Pattern](05%20WebSocket/WebSocket%20Gateway%20Pattern.md) — HTTP when I want something, socket when the server talks first.
- [WebSocket API Design Guide](05%20WebSocket/WebSocket%20API%20Design%20Guide.md) — envelope, auth per connection, scale.

### 6. Knowing if it is behind

- [Consumer Lag and Monitoring](06%20Observability/Consumer%20Lag%20and%20Monitoring.md) — lag vs `latency_ms`, kafka-exporter, the delay drill.

## Topic map in the repo

```text
gps-events-driver          key driver_id   Avro    ETA, nearby, ksqlDB
gps-events-rider           key rider_id    Avro    rider GEO index
eta-updates                key driver_id   Avro    transactional ETA output
driver-anomalies                       JSON    one speed spike
driver-anomaly-windows                 JSON    tumbling 1-minute counts
driver-anomaly-windows-hop             JSON    hopping 1-minute / 15s
driver-anomalies-teleport              JSON    jump vs last position
driver-anomalies-freeze                JSON    low-movement window
```

Same topic + different `groupId` = each service reads the full stream. Same `groupId` = they split the six partitions. A seventh member of one group sits idle.

Groups: `ridestream-eta`, `ridestream-nearby`, `ridestream-rider-geo`, plus ksqlDB's own groups.

## What this project does not cover

- Auth on Kafka, Schema Registry, Redis, or the gateway
- Durable client history (Pub/Sub has no replay)
- Real trip routing (ETA destinations are a stable fake per `driver_id`)
- Production persistence (`docker compose down` wipes Kafka)
