---
tags:
  - ride-stream
  - kafka
  - observability
---

# Consumer Lag and Monitoring

> Part of [RideStream](../RideStream.md). Offsets are in [Kafka From First Principles](../01%20Kafka/Kafka%20From%20First%20Principles.md). This note is the signal I actually alert on, and how it differs from the timestamp in the logs.

The dashboard uses kafka-exporter, not a metrics client inside Nest. Thresholds are low because the local producer is slow on purpose, so the lag drill can actually fire.

---

## Two different "how late" numbers

| | `latency_ms` in app logs | Consumer lag |
| --- | --- | --- |
| Formula | `Date.now() - event.timestamp` | log-end offset − committed offset |
| Unit | Milliseconds | Messages |
| Question it answers | How old was this event when I finished it? | How many records has this group not committed? |
| Where I see it | Worker logs | Kafka UI, `kafka-consumer-groups --describe`, Grafana |

A consumer can show a healthy `latency_ms` on the message it just handled and still be 10,000 records behind. Throughput ("we processed 9,000/s") hides that if the producer is at 9,500/s. Lag is the backlog. That is the health signal.

`latency_ms` looks huge at startup if `CONSUME_FROM_BEGINNING=true`, because replayed events have old timestamps. For a live drill, set it to `false`.

---

## What is scraped

No Nest metrics code for this. kafka-exporter talks to the brokers and exposes `kafka_consumergroup_lag`. Prometheus scrapes it. Grafana reads Prometheus.

| UI | Port | What I open it for |
| --- | --- | --- |
| Kafka UI | 8080 | Topics, groups, who owns which partition |
| Schema Registry | 8081 | Subjects and versions |
| Prometheus | 9090 | Alerts, raw queries |
| Grafana | 3000 | Lag dashboard. Local demo only: `admin` / `admin` |
| kafka-exporter | 9308/metrics | The series Prometheus scrapes |
| Redis Insight | 5540 | `GEOADD` members, manual `PUBLISH` |

Broker JMX on `9101` is still there for JVM tooling. It is not what the dashboard uses.

---

## The alerts

From `monitoring/alerts.yml`:

| Alert | Rule | For |
| --- | --- | --- |
| `ConsumerGroupLagHigh` | sum of lag by group and topic > 50 | 1 minute |
| `ConsumerGroupLagCritical` | same sum > 500 | 2 minutes |

`for` means the condition has to stay true that long. A one-second spike does not page.

---

## The drill

Make ETA slow on purpose. Keep the producer faster than the consumer. Lag grows. Clear the delay and it drains. Other groups are unaffected, because they have their own offsets. See [kafka cluster](../01%20Kafka/kafka%20cluster.md) only if a broker is actually down. Lag from a slow consumer is not a replication failure.

```bash
PROCESSING_DELAY_MS=2000 CONSUME_FROM_BEGINNING=false npm run start:eta
npm run start:producer
```

Watch Grafana, or Prometheus at `localhost:9090/alerts`. Describe the group:

```bash
docker exec ridestream-broker-1 kafka-consumer-groups \
  --bootstrap-server broker-1:29092 \
  --describe --group ridestream-eta
```

Rebalance is a different drill: start `start:nearby` twice with the same `groupId`. Logs show `REBALANCING`, then a new partition assignment. Processing pauses. With at-least-once, an offset around that boundary can be processed twice.

---

## What lag does not tell me

- It is not end-to-end user latency. A caught-up consumer can still be slow to reach the browser if Redis or the gateway is the bottleneck.
- It is not "the producer failed." If nothing is produced, lag stays flat.
- A group with no committed offset yet is not "infinite lag" in a useful sense. `fromBeginning` / `auto.offset.reset` only applies when that group has no commit. After the first commit, restart continues from the bookmark. Identity is the `groupId` string, not the process.

---

## One-liners

| Question | Answer |
| --- | --- |
| Lag or `latency_ms`? | Lag is uncommitted records. `latency_ms` is age of the event I just handled |
| Do I add Prometheus client code in Nest? | No. kafka-exporter reads the brokers |
| Why such a low threshold? | Local volume is small. 50 records for a minute is already "falling behind" in this demo |
| Does a slow ETA stall nearby? | No. Separate groups, separate offsets |
