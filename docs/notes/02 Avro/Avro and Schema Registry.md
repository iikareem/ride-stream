---
tags:
  - ride-stream
  - avro
  - schema-registry
  - kafka
---

# Avro and Schema Registry

> Part of [RideStream](../RideStream.md). Comes after [kafka cluster](../01%20Kafka/kafka%20cluster.md). The contract problem, then how this repo solves it.


---

## The problem

Three readers consume driver GPS: ETA, nearby, ksqlDB. Next month the event needs a `heading` field.

If the payload is JSON:

- An old consumer that does not know `heading` might crash, or silently ignore it and compute a wrong ETA.
- Nothing stops the producer from renaming `speed_kmh` or sending a string where a number used to be.
- Every message carries the field names again. At GPS rates that adds up, and there is still no shared contract.

Schema Registry is the catalog. Avro is the encoding. The message carries a schema **id**, not the schema itself.

---

## What a message looks like on the wire

Confluent wire format, not raw Avro:

```text
[ magic byte 0 ][ 4-byte schema id ][ Avro binary payload ]
```

Producer registers the schema, gets an id, encodes with that id. Consumer reads the id, fetches the schema (cached after the first fetch), decodes.

```text
Producer
  register schema  →  Schema Registry  →  id = 2
  encode [0][id=2][avro bytes]
        │
        ▼
     Kafka
        │
        ▼
Consumer
  read id 2  →  Registry.get(2)  →  decode
```

KafkaJS (`@kafkajs/confluent-schema-registry`) does the header. I do not hand-pack the bytes.

The key is separate. In this repo the key is the raw `driver_id` or `rider_id` string, not Avro. That is why ksqlDB uses `KEY_FORMAT='KAFKA'` and `VALUE_FORMAT='AVRO'`.

---

## Subject names

Registry does not store "the GPS schema" as one global object. It stores **subjects**, and each subject has versions.

TopicNameStrategy (the default) names the value subject after the topic:

```text
gps-events-driver   →   gps-events-driver-value
eta-updates         →   eta-updates-value
gps-events-rider    →   gps-events-rider-value
```

Rename the topic and the subject name moves with it. That is why `gps-events` became `gps-events-driver` instead of staying a vague name once riders had their own stream.

---

## BACKWARD, and the heading change

Compatibility is a rule the Registry enforces when a new version is registered. Compose sets `BACKWARD`.

| Mode | Meaning | Who you upgrade first |
| --- | --- | --- |
| BACKWARD | New schema can read old data | Consumers, then producers |
| FORWARD | Old schema can read new data | Producers; old consumers must tolerate new fields |
| FULL | Both | Strictest |
| NONE | No checks | Avoid |

`*_TRANSITIVE` checks against every older version, not only the previous one.

Adding an optional field with a default is BACKWARD-safe. Removing a required field, or adding a required field with no default, is not. The Registry rejects the registration.

What this repo did:

1. Register v1 (`driver_id`, lat, lon, speed, timestamp, status) under `gps-events-driver-value`.
2. Register v2, same fields plus optional `heading` (`["null", "float"]`, default `null`).
3. Producer encodes with v2. A consumer `decode` uses whatever id is on that message.

v1 is not required for the app to run today. It is there so the Registry shows a history and proves BACKWARD accepted the change. Each message still has one schema id. Missing `heading` on old data becomes `null` because of the default, not because the whole event is undefined.

`0.0` would look like "facing north." `null` means unknown.

Check:

```bash
curl -s http://localhost:8081/subjects/gps-events-driver-value/versions
curl -s http://localhost:8081/subjects/gps-events-driver-value/versions/latest
```

---

## Where the schemas live

Schema Registry is an HTTP API in front of a Kafka topic, usually `_schemas`. It is not Postgres. No volume in this Compose file, so `docker compose down` wipes the catalog. Next startup the app registers v1 then v2 again via `SchemaRegistryService.ensureSchemasRegistered()`.

JSON messages left on the topic from before the Avro cutover will not decode. Wipe the topic when switching formats. Avro binary is not JSON.

---

## What Registry is not

It supports Avro, Protobuf, and JSON Schema. Not arbitrary bytes.

It is for Kafka event contracts (producers, consumers, ksqlDB, Connect). gRPC does not use it. Service-to-service schemas live in `.proto` files, a shared package, or something like Buf.

| Traffic | Where the contract lives |
| --- | --- |
| Kafka events | Confluent Schema Registry |
| gRPC | `.proto` / Buf |
| REST | OpenAPI |

Anomaly topics in this repo are JSON on purpose. ksqlDB writes them. They are derived output, not the GPS contract.

---

## One-liners

| Question | Answer |
| --- | --- |
| Why not JSON forever? | No shared contract, bigger payloads, schema changes are a coordination problem |
| What is on the wire? | Magic `0` + 4-byte schema id + Avro bytes |
| What is a subject? | Named history of schema versions, usually `{topic}-value` |
| Why BACKWARD? | A new reader can still decode old events |
| Why register v1 if we only encode v2? | To see evolution. The running producer only needs v2 |
| Who registers? | The app on startup, or paste into Kafka UI. Subject name must stay the TopicNameStrategy name |
