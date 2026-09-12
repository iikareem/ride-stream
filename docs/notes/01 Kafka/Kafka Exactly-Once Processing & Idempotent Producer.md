> Part of [RideStream](../RideStream.md).


---

## Table of Contents

1. [Delivery Semantics](#1-delivery-semantics)
2. [Partition Log — The Foundation](#2-partition-log)
3. [Partition Offset vs Producer Seq](#3-partition-offset-vs-producer-seq)
4. [Idempotent Producer](#4-idempotent-producer)
    - [How PID and Seq Work](#how-pid-and-seq-work)
    - [Full Send Flow](#full-send-flow)
    - [Retry Flow](#retry-flow)
    - [Seq Rules](#seq-rules)
5. [Producer Restart](#5-producer-restart)
    - [Without transactionalId](#without-transactionalid)
    - [With transactionalId and Epoch](#with-transactionalid-and-epoch)
    - [Why Seq Resets to 0 Is Safe](#why-seq-resets-to-0-is-safe)
6. [Transactions — Consumer Side](#6-transactions)
7. [ksqlDB and Kafka Streams](#7-ksqldb-and-kafka-streams)
8. [⚠️ Exactly-Once Boundary — Kafka vs External Systems](#8-exactly-once-boundary)
9. [Producer Buffer — The Gap Before Kafka](#9-producer-buffer)
10. [Full End-to-End Picture](#10-full-end-to-end-picture)
11. [Summary Tables](#11-summary-tables)
12. [Node.js Quick Reference](#12-nodejs-quick-reference)

---

## 1. Delivery Semantics

In any distributed system, when things fail (network drops, broker crashes, producer retries), you end up with one of three guarantees:

|Semantic|What Happens|Risk|
|---|---|---|
|**At-most-once**|Message may be lost, never duplicated|Data loss|
|**At-least-once**|No loss, but retries cause duplicates|Duplicate data|
|**Exactly-once**|No loss, no duplicates|✅ Correct|

### Why Exactly-Once Is Hard

```
Producer sends message
Broker writes it ✅
Broker sends ack back
Network drops — ack never arrives
Producer doesn't know if it arrived
Producer retries → broker writes it AGAIN ❌
```

In streaming, processing the same message twice produces **wrong data** — wrong aggregations, wrong counts, wrong state. This is why exactly-once matters.

---

## 2. Partition Log

### What Is It?

A partition log is an **append-only file stored on the broker's disk**. Every message written to a partition gets added to the end of this file. Nothing is ever edited or deleted (until retention policy kicks in).

```
Partition-1 log file on broker disk:

offset 0 │ key:'user-123' │ value:'order-A' │ timestamp: 10:00:01
offset 1 │ key:'user-456' │ value:'order-B' │ timestamp: 10:00:02
offset 2 │ key:'user-123' │ value:'order-C' │ timestamp: 10:00:05
offset 3 │ key:'user-789' │ value:'order-D' │ timestamp: 10:00:09
          ↑
          always appends here — never modifies old entries
```

### Why Append-Only?

Appending to the end of a file is the **fastest possible disk operation** — no searching, no updating, no rewriting. This is why Kafka handles millions of messages per second.

```
❌ Database style:   find row → lock → update → reindex
✅ Kafka style:      append to end of file → done
```

### Each Topic-Partition Has Its Own Log

```
Topic: orders
   │
   ├── partition-0  →  /kafka-data/orders-0/  (independent log on disk)
   ├── partition-1  →  /kafka-data/orders-1/  (independent log on disk)
   └── partition-2  →  /kafka-data/orders-2/  (independent log on disk)
```

Each partition has its own file, its own offsets starting from 0, completely independent.

### What Is Inside Each Log Entry

```
{
   offset:    2,               ← position in the log (assigned by broker)
   timestamp: 1719000005000,  ← when it was written
   key:       'user-123',     ← your partition key
   value:     'order-C',      ← your message payload
   headers:   {...},          ← optional metadata
   PID:       42,             ← producer id (internal, broker only)
   Seq:       2,              ← producer seq (internal, broker only)
}
```

The consumer only sees `offset`, `key`, `value`, `headers`, `timestamp`. `PID` and `Seq` are internal broker metadata — invisible to consumers.

### How the Consumer Uses the Log

The consumer tracks its position using the offset and asks the broker for the next batch:

```
Consumer
   │
   │  "give me messages from partition-1 starting at offset 3"
   │ ──────────────────────────────────────────────────────► Broker
   │  ◄──────────────────────────────────────────────────── │
   │  receives offset 3, 4, 5...
```

Multiple consumers can read the same log independently, each at their own offset position.

### Mental Model — Think of a Notebook

```
Notebook (partition log)
   page 1 → order-A    (offset 0)
   page 2 → order-B    (offset 1)
   page 3 → order-C    (offset 2)
   page 4 → order-D    (offset 3)
   page 5 → (blank — next write goes here)

Rules:
   ✅ You can only ADD new pages at the end
   ❌ You cannot erase or edit existing pages
   ✅ Multiple readers can read from any page independently
   ✅ Each reader remembers which page they stopped at
```

---

## 3. Partition Offset vs Producer Seq

These are **two completely different things** that happen to share the concept of a sequence number. This is one of the most important distinctions in Kafka.

### Partition Offset

The offset IS the sequence number of the partition log. Assigned by the **broker** after the message is written.

```
Partition-1 log:
   offset 0 → order-A     ← seq 0 in the log
   offset 1 → order-B     ← seq 1 in the log
   offset 2 → order-C     ← seq 2 in the log
```

### Producer Idempotency Seq

A counter the **producer** assigns to each message before sending, used to detect duplicates.

```
Producer sends to partition-1:
   {PID:42, Seq:0, order-A}
   {PID:42, Seq:1, order-B}
   {PID:42, Seq:2, order-C}
```

### They Look the Same in a Normal Flow — But Are Not

```
Producer Seq:0 → broker writes → offset:0
Producer Seq:1 → broker writes → offset:1
Producer Seq:2 → broker writes → offset:2
```

They diverge when **multiple producers** write to the same partition:

```
Producer A: Seq:0 → offset:0
Producer A: Seq:1 → offset:1
Producer B: Seq:0 → offset:2   ← Producer B has its own Seq from 0
Producer A: Seq:2 → offset:3
```

Producer B's `Seq:0` has nothing to do with `offset:0`.

### Comparison Table

||Partition Offset|Producer Idempotency Seq|
|---|---|---|
|Assigned by|Broker|Producer|
|Purpose|Consumer position tracking|Duplicate detection|
|Scope|Whole partition, all producers|Per producer, per partition|
|Resets?|Never — always grows|Resets on producer restart|
|Visible to consumer?|✅ Yes|❌ No — internal only|
|Point in lifecycle|After message written to log|Before message reaches broker|

### Where Each Lives in the Lifecycle

```
Producer Seq     → stamped BEFORE message reaches broker → deduplication
                         │
                         ▼
Partition Offset → assigned BY broker AFTER message written → consumer tracking
```

---

## 4. Idempotent Producer

Idempotent means: **no matter how many times you send the same message, the broker only writes it once.**

```javascript
// Node.js (kafkajs)
const producer = kafka.producer({
  idempotent: true,
});
```

---

### How PID and Seq Work

**PID (Producer ID)** — assigned by Kafka at startup. Unique per producer instance. **Seq** — counted by the producer itself, per partition, starting from 0.

> Kafka gives the PID. The producer counts the Seq.

---

### Full Send Flow

#### Step 1 — Producer Starts Up (InitProducerId)

```
Your App
   │
   │  kafka.producer({ idempotent: true })
   │  sends InitProducerId request
   │ ──────────────────────────────────────► Broker
   │                                            │
   │                                            │  generates PID = 42
   │                                            │  Epoch = 0
   │  ◄──────────────────────────────────────  │
   │
   │  producer knows: PID = 42
   │  internal seq counter = {} (empty)
```

#### Step 2 — Message Created (No Seq Yet)

```javascript
producer.send({
  topic: 'orders',
  messages: [{ key: 'user-123', value: 'order-A' }]
})
```

```
message = {
   key: 'user-123',
   value: 'order-A',
   partition: ?,    ← not known yet
   seq: ?           ← not assigned yet
}
```

#### Step 3 — Partition Resolved, Seq Assigned

The producer hashes the key to find the partition, then assigns Seq:

```
hash('user-123') → partition 1

internal counter = {
   'orders-partition-1': ???  ← first time here
}
→ start at 0
→ assign Seq = 0

message = {
   key: 'user-123',
   value: 'order-A',
   partition: 1,
   PID: 42,
   Seq: 0           ← stamped AFTER partition is known
}
```

> The Seq is assigned **after** the partition is resolved — not at message creation. The producer builds its counter map lazily, only for partitions it has actually sent to.

#### Step 4 — Sent to Broker, Ack Lost

```
Producer                              Broker (partition 1)
   │                                         │
   │  {PID:42, Seq:0, 'order-A'}             │
   │ ──────────────────────────────────────► │
   │                                         │  writes to log ✅
   │                                         │  remembers:
   │                                         │  PID=42, partition-1, last Seq=0
   │                                         │
   │  ack ◄──────── NETWORK DROP ─────────   │
   │                                         │
   │  producer timeout                       │
   │  "did it arrive? I don't know"          │
   │  → RETRY                                │
```

---

### Retry Flow

```
Producer                              Broker (partition 1)
   │                                         │
   │  {PID:42, Seq:0, 'order-A'}  (retry)   │
   │ ──────────────────────────────────────► │
   │                                         │  checks memory:
   │                                         │  PID=42, Seq=0 on partition-1
   │                                         │  → already committed
   │                                         │  → DISCARD silently ✅
   │  ◄──────────────────────────────────── │
   │  receives ack (as if accepted)          │
```

No duplicate written. The broker used **PID + Seq + Partition** to detect it.

---

### Seq Rules

|Rule|Detail|
|---|---|
|Counter is per topic-partition|Each partition has its own counter from 0|
|Seq is per message, not per key|All keys hashing to the same partition share one counter|
|Assigned after partition is known|Not at message creation time|
|Owned by the producer|Kafka only assigns PID, not Seq|
|Retry uses the same Seq|Never reassigned on retry|
|Next new message increments Seq|Only after previous is acked|

### What the Broker Keeps in Memory

```
PID = 42
   └── orders-partition-0 → last seq = (not used yet)
   └── orders-partition-1 → last seq = 1
   └── orders-partition-2 → last seq = (not used yet)
```

The broker checks each incoming message:

- Seq is **next expected** → write ✅
- Seq is **already seen** → discard silently ✅
- Seq has a **gap** → error ❌ (something is wrong)

---

## 5. Producer Restart

### Without transactionalId

```javascript
kafka.producer({ idempotent: true })
```

On restart → producer gets a **brand new PID**:

```
First run:   PID=42, partition-1 → Seq: 0, 1, 2, 3
CRASH
Second run:  PID=99, partition-1 → Seq: 0, 1, 2, 3
```

The broker treats PID=99 as a completely new producer.

**New events after restart are safe** — PID=99 Seq=0 ≠ PID=42 Seq=0. Different producer entirely.

**But the last message before crash can duplicate:**

```
PID=42 sends {Seq:3, order-D} → broker writes ✅ → CRASH, ack lost
Restarts as PID=99
Retries order-D as {PID:99, Seq:0, order-D}
Broker: brand new PID → writes ✅

order-D now in topic TWICE ❌
```

---

### With transactionalId and Epoch

```javascript
kafka.producer({
  idempotent: true,
  transactionalId: 'orders-producer-1'  // stable, same every restart
})
```

On restart, the broker finds the existing record for this `transactionalId` and bumps the Epoch:

```
Producer restarts
   │
   │  InitProducerId
   │  transactionalId: 'orders-producer-1'
   │ ──────────────────────────────────────► Broker
   │                                            │
   │                                            │  finds existing record
   │                                            │  bumps Epoch: 0 → 1
   │                                            │  aborts old incomplete tx
   │  ◄──────────────────────────────────────  │
   │  PID=42, Epoch=1
   │  Seq resets to 0
```

The incomplete transaction from before the crash is **aborted** — its messages were never committed, so they are invisible to consumers. The retry after restart is a clean new transaction.

### What Epoch Does

**1. Aborts the previous incomplete transaction:**

```
old tx (Epoch=0) was never committed
broker marks it ABORTED
order-D from Epoch=0 → invisible to consumers ✅
retry with Epoch=1 → written once cleanly ✅
```

**2. Fences zombie producers:**

```
Old slow instance wakes up:
   tries to send {PID:42, Epoch:0, ...}
   broker: current Epoch is 1
   broker rejects ❌ "you are stale"
```

---

### Why Seq Resets to 0 Is Safe

Even though Seq resets to 0 on every restart, it is safe because the broker tracks **PID + Epoch + Seq** together — not Seq alone.

```
{PID:42, Epoch:0, Seq:0}  ← first run, first message
{PID:42, Epoch:1, Seq:0}  ← second run, first message

These are NOT the same to the broker ✅
Epoch changed → completely different identity
```

Think of it like a folder name:

```
run-1/message-0   ← Epoch=0, Seq=0
run-2/message-0   ← Epoch=1, Seq=0  ← same Seq, different folder
```

### Restart Comparison

||Idempotent Only|With transactionalId|
|---|---|---|
|PID on restart|New PID|Same PID|
|Seq on restart|Resets to 0|Resets to 0|
|New events duplicated?|❌ No (different PID)|❌ No (different Epoch)|
|Last message before crash duplicated?|✅ Yes — danger|❌ No — tx aborted|
|Zombie producer fencing|❌ No|✅ Yes|

---

## 6. Transactions

### Why Transactions Are Needed

The idempotent producer guarantees **no duplicates into the topic**. But the consumer also needs a guarantee: **process each message exactly once**.

Without transactions:

```
Consumer reads message from input topic
Processes it, writes result to output topic ✅
CRASH before committing offset
Restarts → offset not committed → reads SAME message again
Processes it AGAIN → duplicate result ❌
```

### How Transactions Fix It

Transactions wrap the entire consume → process → produce cycle atomically:

```
BEGIN TRANSACTION
   │
   ├── read 'order-A' from orders-partition-1, offset 0
   ├── process it (aggregate, transform...)
   ├── write result to output topic
   └── commit offset 0        ← inside the same transaction
   │
COMMIT  ← atomic — all or nothing
```

If crash happens before COMMIT → everything rolls back → output never visible → consumer reprocesses from same offset → no duplicate.

### Consumer Must Use read_committed

```javascript
kafka.consumer({
  groupId: 'my-group',
  readUncommitted: false  // default in kafkajs — reads only committed data
})
```

Aborted transaction messages are **completely invisible** to consumers reading at `read_committed`.

---

## 7. ksqlDB and Kafka Streams

### You Don't Do Anything

ksqlDB and Kafka Streams handle exactly-once **entirely internally**. No idempotency keys, no manual transaction code. They are full consume → process → produce pipelines:

```
Input Topic
   │  consume
   ▼
Kafka Streams / ksqlDB
(aggregate, join, filter, window...)
   │  produce
   ▼
Output Topic
```

They wrap this entire cycle in transactions automatically under the hood.

### How to Enable It

**Kafka Streams:**

```java
props.put(
  StreamsConfig.PROCESSING_GUARANTEE_CONFIG,
  StreamsConfig.EXACTLY_ONCE_V2
);
```

**ksqlDB** (server config):

```properties
processing.guarantee=exactly_once_v2
```

That is all. The framework handles everything else.

---

## 8. ⚠️ Exactly-Once Boundary

This is the most critical concept to understand about Kafka's exactly-once guarantees.

> **Kafka's exactly-once only works within the Kafka ecosystem.** The moment your consumer writes to something outside Kafka, the guarantee stops completely.

```
┌─────────────────────────────────────────────┐
│           KAFKA ECOSYSTEM                   │
│                                             │
│  Producer → Broker → Consumer → Kafka Topic │
│                                             │
│  Exactly-once: ✅ GUARANTEED                │
│  Mechanism: transactions + PID + Seq        │
└─────────────────────────────────────────────┘
          │
          │  crosses the boundary
          ▼
┌─────────────────────────────────────────────┐
│           EXTERNAL SYSTEMS                  │
│                                             │
│  PostgreSQL / MySQL / MongoDB               │
│  REST APIs / HTTP endpoints                 │
│  Redis / Elasticsearch                      │
│                                             │
│  Exactly-once: ❌ NOT GUARANTEED            │
│  Kafka has zero control here                │
└─────────────────────────────────────────────┘
```

---

### Consumer Writing to Kafka Topic — Use Transactions

When your consumer reads from one topic and writes to another Kafka topic, use transactions. This is **strongly recommended** — without it you have at-least-once, not exactly-once.

```javascript
// ✅ Kafka to Kafka with transaction
await producer.transaction(async (tx) => {
  const result = process(message)
  await tx.send({ topic: 'output-topic', messages: [{ value: result }] })
  await tx.sendOffsets({
    consumerGroupId: 'my-group',
    topics: [{ topic: 'input-topic', partitions: [{ partition: 0, offset: '5' }] }]
  })
  await tx.commit()
})
```

---

### Consumer Writing to External System — You Handle It

Kafka transactions cannot coordinate with PostgreSQL, HTTP APIs, or any external system. You must handle deduplication yourself using an **idempotency key** in the message payload.

> This is NOT Kafka's internal PID + Seq. Those are invisible to external systems. This is a key **you create** and embed in the payload yourself.

#### Step 1 — Producer adds idempotency key to payload

```javascript
producer.send({
  topic: 'orders',
  messages: [{
    key: 'user-123',
    value: JSON.stringify({
      idempotencyKey: 'uuid-abc-123',   // ← you create this, stable across retries
      orderId: 'order-456',
      amount: 100,
    })
  }]
})
```

#### Step 2 — Consumer checks before writing to external system

```javascript
const { idempotencyKey, orderId, amount } = JSON.parse(message.value)

// Check if already processed
const exists = await db.query(
  'SELECT 1 FROM processed_events WHERE idempotency_key = $1',
  [idempotencyKey]
)

if (exists.rows.length > 0) {
  return  // already processed — skip safely
}

// Safe to write — do both in one DB transaction
await db.query('BEGIN')
await db.query('INSERT INTO orders (id, amount) VALUES ($1, $2)', [orderId, amount])
await db.query('INSERT INTO processed_events (idempotency_key) VALUES ($1)', [idempotencyKey])
await db.query('COMMIT')
```

---

### Decision Table

|Consumer Output|Exactly-Once Strategy|Who Handles It|
|---|---|---|
|Kafka topic → Kafka topic|Kafka transactions|Kafka (with config)|
|Kafka topic → PostgreSQL|Idempotency key in payload + DB check|You|
|Kafka topic → REST API|Idempotency key in request body|You|
|Kafka topic → Redis|Check-before-set|You|
|Kafka Streams / ksqlDB|Built-in EOS config|Framework|

### The Golden Rule

> **Inside Kafka** → use transactions, Kafka handles it. **Outside Kafka** → use an idempotency key in the payload, you handle it.

---

## 9. Producer Buffer

### The Problem

When you call `producer.send()`, the message does **not** go to Kafka immediately. It sits in an **in-memory buffer** first:

```
Your Code
   │
   │  producer.send({ key: 'user-123', value: 'order-A' })
   │
   ▼
Producer Internal Buffer (RAM only)
   │
   │  batches messages, waits for linger.ms or batch is full
   │
   ▼
Kafka Broker (durable, on disk)
```

If the producer process dies while messages are still in the buffer → **they are gone forever**. Kafka never saw them. No idempotency, no transactions, nothing can help.

---

### The Three Scenarios

```
Scenario 1 — dies while in buffer:
   message in RAM only
   producer crashes
   message gone ❌ — Kafka never saw it
   nothing recovers this

Scenario 2 — dies after send, before ack:
   message reached broker ✅
   ack lost in network
   producer restarts → retries → idempotent deduplication ✅

Scenario 3 — dies after ack:
   message confirmed in Kafka ✅
   fully safe ✅
```

Kafka's guarantees only begin at **Scenario 2**.

---

### Solution — Outbox Pattern

Write to your database first, then a separate process forwards to Kafka:

```
Your App
   │
   │  BEGIN DB TRANSACTION
   │    INSERT INTO orders (...)              ← your business data
   │    INSERT INTO outbox (event, sent=false) ← the event to send
   │  COMMIT  ← both or neither
   │
   ▼
Outbox Worker (separate process)
   │
   │  SELECT * FROM outbox WHERE sent = false
   │  sends to Kafka
   │  on ack → UPDATE outbox SET sent = true
   │
   ▼
Kafka Broker ✅
```

If producer dies before sending → outbox row still in DB → worker retries → message eventually reaches Kafka. Nothing lost.

### Buffer vs Broker

```
Buffer = RAM = volatile
   └── dies with the process
   └── Kafka cannot help here
   └── your responsibility to protect

Broker = disk = durable
   └── survives crashes
   └── Kafka guarantees START here
```

---

## 10. Full End-to-End Picture

```
Your App (Producer)
   │
   │  idempotent: true
   │  transactionalId: 'orders-producer-1'
   │
   │  message sits in buffer (RAM) ← your responsibility
   │  send → partition resolved → Seq assigned
   │  {PID:42, Epoch:0, Seq:3, order-D} sent to broker
   │
   ▼
Kafka Broker
   │
   │  checks PID + Epoch + Seq
   │  deduplicates retries within session
   │  on restart: bumps Epoch, aborts old tx
   │  writes each message exactly once to partition log
   │  topic is clean ✅
   │
   ▼
Consumer (your app / Kafka Streams / ksqlDB)
   │
   │  reads at read_committed (no aborted messages)
   │
   │  ┌─ output is Kafka topic ──────────────────────────────┐
   │  │  BEGIN TRANSACTION                                    │
   │  │    read message                                       │
   │  │    process                                            │
   │  │    write result to output topic                       │
   │  │    commit offset (inside tx)                         │
   │  │  COMMIT ← atomic                                      │
   │  └──────────────────────────────────────────────────────┘
   │
   │  ┌─ output is external system ──────────────────────────┐
   │  │  read idempotencyKey from payload                    │
   │  │  check if already processed in DB                    │
   │  │  if yes → skip                                       │
   │  │  if no  → write + mark as processed (one DB tx)      │
   │  └──────────────────────────────────────────────────────┘
   │
   ▼
Output (clean, exactly-once) ✅
```

---

## 11. Summary Tables

### Core Concepts

|Concept|Who Owns It|What It Does|
|---|---|---|
|Partition Log|Broker|Append-only file on disk, stores all messages|
|Partition Offset|Broker assigns|Position of message in the log, used by consumers|
|PID (Producer ID)|Kafka assigns at startup|Identifies the producer instance|
|Epoch|Kafka assigns, bumps on restart|Fences old instances, aborts stale transactions|
|Producer Seq|Producer counts itself|Per-partition counter, detects duplicate sends|
|Idempotent producer|Config: `idempotent: true`|No duplicate writes within one session|
|transactionalId|You set it (stable string)|Protects across restarts, enables transactions|
|Transactions|Framework or your code|Atomic consume + process + produce|
|read_committed|Consumer config|Hides aborted transaction messages|
|EOS in Kafka Streams|One config line|Full pipeline exactly-once automatically|

### Exactly-Once Coverage

|Scenario|Covered by Kafka?|Solution|
|---|---|---|
|Producer retry (same session)|✅ Yes|Idempotent producer|
|Producer restart — new events|✅ Yes|New PID or new Epoch|
|Producer restart — last message|✅ Yes (with transactionalId)|Transaction aborted on restart|
|Consumer crash and reprocess|✅ Yes (with transactions)|Atomic offset commit|
|Output to Kafka topic|✅ Yes|Kafka transactions|
|Output to PostgreSQL / API|❌ No|Idempotency key in payload|
|Messages lost in producer buffer|❌ No|Outbox Pattern|

---

## 12. Node.js Quick Reference

```javascript
// ── PRODUCER ──────────────────────────────────────────────

// Option 1: Idempotent only (within-session safety)
const producer = kafka.producer({
  idempotent: true,
});

// Option 2: Idempotent + restart-safe + transactional (recommended)
const producer = kafka.producer({
  idempotent: true,
  transactionalId: 'orders-producer-1',  // stable, unique per producer instance
});

// Kafka-to-Kafka transactional send
await producer.transaction(async (tx) => {
  await tx.send({
    topic: 'output-topic',
    messages: [{ key: 'user-123', value: JSON.stringify({ result: '...' }) }],
  });
  await tx.sendOffsets({
    consumerGroupId: 'my-group',
    topics: [{ topic: 'input-topic', partitions: [{ partition: 0, offset: '5' }] }]
  });
  await tx.commit();
  // crash here → abort → nothing visible → safe retry on restart
});

// Sending to external system — add idempotency key to payload
await producer.send({
  topic: 'orders',
  messages: [{
    key: 'user-123',
    value: JSON.stringify({
      idempotencyKey: crypto.randomUUID(),  // you create this
      orderId: 'order-456',
      amount: 100,
    })
  }]
});


// ── CONSUMER ──────────────────────────────────────────────

// Always use read_committed (default in kafkajs)
const consumer = kafka.consumer({
  groupId: 'my-group',
  // readUncommitted defaults to false ✅
});

// Consumer writing to external system — check idempotency key
consumer.run({
  eachMessage: async ({ message }) => {
    const { idempotencyKey, orderId, amount } = JSON.parse(message.value)

    const exists = await db.query(
      'SELECT 1 FROM processed_events WHERE idempotency_key = $1',
      [idempotencyKey]
    )
    if (exists.rows.length > 0) return  // already processed

    await db.query('BEGIN')
    await db.query('INSERT INTO orders (id, amount) VALUES ($1, $2)', [orderId, amount])
    await db.query('INSERT INTO processed_events (idempotency_key) VALUES ($1)', [idempotencyKey])
    await db.query('COMMIT')
  }
})


// ── KAFKA STREAMS (Java) ───────────────────────────────────

// One line enables full exactly-once
props.put(
  StreamsConfig.PROCESSING_GUARANTEE_CONFIG,
  StreamsConfig.EXACTLY_ONCE_V2
);
```