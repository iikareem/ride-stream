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
| Live map | `gps-events` | `ridestream-live-map` |
| Anomaly | `gps-events` → `driver-anomalies` | `ridestream-anomaly` |

Same topic + **different** groups = each service gets a full independent stream (own offsets).  
Same topic + **same** group = replicas sharing partitions for scale.

---

### Q15. Different consumers = different commands, each knowing its topic?

Yes on commands and “knows its topic(s).”

Isolation is by **`groupId`**, not always by different topics. ETA and live-map both read `gps-events` with different groups.

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

*(Smaller payloads, enforced schema, safe evolution with compatibility modes.)*

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

## Quick command cheat sheet

```bash
# Infra
docker compose up -d
docker compose logs init-topics
docker compose down          # wipes data (no volumes)

# Apps
npm run start:producer
npm run start:consumer       # run twice = same group, split partitions

# UI
open http://localhost:8080
```

---

## CV one-liner (from README)

> Built a real-time GPS tracking pipeline simulating a ride-sharing backend using Apache Kafka and NestJS. Partitioned events by driver ID for strict ordering, implemented stateful stream processing for anomaly detection, and designed for exactly-once delivery with Schema Registry and Prometheus/Grafana observability.

---

*Last updated from early Phase 1 learning session. Add new Q&As as you go.*
