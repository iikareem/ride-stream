> Part of [RideStream](../RideStream.md).

Here's a clear, practical breakdown — written from the perspective of someone who's used to a single broker and is now stepping into a multi-broker setup.

---

# Kafka Multi-Broker Concepts (From Single → Cluster Thinking)

---

## 1. The 3-Broker Cluster

### What changed from single-broker?

With one broker, everything lives in one place — topics, partitions, data. If it dies, everything dies.

A 3-broker cluster means you have **3 independent Kafka nodes** (Broker 0, Broker 1, Broker 2).
Kafka distributes your topic's partitions across them.

```
Topic: "orders" → 3 partitions  
┌──────────┐ ┌──────────┐ ┌──────────┐  
│ Broker 0 │ │ Broker 1 │ │ Broker 2 │  
│ P0 (L) │ │ P1 (L) │ │ P2 (L) │  
└──────────┘ └──────────┘ └──────────┘  
L = Leader for that partition
```

Each partition has **one leader** (handles all reads/writes) and **replicas** spread across other brokers.

---

## 2. Replication

### The idea

Replication is just Kafka copying your partition data to multiple brokers so you don't lose it when one dies.

When you create a topic, you set a **replication factor**:

```bash
--replication-factor 3
```

This means every partition gets **3 copies** — one on each broker.

```
Partition 0:
  Leader  → Broker 0  ← producers write here
  Replica → Broker 1  ← silently syncing
  Replica → Broker 2  ← silently syncing
```

The replicas that are fully caught up are called **ISR — In-Sync Replicas**.  
If a replica falls behind, Kafka drops it from the ISR list.

---

## 3. `min.insync.replicas`

This is a **safety setting** on the broker or topic level. It answers:

> "How many replicas must acknowledge a write before Kafka considers it successful?"

```
min.insync.replicas = 2
```

Combined with `acks=all` on the producer side, a write only succeeds when  
**at least 2 brokers** have confirmed they received it.

### Practical example:

| Cluster state | min.insync.replicas=2 | What happens? |
| --- | --- | --- |
| All 3 brokers up | ✅ | Writes succeed normally |
| 1 broker down (ISR = 2) | ✅ | Still fine, meets the minimum |
| 2 brokers down (ISR = 1) | ❌ | Writes fail — not enough in-sync replicas |

> **Rule of thumb:** replication-factor=3, min.insync.replicas=2  
> This gives you fault tolerance for 1 broker failure without data loss.

---

## 4. Broker Failure & Leader Election

### What happens when a broker dies?

Say Broker 0 goes down and it was the **leader** for Partition 0.

Kafka (via ZooKeeper or KRaft in newer versions) detects the failure and:

1. Removes the dead broker from the ISR
2. Elects a **new leader** from the remaining in-sync replicas
3. Updates the cluster metadata so producers/consumers know the new leader

```
Before failure:        After Broker 0 dies:
  P0 Leader → Broker 0    P0 Leader → Broker 1  ← elected automatically
  P0 Replica → Broker 1   P0 Replica → Broker 2
  P0 Replica → Broker 2
```

Producers and consumers **automatically reconnect** to the new leader.  
There's a brief pause (milliseconds to a few seconds) but no data loss if replication was healthy.

---

### Leader Election Drill (Mental Model)

Run through this scenario mentally:

```
Cluster:  Broker 0, Broker 1, Broker 2
Topic:    "payments", 1 partition, replication-factor=3
ISR:      [0, 1, 2]
Leader:   Broker 0
```

**Drill 1 — One broker dies:**

- Broker 0 crashes
- Kafka detects via missed heartbeats
- Elects Broker 1 as new leader (first in ISR)
- ISR becomes [1, 2]
- System continues ✅

**Drill 2 — Two brokers die (min.insync.replicas=2):**

- Broker 0 and Broker 1 crash
- ISR = [2], only 1 in-sync replica left
- Kafka refuses new writes (doesn't meet min.insync.replicas)
- Consumers can still read existing data ✅
- Producers get NotEnoughReplicasException ❌

**Drill 3 — Dead broker comes back:**

- Broker 0 restarts, rejoins cluster
- Starts syncing data from current leader
- Re-enters ISR once fully caught up
- May or may not reclaim leadership (depends on `auto.leader.rebalance.enable`)

---

## Key Takeaways

| Concept | One-liner |
| --- | --- |
| Broker cluster | Multiple nodes sharing the load and providing backup |
| Replication factor | How many copies of each partition exist |
| ISR | Replicas that are fully caught up with the leader |
| min.insync.replicas | Minimum ISR count required to allow writes |
| Leader election | Automatic promotion of a replica when leader dies |

---

The core mental shift from single-broker: you're no longer thinking about *one machine*, you're thinking about *which broker is the leader for which partition right now*, and what happens when that changes. Once that clicks, the rest is just configuration tuning.
