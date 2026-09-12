---
tags:
  - ride-stream
  - redis
  - pubsub
  - geo
---

# Redis in RideStream

> Part of [RideStream](../RideStream.md). The encoding behind `GEOADD` is [Geohash — Complete Guide](Geohash%20%E2%80%94%20Complete%20Guide.md). The "gateway does not own business logic" rule is [WebSocket Gateway Pattern](../05%20WebSocket/WebSocket%20Gateway%20Pattern.md).

Geohash is how the score is built. This note is what Redis is doing in the pipeline, and what it is not.

---

## Two jobs, neither of them "be Kafka"

| Job | Command | Key / channel |
| --- | --- | --- |
| Current rider position | `GEOADD` / `GEOSEARCH` | sorted set `riders:geo` |
| Tell a connected rider "a driver moved near you" | `PUBLISH` / `SUBSCRIBE` | channel `user:{riderId}` |

Kafka is the durable log of location events. Redis is the latest point on the map, plus a live hint. `docker compose down` drops Kafka messages. Redis Insight is the only service with a volume, and that volume is for the UI, not a design that Pub/Sub is durable.

If a rider was disconnected when the nearby worker published, that message is gone. On reconnect they only see publishes that happen after they subscribe. Push is a hint. If I ever need "what did I miss," that is a Kafka topic or Redis Streams, not Pub/Sub.

---

## Write path — rider position

```text
gps-events-rider
  → rider-geo worker (group ridestream-rider-geo)
  → GEOADD riders:geo  <lng> <lat> <rider_id>
```

Argument order is longitude, then latitude. Easy to swap. Redis stores a 52-bit geohash integer as the sorted-set score and the rider id as the member. A second `GEOADD` for the same rider replaces the score. The index is "where are you now," not a trail.

How that integer is built, the 9-cell search, and the Haversine filter are in [Geohash — Complete Guide](Geohash%20%E2%80%94%20Complete%20Guide.md).

---

## Read path — who is near this driver

```text
gps-events-driver
  → nearby worker (group ridestream-nearby)
  → GEOSEARCH riders:geo FROMLONLAT <lng> <lat> BYRADIUS <km>
  → for each rider_id: PUBLISH user:{rider_id}  <driver GPS JSON>
```

Empty index means zero publishes (`riders=0`). Rider GEO has to be running first.

`PUBLISH` returns how many subscribers got the message. The worker does not wait for the browser. There is no ack.

Matching stays in the nearby worker. The gateway does not run `GEOSEARCH`.

---

## Why a second Redis connection

A connection in `SUBSCRIBE` mode cannot run `GET`, `GEOADD`, or `PUBLISH`. So `RedisService` opens two:

| Client | Allowed |
| --- | --- |
| `client` | Normal commands. Workers use this for GEOADD, GEOSEARCH, PUBLISH |
| `subClient` | `SUBSCRIBE`, `UNSUBSCRIBE`, and `message` events. Gateway only |

The gateway does not import Socket.IO into the Redis service. It registers a callback (`onUserChannelMessage`). The service only knows a string arrived on a channel.

---

## Join, room, refcount

Room name and channel name are the same string: `user:{userId}`. That is why the handler can do `server.to(channel).emit('drivers', payload)` with no lookup table.

```text
socket emits join { userId }
  → join Socket.IO room user:{id}
  → SUBSCRIBE user:{id}   (only if refcount was 0)
PUBLISH user:{id}
  → subClient "message"
  → emit 'drivers' to the room
last socket for that user disconnects
  → UNSUBSCRIBE
```

Two browser tabs are two sockets. They share a user id, so they share a room and a channel. The refcount is why the first tab closing does not unsubscribe the channel out from under the second tab:

```text
first join     0 → 1    SUBSCRIBE
second join    1 → 2    no second subscribe
first leave    2 → 1    stay subscribed
last leave     1 → 0    UNSUBSCRIBE
```

One emit to the room reaches every socket in it. That is how both tabs get the same driver update.

---

## Pub/Sub vs the Socket.IO Redis adapter

This repo does not use the adapter. The gateway sees every Redis message in Nest, then emits. That is the transform point: parse, drop, rename, then `emit`.

| | Explicit Pub/Sub (this repo) | Socket.IO Redis adapter |
| --- | --- | --- |
| Who writes the socket | Gateway handler | The Socket.IO library |
| Can I change the payload? | Yes | No. Delivery is opaque |
| `PUBLISH` from Redis Insight | Works on `user:{id}` | Does not match adapter channels |
| Many gateway processes | Only the process that subscribed emits | Adapter syncs rooms across processes |

Broadcast-style Pub/Sub is the simple version of the fan-out problem in [WebSocket Gateway Pattern](../05%20WebSocket/WebSocket%20Gateway%20Pattern.md). One gateway process subscribes to the channels its sockets joined. I have not built directed routing (`user → gateway node`). That comes when one process cannot hold the connections.

---

## Auth is not in Redis

The token belongs on the socket handshake, not in the Kafka record and not in the `PUBLISH` body. Those are internal. After the handshake, the connection is the session. See [WebSocket API Design Guide](../05%20WebSocket/WebSocket%20API%20Design%20Guide.md) for per-connection auth. The demo gateway still trusts the `userId` in `join`. The design note is the rule; the demo is the shorter path.

---

## See it locally

The publisher in the running pipeline is `npm run start:nearby`. It consumes driver GPS, `GEOSEARCH`es `riders:geo`, and `PUBLISH`es to each nearby rider. `emit:test` and Redis Insight are the same path without Kafka.

With Redis and the gateway up, and a client joined as `rider-001` listening for `drivers`:

```bash
npm run emit:test -- rider-001 '{"driver_id":"driver-001","latitude":30.04,"longitude":31.23,"speed_kmh":42,"status":"available"}'
```

Or in Redis Insight: `PUBLISH user:rider-001 '{"hello":true}'`.

The gateway serves `client/index.html` at `http://localhost:3001/`. Connect as `rider-001`. The page listens for Socket.IO event `drivers`, shows a latest-per-driver table, and an event log.

## See it locally

The publisher in the running pipeline is `npm run start:nearby`. It consumes driver GPS, `GEOSEARCH`es `riders:geo`, and `PUBLISH`es to each nearby rider. `emit:test` and Redis Insight are the same path without Kafka.

With Redis and the gateway up, and a client joined as `rider-001` listening for `drivers`:

```bash
npm run emit:test -- rider-001 '{"driver_id":"driver-001","latitude":30.04,"longitude":31.23,"speed_kmh":42,"status":"available"}'
```

Or in Redis Insight: `PUBLISH user:rider-001 '{"hello":true}'`.

The gateway serves `client/index.html` at `http://localhost:3001/`. Connect as `rider-001`. The page listens for Socket.IO event `drivers`, shows a latest-per-driver table, and an event log.

## One-liners

| Question | Answer |
| --- | --- |
| Why Redis at all? | Latest position and a live push. Kafka is a bad "who is within 2 km right now" index |
| Does Pub/Sub replay? | No |
| Why two clients? | Subscriber mode cannot run normal commands |
| Why refcount? | Several tabs, one `SUBSCRIBE` |
| Who publishes? | The nearby worker, not the gateway |
| GEO argument order? | Longitude, then latitude |
