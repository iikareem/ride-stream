# Redis Pub/Sub → Socket.IO gateway

Plain-language walkthrough of how live messages reach the browser in this repo.

## The path

```text
Nearby worker / emit:test / Redis Insight
        │
        │  PUBLISH user:rider-001  '{"driver_id":"driver-001",...}'
        ▼
     Redis
        │
        │  message on subscribed channel
        ▼
  Gateway (your Nest code)
        │  parse / shape payload
        │  server.to('user:rider-001').emit('drivers', payload)
        ▼
  Socket.IO room user:rider-001
        ▼
     Browser client
```

The production publisher is **`npm run start:nearby`**: it consumes driver GPS, GEOSEARCHes `riders:geo`, and PUBLISHes to each nearby rider’s channel. `emit:test` and Redis Insight remain smoke tests without Kafka.

Unlike the Socket.IO Redis **adapter**, the gateway **sees every message** in Nest before the client does. That is where you add transform / filter / enrich logic.

## Why a second Redis connection?

A Redis connection in `SUBSCRIBE` mode cannot run normal commands (`GET`, `GEOADD`, …). So:

| Client | Role |
| --- | --- |
| `client` | Commands (GEOADD, PUBLISH from workers, etc.) |
| `subClient` | Only `SUBSCRIBE` / `UNSUBSCRIBE` + `message` events |

## Join flow

1. Client connects with Socket.IO.
2. Client emits `join` with `{ userId: 'rider-001' }`.
3. Gateway joins Socket.IO room `user:rider-001`.
4. Gateway `SUBSCRIBE`s Redis channel `user:rider-001` (refcount if many tabs).
5. Disconnect → `UNSUBSCRIBE` when the last socket for that user leaves.

Room name and Redis channel use the **same string** (`user:{id}`) so the handler can `server.to(channel).emit(...)`.

## Smoke test

With Redis + gateway up, and Postman (or a client) joined as `rider-001` listening for `drivers`:

```bash
npm run emit:test -- rider-001 '{"lat":30.04,"lon":31.23}'
```

Or in Redis Insight: `PUBLISH user:rider-001 '{"hello":true}'`.

## Adapter vs Pub/Sub (why this repo uses Pub/Sub)

| | Redis Pub/Sub (this path) | Socket.IO Redis adapter |
| --- | --- | --- |
| Who delivers to the socket? | **Your** gateway handler → `emit` | Socket.IO library via Redis |
| Can Nest change the payload? | Yes, in the message handler | No (opaque delivery) |
| Redis Insight `PUBLISH` | Works on `user:{id}` | Does not match adapter channels |
| Multi-instance | Each instance that subscribed emits locally | Adapter syncs rooms across processes |

Pub/Sub keeps delivery explicit: the gateway receives each message and emits to Socket.IO.
