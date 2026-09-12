> Part of [RideStream](../RideStream.md).


# WebSocket Gateway Pattern

> **One rule:** use HTTP when *I want something*, use WebSocket when *tell me when something happens*.

RideStream's gateway is this pattern with Redis Pub/Sub in front of Socket.IO. The join, publish, and browser path is in [Redis in RideStream](../04%20Redis/Redis%20in%20RideStream.md). The gateway does not consume Kafka. It joins room `user:{id}`, subscribes to the channel with the same name, and emits Socket.IO event `drivers`. The page is `client/index.html` at `http://localhost:3001/`.

RideStream's gateway is this pattern with Redis Pub/Sub in front of Socket.IO. The join, publish, and browser path is in [Redis in RideStream](../04%20Redis/Redis%20in%20RideStream.md). The gateway does not consume Kafka. It joins room `user:{id}`, subscribes to the channel with the same name, and emits Socket.IO event `drivers`. The page is `client/index.html` at `http://localhost:3001/`.

---

## Table of Contents

1. [What a WebSocket Gateway Is](#1-what-a-websocket-gateway-is)
2. [Why the Pattern Exists](#2-why-the-pattern-exists)
3. [What the Gateway Owns](#3-what-the-gateway-owns)
4. [The Two Directions Are Not the Same](#4-the-two-directions-are-not-the-same)
5. [Example — A Chat App, Step by Step](#5-example--a-chat-app-step-by-step)
6. [Why Not Send Commands Over the Socket?](#6-why-not-send-commands-over-the-socket)
7. [The One Thing Only WebSocket Can Do](#7-the-one-thing-only-websocket-can-do)
8. [When to Break the Rule](#8-when-to-break-the-rule)
9. [The Fan-Out Problem](#9-the-fan-out-problem)
10. [Two Traps](#10-two-traps)
11. [Quick Reference](#11-quick-reference)

---

## 1. What a WebSocket Gateway Is

A **WebSocket Gateway** is a server whose job is *connections*, not *business*.

It holds the open sockets. It does not know what an "order" is.

```
                          ┌──────────────────┐
   clients ═══ ws ════════│ WebSocket        │
   clients ═══ ws ════════│ Gateway          │
   clients ═══ ws ════════│ (no business     │
                          │  logic)          │
                          └────────▲─────────┘
                                   │ subscribe
                             ┌─────┴──────┐
                             │   Redis    │
                             │   PubSub   │
                             └─────▲──────┘
                                   │ publish
                    ┌──────────────┴──────────────┐
                    │  order-service, chat-service │
                    │  (all the real logic)        │
                    └──────────────────────────────┘
```

So yes — the gateway only handles sockets, and it reaches the rest of the system through Redis PubSub (or Kafka / NATS / RPC).

---

## 2. Why the Pattern Exists

A WebSocket connection is **stateful** and **long-lived**. It is glued to one process for minutes or hours.

That is the opposite of how business services should behave. Those should be **stateless**, easy to scale, and safe to restart on every deploy.

| | WebSocket connections | Business services |
|---|---|---|
| State | Stateful, pinned to one node | Stateless |
| Lifetime | Hours | One request |
| Restart cost | Kills every client | Nothing, just retry |
| Scaling driver | Number of users online | Amount of work |

If you mix them in one process:

- Every deploy kills every connection
- You cannot scale them separately (maybe you need **20** gateway nodes for 500k idle users, but only **3** nodes for the actual order processing)

So we split them.

---

## 3. What the Gateway Owns

- The WS handshake, TLS, protocol upgrade
- **Authentication at connect time** — validate the token, resolve the user, attach identity to the connection
- The **connection registry** — which connection belongs to which user, and which rooms/topics it subscribed to
- Heartbeats, ping/pong, dead connection detection
- Backpressure — what to do when a client is slow and the send buffer fills up
- Rate limiting and serialization
- **Fan-out** — take one backend event and write it to the N sockets that care

---

## 4. The Two Directions Are Not the Same

Your app does **two different things**, and they have different needs:

| Direction | Meaning | Example | Road to use |
|---|---|---|---|
| **Client → Server** | I want to *do* something | "Send this message" | **HTTP** |
| **Server → Client** | Something happened | "Ahmed sent you a message" | **WebSocket** |

This is the part people get wrong. Many production systems **do not use the WebSocket for client → server at all**. The socket is used only to *receive* pushes.

---

## 5. Example — A Chat App, Step by Step

You are in a room with Ahmed. You type `hello` and press Send.

### Step 1 — You send your message (HTTP)

```
You  ---- POST /messages {"text": "hello"} ---->  chat-service
You  <---- 200 OK {"id": 55, "status": "saved"} -- chat-service
```

A normal API call. Same as any REST endpoint you already wrote. You get a normal response.

### Step 2 — The server saves it and announces it

```
chat-service  ---> save to database
chat-service  ---> Redis publish: "new message in room 10"
```

### Step 3 — Ahmed receives it (WebSocket)

Ahmed did not ask for anything. He is just sitting there. But the message must reach him.

```
Redis  ---> Gateway  ---ws--->  Ahmed's phone
```

**This is the only step that needs WebSocket**, because the server is talking first.

### Step 4 — You also receive it

You also have a socket open, so you get the same push.

```
Redis  ---> Gateway  ---ws--->  Your phone
```

### The point

Look at Step 1 again. You sent your message with **HTTP**, even though your WebSocket was already open and ready.

**Why?** Because HTTP already knows how to do this. WebSocket does not.

---

## 6. Why Not Send Commands Over the Socket?

### a) You know which answer belongs to which question

**HTTP:** one question, one answer. The library handles it.

```
You ask   ---> "save this message"
You get   <--- "OK, saved, id = 55"
```

**WebSocket:** a socket is just a **pipe**. Data goes in, data comes out. Nothing is connected to anything.

```
You send:     {"action": "save message"}
You send:     {"action": "delete message"}
You send:     {"action": "edit message"}

You receive:  {"status": "ok"}     <-- ok for WHICH one???
You receive:  {"status": "error"}  <-- error for WHICH one???
```

So you must add a **correlation id** yourself:

```
You send:     {"reqId": "aaa", "action": "save message"}
You receive:  {"reqId": "aaa", "status": "ok"}     <-- now you know
```

And you must write code to:

- remember every question you sent (a pending-requests map)
- match every answer to the right question
- clean up old entries
- handle "no answer came after 10 seconds"
- handle "connection died while 5 questions were waiting"

**This is work. HTTP does all of it for you, already.**

### b) Errors that everyone understands

HTTP gives you numbers every tool in the world already knows:

```
401 = not logged in
403 = not allowed
404 = not found
429 = too many requests
500 = server broke
```

Over a socket you invent your own envelope:

```json
{ "error": "NOT_ALLOWED" }
```

Now only your code understands it. Your logs, dashboards, and alerts see nothing.

### c) Retry and idempotency

**HTTP:** the request failed? Send it again with a key so the server does not do it twice.

```
POST /messages
Idempotency-Key: abc-123
```

The server sees the same key twice and replies "already did this one, here is the same answer."

**WebSocket:** the connection died mid-command. Did the server receive it or not? **You don't know.**

- Send again → maybe the message appears twice
- Don't send again → maybe it is lost

You must build the whole dedup story yourself.

### d) Your existing middleware still works

You already have this, and it runs on **HTTP routes**:

```
check token        ✅ works on HTTP routes
check rate limit   ✅ works on HTTP routes
write log          ✅ works on HTTP routes
validate input     ✅ works on HTTP routes
tracing headers    ✅ works on HTTP routes
```

Inside a WebSocket, **none of it runs**. Your infrastructure sees one connection that stays open for two hours — opaque, invisible. You must write all of it again from zero.

Same for tracing: OpenTelemetry propagates through HTTP headers automatically. In your tracing UI, a socket is a single span lasting two hours with nothing inside it.

### e) Load spreads across servers

**HTTP** — every request routed independently:

```
request 1 -> server A
request 2 -> server C
request 3 -> server B
```

**WebSocket** — glued to one node, forever:

```
all your messages -> server A (always)
```

### f) Deploy without breaking

**HTTP:** restart the server, the request fails once, the client retries, fine.

**WebSocket:** restart the server, the connection dies, all in-flight requests are lost, the client must reconnect and figure out what happened.

---

## 7. The One Thing Only WebSocket Can Do

Everything above says HTTP is better. So why use WebSocket at all?

> **HTTP can only answer when you ask.**
> **WebSocket lets the server talk to you when you did *not* ask.**

Ahmed's phone sent no request, but the message still arrived. **Only WebSocket (or SSE) can do that.**

So: use the socket for the one thing only it can do, and use HTTP for everything else. It is not wasteful — a POST costs a few milliseconds on an already-warm connection, and you get a decade of tooling for free.

SSE is enough when the server needs to push and the client never needs to push back.

---

## 8. When to Break the Rule

Send commands over the socket when the data is:

1. **Very frequent** — many times per second
2. **Small and cheap** — losing one is not a problem
3. **Latency-sensitive** — every millisecond matters

Examples:

- Figma-style cursor positions (60/sec)
- Player movement in a game
- Trading order entry where microseconds are money
- WebRTC signaling
- Collaborative editing keystrokes

A chat message or an order submission is **none** of these. It happens once every few seconds and it is important enough to want retries and idempotency. So: HTTP.

---

## 9. The Fan-Out Problem

You have 10 gateway nodes. An event for user X arrives. **Which node holds user X's socket?**

### Option A — Broadcast

Every gateway subscribes to the same Redis channel, receives every event, and drops the ones for users it does not hold.

- ✅ Trivially simple
- ❌ Every node processes 100% of traffic — dies at scale

### Option B — Directed routing

Keep a presence registry in Redis (`user:123 → gateway-node-7`) and publish to a node-specific channel.

- ✅ Scales properly
- ❌ Now you own distributed state: stale entries when a node crashes, one user on three devices across three nodes, TTL refresh, reconnect races

**Start with broadcast. Move to directed routing when the numbers force you.**


---

## 10. Two Traps

### Trap 1 — "No business logic" does not mean "no logic"

The gateway **must** enforce **subscription authorization**.

When a client says `subscribe to chat:room:42`, something must decide whether this user is allowed to hear that room. That is a **security boundary**. Skip it and anyone can subscribe to anything.

### Trap 2 — Redis PubSub is fire-and-forget

No persistence. No delivery guarantee. No replay. If a gateway node is restarting or a client is mid-reconnect, those messages are **gone**.

Two ways to live with it:

1. **Push is a hint, fetch is the truth.** On reconnect, the client re-fetches state over HTTP. The socket only says "something changed, go look." ← *most teams should do this*
2. **Redis Streams or Kafka** with a per-connection cursor, so you can replay from where the client stopped. ← *real work*

### Bonus trap — graceful shutdown

The hardest operational problem in this pattern. You deploy, a node holding 50,000 connections goes away, and all 50,000 clients reconnect **at the same time**. Without exponential backoff **with jitter** on the client, they form a herd and take down the nodes that are still up.

Plan for this on day one. Without jitter, the reconnects arrive as one herd.

---

## 11. Quick Reference

| Question | Answer |
|---|---|
| Send a message | HTTP |
| Create an order | HTTP |
| Delete something | HTTP |
| Receive a new message | WebSocket |
| Receive "order shipped" | WebSocket |
| Receive "user is typing" | WebSocket |
| Cursor position 60×/sec | WebSocket (exception) |

**The rule:**

- **I want something → HTTP**
- **Tell me when something happens → WebSocket**

---

