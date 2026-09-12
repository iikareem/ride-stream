> Part of [RideStream](../RideStream.md).

# WebSocket API Design Guide

> A talking structure for interviews and design discussions. Lead with the four pillars, then go deep only where the interviewer pulls you.

---

## The Answer Structure — 4 Pillars

When someone says *"design a WebSocket API"*, this is the order you speak in:

| # | Pillar | The question it answers |
|---|---|---|
| 1 | **Transport choice** | Do I actually need WebSocket, or is HTTP/SSE enough? |
| 2 | **Message contract** | One pipe, no URLs — how does a message describe itself? |
| 3 | **Connection lifecycle** | How does a connection start, stay alive, and recover? |
| 4 | **Architecture & scale** | Where does business logic live, and how do I run many servers? |

### The 30-Second Opening (say this first)

> "I'd break it into four parts. First, justify the transport — WebSocket only if I need server-initiated pushes or true bidirectional traffic; otherwise SSE or HTTP is simpler. Second, the message contract — since there's one connection and no URL or HTTP method, every message needs an envelope with a type, a routing target, and a correlation id. Third, the connection lifecycle — authentication on connect, heartbeat to detect dead sockets, reconnection with backoff, and one socket per device. Fourth, architecture — the gateway only manages connections, business logic stays in services, and Redis Pub/Sub connects them so I can scale horizontally.
>
> Which part do you want me to go deeper on?"

That last line is the trick. You've shown the full map, and now they pick the branch.

---

## Pillar 1 — Transport Choice

Never open with "I'll use WebSocket." Open by justifying it.

| Option | Direction | Use when |
|---|---|---|
| **HTTP request/response** | Client → Server | Normal CRUD, anything the client initiates |
| **Polling** | Client → Server on a timer | Updates are rare and latency doesn't matter |
| **SSE** (Server-Sent Events) | Server → Client only | Notifications, live dashboards, feeds — plain HTTP, auto-reconnects for free |
| **WebSocket** | Both, persistent | Chat, presence, gaming, collaborative editing, high-frequency two-way traffic |

**Rule of thumb:** if the client never needs to *push* over the socket, SSE is the cheaper answer. WebSocket earns its complexity only when traffic is genuinely bidirectional or very high frequency.

**What WebSocket actually is:** a persistent full-duplex TCP connection that starts life as an HTTP request.

```
Handshake (HTTP Upgrade: 101) → Open → Messaging (both ways) → Close
```

---

## Pillar 2 — Message Contract

### The core problem

In HTTP, the request describes itself: `POST /chat/rooms/42/messages`. The URL says *where*, the method says *what*.

WebSocket has **neither**. There's one connection, opened once, and every message after that is just bytes. The server has no idea whether it's a chat message, a typing indicator, or a heartbeat.

**Everything in this pillar exists to replace what the URL and method gave you for free.**

### 2.1 — There is only one message event

Raw WebSocket exposes exactly three events:

```js
socket.onopen    = () => {}          // connected
socket.onclose   = () => {}          // dropped
socket.onmessage = (event) => {}     // ANY message arrived
```

There is no `socket.on("chat.message")`. That's a Socket.io abstraction, not the protocol. Every message — chat, typing, order, error, pong — arrives through the same `onmessage`, and **you** are the router:

```js
socket.onmessage = (event) => {
  const msg = JSON.parse(event.data);
  switch (msg.type) {
    case "chat.message.received": return handleChat(msg);
    case "user.typing":           return handleTyping(msg);
    case "error":                 return handleError(msg);
    default:                      console.warn("Unknown type:", msg.type);
  }
};
```

> One pipe, one event, one dispatcher. This is *why* the envelope has to exist.

### 2.2 — The envelope

```json
{
  "id": "req_001",
  "namespace": "chat",
  "channel": "room:42",
  "type": "message.send",
  "payload": { "text": "Hello!" },
  "timestamp": "2026-09-10T10:00:00Z",
  "version": "1.0"
}
```

| Field | Role | HTTP equivalent |
|---|---|---|
| `namespace` | Which domain/service owns this | The service or subdomain |
| `channel` | The specific room/topic instance inside it | The resource id in the path |
| `type` | The action to perform | The HTTP method + endpoint |
| `id` | Correlates a response to its request | Nothing — HTTP gets this for free |
| `payload` | The actual data | The body |
| `version` | Lets you evolve the format without breaking old clients | API version |

**Namespace vs channel vs type**, in one line each:
- `namespace: "chat"` — the domain.
- `channel: "room:42"` — *which* conversation inside that domain.
- `type: "message.send"` — what to do once you know where you are.

### 2.3 — Type naming and direction

```
{domain}.{resource}.{action}

chat.message.send
chat.room.join
order.checkout.initiate
```

The router reads left to right — each level narrows the scope.

| Direction | Tense | Example |
|---|---|---|
| Client → Server | Imperative | `message.send`, `room.join` |
| Server → Client | Past tense | `message.received`, `room.joined` |

Now any message in a log tells you who sent it without extra context.

### 2.4 — Correlation (`id`) — why it matters

WebSocket is asynchronous. Responses can arrive **out of order**, and server pushes arrive **unprompted**. So:

- Client sends two requests quickly; the second one finishes first. Without an `id`, the client maps the wrong data to the wrong request.
- An error arrives while three requests are in flight. Without an `id`, you can't tell which one failed.
- A live server push lands at the same moment as a query result. Without an `id`, the client can't tell them apart.

**The fix:** the server echoes the same `id` back. The client keeps a `Map<id, pendingPromise>`, resolves on arrival, deletes the entry.

```
Client → { "id": "req_001", "type": "user.get" }
Server → { "id": "req_001", "type": "user.get.response" }
```

> Server-initiated pushes don't need an `id` — nobody's waiting on them. They just need a clear `type`.

### 2.5 — Errors are just a message type

```json
{
  "id": "req_001",
  "type": "error",
  "payload": {
    "code": "VALIDATION_ERROR",
    "message": "Field 'email' is required",
    "details": { "field": "email" }
  }
}
```

There's no status code in WebSocket, so you build your own taxonomy: `AUTH_*`, `VALIDATION_*`, `NOT_FOUND_*`, `RATE_LIMIT_*`, `SERVER_*`. The echoed `id` tells the client *which* request failed.

---

## Pillar 3 — Connection Lifecycle

Four things, in order: **authenticate → keep alive → recover → deduplicate.**

### 3.1 — Authentication

| Approach | When |
|---|---|
| Token in the first message (`auth.handshake`) | Default choice — works everywhere, easy to rotate |
| HttpOnly cookie on the upgrade request | Browser clients with an existing session |
| `Sec-WebSocket-Protocol` subprotocol header | Token must be present before the upgrade completes |

```json
{ "type": "auth.handshake", "payload": { "token": "Bearer eyJ..." } }
```

Rules: **never put the token in the URL** (it lands in logs and browser history), and reject every business message until auth completes — with a timeout on unauthenticated sockets.

#### How auth persists across messages

> **HTTP authenticates per request. WebSocket authenticates per connection.** The connection object *is* the session.

HTTP re-sends the token every time because the server forgets you the instant the response is sent. WebSocket doesn't have that problem — the connection stays open and the server holds a live object for it. Auth state lives on that object: an `authenticated` flag plus the identity from the verified token. Every later message passes a gate that just reads the flag. No lookup, no database hit, no re-verifying the JWT.

##### The flow

```
1. Client opens socket        → server creates state = { authenticated: false }
2. Client → auth.handshake    → server verifies token, sets state.authenticated = true
                                                        state.user = { id: 7 }
3. Server → auth.ok
4. Client → chat.message.send → gate sees state.authenticated === true → allowed
5. Client → chat.message.send → same state, still true → allowed  (no re-verification)
6. Socket closes              → state is discarded
```

##### The code

```js
wss.on("connection", (socket) => {
  // Runs ONCE per connection — this object belongs to this socket only.
  const state = { authenticated: false, user: null };

  socket.on("message", (raw) => {          // runs on EVERY message, sees the same `state`
    const msg = JSON.parse(raw);

    if (msg.type === "auth.handshake") {   // verify once, remember who this is
      const claims = verifyJwt(msg.payload.token);
      state.authenticated = true;
      state.user = { id: claims.sub };
      return socket.send(JSON.stringify({ id: msg.id, type: "auth.ok" }));
    }

    if (!state.authenticated) {            // the gate every other message passes
      return socket.close(4001, "NOT_AUTHENTICATED");
    }

    route(msg, state.user);                // identity from state — never from the message
  });
});
```

On the client: send `auth.handshake` inside `onopen`, and wait for `auth.ok` before sending anything else.

##### Why the handler already has the right connection

Each connection is a separate TCP socket, so when bytes arrive the network stack fires **only that socket's listeners**. The connection is identified before your code runs — you never search for it. Where you put the state is just a style choice:

| Where the state lives | How you reach it | Cleanup |
|---|---|---|
| Closure variable | Already in scope | Automatic |
| Property on the socket | `socket.state` | Automatic |
| `socket → state` map | `sessions.get(socket)` | **Manual** on close |

> **Inbound: the socket finds you — no lookup. Outbound push: you start from a user id, so you need a real `userId → socket` map.** That asymmetry is why gateways keep a session map at all.

##### Gotchas

| Issue | Why it bites | Fix |
|---|---|---|
| **Impersonation** | If the client sends its own `userId`, anyone can fake anyone | Read identity from server state only |
| **Token expiry** | A 15-min token on a 3-hour socket outlives its own validity | Store `expiresAt`, compare on each message; or refresh over the socket |
| **Revocation** | Logout or ban is invisible to in-memory state | Redis revocation channel → gateway closes that user's sockets |
| **Reconnect** | New connection = empty state | Client re-sends `auth.handshake` before anything else |

### 3.2 — Heartbeat

TCP connections die silently. A proxy or a phone losing signal won't always send a close frame, so the socket looks open on both sides while nothing gets through.

```
Ping every 30–60s → expect pong within 10–15s → no pong = dead → close and reconnect
```

Both sides should do this: the server reaps zombie connections, the client detects a dead link fast instead of sitting in silence.

### 3.3 — Reconnection

```js
function reconnectDelay(attempt) {
  const base   = Math.min(1000 * 2 ** attempt, 30_000);  // cap at 30s
  const jitter = base * 0.3 * Math.random();             // avoid thundering herd
  return base + jitter;
}
```

Jitter is the part people forget. Without it, every client that dropped when your server restarted reconnects at the exact same millisecond and knocks it over again.

**Also mention state recovery:** reconnecting isn't just re-opening a socket. The client must re-authenticate, re-subscribe to its channels, and fetch whatever it missed while offline — usually via a `lastMessageId` or cursor so the server can replay the gap.

### 3.4 — One socket per device (and per browser)

Connections are expensive; namespaces and channels are just routing fields. So:

```
One Device → One Socket → Many namespaces / channels / types (all via message fields)
```

**Never** open a socket per feature:

```js
// ❌ two connections, zero benefit
new WebSocket("wss://api.example.com/ws/chat");
new WebSocket("wss://api.example.com/ws/notifications");

// ✅ one connection, namespace lives in the message
socket.send(JSON.stringify({ namespace: "chat", channel: "room:42", type: "message.send" }));
```

**Server-side deduplication** — new connection for a known `deviceId` kills the old one:

```js
if (connections.has(deviceId)) {
  const old = connections.get(deviceId);
  old.send(JSON.stringify({ type: "session.replaced" }));
  old.close();
}
connections.set(deviceId, socket);
```

**Multi-tab in the browser** — a new tab is a new *view*, not a new *connection*. A `SharedWorker` owns the single socket and fans messages out to every tab:

```
Tab A ──┐
Tab B ──┼── SharedWorker (owns THE socket) ──── Server
Tab C ──┘
```

New tab joins the worker; last tab closing shuts the socket down; a drop is recovered once for everyone. If that's too much machinery for a notification bell, a socket per tab is fine — just cap connections per user on the server.

---

## Pillar 4 — Architecture & Scale

### 4.1 — The one rule

> **The gateway manages connections. Services own business logic. Redis connects them.**

| Responsibility | Owner |
|---|---|
| Holding sockets, auth, heartbeat, routing | WS Gateway |
| Orders, chat, payments, validation | Backend services |
| Which user is on which gateway node | Redis (session map) |
| Delivering a service's output to a client | Redis Pub/Sub → Gateway → socket |

A gateway with business logic in it can't be scaled or deployed independently, which defeats the point.

### 4.2 — The return path (the part interviewers probe)

Services don't hold sockets. So how does an order service reply to a user?

```
Service finishes work
  → publishes to Redis channel "user:7"
  → every gateway node is subscribed
  → the node holding user:7's socket delivers it
```

This is also **how you scale horizontally**. With N gateway nodes, user A may be on node 1 and user B on node 3. Pub/Sub means no node needs to know where anyone else is — it just delivers what it can and ignores the rest.

### 4.3 — Two patterns

**Pattern A — Full WebSocket.** Client sends everything over the socket; the gateway forwards to services; results come back via Pub/Sub.

```
Client ⇄ WS Gateway ──► Services ──► Redis Pub/Sub ──► WS Gateway ──► Client
```

**Pattern B — Hybrid (HTTP in, WebSocket out).** Client actions go over normal HTTP. The socket is receive-only.

```
Client ──POST /order/checkout──► HTTP API ──► Order Service
                                                   │
Client ◄── WS Gateway ◄── Redis Pub/Sub ◄──────────┘
```

| | Pattern A (Full WS) | Pattern B (Hybrid) |
|---|---|---|
| Client sends via | WebSocket | HTTP |
| Server pushes via | WebSocket | WebSocket |
| Immediate ack | Wait for a WS reply | HTTP responds instantly |
| Failure blast radius | Gateway down = both directions dead | HTTP and WS fail independently |
| HTTP ecosystem (caching, tracing, retries) | Lost | Kept |
| Best for | Chat, gaming, collaborative editing | Most production apps — e-commerce, dashboards, notifications |

**Say this out loud in an interview:** Hybrid is the default. Reach for full WebSocket only when client→server traffic is high frequency or genuinely conversational, because then the per-request HTTP overhead actually costs you something.

### 4.4 — Scaling checklist

- **Sticky sessions or a session map** — a socket lives on one node, so either the load balancer pins it or Redis records where it is.
- **Backpressure** — a slow client's send buffer grows without bound. Watch `bufferedAmount`, drop or disconnect past a threshold.
- **Graceful shutdown** — on deploy, send a close frame with a reason so clients back off and reconnect in a staggered way instead of all at once.
- **Fan-out cost** — broadcasting to 10k users in a channel is 10k writes. Room membership belongs in Redis, not in memory on one node.

### 4.5 — Security

- `wss://` only — TLS, never plain `ws://` in production.
- Validate the `Origin` header on the handshake (the browser doesn't enforce CORS for WebSocket).
- Auth gate before any business message is processed.
- Validate and sanitize every incoming message — treat the socket as hostile input, same as an HTTP body.
- Enforce a max frame size (e.g. 64KB) and rate-limit messages per connection.
- Cap connections per user and per IP.

---

## Cheat Sheet

| Pillar | Key point |
|---|---|
| **1. Transport** | Justify WebSocket first. SSE if server-push only; HTTP if client-initiated. |
| **2. Contract** | One pipe, no URL/method → the envelope replaces both. |
| | Three socket events only; you route inside `onmessage` via `type`. |
| | `namespace` = domain, `channel` = room instance, `type` = action. |
| | `id` correlates async responses; pushes need only a `type`. |
| | Errors are a message type with your own code taxonomy. |
| **3. Lifecycle** | Auth in the first message, never in the URL. |
| | Auth is **per-connection, not per-message** — a flag on the connection state, checked by a gate. Reconnect = empty state = re-auth. |
| | Heartbeat 30–60s, dead if no pong in 10–15s. |
| | Reconnect with exponential backoff **plus jitter**, then re-auth and re-subscribe. |
| | One socket per device; SharedWorker for multi-tab; server kills stale sessions. |
| **4. Architecture** | Gateway = connections only. Services = business logic. Redis = the bridge. |
| | Return path: Service → Redis Pub/Sub → Gateway → socket. Also how you scale to N nodes. |
| | Hybrid (HTTP in, WS out) is the default; full WS for chat/gaming/collab. |
| | Watch backpressure, sticky sessions, fan-out cost, graceful shutdown. |
| | `wss://`, Origin check, auth gate, size limits, rate limits. |

---

## Follow-Up Questions You Should Expect

| Question | One-line answer |
|---|---|
| Why not just poll? | Latency and wasted requests; polling can't do sub-second server pushes cheaply. |
| Why not SSE? | SSE is fine — better, even — unless the client also needs to push. |
| How do you scale past one server? | Redis Pub/Sub + session map; any node can deliver to any user it holds. |
| How do you know a connection is dead? | Heartbeat with a pong timeout; TCP alone won't tell you. |
| What happens on reconnect? | Re-auth, re-subscribe, replay missed messages from a cursor. |
| Do you guarantee delivery? | Not by default — add message ids and client acks if you need at-least-once. |
| How do you version the API? | `version` field in the envelope; additive changes to `payload`; never repurpose a `type`. |
