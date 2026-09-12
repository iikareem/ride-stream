---
tags:
  - geohash
  - geo-search
  - redis
  - spatial-indexing
  - system-design
---
> Part of [RideStream](../RideStream.md).

# Geohash — Complete Guide

---

## Table of Contents

1. [The Core Idea](#1-the-core-idea)
2. [How the Code Is Built](#2-how-the-code-is-built)
3. [The Key Property — Shared Prefix = Close Together](#3-the-key-property--shared-prefix--close-together)
4. [Geohash Precision](#4-geohash-precision)
5. [The Edge Problem](#5-the-edge-problem)
6. [How Geohash Is Stored in Redis](#6-how-geohash-is-stored-in-redis)
7. [Write Path — What Happens on GEOADD](#7-write-path--what-happens-on-geoadd)
8. [Read Path — What Happens on GEOSEARCH](#8-read-path--what-happens-on-geosearch)
9. [Both Paths Side by Side](#9-both-paths-side-by-side)
10. [Why It Is Fast](#10-why-it-is-fast)
11. [When to Use Geohash](#11-when-to-use-geohash)
12. [Real Scenarios](#12-real-scenarios)

---

## Before We Start — One Analogy

Think about how you find a book in a library.

You don't walk through every single book. You go to the **floor**, then the **section**, then the **shelf**, then the **book**. The library is organized in a way that lets you skip huge amounts of irrelevant space.

Geohash does the same thing for locations on a map. It organizes geographic data so a database can skip irrelevant areas and jump straight to what's nearby.

---

## 1. The Core Idea

You have millions of locations in a database. A user says "show me everything within 5 km of me." You need to answer that fast.

The naive solution is: check every single location. That's too slow.

Geohash's solution: **assign every location a short code, so that locations with similar codes are physically close to each other.** Then instead of checking everything, you only check locations that share a similar code.

The one thing to keep in mind throughout everything below:

> **Geohash converts a 2D location (lat + lng) into a single 1D number, then stores that number in a sorted list. Everything else follows from that.**

A sorted list is easy and fast to search by range. That is the entire trick.

---

## 2. How the Code Is Built

Imagine printing the entire world map on a piece of paper and drawing a grid over it.

```
┌──────┬──────┬──────┬──────┐
│  A1  │  A2  │  A3  │  A4  │
├──────┼──────┼──────┼──────┤
│  B1  │  B2  │  B3  │  B4  │
├──────┼──────┼──────┼──────┤
│  C1  │  C2  │  C3  │  C4  │
├──────┼──────┼──────┼──────┤
│  D1  │  D2  │  D3  │  D4  │
└──────┴──────┴──────┴──────┘
```

Every point on Earth falls in exactly one cell. Geohash turns each cell into a short string code by repeatedly splitting the map in half and writing down which half your point falls in.

**Think of it like a game of "hotter or colder" played on a map:**

```
Round 1: Is Cairo in the LEFT half or the RIGHT half of the world?
         → RIGHT half   → write "1"

Round 2: Is Cairo in the TOP half or the BOTTOM half of the right side?
         → TOP half     → write "1"

Round 3: Is Cairo in the LEFT half or the RIGHT half of that area?
         → RIGHT half   → write "1"

Round 4: ...
```

After enough rounds, you've zeroed in on a tiny area. The sequence of answers (1s and 0s) gets converted into a short readable string like `"s174dr"`.

That string is the geohash. It's just an address for a cell on the grid.

---

## 3. The Key Property — Shared Prefix = Close Together

> **Locations that are physically close share the same prefix.**

```
Cairo Cafe   →  s174dr7
Nile Grill   →  s174dr3
Tahrir Park  →  s174dq9
London cafe  →  gcpuvp4
```

Cairo Cafe and Nile Grill both start with `s174dr` — they're in the same small area. Tahrir Park starts with `s174d` — same neighborhood, slightly further. London is `gcpu` — completely different prefix, completely different part of the world.

So searching for "nearby" becomes: **find everything that shares the same prefix.**

---

## 4. Geohash Precision

You can control how precise (how small the cell is) by choosing the length of the hash:

```
Length 1  →  continent-sized area   (~5,000 km wide)
Length 3  →  country-sized area     (~150 km wide)
Length 5  →  city-sized area        (~5 km wide)
Length 7  →  neighborhood-sized     (~150 m wide)
Length 9  →  a few meters
```

In practice most apps use length 6 or 7 for nearby searches.

---

## 5. The Edge Problem

Geohash has one well-known weakness. Two locations can be physically right next to each other but have completely different prefixes — if they sit on opposite sides of a cell boundary.

```
┌──────────────┬──────────────┐
│              │              │
│  s174dr  🏪  │  🏪  s174ds  │
│              │              │
└──────────────┴──────────────┘
              ↑
     boundary between cells
```

These two restaurants are only a few meters apart, but they have different geohashes. A prefix search on `s174dr` would miss the one on the right.

**The fix:** always search the cell you're in PLUS the 8 neighboring cells. Every real system that uses Geohash does this automatically (including Redis).

```
┌──────┬──────┬──────┐
│  NW  │  N   │  NE  │
├──────┼──────┼──────┤
│  W   │  YOU │  E   │
├──────┼──────┼──────┤
│  SW  │  S   │  SE  │
└──────┴──────┴──────┘
  Always search all 9 cells
```

---

## 6. How Geohash Is Stored in Redis

Redis converts the geohash string into a 52-bit integer. Then it stores all locations in a sorted set, ordered by that number.

```
Sorted Set "restaurants":
┌─────────────────┬──────────────────────┐
│ score (number)  │ member (your uuid)   │
├─────────────────┼──────────────────────┤
│  3 4 7 6 1 0 0  │  uuid-aaa            │
│  3 4 7 6 7 8 8  │  uuid-bbb            │
│  3 4 7 6 8 6 1  │  uuid-cairo-cafe     │
│  3 4 7 6 9 0 0  │  uuid-nile-grill     │
│  3 5 1 0 0 0 0  │  uuid-far-away-place │
└─────────────────┴──────────────────────┘
         ↑ sorted by number = sorted by location
```

Because close locations produce close numbers, nearby restaurants are literally next to each other in this list. A search is just: "give me everything between score X and score Y."

---

## 7. Write Path — What Happens on `GEOADD`

```
GEOADD restaurants 31.2357 30.0444 "uuid-cafe-123"
                   ─────── ───────  ──────────────
                   lng     lat      your ID (member)
```

Five stages happen internally the moment you call this.

---

### Stage 1 — Slice longitude into binary

Longitude lives between -180 and +180. Geohash asks one question, over and over:

> "Is my value in the LEFT half or the RIGHT half of the current range?"

Each answer is a 0 (left) or 1 (right). After 26 rounds, you have 26 bits that describe exactly where on the horizontal axis your point sits.

```
Starting range: -180 ←──────────────────────→ +180
                                 ↑ midpoint = 0

Round 1:  31.2357 > 0  → RIGHT  → write 1   range is now: 0 → +180
Round 2:  31.2357 < 90 → LEFT   → write 0   range is now: 0 → +90
Round 3:  31.2357 < 45 → LEFT   → write 0   range is now: 0 → +45
Round 4:  31.2357 > 22 → RIGHT  → write 1   range is now: +22.5 → +45
Round 5:  31.2357 < 33 → LEFT   → write 0   range is now: +22.5 → +33.75
... (26 rounds total)

Longitude bits → 1 0 0 1 0 ...  (26 bits)
```

Each round cuts the uncertainty in half. After 26 rounds, you've pinpointed the longitude to within a few meters.

---

### Stage 2 — Slice latitude into binary

Same exact process, but latitude lives between -90 and +90.

```
Starting range: -90 ←──────────────────────→ +90
                                ↑ midpoint = 0

Round 1:  30.0444 > 0  → RIGHT  → write 1   range is now: 0 → +90
Round 2:  30.0444 < 45 → LEFT   → write 0   range is now: 0 → +45
Round 3:  30.0444 > 22 → RIGHT  → write 1   range is now: +22.5 → +45
Round 4:  30.0444 < 33 → LEFT   → write 0   range is now: +22.5 → +33.75
Round 5:  30.0444 > 28 → RIGHT  → write 1   range is now: +28.125 → +33.75
... (26 rounds total)

Latitude bits → 1 0 1 0 1 ...  (26 bits)
```

---

### Stage 3 — Interleave the two bit strings

Now the longitude bits and latitude bits get zipped together, one from each, alternating:

```
Longitude:    1    0    0    1    0    1    1  ...  (26 bits)
Latitude:     1    0    1    0    1    1    0  ...  (26 bits)

Interleaved:
position:     1    2    3    4    5    6    7  ...
source:      lng  lat  lng  lat  lng  lat  lng ...
bit:          1    1    0    0    0    0    1  ...
```

Why interleave? Because it weaves both dimensions into one number. The result is a 52-bit string that encodes BOTH lat and lng together.

The key consequence: two points that are close in BOTH latitude AND longitude will produce bit strings that agree from the very start — making their final numbers numerically close to each other.

---

### Stage 4 — Read the 52 bits as one integer

```
bits:   1 1 0 0 0 0 1 0 1 1 ...  (52 bits total)
         ↓
integer: 3,476,861,756
```

This number IS the geohash score. One number. Encodes both lat and lng.

---

### Stage 5 — Store in the sorted set

```
Key: "restaurants"

┌──────────────────────┬──────────────────────────┐
│ score (geohash int)  │ member (your ID)          │
├──────────────────────┼──────────────────────────┤
│  3,475,900,000       │  uuid-far-south           │
│  3,476,100,000       │  uuid-west-cairo          │
│  3,476,788,000       │  uuid-nile-grill          │
│  3,476,861,756       │  uuid-cafe-123       ← ✅ just inserted
│  3,476,910,000       │  uuid-tahrir-bites        │
│  3,477,200,000       │  uuid-north-cairo         │
│  3,510,000,000       │  uuid-alexandria          │
└──────────────────────┴──────────────────────────┘
          ↑ always sorted smallest → largest
```

Because close locations produce close numbers, nearby restaurants automatically sit next to each other in this list.

---

### Write Path Summary

```
GEOADD restaurants 31.2357 30.0444 "uuid-cafe-123"
        │
        ▼
┌─────────────────────────────────────┐
│  Encode longitude (26 rounds)       │  → 26 bits
│  Encode latitude  (26 rounds)       │  → 26 bits
│  Interleave both                    │  → 52 bits
│  Read as one integer                │  → 3,476,861,756
└─────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────┐
│  Insert into sorted set             │
│  score:  3,476,861,756              │
│  member: "uuid-cafe-123"            │
└─────────────────────────────────────┘
```

Cost: pure arithmetic. No disk lookup. No scanning existing data. Very fast.

---

## 8. Read Path — What Happens on `GEOSEARCH`

```
GEOSEARCH restaurants FROMLONLAT 31.235 30.044 BYRADIUS 5 km ASC
                                 ─────── ──────          ─
                                 your    your           search
                                 lng     lat            radius
```

---

### Stage 1 — Encode the search center

Redis takes your search location and runs the exact same encode process from the write path.

```
31.235, 30.044
    ↓
same 5-stage encode
    ↓
geohash integer: 3,476,860,000   (approximately)
```

---

### Stage 2 — Figure out which number range covers 5 km

Because nearby locations produce nearby numbers, a physical radius (5 km on the ground) translates directly into a numeric range in the sorted set.

```
My geohash:  3,476,860,000

5 km radius covers roughly:
  minimum score: 3,476,200,000
  maximum score: 3,477,500,000
```

This is just arithmetic — no looping through data yet.

---

### Stage 3 — Check 9 cells, not just 1

Geohash cells are square, but your search is a circle. A point that is physically 4.9 km away from you might sit in a neighboring cell whose numeric range is OUTSIDE your calculated range — even though it's within your 5 km radius.

The fix: always check your cell PLUS the 8 surrounding cells. Each neighboring cell also gets translated into a numeric range, and all 9 ranges are searched.

```
┌──────────┬──────────┬──────────┐
│ range 1  │ range 2  │ range 3  │
├──────────┼──────────┼──────────┤
│ range 4  │  YOU     │ range 5  │
│          │ range 6  │          │
├──────────┼──────────┼──────────┤
│ range 7  │ range 8  │ range 9  │
└──────────┴──────────┴──────────┘

Redis searches all 9 numeric ranges in the sorted set.
```

Each range lookup is fast — the sorted set is already ordered, so it's a binary search, not a full scan.

---

### Stage 4 — Collect candidates from the sorted set

```
"Give me all members with score between X and Y"
  → repeated for each of the 9 cell ranges

Result: a small candidate list
┌──────────────────────┬──────────────────┬──────────────────┐
│ score                │ member           │ status           │
├──────────────────────┼──────────────────┼──────────────────┤
│  3,476,510,000       │  uuid-west       │ candidate        │
│  3,476,788,000       │  uuid-nile-grill │ candidate        │
│  3,476,861,756       │  uuid-cafe-123   │ candidate        │
│  3,476,910,000       │  uuid-tahrir     │ candidate        │
└──────────────────────┴──────────────────┴──────────────────┘
```

Out of millions of rows in the sorted set, maybe 20–50 come back as candidates.

---

### Stage 5 — Precise distance filter

The candidates are approximate. Some may be in a cell that overlaps your search area but are actually just outside your 5 km radius.

Redis runs a real Haversine distance calculation (accounts for Earth being a sphere) on each candidate:

```
uuid-west       → actual distance: 6.1 km  ✗ outside 5 km → DROP
uuid-nile-grill → actual distance: 2.8 km  ✓ inside  5 km → KEEP
uuid-cafe-123   → actual distance: 0.3 km  ✓ inside  5 km → KEEP
uuid-tahrir     → actual distance: 4.4 km  ✓ inside  5 km → KEEP
```

This precise calculation runs only on the small candidate list — not on millions of rows.

---

### Stage 6 — Sort and return

```
Result:
  1.  uuid-cafe-123    (0.3 km)
  2.  uuid-nile-grill  (2.8 km)
  3.  uuid-tahrir      (4.4 km)
```

Your application receives these UUIDs, then fetches the full details from your main database using a normal query.

---

### Read Path Summary

```
GEOSEARCH restaurants FROMLONLAT 31.235 30.044 BYRADIUS 5 km ASC
        │
        ▼
┌─────────────────────────────────────────────┐
│  Encode search center → geohash integer     │
└─────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────┐
│  Calculate 9 numeric ranges                 │
│  (your cell + 8 neighbors)                  │
│  that cover the 5 km radius                 │
└─────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────┐
│  Range scan on sorted set                   │
│  "scores between X and Y"  × 9 ranges       │
│  → ~20–50 candidates out of millions        │
└─────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────┐
│  Precise Haversine distance check           │
│  on candidates only                         │
│  → drops points outside the true radius    │
└─────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────┐
│  Sort by distance ASC                       │
│  Return member strings (your UUIDs)         │
└─────────────────────────────────────────────┘
```

---

## 9. Both Paths Side by Side

```
WRITE                              READ
─────                              ────

GEOADD                             GEOSEARCH
  │                                  │
  │  encode lng → 26 bits            │  encode search center → int
  │  encode lat → 26 bits            │
  │  interleave → 52 bits            │  calculate 9 cell ranges
  │  read as integer                 │
  │                                  │  range scan sorted set
  │  insert into sorted set          │  → small candidate list
  │  { score: int, member: uuid }    │
  │                                  │  haversine filter
  ▼                                  │
done                                 │  sort + return UUIDs
                                     ▼
                                    done
```

---

## 10. Why It Is Fast

### Write is fast because:

- Encoding is pure arithmetic — just dividing ranges in half 52 times.
- Inserting into a sorted set is O(log N) — like inserting into a sorted list, not a full rebuild.

### Read is fast because:

- The range scan skips everything outside the 9 numeric ranges — no full scan of millions of rows.
- The expensive distance math (Haversine) only runs on a tiny handful of candidates, not on the whole dataset.

```
Without index:  check all 5,000,000 restaurants → slow
With geohash:   check ~30 candidates → fast
```

---

## 11. When to Use Geohash

- You are working with **points only** (restaurants, users, drivers)
- You need **very fast simple proximity searches** ("what's nearby?")
- You are using **Redis**, Elasticsearch, or a cache layer
- Your data is **read-heavy** (lots of searches, few writes)
- You want **simplicity** — easier to implement and debug
- You are **sharding** data across servers (geohash prefix is a natural shard key)

| Thing | Description |
|---|---|
| What it is | A grid of cells, each with a string code |
| How it stores | One string/number per location |
| How it searches | Find matching prefixes + check 9 neighboring cells |
| Data structure | Sorted set in Redis |
| Speed | Very fast — O(log N) lookup |
| Weakness | Edge problem at cell boundaries, points only |

---

## 12. Real Scenarios

### Scenario A — Uber / Careem (real-time driver matching)

**The need:** Match a rider to the nearest available driver in real-time. Millions of drivers. Locations update every few seconds.

**Answer: Geohash in Redis** — driver locations change constantly, the query is simple ("nearest driver"), and Redis `GEOADD` / `GEOSEARCH` are built for exactly this.

```
Driver opens app → GEOADD drivers <lng> <lat> "driver-uuid"
Rider requests   → GEOSEARCH drivers FROMLONLAT ... BYRADIUS 3 km ASC COUNT 5
```

---

### Scenario B — Social app with "who's nearby right now"

**The need:** Show users who are currently nearby. Locations update as people move. Very high read/write volume.

**Answer: Geohash in Redis** — millions of location updates per minute needs in-memory speed. Simple radius query. Store user UUID in Redis, look up profile details in PostgreSQL when needed.

---

### One Sentence Per Stage

| Stage | Write path | Read path |
|---|---|---|
| 1 | Slice longitude into 26 bits | Encode search center to a number |
| 2 | Slice latitude into 26 bits | Calculate which number range covers your radius |
| 3 | Interleave both bit strings | Expand to 9 neighboring cells |
| 4 | Read bits as one integer | Range scan sorted set → candidates |
| 5 | Store in sorted set | Haversine filter on candidates only |
| 6 | — | Sort and return UUIDs |
