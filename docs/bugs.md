# Bug Report — memflux

---

## BUG-1 · `server.go:26` — Stray `f` causes compilation failure
**Severity:** Blocker  
**File:** `server.go:26`

```go
// current (does not compile)
s.store.Delete(req.Key)f

// fix
s.store.Delete(req.Key)
```

The trailing `f` is a syntax error. The project does not build in its current state.

---

## BUG-2 · `client.go:59` — Inverted health check; replication never executes
**Severity:** Critical  
**File:** `client.go:59`

```go
// current — skips healthy followers, only attempts unhealthy ones
if healthy {
    continue
}

// fix — skip unhealthy followers
if !healthy {
    continue
}
```

All followers start as `healthy = true`. The condition as written skips every healthy follower and only enters the replication loop for unhealthy ones — of which there are none at startup. The net effect is that `LeaderClient.Replicate` is a no-op: it iterates all followers and skips all of them, every time.

---

## BUG-3 · `main.go:51` — LeaderClient discarded; no write path calls `lc.Replicate()`
**Severity:** Critical  
**File:** `main.go:51`

```go
// current — lc is immediately thrown away
lc := memflux.NewLeaderClient(addresses)
_ = lc
```

There is no HTTP server or client-facing gRPC service on the leader. `store.Set` and `store.Delete` have no knowledge of `LeaderClient`. Even with BUG-2 fixed, no code path ever calls `lc.Replicate()`, so writes on the leader are never propagated to followers.

**Recommended fix:**  
Add a write API layer (e.g., an HTTP handler) that calls both the store method and `lc.Replicate()` in sequence:

```go
func handleSet(store *memflux.Store, lc *memflux.LeaderClient) http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
        key, value, ttl := /* parse request */
        store.Set(key, value, ttl)
        lc.Replicate("set", key, value, int64(ttl.Seconds()))
    }
}
```

Do **not** embed `LeaderClient` inside `Store`. Storage should have no awareness of replication topology — that coupling would make `Store` untestable in isolation and violate single responsibility.

---

## BUG-4 · `client.go:78` — Unhealthy followers are never recovered
**Severity:** Moderate  
**File:** `client.go:76–81`

```go
if !success {
    lc.mu.Lock()
    follower.healthy = false
    lc.mu.Unlock()
}
// healthy is never set back to true anywhere
```

Once a follower is marked unhealthy it is permanently excluded. A follower that suffers a transient failure (restart, brief network blip) and comes back online will never receive replication again for the lifetime of the leader process.

**Recommended fix:**  
Mark the follower healthy again when a replication attempt to it succeeds:

```go
if success {
    lc.mu.Lock()
    follower.healthy = true
    lc.mu.Unlock()
} else {
    lc.mu.Lock()
    follower.healthy = false
    lc.mu.Unlock()
    log.Printf("marking follower %s as unhealthy", follower.address)
}
```

Alternatively, add a background goroutine that probes unhealthy followers with `HeartBeat` on an interval and restores them.

---

## BUG-5 · `server.go:28` — Typo in error string
**Severity:** Minor  
**File:** `server.go:28`

```go
// current
Error: "unknown opperation"

// fix
Error: "unknown operation"
```

---

## Ticket Analysis

### Ticket 2 — Is the `follower.healthy` read a real data race?

**No.** Both the read and the write are already under `lc.mu`:

```go
// read
lc.mu.Lock()
healthy := follower.healthy   // guarded
lc.mu.Unlock()

// write
lc.mu.Lock()
follower.healthy = false      // guarded
lc.mu.Unlock()
```

The Go race detector would not flag this. There is a TOCTOU window between the read and the replication attempt (another goroutine could theoretically change `healthy` in between), but that is benign: the worst outcome is a single wasted or duplicated replication attempt.

The real problem with this code is the inverted condition (BUG-2), not a race.

---

### Ticket 3 — Is skipping sequence validation in `server.go` a bug?

**Not under the current architecture.** gRPC uses HTTP/2 over TCP, which guarantees ordered, reliable delivery per stream. With a single leader and one persistent connection per follower, the follower receives `Replicate` calls in the exact order the leader sent them. There is no scenario in this design where sequence numbers could arrive out of order.

The `sequence` field is generated (`atomic.AddInt64`) and transmitted but never read by the receiver — it is dead weight right now. This becomes a real concern only if the architecture changes to:

- Multiple parallel goroutines sending to the same follower connection, or  
- Multiple leaders writing concurrently (no such path exists today)

For now this is a design smell, not a correctness bug. If sequence enforcement is desired for future-proofing, the receiver should reject any request whose sequence is not exactly `lastSeen + 1` and return an error so the leader can retransmit.
