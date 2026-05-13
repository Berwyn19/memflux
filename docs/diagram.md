# memflux Architecture

---

## 1. System Topology

```
                        ┌─────────────────────────────────────────────────┐
                        │                  LEADER  (node-8001)             │
                        │                                                   │
          ┌─────────────►  HTTP :9001   /get /set /delete                  │
          │             │       │                                           │
   Client │             │       ▼                                           │
          │             │   Store (in-memory map + sweep goroutine)         │
          │             │       │                                           │
          │             │       ▼                                           │
          │             │  LeaderClient ──── gRPC Replicate ──────────────►│──────────────────────┐
          │             │       │            :8002                          │                      │
          │             │       └─────────── gRPC Replicate ──────────────►│──────────────────┐   │
          │             │                    :8003                          │                  │   │
          │             │                                                   │                  │   │
          │             │  gRPC :8001  ◄── HeartBeat (from followers)       │                  │   │
          │             └─────────────────────────────────────────────────-┘                  │   │
          │                                                                                    │   │
          │             ┌──────────────────────────────────────────────────┐                  │   │
          │             │               FOLLOWER  (node-8002)              │◄─────────────────┘   │
          └────────────►│                                                  │                      │
                        │  HTTP :9002   /get (read-only)                   │                      │
                        │       │                                          │                      │
                        │       ▼                                          │                      │
                        │  Store (in-memory map + sweep goroutine)         │                      │
                        │       ▲                                          │                      │
                        │       │                                          │                      │
                        │  gRPC :8002  ◄── Replicate (from leader)         │                      │
                        │                                                  │                      │
                        │  heartbeat goroutine ──► gRPC HeartBeat :8001   │                      │
                        └──────────────────────────────────────────────────┘                      │
                                                                                                  │
                        ┌──────────────────────────────────────────────────┐                      │
                        │               FOLLOWER  (node-8003)              │◄─────────────────────┘
                        │  (same structure as node-8002)                   │
                        └──────────────────────────────────────────────────┘
```

---

## 2. Leader Startup Sequence

```
main()
  │
  ├─ flag.Parse()
  │    --port=8001  --role=leader  --followers=:8002,:8003
  │    --http-port derived → 9001
  │
  ├─ memflux.NewStore()
  │    └─ go sweep()  ◄─────────────────── goroutine: evicts expired keys every 1s
  │
  ├─ memflux.NewReplicationServer(store, "node-8001")
  │    └─ holds a reference to the store, serves incoming gRPC calls
  │
  ├─ net.Listen("tcp", ":8001")
  ├─ grpc.NewServer()
  ├─ pb.RegisterReplicationServer(grpcServer, server)
  └─ go grpcServer.Serve(lis)  ◄────────── goroutine: accepts Replicate + HeartBeat RPCs
  │
  ├─ memflux.NewLeaderClient(["localhost:8002", "localhost:8003"])
  │    ├─ grpc.NewClient("localhost:8002")  → Follower{healthy: true}
  │    ├─ grpc.NewClient("localhost:8003")  → Follower{healthy: true}
  │    └─ go recoverFollowers()  ◄────────── goroutine: probes unhealthy followers every 10s
  │
  ├─ go startHTTP("9001", store, lc)  ◄──── goroutine: HTTP API
  │
  └─ select{}  (block forever)
```

---

## 3. Follower Startup Sequence

```
main()
  │
  ├─ flag.Parse()
  │    --port=8002  --role=follower  --leader=localhost:8001
  │    --http-port derived → 9002
  │
  ├─ memflux.NewStore()
  │    └─ go sweep()  ◄─────────────────── goroutine: evicts expired keys every 1s
  │
  ├─ memflux.NewReplicationServer(store, "node-8002")
  │
  ├─ net.Listen("tcp", ":8002")
  ├─ grpc.NewServer()
  ├─ pb.RegisterReplicationServer(grpcServer, server)
  └─ go grpcServer.Serve(lis)  ◄────────── goroutine: accepts Replicate + HeartBeat RPCs
  │
  ├─ lc = nil  (follower has no LeaderClient)
  │
  ├─ go heartbeat goroutine  ◄────────────── goroutine:
  │    ├─ grpc.NewClient("localhost:8001")
  │    └─ every 5s: HeartBeat{leader_id: "node-8002"} → leader :8001
  │         if err → log "leader unreachable"
  │
  ├─ go startHTTP("9002", store, nil)  ◄──── goroutine: HTTP API (lc=nil, writes not replicated)
  │
  └─ select{}  (block forever)
```

---

## 4. Write Path — `POST /set`

```
Client
  │
  │  POST http://localhost:9001/set
  │  Content-Type: application/json
  │  {"key": "foo", "value": "bar", "ttl": 60}
  │
  ▼
Leader HTTP handler (:9001)
  │
  ├─ json.Decode(body) → key="foo", value="bar", ttl=60
  │
  ├─ store.Set("foo", "bar", 60s)
  │    └─ acquires write lock
  │    └─ computes expiresAt = now + 60s
  │    └─ mp["foo"] = entry{value:"bar", expiresAt:...}
  │    └─ releases write lock
  │
  ├─ lc.Replicate("set", "foo", "bar", 60)
  │    │
  │    ├─ sequence = atomic.Add(&lc.sequence, 1)  → seq=1
  │    │
  │    ├─ for follower in [node-8002, node-8003]:
  │    │    │
  │    │    ├─ lock → read follower.healthy → unlock
  │    │    │
  │    │    ├─ if !healthy → skip (will be recovered by recoverFollowers)
  │    │    │
  │    │    └─ retry up to 3×  (500ms timeout each):
  │    │         gRPC Replicate{key:"foo", value:"bar", ttl:60, seq:1, op:"set"}
  │    │              │
  │    │              ▼
  │    │         Follower ReplicationServer.Replicate()
  │    │              ├─ op == "set"
  │    │              ├─ store.Set("foo", "bar", 60s)  (same as leader)
  │    │              └─ return ReplicateResponse{success: true}
  │    │
  │    └─ if all 3 attempts fail → lock → follower.healthy=false → unlock
  │
  └─ 200 OK  ──► Client
```

---

## 5. Read Path — `GET /get?key=foo`

```
Client
  │
  │  GET http://localhost:9001/get?key=foo   (any node)
  │
  ▼
HTTP handler
  │
  ├─ key = r.URL.Query().Get("key")  → "foo"
  │
  └─ store.Get("foo")
       ├─ acquires read lock
       ├─ entry, ok = mp["foo"]
       │    if !ok → missCount++  → return "", false  → 404 Not Found
       │
       ├─ if entry.expiresAt not zero && now > expiresAt
       │    → missCount++ → return "", false  → 404 Not Found
       │
       ├─ hitCount++
       ├─ releases read lock
       └─ return entry.value, true  → 200 "bar\n"
```

---

## 6. Delete Path — `POST /delete`

```
Client
  │
  │  POST http://localhost:9001/delete
  │  {"key": "foo"}
  │
  ▼
Leader HTTP handler
  │
  ├─ json.Decode(body) → key="foo"
  ├─ store.Delete("foo")  → acquires write lock, delete(mp, "foo"), releases lock
  └─ lc.Replicate("delete", "foo", "", 0)
       └─ for each healthy follower:
            gRPC Replicate{key:"foo", op:"delete", seq:N}
                 ├─ op == "delete"
                 ├─ store.Delete("foo")
                 └─ ReplicateResponse{success: true}
```

---

## 7. Background Goroutines

### sweep  — runs on every node, every 1 second
```
ticker fires every 1s
  │
  ├─ pass 1 (read lock):
  │    scan all keys → collect keys where now > expiresAt
  │
  └─ pass 2 (write lock):
       for each expired key:
         re-check expiry (guards against concurrent Set)
         if still expired → delete(mp, key), evictions++
```

### recoverFollowers  — runs on leader only, every 10 seconds
```
ticker fires every 10s
  │
  └─ for each follower:
       if healthy → skip
       gRPC HeartBeat{} → follower :800X  (1s timeout)
         if ok  → lock → follower.healthy=true  → unlock → log "recovered"
         if err → stay unhealthy, retry next tick
```

### heartbeat goroutine  — runs on each follower, every 5 seconds
```
ticker fires every 5s  (time.NewTicker(5 * time.Second), cmd/main.go:75)
  │
  └─ gRPC HeartBeat{leader_id: "node-800X"} → leader :8001  (1s timeout)
       if ok  → (silent)
       if err → log "[node-800X] leader unreachable: ..."

note: the 500ms figure in client.go:65 is the per-attempt deadline on
      gRPC Replicate calls (leader→follower), not a heartbeat interval.
```

---

## 8. Follower Failure & Recovery

```
Timeline:

t=0s   Leader marks node-8002 healthy=true (initial)

t=5s   Leader calls lc.Replicate()
         → sends to node-8002 (healthy)
         → all 3 attempts fail (node-8002 is down)
         → lock → node-8002.healthy = false → unlock

t=6s   Leader calls lc.Replicate() again
         → node-8002: !healthy → skip
         → node-8003: healthy → send successfully

t=10s  recoverFollowers goroutine fires
         → node-8002: !healthy → probe HeartBeat
         → node-8002 still down → err → stays unhealthy

t=20s  recoverFollowers goroutine fires again
         → node-8002: !healthy → probe HeartBeat
         → node-8002 is back → ok
         → lock → node-8002.healthy = true → unlock
         → log "follower localhost:8002 recovered, marking healthy"

t=21s  Leader calls lc.Replicate()
         → node-8002: healthy → sends successfully again
```

---

## 9. Port Summary

| Node       | gRPC port | HTTP port | Role                        |
|------------|-----------|-----------|-----------------------------|
| node-8001  | 8001      | 9001      | Leader — accepts all writes |
| node-8002  | 8002      | 9002      | Follower — serves reads     |
| node-8003  | 8003      | 9003      | Follower — serves reads     |

HTTP port defaults to `gRPC port + 1000` if `--http-port` is not specified.
