# memflux Benchmark Guide

## Running the benchmarks

### Full suite (all benchmarks, 1 second each)
```bash
go test -bench=. -benchmem ./...
```

### Single benchmark
```bash
go test -bench=BenchmarkStoreGet -benchmem ./...
```

### Longer runs for stable numbers (useful before committing results)
```bash
go test -bench=. -benchmem -benchtime=5s ./...
```

### With the race detector
The race detector adds significant overhead but confirms there are no data races under concurrent load.
```bash
go test -race -bench=. -benchmem ./...
```

### With CPU profiling
```bash
go test -bench=. -benchmem -cpuprofile=cpu.prof ./...
go tool pprof cpu.prof
```
Inside pprof, useful commands: `top`, `web` (requires Graphviz), `list <funcname>`.

### With memory profiling
```bash
go test -bench=. -benchmem -memprofile=mem.prof ./...
go tool pprof mem.prof
```

---

## Results

| Benchmark | ops/sec | ns/op | Allocs/op |
|---|---|---|---|
| BenchmarkStoreGet | — | — | — |
| BenchmarkStoreSet | — | — | — |
| BenchmarkStoreDelete | — | — | — |
| BenchmarkStoreGetMiss | — | — | — |
| BenchmarkStoreGetExpired | — | — | — |
| BenchmarkStoreGetParallel | — | — | — |
| BenchmarkStoreSetParallel | — | — | — |
| BenchmarkStoreGetParallelMixedKeys | — | — | — |
| BenchmarkStoreReadWriteParallel | — | — | — |
| BenchmarkSweepWith1000Keys | — | — | — |
| BenchmarkSweepWith10000Keys | — | — | — |
| BenchmarkStoreStats | — | — | — |

---

## What each benchmark measures

### BenchmarkStoreGet
Single-goroutine read of an existing key. This is the floor cost of a cache hit: one `sync.RWMutex` read-lock acquisition, one map lookup, and one expiry check. The number to watch here is `ns/op` — it tells you the minimum latency a caller will ever see on the read path.

### BenchmarkStoreSet
Single-goroutine write of a new key per iteration. Measures write-lock acquisition, TTL computation, and map insertion. Because a new key is inserted each iteration, the map grows throughout the run, so later iterations may be slightly slower as the map rehashes — this is intentional and realistic.

### BenchmarkStoreDelete
Single-goroutine delete. Keys are pre-populated before `b.ResetTimer()` so setup time is excluded. Measures write-lock acquisition and `delete(map, key)`. Compare with `BenchmarkStoreSet` to understand the asymmetry between insert and remove.

### BenchmarkStoreGetMiss
Single-goroutine read of a key that does not exist. Measures the miss path: read-lock, map lookup returning `false`, atomic miss-count increment. Should be marginally faster than `BenchmarkStoreGet` because there is no value to return, but the lock and map probe cost are the same.

### BenchmarkStoreGetExpired
Single-goroutine read of a key that is present in the map but has passed its TTL. The key is set with a 1 ms TTL and the benchmark starts 2 ms later, guaranteeing the entry is expired throughout the run. Because `newTestStore()` omits the sweep goroutine, the entry stays in the map without being evicted — only the expiry check in `Get` fires. This isolates the cost of the time comparison on the hot path.

### BenchmarkStoreGetParallel
Concurrent reads of a single key across all available cores (`GOMAXPROCS`). Because `sync.RWMutex` allows multiple simultaneous readers, throughput should scale near-linearly with core count. A sub-linear result indicates unexpected contention, cache-line sharing on the mutex, or scheduler overhead.

### BenchmarkStoreSetParallel
Concurrent writes, each goroutine inserting a globally unique key via `atomic.AddInt64`. Because `sync.RWMutex` serialises writers, throughput is roughly `1 / (write_lock_hold_time)` regardless of goroutine count. This benchmark shows the maximum write throughput and how the write lock degrades under high write concurrency.

### BenchmarkStoreGetParallelMixedKeys
Concurrent reads spread across 1000 pre-populated keys. Each goroutine cycles through all 1000 keys with a local counter (no atomic overhead). This is more realistic than single-key parallel reads because it exercises a wider slice of the map and avoids hot-spotting on a single map bucket. Compare with `BenchmarkStoreGetParallel` to see how key diversity affects throughput.

### BenchmarkStoreReadWriteParallel
80 % reads and 20 % writes mixed under full concurrency. Operation type is decided by `n % 5 == 0` (write) using a shared atomic counter, which also determines the target key. This simulates the most common real-world cache workload. Pay attention to how `ns/op` compares to the read-only and write-only parallel benchmarks — write lock contention on the minority write goroutines will drag down read latency for the majority.

### BenchmarkSweepWith1000Keys
One full sweep pass over 1000 expired entries. Each iteration creates a fresh store, populates it, waits 2 ms for all TTLs to lapse, then runs a single sweep. Setup time is excluded via `b.StopTimer()` / `b.StartTimer()`. This measures the two-phase scan: a read-locked pass to collect expired keys followed by a write-locked pass to delete them. The result is the latency penalty imposed on the rest of the system during a sweep cycle.

### BenchmarkSweepWith10000Keys
Same as above at 10× scale. Sweep is O(N) in the number of entries. Compare `ns/op` between the 1 000 and 10 000 key variants: if the ratio is approximately 10×, the implementation is linear. A larger ratio would suggest quadratic behaviour or GC pressure from the intermediate slice allocation.

### BenchmarkStoreStats
Concurrent calls to `Stats()` across all cores. `Stats()` uses only `atomic.LoadInt64`, so it never holds a lock. This benchmark confirms that stats retrieval adds no meaningful overhead and does not contend with reader or writer goroutines. The expected result is very low `ns/op` that stays flat regardless of core count.
