package memflux

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// onePassSweep runs a single sweep pass against s, matching the two-phase
// logic inside Store.sweep. Used by the sweep benchmarks to drive sweep work
// without waiting for the ticker.
func onePassSweep(s *Store) {
	s.lock.RLock()
	var expired []string
	for key, e := range s.mp {
		if !e.expiresAt.IsZero() && time.Now().After(e.expiresAt) {
			expired = append(expired, key)
		}
	}
	s.lock.RUnlock()

	s.lock.Lock()
	for _, key := range expired {
		e, ok := s.mp[key]
		if !ok {
			continue
		}
		if !e.expiresAt.IsZero() && time.Now().After(e.expiresAt) {
			atomic.AddInt64(&s.evictions, 1)
			delete(s.mp, key)
		}
	}
	s.lock.Unlock()
}

// ── Single-goroutine baselines ────────────────────────────────────────────────

func BenchmarkStoreGet(b *testing.B) {
	s := newTestStore()
	s.Set("key", "value", 0)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		s.Get("key")
	}
}

func BenchmarkStoreSet(b *testing.B) {
	s := newTestStore()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		s.Set(fmt.Sprintf("key%d", i), "value", 0)
	}
}

func BenchmarkStoreDelete(b *testing.B) {
	s := newTestStore()
	for i := 0; i < b.N; i++ {
		s.Set(fmt.Sprintf("key%d", i), "value", 0)
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		s.Delete(fmt.Sprintf("key%d", i))
	}
}

func BenchmarkStoreGetMiss(b *testing.B) {
	s := newTestStore()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		s.Get("nonexistent")
	}
}

func BenchmarkStoreGetExpired(b *testing.B) {
	s := newTestStore()
	s.Set("key", "value", time.Millisecond)
	time.Sleep(2 * time.Millisecond) // guarantee expiry before measurement starts
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		s.Get("key") // key is in the map but always fails the expiry check
	}
}

// ── Parallel (concurrent) benchmarks ─────────────────────────────────────────

func BenchmarkStoreGetParallel(b *testing.B) {
	s := newTestStore()
	s.Set("key", "value", 0)
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			s.Get("key")
		}
	})
}

func BenchmarkStoreSetParallel(b *testing.B) {
	s := newTestStore()
	var counter int64
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := atomic.AddInt64(&counter, 1)
			s.Set(fmt.Sprintf("key%d", id), "value", 0)
		}
	})
}

func BenchmarkStoreGetParallelMixedKeys(b *testing.B) {
	const numKeys = 1000
	s := newTestStore()
	for i := 0; i < numKeys; i++ {
		s.Set(fmt.Sprintf("key%d", i), "value", 0)
	}
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		// local counter avoids atomic overhead; each goroutine cycles through
		// all 1000 keys independently, giving uniform key distribution.
		var i int64
		for pb.Next() {
			s.Get(fmt.Sprintf("key%d", i%numKeys))
			i++
		}
	})
}

func BenchmarkStoreReadWriteParallel(b *testing.B) {
	const numKeys = 1000
	s := newTestStore()
	for i := 0; i < numKeys; i++ {
		s.Set(fmt.Sprintf("key%d", i), "value", 0)
	}
	var counter int64
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			n := atomic.AddInt64(&counter, 1)
			if n%5 == 0 {
				// 20 % — write
				s.Set(fmt.Sprintf("key%d", n%numKeys), "value", 0)
			} else {
				// 80 % — read
				s.Get(fmt.Sprintf("key%d", n%numKeys))
			}
		}
	})
}

// ── Sweep benchmarks ──────────────────────────────────────────────────────────

// BenchmarkSweepWith1000Keys measures one full sweep pass over 1000 expired
// keys. Setup (populate + sleep) is excluded via StopTimer/StartTimer so only
// the sweep work itself is counted.
func BenchmarkSweepWith1000Keys(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		s := newTestStore()
		for j := 0; j < 1000; j++ {
			s.Set(fmt.Sprintf("key%d", j), "value", time.Millisecond)
		}
		time.Sleep(2 * time.Millisecond) // all keys are now expired
		b.StartTimer()

		onePassSweep(s)
	}
}

// BenchmarkSweepWith10000Keys is BenchmarkSweepWith1000Keys at 10× scale to
// confirm sweep time grows linearly with key count.
func BenchmarkSweepWith10000Keys(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		s := newTestStore()
		for j := 0; j < 10000; j++ {
			s.Set(fmt.Sprintf("key%d", j), "value", time.Millisecond)
		}
		time.Sleep(2 * time.Millisecond)
		b.StartTimer()

		onePassSweep(s)
	}
}

// ── Stats benchmark ───────────────────────────────────────────────────────────

// BenchmarkStoreStats measures Stats() throughput under concurrent load.
// Stats() uses only atomic loads so it should never contend with readers or
// writers, but it is worth confirming this at scale.
func BenchmarkStoreStats(b *testing.B) {
	s := newTestStore()
	s.Set("key", "value", 0)
	for i := 0; i < 100; i++ {
		s.Get("key")
		s.Get("nonexistent")
	}
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			s.Stats()
		}
	})
}
