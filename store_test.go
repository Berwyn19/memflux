package memflux

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// newTestStore creates a store without the background sweep goroutine for deterministic tests.
func newTestStore() *Store {
	return &Store{mp: make(map[string]entry)}
}

// --- Set / Get ---

func TestSetAndGet(t *testing.T) {
	s := newTestStore()
	s.Set("key", "value", 0)
	got, ok := s.Get("key")
	if !ok {
		t.Fatal("expected key to exist")
	}
	if got != "value" {
		t.Fatalf("expected %q, got %q", "value", got)
	}
}

func TestGetMissing(t *testing.T) {
	s := newTestStore()
	_, ok := s.Get("missing")
	if ok {
		t.Fatal("expected missing key to return false")
	}
}

func TestGetBeforeExpiry(t *testing.T) {
	s := newTestStore()
	s.Set("key", "value", time.Hour)
	got, ok := s.Get("key")
	if !ok {
		t.Fatal("expected non-expired key to exist")
	}
	if got != "value" {
		t.Fatalf("expected %q, got %q", "value", got)
	}
}

func TestGetExpired(t *testing.T) {
	s := newTestStore()
	s.Set("key", "value", 50*time.Millisecond)
	time.Sleep(100 * time.Millisecond)
	_, ok := s.Get("key")
	if ok {
		t.Fatal("expected expired key to return false")
	}
}

func TestZeroTTLNeverExpires(t *testing.T) {
	s := newTestStore()
	s.Set("key", "value", 0)
	time.Sleep(20 * time.Millisecond)
	_, ok := s.Get("key")
	if !ok {
		t.Fatal("expected zero-TTL key to never expire")
	}
}

func TestNegativeTTLNeverExpires(t *testing.T) {
	s := newTestStore()
	s.Set("key", "value", -time.Second)
	_, ok := s.Get("key")
	if !ok {
		t.Fatal("expected negative-TTL key to never expire")
	}
}

// --- Overwrite ---

func TestSetOverwriteValue(t *testing.T) {
	s := newTestStore()
	s.Set("key", "first", 0)
	s.Set("key", "second", 0)
	got, ok := s.Get("key")
	if !ok {
		t.Fatal("expected key to exist after overwrite")
	}
	if got != "second" {
		t.Fatalf("expected %q, got %q", "second", got)
	}
}

func TestSetOverwriteExpiredKey(t *testing.T) {
	s := newTestStore()
	s.Set("key", "old", 50*time.Millisecond)
	time.Sleep(100 * time.Millisecond)
	s.Set("key", "new", 0)
	got, ok := s.Get("key")
	if !ok {
		t.Fatal("expected overwritten expired key to be accessible")
	}
	if got != "new" {
		t.Fatalf("expected %q, got %q", "new", got)
	}
}

func TestSetExtendsTTL(t *testing.T) {
	s := newTestStore()
	s.Set("key", "value", 50*time.Millisecond)
	s.Set("key", "value", time.Hour) // reset TTL
	time.Sleep(100 * time.Millisecond)
	_, ok := s.Get("key")
	if !ok {
		t.Fatal("expected key with extended TTL to still exist")
	}
}

// --- Delete ---

func TestDelete(t *testing.T) {
	s := newTestStore()
	s.Set("key", "value", 0)
	s.Delete("key")
	_, ok := s.Get("key")
	if ok {
		t.Fatal("expected key to be gone after delete")
	}
}

func TestDeleteNonExistent(t *testing.T) {
	s := newTestStore()
	s.Delete("nonexistent") // must not panic
}

func TestDeleteDoesNotIncrementEvictions(t *testing.T) {
	s := newTestStore()
	s.Set("key", "value", 0)
	s.Delete("key")
	if stats := s.Stats(); stats.Evictions != 0 {
		t.Fatalf("expected 0 evictions after Delete, got %d", stats.Evictions)
	}
}

// --- Stats ---

func TestStatsInitial(t *testing.T) {
	s := newTestStore()
	stats := s.Stats()
	if stats.Hits != 0 || stats.Misses != 0 || stats.Evictions != 0 {
		t.Fatalf("expected all-zero initial stats, got %+v", stats)
	}
}

func TestStatsHit(t *testing.T) {
	s := newTestStore()
	s.Set("key", "value", 0)
	s.Get("key")
	s.Get("key")
	if stats := s.Stats(); stats.Hits != 2 {
		t.Fatalf("expected 2 hits, got %d", stats.Hits)
	}
}

func TestStatsMissOnMissingKey(t *testing.T) {
	s := newTestStore()
	s.Get("missing")
	s.Get("missing")
	if stats := s.Stats(); stats.Misses != 2 {
		t.Fatalf("expected 2 misses, got %d", stats.Misses)
	}
}

func TestStatsMissOnExpiredKey(t *testing.T) {
	s := newTestStore()
	s.Set("key", "value", 50*time.Millisecond)
	time.Sleep(100 * time.Millisecond)
	s.Get("key") // expired → miss
	stats := s.Stats()
	if stats.Misses != 1 {
		t.Fatalf("expected 1 miss for expired key, got %d", stats.Misses)
	}
	if stats.Hits != 0 {
		t.Fatalf("expected 0 hits, got %d", stats.Hits)
	}
}

func TestStatsHitAndMissMixed(t *testing.T) {
	s := newTestStore()
	s.Set("a", "1", 0)
	s.Get("a") // hit
	s.Get("b") // miss
	s.Get("a") // hit
	s.Get("c") // miss
	stats := s.Stats()
	if stats.Hits != 2 {
		t.Fatalf("expected 2 hits, got %d", stats.Hits)
	}
	if stats.Misses != 2 {
		t.Fatalf("expected 2 misses, got %d", stats.Misses)
	}
}

// --- Sweep (requires NewStore with the background goroutine) ---

func TestSweepEvictsExpiredKeys(t *testing.T) {
	s := NewStore()
	s.Set("a", "1", 10*time.Millisecond)
	s.Set("b", "2", 10*time.Millisecond)
	time.Sleep(1500 * time.Millisecond) // wait for at least one sweep tick
	if stats := s.Stats(); stats.Evictions != 2 {
		t.Fatalf("expected 2 evictions after sweep, got %d", stats.Evictions)
	}
}

func TestSweepRemovesKeyFromMap(t *testing.T) {
	s := NewStore()
	s.Set("key", "value", 10*time.Millisecond)
	time.Sleep(1500 * time.Millisecond)
	s.lock.RLock()
	_, exists := s.mp["key"]
	s.lock.RUnlock()
	if exists {
		t.Fatal("expected sweep to remove expired key from the internal map")
	}
}

func TestSweepDoesNotEvictUnexpiredKeys(t *testing.T) {
	s := NewStore()
	s.Set("key", "value", time.Hour)
	time.Sleep(1500 * time.Millisecond)
	got, ok := s.Get("key")
	if !ok {
		t.Fatal("expected long-TTL key to survive sweep")
	}
	if got != "value" {
		t.Fatalf("expected %q, got %q", "value", got)
	}
	if stats := s.Stats(); stats.Evictions != 0 {
		t.Fatalf("expected 0 evictions, got %d", stats.Evictions)
	}
}

func TestSweepDoesNotEvictZeroTTLKeys(t *testing.T) {
	s := NewStore()
	s.Set("key", "value", 0)
	time.Sleep(1500 * time.Millisecond)
	_, ok := s.Get("key")
	if !ok {
		t.Fatal("expected zero-TTL key to survive sweep")
	}
	if stats := s.Stats(); stats.Evictions != 0 {
		t.Fatalf("expected 0 evictions for zero-TTL key, got %d", stats.Evictions)
	}
}

// --- Edge cases ---

func TestEmptyStringKey(t *testing.T) {
	s := newTestStore()
	s.Set("", "value", 0)
	got, ok := s.Get("")
	if !ok {
		t.Fatal("expected empty-string key to work")
	}
	if got != "value" {
		t.Fatalf("expected %q, got %q", "value", got)
	}
}

func TestEmptyStringValue(t *testing.T) {
	s := newTestStore()
	s.Set("key", "", 0)
	got, ok := s.Get("key")
	if !ok {
		t.Fatal("expected key with empty-string value to exist")
	}
	if got != "" {
		t.Fatalf("expected empty string, got %q", got)
	}
}

func TestMultipleKeys(t *testing.T) {
	s := newTestStore()
	keys := []string{"a", "b", "c", "d", "e"}
	for _, k := range keys {
		s.Set(k, k+"-val", 0)
	}
	for _, k := range keys {
		got, ok := s.Get(k)
		if !ok {
			t.Fatalf("expected key %q to exist", k)
		}
		if want := k + "-val"; got != want {
			t.Fatalf("key %q: expected %q, got %q", k, want, got)
		}
	}
}

// --- Concurrency ---

func TestConcurrentWrites(t *testing.T) {
	s := newTestStore()
	var wg sync.WaitGroup
	for i := range 100 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s.Set(fmt.Sprintf("key-%d", i), fmt.Sprintf("val-%d", i), 0)
		}(i)
	}
	wg.Wait()
}

func TestConcurrentReads(t *testing.T) {
	s := newTestStore()
	s.Set("key", "value", 0)
	var wg sync.WaitGroup
	for range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.Get("key")
		}()
	}
	wg.Wait()
}

func TestConcurrentReadWrite(t *testing.T) {
	s := newTestStore()
	var wg sync.WaitGroup
	for i := range 50 {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			s.Set(fmt.Sprintf("key-%d", i), fmt.Sprintf("val-%d", i), 0)
		}(i)
		go func(i int) {
			defer wg.Done()
			s.Get(fmt.Sprintf("key-%d", i))
		}(i)
	}
	wg.Wait()
}

func TestConcurrentReadWriteDelete(t *testing.T) {
	s := newTestStore()
	var wg sync.WaitGroup
	for i := range 30 {
		wg.Add(3)
		go func(i int) {
			defer wg.Done()
			s.Set(fmt.Sprintf("key-%d", i), fmt.Sprintf("val-%d", i), 0)
		}(i)
		go func(i int) {
			defer wg.Done()
			s.Get(fmt.Sprintf("key-%d", i))
		}(i)
		go func(i int) {
			defer wg.Done()
			s.Delete(fmt.Sprintf("key-%d", i))
		}(i)
	}
	wg.Wait()
}

func TestConcurrentStats(t *testing.T) {
	s := newTestStore()
	s.Set("key", "value", 0)
	var wg sync.WaitGroup
	for range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.Stats()
		}()
	}
	wg.Wait()
}
