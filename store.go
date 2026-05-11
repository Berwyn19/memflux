package main

import (
	"sync"
	"time"
)

type entry struct {
	value     string
	expiresAt time.Time
}

type Store struct {
	lock sync.RWMutex
	mp   map[string]entry
}

func (s *Store) Set(key string, value string, ttl time.Duration) {
	s.lock.Lock()
	defer s.lock.Unlock()

	// Computes expiry time and set key-value to the store
	var expiresAt time.Time

	// If TTL is zero, the entry should never be evicted
	if ttl > 0 {
		expiresAt = time.Now().Add(ttl)
	}
	dataEntry := entry{value, expiresAt}
	s.mp[key] = dataEntry
}

func (s *Store) Get(key string) (string, bool) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	// Fail to get key, immediately return
	entry, ok := s.mp[key]
	if !ok {
		return "", false
	}

	// If expired, reject request
	if !entry.expiresAt.IsZero() && time.Now().After(entry.expiresAt) {
		return "", false
	}

	// Key exists, return value
	return entry.value, true
}

func (s *Store) Delete(key string) {
	s.lock.Lock()
	defer s.lock.Unlock()
	delete(s.mp, key)
}

func (s *Store) sweep() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		<-ticker.C
		// Loop 1: Get the expired data
		s.lock.RLock()
		var expiredKeys []string
		for key, entry := range s.mp {
			if !entry.expiresAt.IsZero() && time.Now().After(entry.expiresAt) {
				expiredKeys = append(expiredKeys, key)
			}
		}
		s.lock.RUnlock()

		// Loop 2: Remove those expired data
		s.lock.Lock()
		for _, key := range expiredKeys {
			entry, ok := s.mp[key]
			if !ok {
				continue
			}
			if !entry.expiresAt.IsZero() && time.Now().After(entry.expiresAt) {
				delete(s.mp, key)
			}
		}
		s.lock.Unlock()
	}
}
