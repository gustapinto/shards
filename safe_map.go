package shards

import (
	"sync"
)

// SafeMap Is a simple, thread safe, strong typed map implementation
type SafeMap[K comparable, V any] struct {
	lock sync.RWMutex
	data map[K]V
}

// NewSafeMap Initializes a safe-map
func NewSafeMap[K comparable, V any]() *SafeMap[K, V] {
	return &SafeMap[K, V]{
		lock: sync.RWMutex{},
		data: make(map[K]V, 0),
	}
}

// Get Returns the current read-locked value for the key
func (s *SafeMap[K, V]) Get(key K) (value V, exists bool) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	v, e := s.data[key]

	return v, e
}

// Set Upserts a key->value value in the map
func (s *SafeMap[K, V]) Set(key K, value V) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.data[key] = value
}

// Del Deletes a key->value pair in the map
func (s *SafeMap[K, V]) Del(key K) {
	s.lock.Lock()
	defer s.lock.Unlock()

	delete(s.data, key)
}

// Lookup Returns a read-locked copy of the current underlying map. Note
// that it does not guarantee ordering
func (s *SafeMap[K, V]) Lookup() map[K]V {
	s.lock.RLock()
	defer s.lock.RUnlock()

	snap := make(map[K]V, len(s.data))
	for k, v := range s.data {
		snap[k] = v
	}

	return snap
}

// Keys Returns a read-locked copy of the current underlying map keys. Note
// that it does not guarantee ordering
func (s *SafeMap[K, V]) Keys() (keys []K) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	for k := range s.data {
		keys = append(keys, k)
	}

	return keys
}

// Values Returns a read-locked copy of the current underlying map values. Note
// that it does not guarantee ordering
func (s *SafeMap[K, V]) Values() (values []V) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	for _, v := range s.data {
		values = append(values, v)
	}

	return values
}

// Clone Creates a read-locked deep copy of this SafeMap
func (s *SafeMap[K, V]) Clone() *SafeMap[K, V] {
	return &SafeMap[K, V]{
		lock: sync.RWMutex{},
		data: s.Lookup(),
	}
}
