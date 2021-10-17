package raft

import "sync"

// Storage is an interface implemented by stable storage providers.
type Storage interface {
	Set(key string, value []byte)

	Get(key string) ([]byte, bool)

	// HasData returns true iff any Sets were made on this Storage.
	HasData() bool
}

// MapStorage is a simple in-memory implementation of Storage for testing.
type MapStorage struct {
	mu sync.Mutex
	m  map[string][]byte
}

func NewMapStorage() *MapStorage {
	return &MapStorage{
		m: make(map[string][]byte),
	}
}

func (m *MapStorage) Set(key string, value []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.m[key] = value
}

func (m *MapStorage) Get(key string) ([]byte, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.m[key]
	return v, ok
}

func (m *MapStorage) HasData() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.m) > 0
}
