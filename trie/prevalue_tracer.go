package trie

import (
	"maps"
	"sync"
)

type PrevalueTracer struct {
	data map[string][]byte
	lock sync.RWMutex
}

func NewPrevalueTracer() *PrevalueTracer {
	return &PrevalueTracer{
		data: make(map[string][]byte),
	}
}

func (t *PrevalueTracer) Put(path []byte, val []byte) {
	t.lock.Lock()
	defer t.lock.Unlock()

	t.data[string(path)] = val
}

func (t *PrevalueTracer) Get(path []byte) []byte {
	t.lock.RLock()
	defer t.lock.RUnlock()

	return t.data[string(path)]
}

func (t *PrevalueTracer) HasList(list [][]byte) []bool {
	t.lock.RLock()
	defer t.lock.RUnlock()

	exists := make([]bool, 0, len(list))
	for _, path := range list {
		_, ok := t.data[string(path)]
		exists = append(exists, ok)
	}
	return exists
}

func (t *PrevalueTracer) Values() map[string][]byte {
	t.lock.RLock()
	defer t.lock.RUnlock()

	return maps.Clone(t.data)
}

func (t *PrevalueTracer) Reset() {
	t.lock.Lock()
	defer t.lock.Unlock()

	clear(t.data)
}

func (t *PrevalueTracer) Copy() *PrevalueTracer {
	t.lock.RLock()
	defer t.lock.RUnlock()

	return &PrevalueTracer{
		data: maps.Clone(t.data),
	}
}
