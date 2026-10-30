package mpt

import (
	"errors"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	archivetrie "github.com/ethereum/go-ethereum/trie/archive"
	"github.com/ethereum/go-ethereum/triedb"
	"github.com/ethereum/go-ethereum/triedb/pathdb"
)

var errUnsupported = errors.New("mpt ethdb adapter: operation unsupported")

const pathTablePrefix = "AMTP/"

type writeCounter struct {
	bytes       atomic.Int64
	writes      atomic.Int64
	totalBytes  atomic.Int64
	totalWrites atomic.Int64
}

func (w *writeCounter) add(key, value []byte) {
	if w == nil {
		return
	}
	n := int64(len(key) + len(value))
	w.bytes.Add(n)
	w.writes.Add(1)
	w.totalBytes.Add(n)
	w.totalWrites.Add(1)
}

func (w *writeCounter) take() (bytes int64, writes int64) {
	if w == nil {
		return 0, 0
	}
	return w.bytes.Swap(0), w.writes.Swap(0)
}

func (w *writeCounter) totals() (bytes int64, writes int64) {
	if w == nil {
		return 0, 0
	}
	return w.totalBytes.Load(), w.totalWrites.Load()
}

type kvStoreAdapter struct {
	store   archivetrie.KVStore
	counter *writeCounter
}

func newKVStoreAdapter(store archivetrie.KVStore) ethdb.KeyValueStore {
	return &kvStoreAdapter{store: store}
}

func (a *kvStoreAdapter) Has(key []byte) (bool, error) {
	value, err := a.store.Get(key)
	if err != nil {
		if isMissingError(err) {
			return false, nil
		}
		return false, err
	}
	return value != nil, nil
}

func (a *kvStoreAdapter) Get(key []byte) ([]byte, error) {
	return a.store.Get(key)
}

func (a *kvStoreAdapter) Put(key, value []byte) error {
	return a.store.Put(key, value)
}

func (a *kvStoreAdapter) Delete(key []byte) error {
	return a.store.Delete(key)
}

func (a *kvStoreAdapter) NewBatch() ethdb.Batch {
	return &batchAdapter{Batcher: a.store.NewBatch(), counter: a.counter}
}

func (a *kvStoreAdapter) NewBatchWithSize(size int) ethdb.Batch {
	type sizedStore interface {
		NewBatchWithSize(int) archivetrie.Batcher
	}
	if store, ok := a.store.(sizedStore); ok {
		return &batchAdapter{Batcher: store.NewBatchWithSize(size), counter: a.counter}
	}
	return a.NewBatch()
}

func (a *kvStoreAdapter) NewIterator(prefix, start []byte) ethdb.Iterator {
	return &failingIterator{err: errUnsupported}
}

func (a *kvStoreAdapter) Stat() (string, error) { return "", errUnsupported }

func (a *kvStoreAdapter) DeleteRange(start, end []byte) error { return errUnsupported }

func (a *kvStoreAdapter) Compact(start, limit []byte) error { return errUnsupported }

func (a *kvStoreAdapter) Close() error { return nil }

type batchAdapter struct {
	archivetrie.Batcher
	counter *writeCounter
}

func (b *batchAdapter) Put(key, value []byte) error {
	b.counter.add(key, value)
	return b.Batcher.Put(key, value)
}

func (b *batchAdapter) Delete(key []byte) error {
	b.counter.add(key, nil)
	return b.Batcher.Delete(key)
}

func (b *batchAdapter) Replay(ethdb.KeyValueWriter) error { return errUnsupported }

type failingIterator struct {
	err error
}

func (it *failingIterator) Next() bool    { return false }
func (it *failingIterator) Error() error  { return it.err }
func (it *failingIterator) Key() []byte   { return nil }
func (it *failingIterator) Value() []byte { return nil }
func (it *failingIterator) Release()      {}

func openPathDatabase(store archivetrie.KVStore, cleanCacheBytes, writeBufferBytes int) (*triedb.Database, *writeCounter, error) {
	if store == nil {
		return nil, nil, errors.New("mpt ethdb adapter: nil store")
	}
	counter := &writeCounter{}
	disk := rawdb.NewDatabase(&kvStoreAdapter{store: store, counter: counter})
	namespaced := rawdb.NewTable(disk, pathTablePrefix)
	return triedb.NewDatabase(namespaced, &triedb.Config{
		PathDB: &pathdb.Config{
			StateHistory:    0,
			CleanCacheSize:  cleanCacheBytes,
			WriteBufferSize: writeBufferBytes,
		},
	}), counter, nil
}
