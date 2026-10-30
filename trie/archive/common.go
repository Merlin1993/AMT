package archive

import "errors"

var ErrNodeNotFound = errors.New("node not found")

type KVStore interface {
	Put(key, value []byte) error
	Delete(key []byte) error
	Get(key []byte) ([]byte, error)
	NewBatch() Batcher
}

type Batcher interface {
	Put(key, value []byte) error
	Delete(key []byte) error
	Write() error
	Reset()
	ValueSize() int
}
