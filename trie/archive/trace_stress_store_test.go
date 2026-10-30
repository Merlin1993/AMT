package archive

import (
	"os"
	"path/filepath"

	"github.com/ethereum/go-ethereum/ethdb"
)

func getDirSize(path string) int64 {
	var size int64
	_ = filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size
}

type stressDBAdapter struct{ ethdb.KeyValueStore }

func (db *stressDBAdapter) NewBatch() Batcher { return db.KeyValueStore.NewBatch() }

func (db *stressDBAdapter) NewBatchWithSize(size int) Batcher {
	return db.KeyValueStore.NewBatchWithSize(size)
}

func (db *stressDBAdapter) Compact() error {
	if store, ok := db.KeyValueStore.(interface{ Compact([]byte, []byte) error }); ok {
		return store.Compact(nil, nil)
	}
	return nil
}
