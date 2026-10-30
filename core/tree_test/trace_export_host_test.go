package tree

import (
	"flag"
	"fmt"

	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/ethdb/leveldb"
	"github.com/ethereum/go-ethereum/ethdb/memorydb"
	"github.com/ethereum/go-ethereum/triedb"
)

var (
	dataDir   = flag.String("traceDataDir", "", "Transaction and block CSV directory")
	startIdx  = flag.Int("traceStartFile", 1, "First transaction file, one based")
	endIdx    = flag.Int("traceEndFile", 0, "Last transaction file, one based; zero uses all")
	maxBlocks = flag.Int("traceMaxBlocks", 0, "Maximum exported blocks; zero uses all")
)

type traceExportConfig struct {
	DbDir, DataDir           string
	StartFileIdx, EndFileIdx int
	MaxBlocks                int
	UseMemory                bool
}

type traceExportHost struct {
	db     ethdb.Database
	trieDB *triedb.Database
	sdb    state.Database
}

func newTraceExportHost(cfg *traceExportConfig) (*traceExportHost, error) {
	var store ethdb.KeyValueStore
	var err error
	if cfg.UseMemory {
		store = memorydb.New()
	} else {
		if cfg.DbDir == "" {
			return nil, fmt.Errorf("trace state database directory is required")
		}
		store, err = leveldb.New(cfg.DbDir, 1024, 1024, "amt-trace-export", false)
		if err != nil {
			return nil, err
		}
	}
	db := rawdb.NewDatabase(store)
	sdb := state.NewKVDatabase(db)
	return &traceExportHost{db: db, trieDB: sdb.TrieDB(), sdb: sdb}, nil
}

func (h *traceExportHost) Close() {
	if h.trieDB != nil {
		_ = h.trieDB.Close()
	}
	if h.db != nil {
		_ = h.db.Close()
	}
}
