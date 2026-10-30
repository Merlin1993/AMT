package mpt

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb/memorydb"
	gethtrie "github.com/ethereum/go-ethereum/trie/archive/mpt/hx"
)

func TestMPTRootDeterministicUnderFilterSaturation(t *testing.T) {
	build := func() []byte {
		db := &testStore{memorydb.New()}
		tr, err := New(nil, db, &Config{
			ShardDepthBits: 4,
			CuckooBuckets:  2,
			CuckooSlots:    3,
			BucketCapacity: 8,
		})
		if err != nil {
			t.Fatal(err)
		}
		const n = 24
		entries := make([]gethtrie.ExtractedEntry, 0, n)
		for i := 0; i < n; i++ {
			entries = append(entries, gethtrie.ExtractedEntry{
				Key:   saturateTestKey(i),
				Value: []byte(fmt.Sprintf("value-%d", i)),
			})
		}
		tr.mu.Lock()
		if err := tr.ensureHotLocked(); err != nil {
			tr.mu.Unlock()
			t.Fatal(err)
		}
		if err := tr.mountEntriesLocked([]byte{1}, entries); err != nil {
			tr.mu.Unlock()
			t.Fatalf("mount: %v", err)
		}
		tr.mu.Unlock()
		root, err := tr.Commit()
		if err != nil {
			t.Fatalf("commit: %v", err)
		}
		return root
	}
	r1 := build()
	r2 := build()
	if !bytes.Equal(r1, r2) {
		t.Fatalf("root nondeterministic under filter rebuild: %x vs %x", r1, r2)
	}
}
