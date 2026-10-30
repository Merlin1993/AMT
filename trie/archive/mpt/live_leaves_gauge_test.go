package mpt

import (
	"fmt"
	"testing"
)

func TestLiveHotLeavesGauge(t *testing.T) {
	db := newTestKVStore()
	tr := newTestTrie(t, db, false)
	const N = 4000
	keys := make([][]byte, N)
	countHot := func() int {
		t.Helper()
		hot, err := tr.hotTrieLocked()
		if err != nil {
			t.Fatal(err)
		}
		count := 0
		if err := hot.CollectLeaves(func(_, _ []byte) bool { count++; return true }); err != nil {
			t.Fatal(err)
		}
		return count
	}
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("leafverifkey%08x", i))
		if err := tr.Put(keys[i], []byte{byte(i), byte(i >> 8)}); err != nil {
			t.Fatal(err)
		}
	}
	for _, k := range keys[:1200] {
		if err := tr.Delete(k); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := tr.Commit(); err != nil {
		t.Fatal(err)
	}
	walk := countHot()
	if got := tr.LiveHotLeaves(); got != int64(walk) {
		t.Fatalf("after puts/deletes: gauge=%d walk=%d (expect both %d)", got, walk, N-1200)
	}

	if err := tr.PruneNextShard(); err != nil {
		t.Fatal(err)
	}
	if _, err := tr.Commit(); err != nil {
		t.Fatal(err)
	}
	walk = countHot()
	if got := tr.LiveHotLeaves(); got != int64(walk) {
		t.Fatalf("after prune: gauge=%d walk=%d", got, walk)
	}

	if err := tr.Put(keys[0], []byte{0xaa, 0xbb}); err != nil {
		t.Fatal(err)
	}
	if _, err := tr.Commit(); err != nil {
		t.Fatal(err)
	}
	walk = countHot()
	if got := tr.LiveHotLeaves(); got != int64(walk) {
		t.Fatalf("after reinsert: gauge=%d walk=%d", got, walk)
	}
	if got := tr.LiveBuckets(); got < 0 {
		t.Fatalf("LiveBuckets negative: %d", got)
	}
}
