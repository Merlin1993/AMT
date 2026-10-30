package mpt

import (
	"testing"

	"github.com/ethereum/go-ethereum/ethdb/memorydb"
)

func TestSingletonTopUpAtSameMount(t *testing.T) {
	db := &testStore{memorydb.New()}
	tr := newTestTrie(t, db, false)
	first := []byte{0x00, 0x11}
	second := []byte{0x00, 0x12}
	if err := tr.Put(first, []byte{1}); err != nil {
		t.Fatal(err)
	}
	if _, err := tr.Commit(); err != nil {
		t.Fatal(err)
	}
	expirePrune(t, tr, 0)
	if _, err := tr.Commit(); err != nil {
		t.Fatal(err)
	}
	b := bucketOfKey(t, tr, first)
	if b == nil || b.count != 1 || b.prefixBits != 16 {
		t.Fatalf("initial singleton = %+v", b)
	}
	if err := tr.Put(second, []byte{2}); err != nil {
		t.Fatal(err)
	}
	if _, err := tr.Commit(); err != nil {
		t.Fatal(err)
	}
	expirePrune(t, tr, 0)
	if _, err := tr.Commit(); err != nil {
		t.Fatal(err)
	}
	if b.count != 2 || len(tr.buckets) != 1 {
		t.Fatalf("same-parent top-up left %d buckets, original count=%d", len(tr.buckets), b.count)
	}
	for _, key := range [][]byte{first, second} {
		if _, archived, err := tr.GetValueRef(key); err != nil || !archived {
			t.Fatalf("archived key %x missing after top-up: %v", key, err)
		}
	}
}
