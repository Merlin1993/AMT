package mpt

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb/memorydb"
)

func epochTestKey(domain, seq int) []byte {
	key := make([]byte, 32)
	key[0] = byte(domain) << 4
	key[29] = byte(seq >> 8)
	key[30] = byte(seq)
	key[31] = byte(seq * 7)
	return key
}

func leafEpoch(t *testing.T, tr *Trie, key []byte) (byte, bool) {
	t.Helper()
	if err := tr.ensureHotLocked(); err != nil {
		t.Fatal(err)
	}
	epoch, ok, err := tr.hot.LeafEpoch(key)
	if err != nil {
		t.Fatal(err)
	}
	return epoch, ok
}

func TestMPTEpochDynamicAssignment(t *testing.T) {
	db := &testStore{memorydb.New()}
	tr := newTestTrie(t, db, false)

	k50 := epochTestKey(5, 0)
	if err := tr.Put(k50, []byte("v50")); err != nil {
		t.Fatal(err)
	}
	if epoch, ok := leafEpoch(t, tr, k50); !ok || epoch != 1 {
		t.Fatalf("unpruned domain write: epoch %d ok %v, want 1", epoch, ok)
	}

	for i := 0; i < 3; i++ {
		if err := tr.PruneNextShard(); err != nil {
			t.Fatal(err)
		}
	}
	k21 := epochTestKey(2, 1)
	k71 := epochTestKey(7, 1)
	if err := tr.Put(k21, []byte("v21")); err != nil {
		t.Fatal(err)
	}
	if err := tr.Put(k71, []byte("v71")); err != nil {
		t.Fatal(err)
	}
	if epoch, _ := leafEpoch(t, tr, k21); epoch != 0 {
		t.Fatalf("pruned domain write: epoch %d, want base bit 0", epoch)
	}
	if epoch, _ := leafEpoch(t, tr, k71); epoch != 1 {
		t.Fatalf("unpruned domain write: epoch %d, want 1", epoch)
	}

	for i := 0; i < 13; i++ {
		if err := tr.PruneNextShard(); err != nil {
			t.Fatal(err)
		}
	}
	k32 := epochTestKey(3, 2)
	if err := tr.Put(k32, []byte("v32")); err != nil {
		t.Fatal(err)
	}
	if epoch, _ := leafEpoch(t, tr, k32); epoch != 0 {
		t.Fatalf("post-rollover write: epoch %d, want 1-base=0", epoch)
	}

	root, err := tr.Commit()
	if err != nil {
		t.Fatal(err)
	}
	tr2, err := New(root, db, &tr.config)
	if err != nil {
		t.Fatal(err)
	}
	k49 := epochTestKey(4, 9)
	if err := tr2.Put(k49, []byte("v49")); err != nil {
		t.Fatal(err)
	}
	if epoch, _ := leafEpoch(t, tr2, k49); epoch != 0 {
		t.Fatalf("post-reload write: epoch %d, want 0 (persisted base bit 1)", epoch)
	}
	if epoch, ok := leafEpoch(t, tr2, k32); !ok || epoch != 0 {
		t.Fatalf("reloaded leaf k32: epoch %d ok %v, want 0", epoch, ok)
	}
	if val, err := tr2.Get(k50); err != nil || string(val) != "v50" {
		t.Fatalf("archived k50: %q err %v, want v50", val, err)
	}
}

func TestMPTEpochDeterministicRoot(t *testing.T) {
	run := func() []byte {
		db := &testStore{memorydb.New()}
		tr := newTestTrie(t, db, false)
		for i := 0; i < 120; i++ {
			key := epochTestKey(i%16, i)
			if err := tr.Put(key, []byte(fmt.Sprintf("v%d", i))); err != nil {
				t.Fatal(err)
			}
			if i%30 == 29 {
				if err := tr.PruneNextShard(); err != nil {
					t.Fatal(err)
				}
			}
		}
		for i := 0; i < 40; i++ {
			key := epochTestKey((i*3)%16, i)
			if i%4 == 3 {
				if err := tr.Delete(key); err != nil {
					t.Fatal(err)
				}
			} else if err := tr.Put(key, []byte(fmt.Sprintf("w%d", i))); err != nil {
				t.Fatal(err)
			}
		}
		root, err := tr.Commit()
		if err != nil {
			t.Fatal(err)
		}
		return root
	}
	a, b := run(), run()
	if !bytes.Equal(a, b) {
		t.Fatalf("non-deterministic roots: %x vs %x", a, b)
	}
}

func TestMPTEpochBitmapAggregatePropagation(t *testing.T) {
	db := &testStore{memorydb.New()}
	tr := newTestTrie(t, db, false)

	verify := func(stage string) {
		t.Helper()
		if err := tr.ensureHotLocked(); err != nil {
			t.Fatalf("%s: %v", stage, err)
		}
		if err := tr.hot.VerifyAggregates(); err != nil {
			t.Fatalf("%s: %v", stage, err)
		}
	}

	for round := 0; round < 4; round++ {
		for i := 0; i < 80; i++ {
			key := epochTestKey((i+round)%16, round*80+i)
			if err := tr.Put(key, []byte(fmt.Sprintf("r%d-%d", round, i))); err != nil {
				t.Fatal(err)
			}
		}
		verify(fmt.Sprintf("round %d writes", round))
		for i := 0; i < 20; i++ {
			if err := tr.Delete(epochTestKey((i*5+round)%16, round*37+i)); err != nil {
				t.Fatal(err)
			}
		}
		verify(fmt.Sprintf("round %d deletes", round))
		if err := tr.PruneNextShard(); err != nil {
			t.Fatal(err)
		}
		verify(fmt.Sprintf("round %d prune", round))
		if _, err := tr.Commit(); err != nil {
			t.Fatal(err)
		}
		verify(fmt.Sprintf("round %d post-commit reopen", round))
	}
}
