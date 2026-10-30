package mpt

import (
	"bytes"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb/memorydb"
)

func verifyAllBucketsLocked(t *testing.T, tr *Trie) {
	t.Helper()
	for _, b := range tr.buckets {
		if b.payloadGone {
			continue
		}
		if err := tr.loadBucketLocked(b); err != nil {
			t.Fatalf("load bucket %x: %v", b.path, err)
		}
		if b.count != len(b.entries) {
			t.Fatalf("bucket %x: count=%d but payload holds %d entries", b.path, b.count, len(b.entries))
		}
		if err := tr.verifyBucketLocked(b); err != nil {
			t.Fatalf("bucket %x: %v", b.path, err)
		}
	}
}

func archivedCopiesLocked(t *testing.T, tr *Trie, key []byte) int {
	t.Helper()
	copies := 0
	for _, b := range tr.buckets {
		if b.payloadGone {
			continue
		}
		if err := tr.loadBucketLocked(b); err != nil {
			t.Fatalf("load bucket %x: %v", b.path, err)
		}
		if _, ok := b.entries[string(key)]; ok {
			copies++
		}
	}
	return copies
}

func TestMPTBlindWriteSameBucketCommitment(t *testing.T) {
	db := &testStore{memorydb.New()}
	tr := newTestTrie(t, db, true)

	key := make([]byte, 32)
	key[0] = 0x0a
	v1 := bytes.Repeat([]byte{0x11}, 32)
	v2 := bytes.Repeat([]byte{0x22}, 32)

	if err := tr.Put(key, v1); err != nil {
		t.Fatal(err)
	}
	expirePrune(t, tr, 0)

	if err := tr.Put(key, v2); err != nil {
		t.Fatal(err)
	}
	expirePrune(t, tr, 0)

	tr.mu.Lock()
	if copies := archivedCopiesLocked(t, tr, key); copies != 1 {
		t.Fatalf("want exactly 1 archived copy after same-bucket rewrite, got %d", copies)
	}
	verifyAllBucketsLocked(t, tr)
	tr.mu.Unlock()

	got, err := tr.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, v2) {
		t.Fatalf("Get returned stale value %x, want %x", got, v2)
	}
	tr.mu.Lock()
	if copies := archivedCopiesLocked(t, tr, key); copies != 0 {
		t.Fatalf("promotion left %d archived copies behind", copies)
	}
	verifyAllBucketsLocked(t, tr)
	tr.mu.Unlock()
}

func TestMPTBlindWriteCrossBucketDuplicate(t *testing.T) {
	db := &testStore{memorydb.New()}
	tr, err := New(nil, db, &Config{
		ShardDepthBits:            4,
		CuckooBuckets:             32,
		CuckooSlots:               4,
		BucketCapacity:            2,
		ActivateArchivedKeyOnRead: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	domain0Key := func(tag byte) []byte {
		k := make([]byte, 32)
		k[0] = tag & 0x0f
		k[31] = tag
		return k
	}
	k1, k2, k3 := domain0Key(1), domain0Key(2), domain0Key(3)
	v1 := bytes.Repeat([]byte{0x11}, 32)
	v2 := bytes.Repeat([]byte{0x22}, 32)
	w1 := bytes.Repeat([]byte{0x33}, 32)
	w3 := bytes.Repeat([]byte{0x44}, 32)

	if err := tr.Put(k1, v1); err != nil {
		t.Fatal(err)
	}
	if err := tr.Put(k2, w1); err != nil {
		t.Fatal(err)
	}
	expirePrune(t, tr, 0)

	if err := tr.Put(k1, v2); err != nil {
		t.Fatal(err)
	}
	if err := tr.Put(k3, w3); err != nil {
		t.Fatal(err)
	}
	checkWalk := func(stage string) {
		t.Helper()
		if err := tr.ForEach(func(key, value []byte) bool {
			if bytes.Equal(key, k1) && !bytes.Equal(value, v2) {
				t.Errorf("%s: ForEach returned %x for k1, want %x", stage, value, v2)
			}
			return true
		}); err != nil {
			t.Fatal(err)
		}
	}
	checkWalk("hot rewrite")
	expirePrune(t, tr, 0)

	tr.mu.Lock()
	if copies := archivedCopiesLocked(t, tr, k1); copies != 2 {
		t.Fatalf("setup: want k1 duplicated across 2 buckets, got %d", copies)
	}
	verifyAllBucketsLocked(t, tr)
	tr.mu.Unlock()
	checkWalk("archived rewrite")

	got, err := tr.Get(k1)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, v2) {
		t.Fatalf("Get returned stale value %x, want newest %x", got, v2)
	}
	tr.mu.Lock()
	if copies := archivedCopiesLocked(t, tr, k1); copies != 0 {
		t.Fatalf("promotion left %d archived copies of k1 behind", copies)
	}
	if copies := archivedCopiesLocked(t, tr, k2); copies != 1 {
		t.Fatalf("unrelated key k2 lost: %d copies", copies)
	}
	if copies := archivedCopiesLocked(t, tr, k3); copies != 1 {
		t.Fatalf("unrelated key k3 lost: %d copies", copies)
	}
	verifyAllBucketsLocked(t, tr)
	tr.mu.Unlock()

	if got, err := tr.Get(k2); err != nil || !bytes.Equal(got, w1) {
		t.Fatalf("k2: got %x err %v, want %x", got, err, w1)
	}
	if got, err := tr.Get(k3); err != nil || !bytes.Equal(got, w3) {
		t.Fatalf("k3: got %x err %v, want %x", got, err, w3)
	}
	tr.mu.Lock()
	verifyAllBucketsLocked(t, tr)
	tr.mu.Unlock()
}

func TestMPTBlindDeleteThenRewrite(t *testing.T) {
	db := &testStore{memorydb.New()}
	tr := newTestTrie(t, db, true)

	key := make([]byte, 32)
	key[0] = 0x05
	key[31] = 0x99
	v1 := bytes.Repeat([]byte{0x11}, 32)
	v2 := bytes.Repeat([]byte{0x22}, 32)

	if err := tr.Put(key, v1); err != nil {
		t.Fatal(err)
	}
	expirePrune(t, tr, 0)

	if err := tr.Delete(key); err != nil {
		t.Fatal(err)
	}
	if err := tr.Put(key, v2); err != nil {
		t.Fatal(err)
	}
	expirePrune(t, tr, 0)

	tr.mu.Lock()
	verifyAllBucketsLocked(t, tr)
	tr.mu.Unlock()

	got, err := tr.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, v2) {
		t.Fatalf("Get returned stale value %x, want %x", got, v2)
	}
	tr.mu.Lock()
	if copies := archivedCopiesLocked(t, tr, key); copies != 0 {
		t.Fatalf("promotion left %d archived copies behind", copies)
	}
	verifyAllBucketsLocked(t, tr)
	tr.mu.Unlock()
}
