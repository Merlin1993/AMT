package mpt

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb/memorydb"
)

func TestMPTArchivePayloadBytesAccounting(t *testing.T) {
	db := &testStore{memorydb.New()}
	tr := newTestTrie(t, db, true)

	entries := map[string]string{
		string([]byte{0x00, 'a'}): "v",
		string([]byte{0x01, 'b'}): "vvvv",
		string([]byte{0x02, 'c'}): "vvvvvvv",
		string([]byte{0x03, 'd'}): "vv",
		string([]byte{0x04, 'e'}): "vvvvvvvvv",
		string([]byte{0x05, 'f'}): "vvv",
	}
	for key, value := range entries {
		if err := tr.Put([]byte(key), []byte(value)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := tr.Commit(); err != nil {
		t.Fatal(err)
	}
	if got := tr.ArchivePayloadBytes(); got != 0 {
		t.Fatalf("before prune: archiveBytes = %d, want 0", got)
	}

	expirePrune(t, tr, 0)
	root, err := tr.Commit()
	if err != nil {
		t.Fatal(err)
	}

	var want int64
	if len(tr.buckets) == 0 {
		t.Fatal("nothing archived; test setup broken")
	}
	for _, b := range tr.buckets {
		if got, encoded := bucketRecordSize(b.entries), int64(len(tr.encodeBucket(b))); got != encoded {
			t.Fatalf("bucketRecordSize = %d, len(encodeBucket) = %d", got, encoded)
		}
		want += bucketRecordSize(b.entries)
	}
	if got := tr.ArchivePayloadBytes(); got != want {
		t.Fatalf("after prune: archiveBytes = %d, want %d", got, want)
	}

	reloaded, err := New(root, db, &Config{ShardDepthBits: 4, ActivateArchivedKeyOnRead: true})
	if err != nil {
		t.Fatal(err)
	}
	if got := reloaded.ArchivePayloadBytes(); got != want {
		t.Fatalf("after reload: archiveBytes = %d, want %d", got, want)
	}

	keys := make([]string, 0, len(entries))
	for key := range entries {
		keys = append(keys, key)
	}
	for _, key := range keys {
		got, err := reloaded.Get([]byte(key))
		if err != nil || !bytes.Equal(got, []byte(entries[key])) {
			t.Fatalf("resurrect Get(%x) = %x, %v", key, got, err)
		}
	}
	if got := reloaded.ArchivePayloadBytes(); got != 0 {
		t.Fatalf("after resurrecting all: archiveBytes = %d, want 0 (buckets=%d)", got, len(reloaded.buckets))
	}
}

func TestMPTArchivePayloadBytesAppendSplit(t *testing.T) {
	db := &testStore{memorydb.New()}
	tr := newTestTrie(t, db, false)

	const n = 250
	for i := 0; i < n; i++ {
		key := []byte{0x00, byte(i), byte(i >> 8)}
		if err := tr.Put(key, []byte(fmt.Sprintf("value-%04d", i))); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := tr.Commit(); err != nil {
		t.Fatal(err)
	}
	expirePrune(t, tr, 0)
	if _, err := tr.Commit(); err != nil {
		t.Fatal(err)
	}

	var want int64
	var count int
	for _, b := range tr.buckets {
		want += bucketRecordSize(b.entries)
		count += b.count
	}
	if count != n {
		t.Fatalf("archived entries = %d, want %d", count, n)
	}
	if got := tr.ArchivePayloadBytes(); got != want {
		t.Fatalf("archiveBytes = %d, want %d", got, want)
	}
}
