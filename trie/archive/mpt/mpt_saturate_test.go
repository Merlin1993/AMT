package mpt

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb/memorydb"
	gethtrie "github.com/ethereum/go-ethereum/trie/archive/mpt/hx"
)

func saturateTestKey(i int) []byte {
	key := make([]byte, 32)
	key[0] = 0x10
	binary.BigEndian.PutUint64(key[24:], uint64(i))
	return key
}

func TestMPTCuckooSaturationRotatesSiblingBuckets(t *testing.T) {
	db := &testStore{memorydb.New()}
	tr, err := New(nil, db, &Config{
		ShardDepthBits: 4,
		CuckooBuckets:  1,
		CuckooSlots:    2,
		BucketCapacity: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	const n = 11
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
		t.Fatalf("mountEntriesLocked failed on saturated filter: %v", err)
	}
	tr.mu.Unlock()

	for i, e := range entries {
		value, err := tr.Get(e.Key)
		if err != nil {
			t.Fatalf("Get(%d) after saturation rotation: %v", i, err)
		}
		if !bytes.Equal(value, e.Value) {
			t.Fatalf("Get(%d) = %q, want %q", i, value, e.Value)
		}
	}

	tr.mu.Lock()
	defer tr.mu.Unlock()
	total := 0
	for path, b := range tr.buckets {
		if b.count != len(b.entries) {
			t.Fatalf("bucket %x: stub count %d, payload %d entries", path, b.count, len(b.entries))
		}
		if b.count > 2 {
			t.Fatalf("bucket %x holds %d entries, exceeds the 1x2 filter", path, b.count)
		}
		if err := tr.verifyBucketLocked(b); err != nil {
			t.Fatalf("bucket %x: %v", path, err)
		}
		for key := range b.entries {
			if !b.filter.Lookup([]byte(key)) {
				t.Fatalf("bucket %x: filter lost fingerprint of resident key %x", path, []byte(key))
			}
		}
		total += b.count
	}
	if total != n {
		t.Fatalf("buckets hold %d entries, mounted %d", total, n)
	}
	if tr.residentEntries != n {
		t.Fatalf("residentEntries = %d, want %d", tr.residentEntries, n)
	}
	if len(tr.buckets) < 2 {
		t.Fatalf("saturation should force sibling buckets, got %d", len(tr.buckets))
	}
}

func TestMPTBucketAppendSaturationKeepsPrefix(t *testing.T) {
	db := &testStore{memorydb.New()}
	tr, err := New(nil, db, &Config{
		ShardDepthBits: 4,
		CuckooBuckets:  1,
		CuckooSlots:    3,
		BucketCapacity: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	tr.mu.Lock()
	defer tr.mu.Unlock()
	if err := tr.ensureHotLocked(); err != nil {
		t.Fatal(err)
	}
	seed := []gethtrie.ExtractedEntry{
		{Key: saturateTestKey(0), Value: []byte("v0")},
		{Key: saturateTestKey(1), Value: []byte("v1")},
	}
	if err := tr.mountEntriesLocked([]byte{1}, seed); err != nil {
		t.Fatal(err)
	}
	var b *bucket
	for _, cand := range tr.buckets {
		b = cand
	}
	if b == nil || b.count != 2 {
		t.Fatalf("seed bucket: %+v", b)
	}

	appended := []gethtrie.ExtractedEntry{
		{Key: saturateTestKey(2), Value: []byte("v2")},
		{Key: saturateTestKey(3), Value: []byte("v3")},
	}
	consumed, err := tr.bucketAppendLocked(b, appended)
	if !errors.Is(err, errBucketSaturated) {
		t.Fatalf("append err = %v, want errBucketSaturated", err)
	}
	if consumed != 1 {
		t.Fatalf("consumed = %d, want 1", consumed)
	}
	if b.count != 3 {
		t.Fatalf("bucket count = %d, want 3", b.count)
	}
	if err := tr.verifyBucketLocked(b); err != nil {
		t.Fatal(err)
	}
	for _, e := range appended[:1] {
		if _, ok := b.entries[string(e.Key)]; !ok {
			t.Fatalf("absorbed entry %x missing from bucket", e.Key)
		}
	}
	if _, ok := b.entries[string(appended[1].Key)]; ok {
		t.Fatalf("rejected entry %x leaked into the bucket", appended[1].Key)
	}
	for key := range b.entries {
		if !b.filter.Lookup([]byte(key)) {
			t.Fatalf("filter lost fingerprint of resident key %x", []byte(key))
		}
	}

	filterBefore := b.filter.Encode()
	consumed, err = tr.bucketAppendLocked(b, appended[1:])
	if !errors.Is(err, errBucketSaturated) || consumed != 0 {
		t.Fatalf("full-bucket append = (%d, %v), want (0, errBucketSaturated)", consumed, err)
	}
	if b.count != 3 {
		t.Fatalf("full-bucket append mutated count: %d", b.count)
	}
	if !bytes.Equal(b.filter.Encode(), filterBefore) {
		t.Fatal("full-bucket append mutated the filter after a failed insertion")
	}
}
