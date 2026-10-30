package mpt

import (
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb/memorydb"
)

func TestArchivedGetRejectsCorruptBucketOnEveryRead(t *testing.T) {
	for _, activate := range []bool{false, true} {
		t.Run(map[bool]string{false: "read", true: "resurrection"}[activate], func(t *testing.T) {
			tr := newTestTrie(t, &testStore{memorydb.New()}, activate)
			key := []byte{0x00, 'v'}
			if err := tr.Put(key, []byte("original")); err != nil {
				t.Fatal(err)
			}
			expirePrune(t, tr, 0)

			corrupted := false
			for _, bucket := range tr.buckets {
				if _, ok := bucket.entries[string(key)]; ok {
					bucket.entries[string(key)] = []byte("forged")
					corrupted = true
					break
				}
			}
			if !corrupted {
				t.Fatal("key was not archived")
			}
			for i := 0; i < 2; i++ {
				if _, err := tr.Get(key); err == nil || !strings.Contains(err.Error(), "ECMH commitment mismatch") {
					t.Fatalf("read %d: got error %v, want commitment mismatch", i+1, err)
				}
			}
		})
	}
}
