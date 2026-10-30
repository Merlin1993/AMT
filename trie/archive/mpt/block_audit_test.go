package mpt

import (
	"bytes"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb/memorydb"
	archivetrie "github.com/ethereum/go-ethereum/trie/archive"
)

func TestBlockAuditDeduplicatesBucketAndCommits(t *testing.T) {
	db := &testStore{memorydb.New()}
	tr := newTestTrie(t, db, true)
	keys := [][]byte{{0x00, 0x11}, {0x00, 0x12}}
	for i, key := range keys {
		if err := tr.Put(key, []byte{byte(i + 1)}); err != nil {
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
	tr.config.VerifyAtCommit = true
	tr.config.VerifyBucketWorkers = 8
	tr.config.VerifyItemWorkers = 2
	before, _ := archivetrie.MPTRedemptionVerifyStats()
	b := bucketOfKey(t, tr, keys[0])
	wantProofBytes := int64(len(b.metaBytes)) + b.payloadBytes
	for i, key := range keys {
		got, err := tr.Get(key)
		if err != nil || !bytes.Equal(got, []byte{byte(i + 1)}) {
			t.Fatalf("Get(%x) = %x, %v", key, got, err)
		}
	}
	if buckets, proofBytes := tr.PendingProofStats(); buckets != 1 || proofBytes != wantProofBytes {
		t.Fatalf("deduplicated proof = %d buckets, %d bytes; want 1, %d", buckets, proofBytes, wantProofBytes)
	}
	if _, err := tr.Commit(); err != nil {
		t.Fatal(err)
	}
	after, _ := archivetrie.MPTRedemptionVerifyStats()
	if after-before != 1 || len(tr.pendingAudits) != 0 {
		t.Fatalf("audited buckets = %d, pending = %d", after-before, len(tr.pendingAudits))
	}
}

func TestBlockAuditFailureRollsBackSpeculativeRedemption(t *testing.T) {
	for _, backend := range []string{BackendHash, BackendPath} {
		t.Run(backend, func(t *testing.T) {
			db := &testStore{memorydb.New()}
			tr := newTestTrieWithBackend(t, db, backend, true)
			key := []byte{0x00, 0x11}
			value := []byte("original")
			if err := tr.Put(key, value); err != nil {
				t.Fatal(err)
			}
			if _, err := tr.Commit(); err != nil {
				t.Fatal(err)
			}
			expirePrune(t, tr, 0)
			root, err := tr.Commit()
			if err != nil {
				t.Fatal(err)
			}
			tr.config.VerifyAtCommit = true
			if _, err := tr.Get(key); err != nil {
				t.Fatal(err)
			}
			tr.pendingAudits[0].entries[0].Value[0] ^= 0xff
			if _, err := tr.Commit(); err == nil {
				t.Fatal("corrupt proof committed")
			}
			if !bytes.Equal(root, tr.rootBytes()) || tr.dirty || len(tr.pendingAudits) != 0 {
				t.Fatal("failed audit left speculative state")
			}
			if _, archived, err := tr.GetValueRef(key); err != nil || !archived {
				t.Fatalf("rollback did not restore archived value: archived=%t err=%v", archived, err)
			}
		})
	}
}
