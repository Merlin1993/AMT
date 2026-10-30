package mpt

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

func TestBucketItemSHA256Input(t *testing.T) {
	key := []byte{0x12, 0x34, 0x56, 0x78}
	value := []byte("payload")
	const want = "12345678239f59ed55e737c77147cf55ad0c1b030b6d7ee748a7426952f9b852d5a935e5"
	if got := hex.EncodeToString(bucketItem(key, value)); got != want {
		t.Fatalf("ECMH bucket item mismatch: %s", got)
	}
	const wantDigest = "b74ff5c16b697325ea5a862a81c4cb01a4e310f502a3331ef718990719e74af6"
	if got := sha256.Sum256(bucketItem(key, value)); hex.EncodeToString(got[:]) != wantDigest {
		t.Fatalf("ECMH entry digest mismatch: %x", got)
	}
}
