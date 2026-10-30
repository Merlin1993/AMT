package mpt

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb/memorydb"
)

func shardBitKey13(prefix12 uint16, boundaryBit, low3, seq int) []byte {
	key := make([]byte, 32)
	key[0] = byte(prefix12 >> 4)
	key[1] = byte(prefix12&0xf)<<4 | byte(boundaryBit&1)<<3 | byte(low3&0x7)
	key[29] = byte(seq >> 8)
	key[30] = byte(seq)
	key[31] = byte(seq*7 + 1)
	return key
}

func TestMPTShardBitBoundarySplitD13(t *testing.T) {
	db := &testStore{memorydb.New()}
	tr, err := New(nil, db, &Config{ShardDepthBits: 13, CuckooBuckets: 32, CuckooSlots: 4})
	if err != nil {
		t.Fatal(err)
	}
	if got := tr.domainCount(); got != 8192 {
		t.Fatalf("domainCount = %d, want 2^13 = 8192", got)
	}

	kFreshA := shardBitKey13(0x200, 0, 5, 999)
	if err := tr.Put(kFreshA, []byte("fresh-A")); err != nil {
		t.Fatal(err)
	}

	tr.pruneDomainIdx = 0x401

	const prefix12 = 0x200
	var keysA, keysB [][]byte
	for i := 0; i < 4; i++ {
		kA := shardBitKey13(prefix12, 0, i, i)
		kB := shardBitKey13(prefix12, 1, i, 1000+i)
		vA := []byte(fmt.Sprintf("a-%d", i))
		vB := []byte(fmt.Sprintf("b-%d", i))
		if err := tr.Put(kA, vA); err != nil {
			t.Fatal(err)
		}
		if err := tr.Put(kB, vB); err != nil {
			t.Fatal(err)
		}
		keysA = append(keysA, kA)
		keysB = append(keysB, kB)
	}
	for _, k := range keysA {
		if got := tr.domainID(k); got != 0x400 {
			t.Fatalf("A key %x routes to domain %d, want 0x400", k[:4], got)
		}
		if epoch, ok := leafEpoch(t, tr, k); !ok || epoch != 0 {
			t.Fatalf("A key %x: epoch %d ok %v, want base bit 0", k[:4], epoch, ok)
		}
	}
	for _, k := range keysB {
		if got := tr.domainID(k); got != 0x401 {
			t.Fatalf("B key %x routes to domain %d, want 0x401", k[:4], got)
		}
		if epoch, ok := leafEpoch(t, tr, k); !ok || epoch != 1 {
			t.Fatalf("B key %x: epoch %d ok %v, want 1", k[:4], epoch, ok)
		}
	}

	tr.pruneDomainIdx = 0x400
	if err := tr.PruneNextShard(); err != nil {
		t.Fatal(err)
	}

	for i, k := range keysA {
		if _, fromArchive, err := tr.GetValueRef(k); err != nil || !fromArchive {
			t.Fatalf("A key %d after prune: fromArchive=%v err=%v; want an archive hit", i, fromArchive, err)
		}
	}
	b := bucketOfKey(t, tr, keysA[0])
	if b == nil {
		t.Fatal("no bucket claims the pruned A keys")
	}
	if b.count != len(keysA) {
		t.Fatalf("archived count = %d, want exactly the %d expired A leaves", b.count, len(keysA))
	}
	if _, fromArchive, err := tr.GetValueRef(kFreshA); err != nil || fromArchive {
		t.Fatalf("fresh A leaf after prune: fromArchive=%v err=%v; want a hot hit (epoch 1 survives)", fromArchive, err)
	}
	for i, k := range keysB {
		if _, fromArchive, err := tr.GetValueRef(k); err != nil || fromArchive {
			t.Fatalf("B key %d after prune: fromArchive=%v err=%v; want a hot hit (sibling domain untouched)", i, fromArchive, err)
		}
		if epoch, ok := leafEpoch(t, tr, k); !ok || epoch != 1 {
			t.Fatalf("B key %d after prune: epoch %d ok %v, want a live epoch-1 leaf", i, epoch, ok)
		}
	}
	root, err := tr.Commit()
	if err != nil {
		t.Fatal(err)
	}
	reloaded, err := New(root, db, &Config{ShardDepthBits: 13, CuckooBuckets: 32, CuckooSlots: 4})
	if err != nil {
		t.Fatal(err)
	}
	for i, k := range keysA {
		got, err := reloaded.Get(k)
		if err != nil || !bytes.Equal(got, []byte(fmt.Sprintf("a-%d", i))) {
			t.Fatalf("reloaded A key %d: %q err %v", i, got, err)
		}
	}
	for i, k := range keysB {
		got, err := reloaded.Get(k)
		if err != nil || !bytes.Equal(got, []byte(fmt.Sprintf("b-%d", i))) {
			t.Fatalf("reloaded B key %d: %q err %v", i, got, err)
		}
	}
	if got, err := reloaded.Get(kFreshA); err != nil || !bytes.Equal(got, []byte("fresh-A")) {
		t.Fatalf("reloaded fresh A: %q err %v", got, err)
	}
}

func TestMPTShardBitDeterministicRootD13(t *testing.T) {
	run := func() []byte {
		db := &testStore{memorydb.New()}
		tr, err := New(nil, db, &Config{ShardDepthBits: 13, CuckooBuckets: 32, CuckooSlots: 4})
		if err != nil {
			t.Fatal(err)
		}
		rng := rand.New(rand.NewPCG(0x5eed13, 20260923))
		mkKey := func() []byte {
			k := make([]byte, 32)
			k[0] = byte(rng.IntN(8))
			for i := 8; i < 32; i += 8 {
				v := rng.Uint64()
				for j := 0; j < 8; j++ {
					k[i+j] = byte(v >> (8 * j))
				}
			}
			return k
		}
		pool := make([][]byte, 0, 256)
		for i := 0; i < 128; i++ {
			pool = append(pool, mkKey())
		}
		for batch := 0; batch < 40; batch++ {
			for i := 0; i < 50; i++ {
				switch rng.IntN(4) {
				case 0, 1:
					k := pool[rng.IntN(len(pool))]
					if rng.IntN(5) == 0 {
						k = mkKey()
						pool = append(pool, k)
					}
					if err := tr.Put(k, []byte(fmt.Sprintf("b%d-i%d", batch, i))); err != nil {
						t.Fatal(err)
					}
				case 2:
					if err := tr.Delete(pool[rng.IntN(len(pool))]); err != nil {
						t.Fatal(err)
					}
				default:
					_, _ = tr.Get(pool[rng.IntN(len(pool))])
				}
			}
			if batch%2 == 1 {
				if err := tr.PruneNextShard(); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := tr.Commit(); err != nil {
				t.Fatal(err)
			}
		}
		return tr.Root()
	}
	a, b := run(), run()
	if !bytes.Equal(a, b) {
		t.Fatalf("non-deterministic roots at D=13: %x vs %x", a, b)
	}
}

func TestMPTShardBitCommitReloadWalkD13(t *testing.T) {
	db := &testStore{memorydb.New()}
	tr, err := New(nil, db, &Config{ShardDepthBits: 13, CuckooBuckets: 32, CuckooSlots: 4})
	if err != nil {
		t.Fatal(err)
	}
	live := map[string][]byte{}
	put := func(k []byte, tag string) {
		v := []byte(fmt.Sprintf("v-%s", tag))
		if err := tr.Put(k, v); err != nil {
			t.Fatal(err)
		}
		live[string(k)] = v
	}

	tr.pruneDomainIdx = 1
	for i := 0; i < 3; i++ {
		put(shardBitKey13(0x000, 0, i, i), fmt.Sprintf("d0-%d", i))
	}
	for p, prefix12 := range []uint16{0x200, 0xabc, 0xfff} {
		for bit := 0; bit < 2; bit++ {
			put(shardBitKey13(prefix12, bit, p, 3000+8*p+bit), fmt.Sprintf("p%d-b%d", p, bit))
		}
	}
	rng := rand.New(rand.NewPCG(0xb17, 13))
	for i := 0; i < 64; i++ {
		k := make([]byte, 32)
		k[0] = byte(rng.IntN(8))
		for j := 8; j < 32; j += 8 {
			v := rng.Uint64()
			for q := 0; q < 8; q++ {
				k[j+q] = byte(v >> (8 * q))
			}
		}
		put(k, fmt.Sprintf("rng-%d", i))
	}
	if _, err := tr.Commit(); err != nil {
		t.Fatal(err)
	}

	tr.pruneDomainIdx = 0
	if err := tr.PruneNextShard(); err != nil {
		t.Fatal(err)
	}
	root, err := tr.Commit()
	if err != nil {
		t.Fatal(err)
	}
	wantIdx, wantBase := tr.pruneDomainIdx, tr.baseBit

	reloaded, err := New(root, db, &Config{ShardDepthBits: 13, CuckooBuckets: 32, CuckooSlots: 4})
	if err != nil {
		t.Fatal(err)
	}
	if reloaded.pruneDomainIdx != wantIdx || reloaded.baseBit != wantBase {
		t.Fatalf("schedule did not persist: got (idx %d, base %d), want (%d, %d)",
			reloaded.pruneDomainIdx, reloaded.baseBit, wantIdx, wantBase)
	}
	for k, want := range live {
		got, err := reloaded.Get([]byte(k))
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("reloaded Get(%x) = %q, %v; want %q", []byte(k)[:4], got, err, want)
		}
	}
	for i := 0; i < 3; i++ {
		k := shardBitKey13(0x000, 0, i, i)
		if _, fromArchive, err := reloaded.GetValueRef(k); err != nil || !fromArchive {
			t.Fatalf("d0 key %d after reload: fromArchive=%v err=%v; want an archive hit", i, fromArchive, err)
		}
	}
	if _, fromArchive, err := reloaded.GetValueRef(shardBitKey13(0xabc, 1, 1, 3009)); err != nil || fromArchive {
		t.Fatalf("unpruned key after reload: fromArchive=%v err=%v; want a hot hit", fromArchive, err)
	}
	if err := reloaded.ensureHotLocked(); err != nil {
		t.Fatal(err)
	}
	if err := reloaded.hot.VerifyAggregates(); err != nil {
		t.Fatalf("aggregates after reload: %v", err)
	}
	again, err := reloaded.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(again, root) {
		t.Fatalf("recommit after reload: %x != committed %x", again, root)
	}
}

func TestMPTShardDepthBits19Sanity(t *testing.T) {
	db := &testStore{memorydb.New()}
	tr, err := New(nil, db, &Config{ShardDepthBits: 19, CuckooBuckets: 32, CuckooSlots: 4})
	if err != nil {
		t.Fatal(err)
	}
	if got := tr.domainCount(); got != 524288 {
		t.Fatalf("domainCount = %d, want 2^19 = 524288", got)
	}
	if got := tr.domainID(bytes.Repeat([]byte{0xff}, 32)); got != 0x7ffff {
		t.Fatalf("domainID(all-ones) = %d, want 0x7ffff", got)
	}

	mk := func(bit18, seq int) []byte {
		k := make([]byte, 32)
		k[2] = byte(bit18) << 5
		k[29] = byte(seq >> 8)
		k[30] = byte(seq)
		k[31] = byte(seq*3 + 1)
		return k
	}
	if got := tr.domainID(mk(0, 0)); got != 0 {
		t.Fatalf("domainID(bit18=0 key) = %d, want 0", got)
	}
	if got := tr.domainID(mk(1, 0)); got != 1 {
		t.Fatalf("domainID(bit18=1 key) = %d, want 1", got)
	}

	tr.pruneDomainIdx = 1
	var keysA, keysB [][]byte
	for i := 0; i < 3; i++ {
		kA, kB := mk(0, i), mk(1, 100+i)
		if err := tr.Put(kA, []byte(fmt.Sprintf("A%d", i))); err != nil {
			t.Fatal(err)
		}
		if err := tr.Put(kB, []byte(fmt.Sprintf("B%d", i))); err != nil {
			t.Fatal(err)
		}
		keysA = append(keysA, kA)
		keysB = append(keysB, kB)
	}
	tr.pruneDomainIdx = 0
	if err := tr.PruneNextShard(); err != nil {
		t.Fatal(err)
	}
	for i, k := range keysA {
		if _, fromArchive, err := tr.GetValueRef(k); err != nil || !fromArchive {
			t.Fatalf("D19 A key %d: fromArchive=%v err=%v; want an archive hit", i, fromArchive, err)
		}
	}
	for i, k := range keysB {
		if _, fromArchive, err := tr.GetValueRef(k); err != nil || fromArchive {
			t.Fatalf("D19 B key %d: fromArchive=%v err=%v; want a hot hit", i, fromArchive, err)
		}
	}
	if _, err := tr.Commit(); err != nil {
		t.Fatal(err)
	}
}
