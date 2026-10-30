package mpt

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb/memorydb"
	archivetrie "github.com/ethereum/go-ethereum/trie/archive"
	"github.com/ethereum/go-ethereum/trie/archive/cuckoo"
)

func TestMPTKeyHasBitPrefixBoundary(t *testing.T) {
	key := []byte{0xab, 0xcd, 0xef} // 10101011 11001101 11101111
	var buf [32]byte
	copy(buf[:], key)

	if !keyHasBitPrefix(key, &buf, 0) {
		t.Fatal("0-bit prefix must match")
	}
	if !keyHasBitPrefix(key, &buf, 1) {
		t.Fatal("1-bit prefix must match")
	}
	var wrong [32]byte
	wrong[0] = 0xc0
	if keyHasBitPrefix(key, &wrong, 2) {
		t.Fatal("2-bit prefix diverging at bit 1 must not match")
	}
	var p19 [32]byte
	copy(p19[:], key)
	if !keyHasBitPrefix(key, &p19, 19) {
		t.Fatal("19-bit self prefix must match")
	}
	p19[2] ^= 0x20
	if keyHasBitPrefix(key, &p19, 19) {
		t.Fatal("prefix diverging at bit 18 must not match")
	}
	if !keyHasBitPrefix(key, &p19, 18) {
		t.Fatal("18-bit prefix must match (divergence is past it)")
	}
	full := make([]byte, 32)
	for i := range full {
		full[i] = byte(i * 7)
	}
	var p256 [32]byte
	copy(p256[:], full)
	if !keyHasBitPrefix(full, &p256, 256) {
		t.Fatal("256-bit self prefix must match")
	}
	p256[31] ^= 1
	if keyHasBitPrefix(full, &p256, 256) {
		t.Fatal("256-bit prefix with last bit flipped must not match")
	}
	if keyHasBitPrefix(key, &buf, 25*8) {
		t.Fatal("prefix longer than the key must not match")
	}
}

func TestMPTCoveragePrefixBits(t *testing.T) {
	if _, n := coveragePrefixBits(nil); n != 0 {
		t.Fatalf("empty entries: prefixBits = %d, want 0", n)
	}
	single := map[string][]byte{string([]byte{0x12, 0x34}): []byte("v")}
	buf, n := coveragePrefixBits(single)
	if n != 16 || buf[0] != 0x12 || buf[1] != 0x34 {
		t.Fatalf("single entry: bits=%d buf=%x; want 16 bits of 1234", n, buf[:2])
	}
	pair := map[string][]byte{
		string([]byte{0x00, 0x6b}): []byte("a"),
		string([]byte{0x00, 0x6c}): []byte("b"),
	}
	buf, n = coveragePrefixBits(pair)
	if n != 13 {
		t.Fatalf("pair LCP = %d bits, want 13", n)
	}
	if buf[0] != 0x00 || buf[1] != 0x68 {
		t.Fatalf("pair prefix = %x, want canonical 0068", buf[:2])
	}
	disjoint := map[string][]byte{
		string([]byte{0x00}): []byte("a"),
		string([]byte{0x80}): []byte("b"),
	}
	if _, n := coveragePrefixBits(disjoint); n != 0 {
		t.Fatalf("disjoint keys: prefixBits = %d, want 0", n)
	}
	long := append(bytes.Repeat([]byte{0x5a}, 32), 0xff, 0xff)
	capped := map[string][]byte{
		string(bytes.Repeat([]byte{0x5a}, 32)): []byte("a"),
		string(long):                           []byte("b"),
	}
	buf, n = coveragePrefixBits(capped)
	if n != 256 {
		t.Fatalf("capped LCP = %d bits, want 256", n)
	}
	if buf[0] != 0x5a || buf[31] != 0x5a {
		t.Fatalf("capped prefix wrong: %x…%x", buf[:1], buf[31:])
	}
}

func TestMPTStubMetaV3RoundTrip(t *testing.T) {
	filter := cuckoo.New(32, 4)
	for _, k := range [][]byte{{0x00, 'a'}, {0x00, 'b'}} {
		if err := filter.Insert(k); err != nil {
			t.Fatal(err)
		}
	}
	for _, bits := range []uint16{0, 1, 7, 13, 19, 255, 256} {
		b := &bucket{
			path:       []byte{0x00, 0x01},
			filter:     filter,
			count:      2,
			prefixBits: bits,
		}
		for i := range b.prefixBuf {
			b.prefixBuf[i] = byte(i*3 + 1)
		}
		b.commitment[0] = 0x42
		rec := b.stub()
		if len(b.metaBytes) == 0 {
			t.Fatal("stub() did not cache the rendered record")
		}
		buf, gotBits, filterBytes, commitment, count, err := decodeStubMeta(b.metaBytes)
		if err != nil {
			t.Fatalf("bits=%d decode: %v", bits, err)
		}
		if gotBits != bits {
			t.Fatalf("bits=%d: decoded prefixBits=%d", bits, gotBits)
		}
		pbytes := (int(bits) + 7) / 8
		if !bytes.Equal(buf[:pbytes], b.prefixBuf[:pbytes]) {
			t.Fatalf("bits=%d: prefix bytes %x != %x", bits, buf[:pbytes], b.prefixBuf[:pbytes])
		}
		if !bytes.Equal(filterBytes, filter.Encode()) {
			t.Fatalf("bits=%d: filter bytes differ", bits)
		}
		if commitment != b.commitment || count != 2 {
			t.Fatalf("bits=%d: commitment/count mismatch", bits)
		}
		if b.metaHash != rec.ContentHash {
			t.Fatalf("bits=%d: anchor hash mismatch", bits)
		}
	}
}

func TestMPTStubMetaRejectsInvalidRecords(t *testing.T) {
	if _, _, _, _, _, err := decodeStubMeta(append([]byte{9}, make([]byte, 43)...)); err == nil {
		t.Fatal("unknown version must fail")
	}
	if _, _, _, _, _, err := decodeStubMeta([]byte{3, 0, 19, 0x00}); err == nil {
		t.Fatal("truncated record must fail")
	}
	bad := []byte{3, 1, 0x21}
	bad = append(bad, make([]byte, 4+33+4)...)
	if _, _, _, _, _, err := decodeStubMeta(bad); err == nil {
		t.Fatal("prefixBits > 256 must fail")
	}
}

func TestMPTDuplicateArchivedKeyCounter(t *testing.T) {
	db := &testStore{memorydb.New()}
	tr, err := New(nil, db, &Config{
		ShardDepthBits: 4,
		CuckooBuckets:  32,
		CuckooSlots:    4,
		BucketCapacity: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	key := []byte{0x00, 'x'}
	if err := tr.Put(key, []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if _, err := tr.Commit(); err != nil {
		t.Fatal(err)
	}
	expirePrune(t, tr, 0)
	if _, err := tr.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := tr.Put(key, []byte("v2")); err != nil {
		t.Fatal(err)
	}
	if _, err := tr.Commit(); err != nil {
		t.Fatal(err)
	}
	expirePrune(t, tr, 0)
	if _, err := tr.Commit(); err != nil {
		t.Fatal(err)
	}
	if got := len(tr.buckets); got != 2 {
		t.Fatalf("buckets = %d, want 2 (sibling per copy)", got)
	}
	before := archivetrie.MPTDuplicateArchivedKeys()
	if _, fromArchive, err := tr.GetValueRef(key); err != nil || !fromArchive {
		t.Fatalf("GetValueRef = archive=%v err=%v; want an archive hit", fromArchive, err)
	}
	if d := archivetrie.MPTDuplicateArchivedKeys() - before; d != 1 {
		t.Fatalf("duplicate counter +%d, want +1", d)
	}
	if _, _, err := tr.GetValueRef([]byte{0x00, 'z'}); err != ErrNotFound {
		t.Fatalf("absent probe err = %v, want ErrNotFound", err)
	}
	if d := archivetrie.MPTDuplicateArchivedKeys() - before; d != 1 {
		t.Fatalf("absent probe moved the counter: +%d total, want +1", d)
	}
}

func TestMPTSiblingShardBitSkipD19(t *testing.T) {
	db := &testStore{memorydb.New()}
	tr, err := New(nil, db, &Config{ShardDepthBits: 19, CuckooBuckets: 32, CuckooSlots: 4})
	if err != nil {
		t.Fatal(err)
	}
	mk := func(bit18, seq int) []byte {
		k := make([]byte, 32)
		k[2] = byte(bit18) << 5
		k[29] = byte(seq >> 8)
		k[30] = byte(seq)
		k[31] = byte(seq*3 + 1)
		return k
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
	if _, err := tr.Commit(); err != nil {
		t.Fatal(err)
	}
	tr.pruneDomainIdx = 0
	if err := tr.PruneNextShard(); err != nil {
		t.Fatal(err)
	}
	tr.baseBit = 1
	if err := tr.PruneNextShard(); err != nil {
		t.Fatal(err)
	}
	if _, err := tr.Commit(); err != nil {
		t.Fatal(err)
	}
	if got := len(tr.buckets); got != 2 {
		t.Fatalf("buckets = %d, want 2 sibling buckets", got)
	}

	before := archivetrie.LastUpdateDiagnostics()
	if _, fromArchive, err := tr.GetValueRef(keysA[0]); err != nil || !fromArchive {
		t.Fatalf("shard-0 probe: archive=%v err=%v; want an archive hit", fromArchive, err)
	}
	mid := archivetrie.LastUpdateDiagnostics()
	if _, fromArchive, err := tr.GetValueRef(keysB[0]); err != nil || !fromArchive {
		t.Fatalf("shard-1 probe: archive=%v err=%v; want an archive hit", fromArchive, err)
	}
	after := archivetrie.LastUpdateDiagnostics()

	d1 := mid.ArchiveFilterLookups - before.ArchiveFilterLookups
	d2 := after.ArchiveFilterLookups - mid.ArchiveFilterLookups
	if d1 != 1 || d2 != 1 {
		t.Fatalf("filter lookups per probe = %d, %d; want 1, 1 (sibling skipped via bit coverage)", d1, d2)
	}
	if d := after.ArchiveFilterPositives - before.ArchiveFilterPositives; d != 2 {
		t.Fatalf("positives +%d, want 2", d)
	}
}
