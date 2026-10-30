package ecmh

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"math/rand"
	"strings"
	"testing"

	bytemaresecp256k1 "github.com/bytemare/secp256k1"
	secp256k1 "github.com/decred/dcrd/dcrec/secp256k1/v4"
)

func makeItem(r *rand.Rand) []byte {
	item := make([]byte, 64)
	r.Read(item)
	return item
}

func makeItems(r *rand.Rand, n int) [][]byte {
	items := make([][]byte, n)
	for i := range items {
		items[i] = makeItem(r)
	}
	return items
}

func TestCreateEmpty(t *testing.T) {
	c := Create(nil)
	if !c.IsZero() {
		t.Fatalf("Create(nil) = %x, want zero (infinity)", c)
	}
	if got := Create([][]byte{}); !got.IsZero() {
		t.Fatalf("Create(empty) = %x, want zero (infinity)", got)
	}
}

func TestHashToCurve(t *testing.T) {
	inputs := [][]byte{
		[]byte(""),
		[]byte("a"),
		[]byte("hello ecmh"),
		make([]byte, 64),
	}
	for i, in := range inputs {
		p1 := HashToCurve(in)
		if len(p1) != 33 {
			t.Fatalf("case %d: HashToCurve length = %d, want 33", i, len(p1))
		}
		if _, err := secp256k1.ParsePubKey(p1); err != nil {
			t.Fatalf("case %d: ParsePubKey failed: %v", i, err)
		}
		p2 := HashToCurve(in)
		if string(p1) != string(p2) {
			t.Fatalf("case %d: HashToCurve not deterministic", i)
		}
	}
	if string(HashToCurve([]byte("x"))) == string(HashToCurve([]byte("y"))) {
		t.Fatal("HashToCurve collided for distinct inputs")
	}
}

func TestHashToCurveRFC9380Input(t *testing.T) {
	item := []byte("abc")
	digest := sha256.Sum256(item)
	point, err := bytemaresecp256k1.HashToGroup(digest[:], []byte(mptRFC9380DST))
	if err != nil {
		t.Fatal(err)
	}
	if got := HashToCurve(item); !bytes.Equal(got, point.Encode()) {
		t.Fatalf("RFC 9380 mapping mismatch: %x", got)
	}
}

func TestRFC9380OfficialVectors(t *testing.T) {
	const dst = "QUUX-V01-CS02-with-secp256k1_XMD:SHA-256_SSWU_RO_"
	vectors := []struct{ msg, x, y string }{
		{"", "c1cae290e291aee617ebaef1be6d73861479c48b841eaba9b7b5852ddfeb1346", "64fa678e07ae116126f08b022a94af6de15985c996c3a91b64c406a960e51067"},
		{"abc", "3377e01eab42db296b512293120c6cee72b6ecf9f9205760bd9ff11fb3cb2c4b", "7f95890f33efebd1044d382a01b1bee0900fb6116f94688d487c6c7b9c8371f6"},
		{"abcdef0123456789", "bac54083f293f1fe08e4a70137260aa90783a5cb84d3f35848b324d0674b0e3a", "4436476085d4c3c4508b60fcf4389c40176adce756b398bdee27bca19758d828"},
		{"q128_" + strings.Repeat("q", 128), "e2167bc785333a37aa562f021f1e881defb853839babf52a7f72b102e41890e9", "f2401dd95cc35867ffed4f367cd564763719fbc6a53e969fb8496a1e6685d873"},
		{"a512_" + strings.Repeat("a", 512), "e3c8d35aaaf0b9b647e88a0a0a7ee5d5bed5ad38238152e4e6fd8c1f8cb7c998", "8446eeb6181bf12f56a9d24e262221cc2f0c4725c7e3803024b5888ee5823aa6"},
	}
	for i, v := range vectors {
		point, err := bytemaresecp256k1.HashToGroup([]byte(v.msg), []byte(dst))
		if err != nil {
			t.Fatalf("vector %d: %v", i, err)
		}
		want, err := hex.DecodeString(v.x + v.y)
		if err != nil {
			t.Fatal(err)
		}
		got := point.EncodeUncompressed()
		if len(got) != 65 || got[0] != 4 || !bytes.Equal(got[1:], want) {
			t.Fatalf("vector %d mismatch: got %x", i, got)
		}
	}
}

func TestRoundTrip(t *testing.T) {
	r := rand.New(rand.NewSource(42))
	items := makeItems(r, 5)
	a := Create(items)

	extra := makeItem(r)
	b := BlindAppend(a, extra)
	if b == a {
		t.Fatal("BlindAppend did not change the commitment")
	}
	if got := Delete(b, extra); got != a {
		t.Fatalf("Delete(BlindAppend(A, x), x) != A\n got: %x\nwant: %x", got, a)
	}
	c := BlindAppend(Commitment{}, extra)
	if c.IsZero() {
		t.Fatal("BlindAppend(zero, x) is still zero")
	}
	if got := Delete(c, extra); !got.IsZero() {
		t.Fatalf("Delete(Append(zero, x), x) = %x, want zero (infinity)", got)
	}
}

func TestAppendEqualsCreate(t *testing.T) {
	r := rand.New(rand.NewSource(7))
	items := makeItems(r, 4)

	c := Commitment{}
	for _, it := range items {
		c = BlindAppend(c, it)
	}
	if want := Create(items); c != want {
		t.Fatalf("incremental BlindAppend != Create\n got: %x\nwant: %x", c, want)
	}
	c2 := Delete(c, items[1])
	if want := Create([][]byte{items[0], items[2], items[3]}); c2 != want {
		t.Fatalf("Delete mismatch\n got: %x\nwant: %x", c2, want)
	}
}

func TestCommutative(t *testing.T) {
	r := rand.New(rand.NewSource(99))
	items := makeItems(r, 10)
	base := Create(items)

	perm := rand.New(rand.NewSource(1)).Perm(len(items))
	shuffled := make([][]byte, len(items))
	for i, j := range perm {
		shuffled[i] = items[j]
	}
	if got := Create(shuffled); got != base {
		t.Fatalf("Create not commutative\n got: %x\nwant: %x", got, base)
	}

	dup := Create([][]byte{items[0], items[0]})
	if dup == Create([][]byte{items[0]}) {
		t.Fatal("multiset multiplicity ignored: {a,a} == {a}")
	}
	x := Create([][]byte{items[0], items[0], items[1]})
	y := Create([][]byte{items[1], items[0], items[0]})
	if x != y {
		t.Fatal("multiset with duplicates not order-independent")
	}
}

func TestVerify(t *testing.T) {
	r := rand.New(rand.NewSource(2024))
	items := makeItems(r, 8)
	c := Create(items)

	if !Verify(c, items) {
		t.Fatal("Verify failed for genuine commitment")
	}
	if !Verify(Commitment{}, nil) {
		t.Fatal("Verify(zero, nil) failed")
	}
	if Verify(c, nil) {
		t.Fatal("Verify(c, nil) succeeded for non-zero c")
	}
	if Verify(c, items[:len(items)-1]) {
		t.Fatal("Verify succeeded with an item dropped")
	}
}

func TestVerifyTamper(t *testing.T) {
	r := rand.New(rand.NewSource(31337))
	items := makeItems(r, 3)
	c := Create(items)

	for pos := 0; pos < 64; pos++ {
		tampered := make([][]byte, len(items))
		for i, it := range items {
			cp := make([]byte, len(it))
			copy(cp, it)
			tampered[i] = cp
		}
		tampered[1][pos] ^= 0x01
		if Verify(c, tampered) {
			t.Fatalf("Verify succeeded with byte %d tampered", pos)
		}
	}
	for pos := 0; pos < 33; pos++ {
		bad := c
		bad[pos] ^= 0x01
		if Verify(bad, items) {
			t.Fatalf("Verify succeeded with commitment byte %d tampered", pos)
		}
	}
}

func TestVerify100(t *testing.T) {
	r := rand.New(rand.NewSource(100))
	items := makeItems(r, 100)
	c := Create(items)
	if c.IsZero() {
		t.Fatal("commitment of 100 items is zero (astronomically unlikely)")
	}
	if !Verify(c, items) {
		t.Fatal("Verify failed for 100 fixed-seed items")
	}
	inc := Commitment{}
	for _, it := range items {
		inc = BlindAppend(inc, it)
	}
	if inc != c {
		t.Fatal("incremental construction over 100 items != Create")
	}
	for _, it := range items {
		inc = Delete(inc, it)
	}
	if !inc.IsZero() {
		t.Fatal("deleting all 100 items did not return to infinity")
	}
}
