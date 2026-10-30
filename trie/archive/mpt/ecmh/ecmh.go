// 桶承诺是各条目曲线点的和；追加或删除只需更新对应条目，无须遍历整个桶。
package ecmh

import (
	"crypto/sha256"
	"sync"

	bytemaresecp256k1 "github.com/bytemare/secp256k1"
	secp256k1 "github.com/decred/dcrd/dcrec/secp256k1/v4"
)

// Commitment 使用 33 字节压缩曲线点；全零值表示空集合。
type Commitment [33]byte

// Entry 保存归档键及其原始值。
type Entry struct {
	Key, Value []byte
}

const mptRFC9380DST = "AMT-MPT-ECMH-v1-secp256k1_XMD:SHA-256_SSWU_RO_"

// HashToCurve 按 RFC 9380 将条目映射到曲线点。
func HashToCurve(data []byte) (pointBytes33 []byte) {
	digest := sha256.Sum256(data)
	point, err := bytemaresecp256k1.HashToGroup(digest[:], []byte(mptRFC9380DST))
	if err != nil {
		panic(err)
	}
	return point.Encode()
}

func itemPoint(item []byte) secp256k1.JacobianPoint {
	pub, err := secp256k1.ParsePubKey(HashToCurve(item))
	if err != nil {
		panic("ecmh: HashToCurve returned an unparseable point")
	}
	var p secp256k1.JacobianPoint
	pub.AsJacobian(&p)
	return p
}

func decodePoint(c Commitment) secp256k1.JacobianPoint {
	var p secp256k1.JacobianPoint
	if c.IsZero() {
		return p
	}
	pub, err := secp256k1.ParsePubKey(c[:])
	if err != nil {
		panic("ecmh: invalid commitment encoding")
	}
	pub.AsJacobian(&p)
	return p
}

func encodePoint(p *secp256k1.JacobianPoint) Commitment {
	var c Commitment
	if p.Z.IsZero() || (p.X.IsZero() && p.Y.IsZero()) {
		return c
	}
	p.ToAffine()
	pub := secp256k1.NewPublicKey(&p.X, &p.Y)
	copy(c[:], pub.SerializeCompressed())
	return c
}

func Create(items [][]byte) Commitment {
	var sum secp256k1.JacobianPoint
	for _, item := range items {
		p := itemPoint(item)
		secp256k1.AddNonConst(&sum, &p, &sum)
	}
	return encodePoint(&sum)
}

// CreateEntriesParallel 并行计算各条目的值哈希及曲线点。
func CreateEntriesParallel(entries []Entry, workers int) Commitment {
	if len(entries) == 0 {
		return Commitment{}
	}
	workers = max(1, min(workers, len(entries)))
	partials := make([]secp256k1.JacobianPoint, workers)
	addRange := func(worker int) {
		for _, entry := range entries[worker*len(entries)/workers : (worker+1)*len(entries)/workers] {
			valueHash := sha256.Sum256(entry.Value)
			item := make([]byte, 0, len(entry.Key)+len(valueHash))
			item = append(item, entry.Key...)
			item = append(item, valueHash[:]...)
			point := itemPoint(item)
			secp256k1.AddNonConst(&partials[worker], &point, &partials[worker])
		}
	}
	var wg sync.WaitGroup
	for worker := 1; worker < workers; worker++ {
		worker := worker
		wg.Add(1)
		go func() { defer wg.Done(); addRange(worker) }()
	}
	addRange(0)
	wg.Wait()
	var sum secp256k1.JacobianPoint
	for i := range partials {
		secp256k1.AddNonConst(&sum, &partials[i], &sum)
	}
	return encodePoint(&sum)
}

// BlindAppend 只凭现有承诺和新条目更新桶承诺。
func BlindAppend(c Commitment, item []byte) Commitment {
	sum := decodePoint(c)
	p := itemPoint(item)
	secp256k1.AddNonConst(&sum, &p, &sum)
	return encodePoint(&sum)
}

func Delete(c Commitment, item []byte) Commitment {
	sum := decodePoint(c)
	p := itemPoint(item)
	p.Y.Negate(1).Normalize()
	secp256k1.AddNonConst(&sum, &p, &sum)
	return encodePoint(&sum)
}

func Verify(c Commitment, items [][]byte) bool {
	return Create(items) == c
}

func (c Commitment) IsZero() bool {
	return c == Commitment{}
}
