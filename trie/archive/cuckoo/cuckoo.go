package cuckoo

import (
	"encoding/binary"
	"errors"
	"math/bits"
	"sync"

	"github.com/ethereum/go-ethereum/crypto"
)

const (
	numBuckets     = 32
	slotsPerBucket = 4
	maxKicks       = 500
)

var (
	ErrFull                    = errors.New("cuckoo filter is full")
	alternateHashOnce          sync.Once
	alternateHashByFingerprint [1 << 16]uint32
)

func initAlternateHashes() {
	var fpBytes [2]byte
	for fp := range alternateHashByFingerprint {
		binary.BigEndian.PutUint16(fpBytes[:], uint16(fp))
		h := crypto.Keccak256Hash(fpBytes[:])
		alternateHashByFingerprint[fp] = binary.BigEndian.Uint32(h[0:4])
	}
}

func alternateHash(fp uint16) uint32 {
	alternateHashOnce.Do(initAlternateHashes)
	return alternateHashByFingerprint[fp]
}

// Filter 是一个紧凑的布谷鸟过滤器。
type Filter struct {
	buckets        [][]uint16
	numBuckets     int
	slotsPerBucket int
	count          int
}

// New 创建一个新的布谷鸟过滤器。
func New(buckets, slots int) *Filter {
	if buckets == 0 {
		buckets = 32
	}
	if slots == 0 {
		slots = 4
	}
	f := &Filter{
		numBuckets:     buckets,
		slotsPerBucket: slots,
		buckets:        make([][]uint16, buckets),
	}
	for i := range f.buckets {
		f.buckets[i] = make([]uint16, slots)
	}
	return f
}

// hash 返回数据的原始哈希值和指纹。
func (f *Filter) hash(data []byte) (uint, uint16) {
	h := crypto.Keccak256Hash(data)
	i1 := uint(binary.BigEndian.Uint32(h[0:4])) & uint(f.numBuckets-1)
	fp := uint16(binary.BigEndian.Uint16(h[4:6]))
	if fp == 0 {
		fp = 1 // 0 保留给空槽位
	}
	return i1, fp
}

// alternateIndex 返回给定的索引和指纹的备选索引。
// 采用部分键布谷鸟哈希：第二个桶的位置由首桶位置与指纹哈希异或得到。
func (f *Filter) alternateIndex(i uint, fp uint16) uint {
	// 异或操作确保我们可以从 i1 计算出 i2，反之亦然。
	// 由于 numBuckets 是 2 的幂，使用位与操作将其限制在范围内。
	return (i ^ uint(alternateHash(fp))) & uint(f.numBuckets-1)
}

// Insert 将数据添加到过滤器中。
func (f *Filter) Insert(data []byte) error {
	i1, fp := f.hash(data)

	for s := 0; s < f.slotsPerBucket; s++ {
		if f.buckets[i1][s] == 0 {
			f.buckets[i1][s] = fp
			f.count++
			return nil
		}
	}

	i2 := f.alternateIndex(i1, fp)
	for s := 0; s < f.slotsPerBucket; s++ {
		if f.buckets[i2][s] == 0 {
			f.buckets[i2][s] = fp
			f.count++
			return nil
		}
	}

	// 两边都满时，从主桶开始确定性地踢出指纹。
	i := i1
	type swap struct {
		bucket uint
		slot   int
		oldFP  uint16
	}
	var swaps [maxKicks]swap

	for n := 0; n < maxKicks; n++ {
		// 固定踢出槽位顺序，保证相同输入得到相同布局。
		slot := n % f.slotsPerBucket
		swaps[n] = swap{bucket: i, slot: slot, oldFP: f.buckets[i][slot]}
		f.buckets[i][slot], fp = fp, f.buckets[i][slot]
		i = f.alternateIndex(i, fp)

		for s := 0; s < f.slotsPerBucket; s++ {
			if f.buckets[i][s] == 0 {
				f.buckets[i][s] = fp
				f.count++
				return nil
			}
		}
	}
	for n := maxKicks - 1; n >= 0; n-- {
		s := swaps[n]
		f.buckets[s.bucket][s.slot] = s.oldFP
	}

	return ErrFull
}

// Lookup 检查数据是否在过滤器中。
func (f *Filter) Lookup(data []byte) bool {
	i1, fp := f.hash(data)
	for s := 0; s < f.slotsPerBucket; s++ {
		if f.buckets[i1][s] == fp {
			return true
		}
	}

	i2 := f.alternateIndex(i1, fp)
	for s := 0; s < f.slotsPerBucket; s++ {
		if f.buckets[i2][s] == fp {
			return true
		}
	}

	return false
}

// Delete 从过滤器中移除数据。
func (f *Filter) Delete(data []byte) bool {
	i1, fp := f.hash(data)
	for s := 0; s < f.slotsPerBucket; s++ {
		if f.buckets[i1][s] == fp {
			f.buckets[i1][s] = 0
			f.count--
			return true
		}
	}

	i2 := f.alternateIndex(i1, fp)
	for s := 0; s < f.slotsPerBucket; s++ {
		if f.buckets[i2][s] == fp {
			f.buckets[i2][s] = 0
			f.count--
			return true
		}
	}

	return false
}

// Count 返回过滤器中元素的数量。
func (f *Filter) Count() int {
	return f.count
}

// Reset 清空过滤器。
func (f *Filter) Reset() {
	for i := 0; i < f.numBuckets; i++ {
		for s := 0; s < f.slotsPerBucket; s++ {
			f.buckets[i][s] = 0
		}
	}
	f.count = 0
}

// Encode 返回过滤器的二进制表示形式。
// 格式如下：
// - 4 字节：numBuckets (BigEndian)
// - 4 字节：slotsPerBucket (BigEndian)
// - (numBuckets * slotsPerBucket / 8) 字节：位图
// - N*2 字节：仅存储已占用槽位的原始 uint16 指纹（大端序）
func (f *Filter) Encode() []byte {
	totalSlots := f.numBuckets * f.slotsPerBucket
	bitmaskSize := (totalSlots + 7) / 8
	bitmask := make([]byte, bitmaskSize)
	fingerprints := make([]byte, 0, f.count*2)

	for i := 0; i < f.numBuckets; i++ {
		for s := 0; s < f.slotsPerBucket; s++ {
			slotIdx := i*f.slotsPerBucket + s
			if f.buckets[i][s] != 0 {
				bitmask[slotIdx/8] |= (1 << (7 - (slotIdx % 8)))
				var fpBytes [2]byte
				binary.BigEndian.PutUint16(fpBytes[:], f.buckets[i][s])
				fingerprints = append(fingerprints, fpBytes[:]...)
			}
		}
	}

	res := make([]byte, 8+bitmaskSize+len(fingerprints))
	binary.BigEndian.PutUint32(res[0:4], uint32(f.numBuckets))
	binary.BigEndian.PutUint32(res[4:8], uint32(f.slotsPerBucket))
	copy(res[8:8+bitmaskSize], bitmask)
	copy(res[8+bitmaskSize:], fingerprints)
	return res
}

// Decode 从二进制数据中加载过滤器。
func (f *Filter) Decode(data []byte, _, _ int) error {
	if len(data) < 8 {
		return errors.New("data too short for header")
	}

	encodedBuckets := int(binary.BigEndian.Uint32(data[0:4]))
	encodedSlots := int(binary.BigEndian.Uint32(data[4:8]))
	if encodedBuckets <= 0 || encodedBuckets > 1<<20 || encodedBuckets&(encodedBuckets-1) != 0 {
		return errors.New("invalid encoded cuckoo bucket count")
	}
	if encodedSlots <= 0 || encodedSlots > 64 {
		return errors.New("invalid encoded cuckoo slot count")
	}

	totalSlots := encodedBuckets * encodedSlots
	bitmaskSize := (totalSlots + 7) / 8
	if len(data) < 8+bitmaskSize {
		return errors.New("data too short for bitmask")
	}

	bitmask := data[8 : 8+bitmaskSize]
	fingerprints := data[8+bitmaskSize:]
	occupied := 0
	for _, value := range bitmask {
		occupied += bits.OnesCount8(value)
	}
	if len(fingerprints) != occupied*2 {
		return errors.New("invalid cuckoo fingerprint payload length")
	}
	if unused := bitmaskSize*8 - totalSlots; unused > 0 && bitmask[bitmaskSize-1]&byte((1<<unused)-1) != 0 {
		return errors.New("invalid cuckoo bitmask padding")
	}

	next := New(encodedBuckets, encodedSlots)
	fpIdx := 0
	for i := 0; i < encodedBuckets; i++ {
		for s := 0; s < encodedSlots; s++ {
			slotIdx := i*encodedSlots + s
			if (bitmask[slotIdx/8] & (1 << (7 - (slotIdx % 8)))) != 0 {
				fp := binary.BigEndian.Uint16(fingerprints[fpIdx : fpIdx+2])
				fpIdx += 2
				if fp == 0 {
					return errors.New("invalid zero cuckoo fingerprint")
				}
				next.buckets[i][s] = fp
				next.count++
			}
		}
	}
	*f = *next
	return nil
}
