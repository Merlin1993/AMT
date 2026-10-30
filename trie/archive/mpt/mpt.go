// 热层是一棵十六叉树；分片仅用于按前缀轮转剪枝，冷数据存入档案桶。
package mpt

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"container/list"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/lru"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	archivetrie "github.com/ethereum/go-ethereum/trie/archive"
	"github.com/ethereum/go-ethereum/trie/archive/cuckoo"
	"github.com/ethereum/go-ethereum/trie/archive/mpt/ecmh"
	gethtrie "github.com/ethereum/go-ethereum/trie/archive/mpt/hx"
	"github.com/ethereum/go-ethereum/trie/trienode"
	"github.com/ethereum/go-ethereum/triedb"
	"github.com/ethereum/go-ethereum/triedb/database"
)

var (
	ErrNotFound  = errors.New("mpt archive: key not found")
	archiveMagic = [4]byte{'M', 'A', 'R', 'C'}
)

const (
	recordVersion = byte(2)
	maxRecordSize = 1 << 30

	// 论文中的每桶最大条目数。
	defaultBucketCapacity = 100

	DefaultBucketCapacity = defaultBucketCapacity
)

const (
	BackendHash = "hashdb"
	BackendPath = "pathdb"
)

var (
	nodePrefix     = []byte{'M', 'P', 'T', 'N', 1}
	archivePrefix  = []byte{'M', 'P', 'T', 'A', 1}
	schedulePrefix = []byte{'M', 'P', 'T', 'S', 1}
	stubMetaPrefix = []byte{'M', 'P', 'T', 'B', 1}
)

const scheduleVersion = byte(3)

// Config 控制分片深度、归档桶和冷热层的存储参数。
type Config struct {
	ShardDepthBits int

	CuckooBuckets int
	CuckooSlots   int

	ActivateArchivedKeyOnRead bool
	VerifyAtCommit            bool
	VerifyBucketWorkers       int
	VerifyItemWorkers         int

	Backend string

	NodeCacheBytes int64

	CleanCacheBytes  int
	WriteBufferBytes int

	ForceCommitEveryBatches int

	AsyncPrune bool

	ArchiveResidentEntries int

	BucketCapacity int
}

const defaultNodeCacheBytes = 64 << 20

const (
	defaultCleanCacheBytes  = 64 << 20
	defaultWriteBufferBytes = 256 << 20
)

const defaultArchiveResidentEntries = 1 << 22

func defaultConfig() *Config {
	return &Config{
		ShardDepthBits: 16,
		CuckooBuckets:  32,
		CuckooSlots:    4,
		Backend:        BackendHash,
	}
}

// Trie 同时维护热树、归档桶索引和分片剪枝进度。
type Trie struct {
	mu     sync.Mutex
	db     archivetrie.KVStore
	config Config
	ndb    database.NodeDatabase

	tdb        *triedb.Database
	pathWrites *writeCounter

	hot   *gethtrie.Trie
	root  common.Hash
	block uint64

	dirty bool

	buckets           map[string]*bucket
	bucketSeq         uint64
	commitsSinceFlush int

	archiveBytes int64

	liveHotLeaves int64

	lastTdbCommitDur time.Duration

	archiveLRU      *list.List
	residentEntries int
	bucketIO        bucketIOStats

	baseBit        byte
	pruneDomainIdx int
	scheduleDirty  bool

	prefetchMu   sync.Mutex
	prefetchBusy bool

	dirtyBuckets []*bucket

	pendingAudits     []bucketAudit
	auditedPaths      map[string]struct{}
	pendingProofBytes int64
	checkpoint        committedState
}

type bucketAudit struct {
	path       []byte
	commitment ecmh.Commitment
	metaHash   [32]byte
	metaBytes  []byte
	entries    []ecmh.Entry
}

type bucketIOStats struct {
	metaReads, payloadReads int64
	metaNanos, payloadNanos int64
}

type committedState struct {
	block, bucketSeq                  uint64
	archiveBytes, liveHotLeaves       int64
	baseBit                           byte
	pruneDomainIdx, commitsSinceFlush int
}

// bucket 保存归档载荷，并以过滤器和 ECMH 承诺支持查找与验证。
type bucket struct {
	path         []byte
	mount        []byte
	prefixBuf    [32]byte
	prefixBits   uint16
	entries      map[string][]byte
	filter       *cuckoo.Filter
	commitment   ecmh.Commitment
	count        int
	dirty        bool
	payloadGone  bool
	elem         *list.Element
	metaBytes    []byte
	metaHash     [32]byte
	payloadBytes int64
}

func New(root []byte, db archivetrie.KVStore, config *Config) (*Trie, error) {
	if db == nil {
		return nil, errors.New("mpt archive: nil database")
	}
	cfg := *defaultConfig()
	if config != nil {
		cfg = *config
		if cfg.CuckooBuckets <= 0 {
			cfg.CuckooBuckets = 32
		}
		if cfg.CuckooSlots <= 0 {
			cfg.CuckooSlots = 4
		}
		if cfg.Backend == "" {
			cfg.Backend = BackendHash
		}
		if cfg.ShardDepthBits <= 0 {
			cfg.ShardDepthBits = 16
		}
	}
	if cfg.ShardDepthBits > 32 {
		return nil, fmt.Errorf("mpt archive: shard depth %d bits exceeds the 32-bit prune schedule record (max 32)", cfg.ShardDepthBits)
	}
	if cfg.Backend != BackendHash && cfg.Backend != BackendPath {
		return nil, fmt.Errorf("mpt archive: unknown backend %q", cfg.Backend)
	}
	if cfg.CleanCacheBytes == 0 {
		cfg.CleanCacheBytes = defaultCleanCacheBytes
	}
	if cfg.WriteBufferBytes == 0 {
		cfg.WriteBufferBytes = defaultWriteBufferBytes
	}
	if cfg.NodeCacheBytes == 0 {
		cfg.NodeCacheBytes = defaultNodeCacheBytes
	}
	if cfg.ArchiveResidentEntries <= 0 {
		cfg.ArchiveResidentEntries = defaultArchiveResidentEntries
	}
	if cfg.BucketCapacity <= 0 {
		cfg.BucketCapacity = defaultBucketCapacity
	}
	if cfg.VerifyBucketWorkers <= 0 {
		cfg.VerifyBucketWorkers = 8
	}
	if cfg.VerifyItemWorkers <= 0 {
		cfg.VerifyItemWorkers = 2
	}

	t := &Trie{
		db:         db,
		config:     cfg,
		root:       normalizeRootHash(root),
		buckets:    make(map[string]*bucket),
		archiveLRU: list.New(),
	}
	switch cfg.Backend {
	case BackendPath:
		if t.root != (common.Hash{}) {
			return nil, errors.New("mpt archive: path backend cannot adopt an existing root; it has no layer history for it")
		}
		tdb, counter, err := openPathDatabase(db, cfg.CleanCacheBytes, cfg.WriteBufferBytes)
		if err != nil {
			return nil, err
		}
		t.tdb = tdb
		t.pathWrites = counter
		t.ndb = tdb
	default:
		t.ndb = &nodeDatabase{store: db, cache: newNodeCache(cfg.NodeCacheBytes)}
	}
	if err := t.loadSchedule(); err != nil {
		return nil, err
	}
	t.saveCheckpointLocked()
	return t, nil
}

func scheduleKey() []byte { return schedulePrefix }

func (t *Trie) encodeSchedule() []byte {
	data := make([]byte, 22)
	data[0] = scheduleVersion
	data[1] = t.baseBit & 1
	binary.BigEndian.PutUint32(data[2:6], uint32(t.pruneDomainIdx))
	binary.BigEndian.PutUint64(data[6:14], t.bucketSeq)
	binary.BigEndian.PutUint64(data[14:22], uint64(t.archiveBytes))
	return data
}

func (t *Trie) loadSchedule() error {
	data, err := optionalGet(t.db, scheduleKey())
	if err != nil {
		return err
	}
	if len(data) == 0 {
		return nil
	}
	if len(data) != 22 || data[0] != scheduleVersion {
		return errors.New("mpt archive: invalid prune schedule record")
	}
	t.bucketSeq = binary.BigEndian.Uint64(data[6:14])
	t.archiveBytes = int64(binary.BigEndian.Uint64(data[14:22]))
	t.baseBit = data[1] & 1
	idx := int(binary.BigEndian.Uint32(data[2:6]))
	if idx < 0 || idx >= t.domainCount() {
		t.baseBit = 0
		idx = 0
	}
	t.pruneDomainIdx = idx
	return nil
}

func (t *Trie) epochForKeyLocked(key []byte) byte {
	if t.domainID(key) < t.pruneDomainIdx {
		return t.baseBit
	}
	return 1 - t.baseBit
}

func (t *Trie) Root() []byte {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.root == (common.Hash{}) || t.root == types.EmptyRootHash {
		return nil
	}
	return t.root.Bytes()
}

func normalizeRootHash(root []byte) common.Hash {
	if len(root) != common.HashLength || bytes.Equal(root, types.EmptyRootHash.Bytes()) {
		return common.Hash{}
	}
	return common.BytesToHash(root)
}

func prefixed(prefix, value []byte) []byte {
	key := make([]byte, len(prefix)+len(value))
	copy(key, prefix)
	copy(key[len(prefix):], value)
	return key
}

func nodeKey(hash common.Hash) []byte { return prefixed(nodePrefix, hash.Bytes()) }

func optionalGet(store archivetrie.KVStore, key []byte) ([]byte, error) {
	value, err := store.Get(key)
	if err != nil && isMissingError(err) {
		return nil, nil
	}
	return value, err
}

func isMissingError(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "not found") || strings.Contains(message, "notfound")
}

func bucketKey(path []byte) []byte { return prefixed(archivePrefix, path) }

func (t *Trie) ArchivePayloadBytes() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.archiveBytes
}

func (t *Trie) LiveHotLeaves() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.liveHotLeaves
}

func (t *Trie) LiveBuckets() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return int64(len(t.buckets))
}

func (t *Trie) BucketOccupancy() (counts []int64, empty int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	counts = make([]int64, 0, len(t.buckets))
	for _, b := range t.buckets {
		counts = append(counts, int64(b.count))
		if b.count == 0 {
			empty++
		}
	}
	return counts, empty
}

func (t *Trie) domainCount() int { return 1 << uint(t.config.ShardDepthBits) }

func (t *Trie) domainID(key []byte) int { return domainIDOf(key, t.config.ShardDepthBits) }

func domainIDOf(key []byte, depth int) int {
	id := 0
	for i := 0; i < depth; i++ {
		var bit byte
		if idx := i / 8; idx < len(key) {
			bit = (key[idx] >> (7 - uint(i%8))) & 1
		}
		id = id<<1 | int(bit)
	}
	return id
}

func (t *Trie) ensureHotLocked() error {
	if t.hot != nil {
		return nil
	}
	if t.root == (common.Hash{}) {
		t.hot = gethtrie.NewEmpty(t.ndb)
		t.hot.SetEpochPolicy(t.epochForKeyLocked)
		return nil
	}
	hot, err := gethtrie.New(t.root, common.Hash{}, t.ndb)
	if err != nil {
		return err
	}
	hot.SetEpochPolicy(t.epochForKeyLocked)
	t.hot = hot
	return nil
}

func (t *Trie) markDirtyLocked() { t.dirty = true }

func (t *Trie) markBucketDirtyLocked(b *bucket) {
	if b.dirty {
		return
	}
	b.dirty = true
	t.dirtyBuckets = append(t.dirtyBuckets, b)
}

func bucketItem(key, value []byte) []byte {
	item := make([]byte, 0, len(key)+32)
	item = append(item, key...)
	valueHash := sha256.Sum256(value)
	return append(item, valueHash[:]...)
}

func nibbleLCPLen(entries []gethtrie.ExtractedEntry) int {
	if len(entries) == 0 {
		return 0
	}
	lcp := len(entries[0].Key) * 2
	for _, e := range entries[1:] {
		if n := commonNibblePrefix(entries[0].Key, e.Key); n < lcp {
			lcp = n
		}
	}
	return lcp
}

func commonNibblePrefix(a, b []byte) int {
	n := 0
	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] == b[i] {
			n += 2
			continue
		}
		if a[i]>>4 == b[i]>>4 {
			n++
		}
		return n
	}
	return n
}

func keyNibblesPrefix(key []byte, n int) []byte {
	out := make([]byte, n)
	for i := 0; i < n; i++ {
		if i%2 == 0 {
			out[i] = key[i/2] >> 4
		} else {
			out[i] = key[i/2] & 0x0f
		}
	}
	return out
}

func keyHasBitPrefix(key []byte, prefix *[32]byte, prefixBits uint16) bool {
	n := int(prefixBits)
	if n > len(key)*8 {
		return false
	}
	full := n / 8
	for i := 0; i < full; i++ {
		if key[i] != prefix[i] {
			return false
		}
	}
	if rem := uint(n % 8); rem != 0 {
		mask := byte(0xff << (8 - rem))
		if key[full]&mask != prefix[full]&mask {
			return false
		}
	}
	return true
}

func commonBitPrefixLen(a, b []byte) int {
	m := min(len(a), len(b))
	i := 0
	for i < m && a[i] == b[i] {
		i++
	}
	if i == m {
		return m * 8
	}
	return i*8 + bits.LeadingZeros8(a[i]^b[i])
}

func coveragePrefixBits(entries map[string][]byte) ([32]byte, uint16) {
	var buf [32]byte
	lcp := -1
	for key := range entries {
		k := []byte(key)
		if lcp < 0 {
			lcp = min(len(k)*8, 256)
			copy(buf[:], k)
			continue
		}
		if n := commonBitPrefixLen(buf[:], k); n < lcp {
			lcp = n
		}
	}
	if lcp <= 0 {
		return [32]byte{}, 0
	}
	if rem := uint(lcp % 8); rem != 0 {
		buf[lcp/8] &= 0xff << (8 - rem)
	}
	for i := (lcp + 7) / 8; i < 32; i++ {
		buf[i] = 0
	}
	return buf, uint16(lcp)
}

func (t *Trie) bucketForStubLocked(mountPath []byte, st *gethtrie.Stub) (*bucket, error) {
	if b := t.buckets[string(st.Path)]; b != nil {
		return b, nil
	}
	ioStart := time.Now()
	data, err := optionalGet(t.db, stubMetaKey(st.Path))
	t.bucketIO.metaReads++
	t.bucketIO.metaNanos += time.Since(ioStart).Nanoseconds()
	if err != nil {
		return nil, err
	}
	prefixBuf, prefixBits, filterBytes, commitment, count, err := decodeStubMeta(data)
	if err != nil {
		return nil, err
	}
	if got := crypto.Keccak256Hash(data); [32]byte(got) != st.ContentHash {
		return nil, fmt.Errorf("mpt archive: stub %x content hash mismatch: record %x, blob anchors %x", st.Path, got[:], st.ContentHash[:])
	}
	filter := cuckoo.New(0, 0)
	if len(filterBytes) > 0 {
		if err := filter.Decode(filterBytes, 0, 0); err != nil {
			return nil, err
		}
	}
	b := &bucket{
		path:       bytes.Clone(st.Path),
		mount:      bytes.Clone(mountPath),
		prefixBuf:  prefixBuf,
		prefixBits: prefixBits,
		filter:     filter,
		commitment: commitment,
		count:      count,
		metaBytes:  data,
		metaHash:   st.ContentHash,
	}
	t.buckets[string(st.Path)] = b
	archivetrie.RecordMPTStubMetaFetch(int64(len(data)))
	return b, nil
}

func (b *bucket) stub() *gethtrie.Stub {
	filterBytes := b.filter.Encode()
	pbytes := (int(b.prefixBits) + 7) / 8
	rec := make([]byte, 0, 1+2+pbytes+4+len(filterBytes)+33+4)
	rec = append(rec, stubMetaVersion)
	var plen [2]byte
	binary.BigEndian.PutUint16(plen[:], b.prefixBits)
	rec = append(rec, plen[:]...)
	rec = append(rec, b.prefixBuf[:pbytes]...)
	var size [4]byte
	binary.BigEndian.PutUint32(size[:], uint32(len(filterBytes)))
	rec = append(rec, size[:]...)
	rec = append(rec, filterBytes...)
	rec = append(rec, b.commitment[:]...)
	binary.BigEndian.PutUint32(size[:], uint32(b.count))
	rec = append(rec, size[:]...)
	b.metaBytes = rec
	b.metaHash = [32]byte(crypto.Keccak256Hash(rec))
	return &gethtrie.Stub{
		Path:        bytes.Clone(b.path),
		ContentHash: b.metaHash,
	}
}

const stubMetaVersion = byte(3)

func stubMetaKey(path []byte) []byte { return prefixed(stubMetaPrefix, path) }

func decodeStubMeta(data []byte) (prefixBuf [32]byte, prefixBits uint16, filter []byte, commitment ecmh.Commitment, count int, err error) {
	if len(data) < 1+2+4+33+4 || data[0] != stubMetaVersion {
		return prefixBuf, 0, nil, commitment, 0, errors.New("mpt archive: invalid stub meta record")
	}
	rest := data[1:]
	prefixBits = binary.BigEndian.Uint16(rest[0:2])
	pbytes := (int(prefixBits) + 7) / 8
	if pbytes > 32 || len(data) < 1+2+pbytes+4+33+4 {
		return prefixBuf, 0, nil, commitment, 0, errors.New("mpt archive: invalid stub meta record")
	}
	copy(prefixBuf[:], rest[2:2+pbytes])
	rest = rest[2+pbytes:]
	if len(rest) < 4 {
		return prefixBuf, 0, nil, commitment, 0, errors.New("mpt archive: invalid stub meta record")
	}
	filterLen := int(binary.BigEndian.Uint32(rest[:4]))
	if 4+filterLen+33+4 != len(rest) {
		return prefixBuf, 0, nil, commitment, 0, errors.New("mpt archive: invalid stub meta layout")
	}
	filter = rest[4 : 4+filterLen]
	copy(commitment[:], rest[4+filterLen:4+filterLen+33])
	count = int(binary.BigEndian.Uint32(rest[4+filterLen+33:]))
	return prefixBuf, prefixBits, filter, commitment, count, nil
}

func (t *Trie) makeResidentLocked(b *bucket) {
	if b.elem == nil {
		b.elem = t.archiveLRU.PushBack(b)
		t.residentEntries += b.count
	} else {
		t.archiveLRU.MoveToBack(b.elem)
	}
	t.evictArchivesLocked(true)
}

func (t *Trie) touchResidentLocked(b *bucket) {
	if b.elem != nil {
		t.archiveLRU.MoveToBack(b.elem)
	}
}

func (t *Trie) evictArchivesLocked(protectMRU bool) {
	limit := t.config.ArchiveResidentEntries
	var mru *list.Element
	if protectMRU {
		mru = t.archiveLRU.Back()
	}
	budget := t.archiveLRU.Len()
	scanned := 0
	for t.residentEntries > limit && scanned < budget {
		elem := t.archiveLRU.Front()
		if elem == nil {
			return
		}
		b := elem.Value.(*bucket)
		scanned++
		if b.dirty || elem == mru {
			t.archiveLRU.MoveToBack(elem)
			continue
		}
		t.archiveLRU.Remove(elem)
		b.elem = nil
		t.residentEntries -= b.count
		b.entries = nil
	}
}

func (t *Trie) loadBucketLocked(b *bucket) error {
	if b.entries != nil {
		t.touchResidentLocked(b)
		return nil
	}
	ioStart := time.Now()
	data, err := optionalGet(t.db, bucketKey(b.path))
	t.bucketIO.payloadReads++
	t.bucketIO.payloadNanos += time.Since(ioStart).Nanoseconds()
	if err != nil {
		return err
	}
	b.payloadBytes = int64(len(data))
	b.entries = make(map[string][]byte)
	if len(data) != 0 {
		if err := t.decodeBucket(data, b); err != nil {
			return err
		}
	}
	if len(b.entries) != b.count {
		return fmt.Errorf("mpt archive: bucket %x payload holds %d entries, stub says %d", b.path, len(b.entries), b.count)
	}
	t.makeResidentLocked(b)
	return nil
}

func (t *Trie) decodeBucket(data []byte, b *bucket) error {
	if len(data) < 9 || len(data) > maxRecordSize || !bytes.Equal(data[:4], archiveMagic[:]) || data[4] != recordVersion {
		return errors.New("mpt archive: invalid bucket record")
	}
	count := int(binary.BigEndian.Uint32(data[5:9]))
	if count != b.count {
		return fmt.Errorf("mpt archive: bucket %x payload count %d, stub says %d", b.path, count, b.count)
	}
	offset := 9
	for i := 0; i < count; i++ {
		if offset+8 > len(data) {
			return errors.New("mpt archive: truncated bucket entry")
		}
		keyLen := int(binary.BigEndian.Uint32(data[offset : offset+4]))
		valLen := int(binary.BigEndian.Uint32(data[offset+4 : offset+8]))
		offset += 8
		if keyLen <= 0 || valLen < 0 || offset+keyLen+valLen > len(data) {
			return errors.New("mpt archive: invalid bucket entry")
		}
		key := bytes.Clone(data[offset : offset+keyLen])
		offset += keyLen
		value := bytes.Clone(data[offset : offset+valLen])
		offset += valLen
		b.entries[string(key)] = value
	}
	if offset != len(data) {
		return errors.New("mpt archive: trailing bucket bytes")
	}
	return nil
}

func bucketRecordSize(entries map[string][]byte) int64 {
	size := int64(9)
	for key, value := range entries {
		size += int64(8 + len(key) + len(value))
	}
	return size
}

func (t *Trie) encodeBucket(b *bucket) []byte {
	keys := make([]string, 0, len(b.entries))
	for key := range b.entries {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	size := 9
	for _, key := range keys {
		size += 8 + len(key) + len(b.entries[key])
	}
	data := make([]byte, size)
	copy(data[:4], archiveMagic[:])
	data[4] = recordVersion
	binary.BigEndian.PutUint32(data[5:9], uint32(len(keys)))
	offset := 9
	for _, key := range keys {
		value := b.entries[key]
		binary.BigEndian.PutUint32(data[offset:offset+4], uint32(len(key)))
		binary.BigEndian.PutUint32(data[offset+4:offset+8], uint32(len(value)))
		offset += 8
		copy(data[offset:], key)
		offset += len(key)
		copy(data[offset:], value)
		offset += len(value)
	}
	return data
}

var errBucketSaturated = errors.New("mpt archive: bucket saturated")

func (t *Trie) cuckooSoftCap() int {
	return t.config.CuckooBuckets * t.config.CuckooSlots * 7 / 8
}

func (t *Trie) createBucketLocked(nodePath []byte, entries []gethtrie.ExtractedEntry) (int, error) {
	filter := cuckoo.New(t.config.CuckooBuckets, t.config.CuckooSlots)
	items := make([][]byte, 0, len(entries))
	payload := make(map[string][]byte, len(entries))
	consumed := 0
	for _, e := range entries {
		if err := filter.Insert(e.Key); err != nil {
			if consumed == 0 {
				return 0, fmt.Errorf("mpt archive: cuckoo overflow in bucket of %d entries: %w", len(entries), err)
			}
			break
		}
		payload[string(e.Key)] = bytes.Clone(e.Value)
		items = append(items, bucketItem(e.Key, e.Value))
		consumed++
	}
	path := make([]byte, len(nodePath)+8)
	copy(path, nodePath)
	binary.BigEndian.PutUint64(path[len(nodePath):], t.bucketSeq)
	t.bucketSeq++
	t.scheduleDirty = true
	prefixBuf, prefixBits := coveragePrefixBits(payload)
	b := &bucket{
		path:         path,
		prefixBuf:    prefixBuf,
		prefixBits:   prefixBits,
		entries:      payload,
		filter:       filter,
		commitment:   ecmh.Create(items),
		count:        consumed,
		dirty:        true,
		payloadBytes: bucketRecordSize(payload),
	}
	mp, err := t.hot.MountStub(nodePath, b.stub())
	if err != nil {
		return 0, err
	}
	b.mount = mp
	if stubs, err := t.hot.StubsAt(mp); err == nil {
		archivetrie.RecordMPTNodeStubCount(len(stubs))
	}
	t.archiveBytes += bucketRecordSize(b.entries)
	t.buckets[string(path)] = b
	t.dirtyBuckets = append(t.dirtyBuckets, b)
	t.makeResidentLocked(b)
	t.markDirtyLocked()
	return consumed, nil
}

func (t *Trie) bucketAppendLocked(b *bucket, entries []gethtrie.ExtractedEntry) (int, error) {
	if err := t.loadBucketLocked(b); err != nil {
		return 0, err
	}
	consumed := 0
	newKeys := 0
	saturated := false
	var added int64
	for _, e := range entries {
		if old, ok := b.entries[string(e.Key)]; ok {
			// 同桶盲写覆盖旧值时，先从承诺中减去旧条目，再加入新条目。
			// 过滤器和条目数不变。
			b.commitment = ecmh.Delete(b.commitment, bucketItem(e.Key, old))
			b.commitment = ecmh.BlindAppend(b.commitment, bucketItem(e.Key, e.Value))
			b.entries[string(e.Key)] = bytes.Clone(e.Value)
			added += int64(len(e.Value) - len(old))
			consumed++
			continue
		}
		if err := b.filter.Insert(e.Key); err != nil {
			saturated = true
			break
		}
		b.entries[string(e.Key)] = bytes.Clone(e.Value)
		b.commitment = ecmh.BlindAppend(b.commitment, bucketItem(e.Key, e.Value))
		added += int64(8 + len(e.Key) + len(e.Value))
		consumed++
		newKeys++
	}
	if consumed == 0 {
		if saturated {
			return 0, errBucketSaturated
		}
		return 0, nil
	}
	b.count += newKeys
	t.archiveBytes += added
	b.payloadBytes += added
	t.residentEntries += newKeys
	b.prefixBuf, b.prefixBits = coveragePrefixBits(b.entries)
	t.markBucketDirtyLocked(b)
	t.touchResidentLocked(b)
	if err := t.hot.ReplaceStub(b.mount, b.path, b.stub()); err != nil {
		return consumed, err
	}
	if saturated {
		return consumed, errBucketSaturated
	}
	return consumed, nil
}

func (t *Trie) bucketDeleteLocked(b *bucket, key, value []byte) error {
	b.filter.Delete(key)
	b.commitment = ecmh.Delete(b.commitment, bucketItem(key, value))
	delete(b.entries, string(key))
	b.count--
	t.archiveBytes -= int64(8 + len(key) + len(value))
	b.payloadBytes -= int64(8 + len(key) + len(value))
	t.residentEntries--
	t.markBucketDirtyLocked(b)
	t.touchResidentLocked(b)
	if b.count == 0 {
		t.archiveBytes -= 9
		b.payloadBytes = 0
		if err := t.hot.ReplaceStub(b.mount, b.path, nil); err != nil {
			return err
		}
		b.payloadGone = true
		b.entries = nil
		if b.elem != nil {
			t.archiveLRU.Remove(b.elem)
			b.elem = nil
		}
		return nil
	}
	return t.hot.ReplaceStub(b.mount, b.path, b.stub())
}

// canAppendAtMount 允许同一分片、同一挂载点的未满桶扩展覆盖前缀。
func (t *Trie) canAppendAtMount(b *bucket, key, mount []byte) bool {
	if b.prefixBits == 0 || keyHasBitPrefix(key, &b.prefixBuf, b.prefixBits) {
		return true
	}
	depth := t.config.ShardDepthBits
	return int(b.prefixBits) >= depth &&
		keyHasBitPrefix(key, &b.prefixBuf, uint16(depth)) &&
		commonNibblePrefix(b.prefixBuf[:], key) >= len(mount)
}

// 优先追加到同挂载点下前缀最接近的未满桶；剩余条目沿公共前缀下沉。
func (t *Trie) mountEntriesLocked(domainPrefix []byte, entries []gethtrie.ExtractedEntry) error {
	capacity := t.config.BucketCapacity
	softCap := t.cuckooSoftCap()
	nodePath := domainPrefix
	rest := entries
	for len(rest) > 0 {
		mp, err := t.hot.MountPoint(nodePath)
		if err != nil {
			return err
		}
		nodePath = mp
		stubs, err := t.hot.StubsAt(nodePath)
		if err != nil {
			return err
		}
		candidates := make([]*bucket, 0, len(stubs))
		for _, st := range stubs {
			b, err := t.bucketForStubLocked(nodePath, st)
			if err != nil {
				return err
			}
			if min(capacity-b.count, softCap-b.count) > 0 {
				candidates = append(candidates, b)
			}
		}
		sort.Slice(candidates, func(i, j int) bool {
			a, b := candidates[i], candidates[j]
			aScore := min(int(a.prefixBits), commonBitPrefixLen(a.prefixBuf[:], rest[0].Key))
			bScore := min(int(b.prefixBits), commonBitPrefixLen(b.prefixBuf[:], rest[0].Key))
			if aScore != bScore {
				return aScore > bScore
			}
			if a.count != b.count {
				return a.count < b.count
			}
			return bytes.Compare(a.path, b.path) < 0
		})
		for _, b := range candidates {
			if len(rest) == 0 {
				break
			}
			room := min(capacity-b.count, softCap-b.count)
			n := 0
			for n < room && n < len(rest) && t.canAppendAtMount(b, rest[n].Key, nodePath) {
				n++
			}
			if n == 0 {
				continue
			}
			consumed, err := t.bucketAppendLocked(b, rest[:n])
			rest = rest[consumed:]
			if err != nil && !errors.Is(err, errBucketSaturated) {
				return err
			}
		}
		if len(rest) == 0 {
			break
		}
		for len(rest) >= capacity {
			consumed, err := t.createBucketLocked(nodePath, rest[:capacity])
			if err != nil {
				return err
			}
			rest = rest[consumed:]
		}
		if len(rest) == 0 {
			break
		}
		if lcp := nibbleLCPLen(rest); lcp > len(nodePath) {
			mp2, err := t.hot.MountPoint(keyNibblesPrefix(rest[0].Key, lcp))
			if err != nil {
				return err
			}
			if len(mp2) > len(nodePath) {
				nodePath = mp2
				continue
			}
		}
		consumed, err := t.createBucketLocked(nodePath, rest)
		if err != nil {
			return err
		}
		rest = rest[consumed:]
	}
	return nil
}

func bucketSeqOf(path []byte) uint64 {
	return binary.BigEndian.Uint64(path[len(path)-8:])
}

func (t *Trie) archiveProbeLocked(key []byte) (*bucket, []*bucket, []byte, bool, error) {
	// 同一挂载点可能有多个桶；过滤器误报不能遮蔽后续真正命中的桶。
	var candidates []*bucket
	var walkErr error
	err := t.hot.StubsOnPath(key, func(mountPath []byte, st *gethtrie.Stub) bool {
		b, err := t.bucketForStubLocked(mountPath, st)
		if err != nil {
			walkErr = err
			return true
		}
		if b.prefixBits > 0 && !keyHasBitPrefix(key, &b.prefixBuf, b.prefixBits) {
			return false
		}
		if b.filter != nil {
			// 逐次记录过滤器查询耗时，用于计算分位数。
			filterStart := time.Now()
			filterHit := b.filter.Lookup(key)
			archivetrie.RecordMPTFilterProbeLatency(int64(time.Since(filterStart)))
			if !filterHit {
				archivetrie.RecordMPTFilterLookup(false)
				return false
			}
			archivetrie.RecordMPTFilterLookup(true)
		}
		candidates = append(candidates, b)
		return false
	})
	if err != nil {
		return nil, nil, nil, false, err
	}
	if walkErr != nil {
		return nil, nil, nil, false, walkErr
	}
	var hits []*bucket
	for _, b := range candidates {
		if err := t.loadBucketLocked(b); err != nil {
			return nil, nil, nil, false, err
		}
		if _, ok := b.entries[string(key)]; ok {
			hits = append(hits, b)
			continue
		}
		archivetrie.RecordMPTFilterFalsePositive()
	}
	if len(hits) == 0 {
		return nil, nil, nil, false, nil
	}
	if len(hits) >= 2 {
		archivetrie.RecordMPTDuplicateArchivedKey()
	}
	newest := 0
	for i := 1; i < len(hits); i++ {
		if bucketSeqOf(hits[i].path) > bucketSeqOf(hits[newest].path) {
			newest = i
		}
	}
	if newest != 0 {
		hits[0], hits[newest] = hits[newest], hits[0]
	}
	return hits[0], hits, bytes.Clone(hits[0].entries[string(key)]), true, nil
}

// 每次从归档桶读取值前，按桶内全部条目重算承诺。
func (t *Trie) verifyBucketLocked(b *bucket) error {
	if err := t.loadBucketLocked(b); err != nil {
		return err
	}
	items := make([][]byte, 0, len(b.entries))
	for key, value := range b.entries {
		items = append(items, bucketItem([]byte(key), value))
	}
	if !ecmh.Verify(b.commitment, items) {
		return fmt.Errorf("mpt archive: ECMH commitment mismatch on bucket %x", b.path)
	}
	return nil
}

func (t *Trie) saveCheckpointLocked() {
	t.checkpoint = committedState{
		block: t.block, bucketSeq: t.bucketSeq,
		archiveBytes: t.archiveBytes, liveHotLeaves: t.liveHotLeaves,
		baseBit: t.baseBit, pruneDomainIdx: t.pruneDomainIdx,
		commitsSinceFlush: t.commitsSinceFlush,
	}
}

// rollbackUncommittedLocked 丢弃未提交的热树与归档桶修改。
func (t *Trie) rollbackUncommittedLocked() {
	t.hot = nil
	t.buckets = make(map[string]*bucket)
	t.archiveLRU = list.New()
	t.residentEntries = 0
	t.dirtyBuckets = nil
	t.dirty, t.scheduleDirty = false, false
	t.pendingAudits, t.auditedPaths = nil, nil
	t.pendingProofBytes = 0
	t.block, t.bucketSeq = t.checkpoint.block, t.checkpoint.bucketSeq
	t.archiveBytes, t.liveHotLeaves = t.checkpoint.archiveBytes, t.checkpoint.liveHotLeaves
	t.baseBit, t.pruneDomainIdx = t.checkpoint.baseBit, t.checkpoint.pruneDomainIdx
	t.commitsSinceFlush = t.checkpoint.commitsSinceFlush
}

// captureBucketAuditLocked 保存桶在本区块首次复活前的完整快照。
func (t *Trie) captureBucketAuditLocked(b *bucket) {
	path := string(b.path)
	if _, present := t.auditedPaths[path]; present {
		return
	}
	if t.auditedPaths == nil {
		t.auditedPaths = make(map[string]struct{})
	}
	t.auditedPaths[path] = struct{}{}
	audit := bucketAudit{
		path: bytes.Clone(b.path), commitment: b.commitment,
		metaHash: b.metaHash, metaBytes: bytes.Clone(b.metaBytes),
		entries: make([]ecmh.Entry, 0, len(b.entries)),
	}
	for key, value := range b.entries {
		audit.entries = append(audit.entries, ecmh.Entry{Key: []byte(key), Value: bytes.Clone(value)})
	}
	t.pendingAudits = append(t.pendingAudits, audit)
	t.pendingProofBytes += int64(len(b.metaBytes)) + b.payloadBytes
}

func (t *Trie) PendingProofStats() (buckets, proofBytes int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return int64(len(t.pendingAudits)), t.pendingProofBytes
}

func verifyBucketAudit(audit bucketAudit, itemWorkers int) error {
	if got := [32]byte(crypto.Keccak256Hash(audit.metaBytes)); got != audit.metaHash {
		return fmt.Errorf("mpt archive: stub metadata mismatch on bucket %x", audit.path)
	}
	_, _, _, commitment, count, err := decodeStubMeta(audit.metaBytes)
	if err != nil {
		return fmt.Errorf("mpt archive: bucket %x: %w", audit.path, err)
	}
	if commitment != audit.commitment || count != len(audit.entries) ||
		ecmh.CreateEntriesParallel(audit.entries, itemWorkers) != commitment {
		return fmt.Errorf("mpt archive: ECMH commitment mismatch on bucket %x", audit.path)
	}
	return nil
}

// auditPendingLocked 并行验证本区块所有不同的原始归档桶。
func (t *Trie) auditPendingLocked() error {
	if len(t.pendingAudits) == 0 {
		return nil
	}
	started := time.Now()
	workers := min(t.config.VerifyBucketWorkers, len(t.pendingAudits))
	jobs := make(chan int)
	errorsByBucket := make([]error, len(t.pendingAudits))
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for index := range jobs {
				errorsByBucket[index] = verifyBucketAudit(t.pendingAudits[index], t.config.VerifyItemWorkers)
			}
		}()
	}
	for index := range t.pendingAudits {
		jobs <- index
	}
	close(jobs)
	wg.Wait()
	archivetrie.RecordMPTRedemptionVerifyBatch(int64(len(t.pendingAudits)), time.Since(started))
	for _, err := range errorsByBucket {
		if err != nil {
			t.rollbackUncommittedLocked()
			return err
		}
	}
	return nil
}

// Get 先查热树；命中归档后验证承诺，并按配置决定是否复活。
func (t *Trie) Get(key []byte) ([]byte, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.ensureHotLocked(); err != nil {
		return nil, err
	}
	value, err := t.hot.Get(key)
	if err != nil {
		return nil, err
	}
	if len(value) != 0 {
		return bytes.Clone(value), nil
	}
	ioBefore := t.bucketIO
	b, hits, value, fromArchive, err := t.archiveProbeLocked(key)
	archivetrie.RecordMPTBucketReadIO(
		t.bucketIO.metaReads-ioBefore.metaReads, t.bucketIO.metaNanos-ioBefore.metaNanos,
		t.bucketIO.payloadReads-ioBefore.payloadReads, t.bucketIO.payloadNanos-ioBefore.payloadNanos,
	)
	if err != nil {
		return nil, err
	}
	if !fromArchive {
		return nil, ErrNotFound
	}
	if t.config.VerifyAtCommit && t.config.ActivateArchivedKeyOnRead {
		for _, target := range hits {
			t.captureBucketAuditLocked(target)
		}
	} else {
		// 未启用提交前审计时，读取前逐桶验证。
		verifyTargets := hits[:1]
		if t.config.ActivateArchivedKeyOnRead {
			verifyTargets = hits
		}
		for _, target := range verifyTargets {
			verifyStart := time.Now()
			if err := t.verifyBucketLocked(target); err != nil {
				return nil, err
			}
			archivetrie.RecordMPTRedemptionVerify(time.Since(verifyStart))
		}
	}
	if t.config.ActivateArchivedKeyOnRead {
		promotionStart := time.Now()
		proofBytes := int64(len(b.metaBytes)) + b.payloadBytes
		// 盲写可能留下多个旧副本，复活时逐桶删除并同步更新承诺。
		for _, hb := range hits {
			if err := t.bucketDeleteLocked(hb, key, hb.entries[string(key)]); err != nil {
				archivetrie.RecordMPTReadPromotion(time.Since(promotionStart), false)
				return nil, err
			}
		}
		if err := t.hot.Update(key, value); err != nil {
			archivetrie.RecordMPTReadPromotion(time.Since(promotionStart), false)
			return nil, err
		}
		t.markDirtyLocked()
		archivetrie.RecordMPTReadPromotion(time.Since(promotionStart), true)
		archivetrie.RecordMPTRedemptionProof(proofBytes)
		// 每次复活均记录证明大小和耗时。
		archivetrie.RecordMPTRedemptionProofHistogram(proofBytes, int64(time.Since(promotionStart)))
	}
	return value, nil
}

func (t *Trie) GetValueRef(key []byte) ([]byte, bool, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.ensureHotLocked(); err != nil {
		return nil, false, err
	}
	value, err := t.hot.Get(key)
	if err != nil {
		return nil, false, err
	}
	if len(value) != 0 {
		return crypto.Keccak256(value), false, nil
	}
	_, _, archived, ok, err := t.archiveProbeLocked(key)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, ErrNotFound
	}
	return crypto.Keccak256(archived), true, nil
}

func (t *Trie) SetActivateArchivedKeyOnRead(enabled bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.config.ActivateArchivedKeyOnRead = enabled
}

func (t *Trie) Put(key, value []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.ensureHotLocked(); err != nil {
		return err
	}
	if err := t.hot.Update(key, value); err != nil {
		return err
	}
	t.markDirtyLocked()
	return nil
}

func (t *Trie) Delete(key []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.ensureHotLocked(); err != nil {
		return err
	}
	if err := t.hot.Delete(key); err != nil {
		return err
	}
	t.markDirtyLocked()
	return nil
}

func (t *Trie) PruneNextShard() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.pruneNextDomainLocked()
}

const prunePrefetchNodeBudget = 8192

func (t *Trie) domainPrefixNibbles(id int) []byte {
	fullNibbles := t.config.ShardDepthBits / 4
	prefix := make([]byte, fullNibbles)
	for i := range prefix {
		shift := uint(t.config.ShardDepthBits - 4*(i+1))
		prefix[i] = byte((id >> shift) & 0xf)
	}
	return prefix
}

func (t *Trie) startPrunePrefetchLocked(id int) {
	if !t.config.AsyncPrune || t.tdb == nil || t.root == (common.Hash{}) {
		return
	}
	t.prefetchMu.Lock()
	if t.prefetchBusy {
		t.prefetchMu.Unlock()
		return
	}
	t.prefetchBusy = true
	t.prefetchMu.Unlock()

	root := t.root
	prefix := t.domainPrefixNibbles(id)
	ndb := t.ndb
	go func() {
		defer func() {
			_ = recover()
			t.prefetchMu.Lock()
			t.prefetchBusy = false
			t.prefetchMu.Unlock()
		}()
		hot, err := gethtrie.New(root, common.Hash{}, ndb)
		if err != nil {
			return
		}
		hot.PrefetchPrefix(prefix, prunePrefetchNodeBudget)
	}()
}

// 每次轮转只处理一个分片，过期叶子移入归档桶。
func (t *Trie) pruneNextDomainLocked() error {
	id := t.pruneDomainIdx
	defer func() {
		t.pruneDomainIdx = (id + 1) % t.domainCount()
		if t.pruneDomainIdx == 0 {
			t.baseBit ^= 1
		}
		t.scheduleDirty = true
	}()
	if err := t.ensureHotLocked(); err != nil {
		return err
	}
	prefix := t.domainPrefixNibbles(id)
	resolveBefore := t.hot.ResolvedNodes()
	extractStart := time.Now()
	extracted, err := t.hot.ExtractDomain(prefix, t.baseBit, func(key []byte) bool {
		return domainIDOf(key, t.config.ShardDepthBits) == id
	})
	extractDur := time.Since(extractStart)
	if err != nil {
		return err
	}
	pruneResolved := t.hot.ResolvedNodes() - resolveBefore
	if len(extracted) == 0 {
		archivetrie.SetMPTPruneDiagnostics(0, pruneResolved, extractDur.Nanoseconds(), 0)
		t.startPrunePrefetchLocked((id + 1) % t.domainCount())
		return nil
	}
	t.liveHotLeaves -= int64(len(extracted))
	mountStart := time.Now()
	if err := t.mountEntriesLocked(prefix, extracted); err != nil {
		return err
	}
	mountDur := time.Since(mountStart)
	archivetrie.SetMPTPruneDiagnostics(int64(len(extracted)), pruneResolved, extractDur.Nanoseconds(), mountDur.Nanoseconds())
	t.markDirtyLocked()
	t.startPrunePrefetchLocked((id + 1) % t.domainCount())
	return nil
}

func (t *Trie) Hash() ([]byte, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.ensureHotLocked(); err != nil {
		return nil, err
	}
	hash := t.hot.Hash()
	if hash == (common.Hash{}) || hash == types.EmptyRootHash {
		return nil, nil
	}
	return hash.Bytes(), nil
}

func writeNodeSet(batch archivetrie.Batcher, set *trienode.NodeSet) error {
	if set == nil {
		return nil
	}
	for _, node := range set.Nodes {
		if node == nil || node.IsDeleted() {
			continue
		}
		if err := batch.Put(nodeKey(node.Hash), node.Blob); err != nil {
			return err
		}
	}
	return nil
}

type stagedBytes struct {
	hotNode, archive, index, stubMeta int64
}

func (s *stagedBytes) add(key []byte, value []byte) {
	if len(key) < 4 || key[0] != 'M' || key[1] != 'P' || key[2] != 'T' {
		return
	}
	n := int64(len(key) + len(value))
	switch key[3] {
	case 'N':
		s.hotNode += n
	case 'A':
		s.archive += n
	case 'I', 'S':
		s.index += n
	case 'B':
		s.stubMeta += n
	}
}

type recordingBatcher struct {
	archivetrie.Batcher
	staged *stagedBytes
}

func (r *recordingBatcher) Put(key, value []byte) error {
	r.staged.add(key, value)
	return r.Batcher.Put(key, value)
}

func (r *recordingBatcher) Delete(key []byte) error {
	r.staged.add(key, nil)
	return r.Batcher.Delete(key)
}

func (t *Trie) CommitToBatch(batch archivetrie.Batcher, destructive bool) ([]byte, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.commitToBatchLocked(batch)
}

func (t *Trie) Commit() ([]byte, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	batch := t.db.NewBatch()
	defer batch.Reset()
	root, err := t.commitToBatchLocked(batch)
	if err != nil {
		return nil, err
	}
	if err := batch.Write(); err != nil {
		return nil, err
	}
	return root, nil
}

func (t *Trie) commitToBatchLocked(batch archivetrie.Batcher) ([]byte, error) {
	if t.config.VerifyAtCommit {
		if err := t.auditPendingLocked(); err != nil {
			return nil, err
		}
	}
	archivetrie.SetMPTArchivePayloadBytes(t.archiveBytes)
	archivetrie.SetMPTLiveGauges(t.liveHotLeaves, int64(len(t.buckets)))
	staged := &stagedBytes{}
	rec := &recordingBatcher{Batcher: batch, staged: staged}
	var hxDur, stageDur, tdbCommitDur, archiveDur time.Duration
	defer func() {
		archivetrie.SetMPTCommitPhaseNanos(hxDur.Nanoseconds(), stageDur.Nanoseconds(), tdbCommitDur.Nanoseconds(), archiveDur.Nanoseconds())
		// pathdb 自行写入的节点也要计入热层写入量。
		if t.pathWrites != nil {
			written, _ := t.pathWrites.take()
			staged.hotNode += written
		}
		archivetrie.SetMPTCommitStagedBytes(staged.hotNode, staged.archive, staged.index, staged.stubMeta)
		if cacheStats, ok := t.ndb.(*nodeDatabase); ok {
			archivetrie.SetMPTNodeCacheStats(cacheStats.cache.stats())
		} else {
			archivetrie.SetMPTNodeCacheStats(0, 0)
		}
	}()
	if !t.dirty && !t.scheduleDirty {
		t.pendingAudits, t.auditedPaths = nil, nil
		t.pendingProofBytes = 0
		return t.rootBytes(), nil
	}
	if t.dirty {
		if err := t.ensureHotLocked(); err != nil {
			return nil, err
		}
		hxStart := time.Now()
		newRoot, nodes := t.hot.Commit()
		hxDur = time.Since(hxStart)
		t.liveHotLeaves += t.hot.LeafDelta()
		t.hot = nil
		stageStart := time.Now()
		if err := t.stageNodesLocked(rec, newRoot, nodes); err != nil {
			return nil, err
		}
		stageDur = time.Since(stageStart)
		tdbCommitDur = t.lastTdbCommitDur
		t.lastTdbCommitDur = 0
		t.root = newRoot
		t.block++
		t.dirty = false
	}
	archiveStart := time.Now()
	for _, b := range t.dirtyBuckets {
		if !b.dirty {
			continue
		}
		if b.payloadGone {
			if err := rec.Delete(bucketKey(b.path)); err != nil {
				return nil, err
			}
			if err := rec.Delete(stubMetaKey(b.path)); err != nil {
				return nil, err
			}
			delete(t.buckets, string(b.path))
			continue
		}
		if err := rec.Put(bucketKey(b.path), t.encodeBucket(b)); err != nil {
			return nil, err
		}
		if len(b.metaBytes) == 0 {
			return nil, fmt.Errorf("mpt archive: dirty bucket %x has no rendered stub meta", b.path)
		}
		if err := rec.Put(stubMetaKey(b.path), b.metaBytes); err != nil {
			return nil, err
		}
		b.dirty = false
	}
	t.dirtyBuckets = t.dirtyBuckets[:0]
	if t.scheduleDirty {
		if err := rec.Put(scheduleKey(), t.encodeSchedule()); err != nil {
			return nil, err
		}
		t.scheduleDirty = false
	}
	archiveDur = time.Since(archiveStart)
	t.evictArchivesLocked(false)
	t.pendingAudits, t.auditedPaths = nil, nil
	t.pendingProofBytes = 0
	t.saveCheckpointLocked()
	return t.rootBytes(), nil
}

func (t *Trie) stageNodesLocked(rec *recordingBatcher, newRoot common.Hash, nodes *trienode.NodeSet) error {
	if t.tdb == nil {
		return writeNodeSet(rec, nodes)
	}
	if nodes == nil || len(nodes.Nodes) == 0 {
		return nil
	}
	parent := t.parentLocked()
	if newRoot == parent {
		return nil
	}
	if err := t.tdb.Update(newRoot, parent, t.block, trienode.NewWithNodeSet(nodes), triedb.NewStateSet()); err != nil {
		return err
	}
	t.commitsSinceFlush++
	if n := t.config.ForceCommitEveryBatches; n > 0 && t.commitsSinceFlush >= n {
		commitStart := time.Now()
		if err := t.tdb.Commit(newRoot, false); err != nil {
			return err
		}
		t.lastTdbCommitDur = time.Since(commitStart)
		t.commitsSinceFlush = 0
	}
	return nil
}

func (t *Trie) parentLocked() common.Hash {
	if t.root == (common.Hash{}) {
		return types.EmptyRootHash
	}
	return t.root
}

func (t *Trie) rootBytes() []byte {
	if t.root == (common.Hash{}) {
		return nil
	}
	return t.root.Bytes()
}

func (t *Trie) Flush() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.tdb == nil || t.root == (common.Hash{}) {
		return nil
	}
	t.commitsSinceFlush = 0
	return t.tdb.Commit(t.root, false)
}

func (t *Trie) FlushDurable() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.dirty || t.scheduleDirty {
		return fmt.Errorf("durable flush requires a committed trie")
	}
	if t.tdb == nil || t.root == (common.Hash{}) {
		return nil
	}
	t.commitsSinceFlush = 0
	if err := t.tdb.CommitAll(t.root); err != nil {
		return err
	}
	t.hot = nil
	return nil
}

func (t *Trie) PathStats() (written, writes, buffered, diff int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.tdb == nil {
		return 0, 0, 0, 0
	}
	written, writes = t.pathWrites.totals()
	diffs, nodes, _ := t.tdb.Size()
	return written, writes, int64(nodes), int64(diffs)
}

func (t *Trie) hotTrieLocked() (*gethtrie.Trie, error) {
	if t.hot != nil {
		return t.hot, nil
	}
	if t.root == (common.Hash{}) {
		return gethtrie.NewEmpty(t.ndb), nil
	}
	return gethtrie.New(t.root, common.Hash{}, t.ndb)
}

func (t *Trie) ForEach(fn func(key, value []byte) bool) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.ensureHotLocked(); err != nil {
		return err
	}
	values := make(map[string][]byte)
	hotKeys := make(map[string]struct{})
	archivedSeq := make(map[string]uint64)
	if err := t.hot.CollectLeaves(func(key, value []byte) bool {
		id := string(key)
		values[id] = bytes.Clone(value)
		hotKeys[id] = struct{}{}
		return true
	}); err != nil {
		return err
	}
	var loadErr error
	err := t.hot.AllStubs(func(mountPath []byte, stub *gethtrie.Stub) {
		if loadErr != nil {
			return
		}
		b, err := t.bucketForStubLocked(mountPath, stub)
		if err == nil {
			err = t.loadBucketLocked(b)
		}
		if err != nil {
			loadErr = err
			return
		}
		sequence := bucketSeqOf(b.path)
		for key, value := range b.entries {
			if _, hot := hotKeys[key]; hot {
				continue
			}
			if previous, exists := archivedSeq[key]; exists && previous >= sequence {
				continue
			}
			values[key] = bytes.Clone(value)
			archivedSeq[key] = sequence
		}
	})
	if err != nil {
		return err
	}
	if loadErr != nil {
		return loadErr
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if !fn([]byte(key), values[key]) {
			break
		}
	}
	return nil
}

type nodeDatabase struct {
	store archivetrie.KVStore
	cache *nodeCache
}

type nodeCache struct {
	enabled bool
	c       *lru.SizeConstrainedCache[common.Hash, []byte]
	gets    atomic.Int64
	hits    atomic.Int64
}

func newNodeCache(maxBytes int64) *nodeCache {
	if maxBytes <= 0 {
		return &nodeCache{}
	}
	return &nodeCache{enabled: true, c: lru.NewSizeConstrainedCache[common.Hash, []byte](uint64(maxBytes))}
}

func (nc *nodeCache) get(hash common.Hash) ([]byte, bool) {
	if nc == nil || !nc.enabled {
		return nil, false
	}
	nc.gets.Add(1)
	if blob, ok := nc.c.Get(hash); ok {
		nc.hits.Add(1)
		return blob, true
	}
	return nil, false
}

func (nc *nodeCache) put(hash common.Hash, blob []byte) {
	if nc == nil || !nc.enabled {
		return
	}
	nc.c.Add(hash, blob)
}

func (nc *nodeCache) stats() (gets, hits int64) {
	if nc == nil {
		return 0, 0
	}
	return nc.gets.Load(), nc.hits.Load()
}

func (d *nodeDatabase) NodeReader(common.Hash) (database.NodeReader, error) {
	return nodeReader{store: d.store, cache: d.cache}, nil
}

type nodeReader struct {
	store archivetrie.KVStore
	cache *nodeCache
}

func (r nodeReader) Node(_ common.Hash, _ []byte, hash common.Hash) ([]byte, error) {
	if blob, ok := r.cache.get(hash); ok {
		return blob, nil
	}
	blob, err := r.store.Get(nodeKey(hash))
	if err != nil {
		return nil, err
	}
	if len(blob) == 0 {
		return nil, fmt.Errorf("mpt archive: node %x not found", hash)
	}
	r.cache.put(hash, blob)
	return blob, nil
}
