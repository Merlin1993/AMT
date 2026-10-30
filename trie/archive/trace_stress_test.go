package archive

import (
	"bufio"
	"compress/gzip"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/ethdb/leveldb"
	trieutils "github.com/ethereum/go-ethereum/trie/utils"
	"github.com/shirou/gopsutil/process"
)

var (
	traceStressInputDir                  = flag.String("traceStressInputDir", "", "Directory containing state_access_trace_*.csv.gz files")
	traceStressBaseDir                   = flag.String("traceStressBaseDir", "", "Fresh output directory for TestAMTTraceStress")
	traceStressOps                       = flag.Int64("traceStressOps", 100000000, "Maximum ordered trace operations; zero is unlimited when a block boundary is set")
	traceStressBatchSize                 = flag.Int("traceStressBatchSize", 1000, "Trace operations per prune/commit cycle when block replay is disabled")
	traceStressBatchByBlock              = flag.Bool("traceStressBatchByBlock", false, "Commit once per source block, including blocks without trace records")
	traceStressMetricsBatches            = flag.Int("traceStressMetricsBatches", 250, "Commit batches per metrics row")
	traceStressStartFile                 = flag.Int("traceStressStartFile", 0, "Zero-based sorted trace shard index to start from")
	traceStressFileLimit                 = flag.Int("traceStressFileLimit", 0, "Maximum trace shards to read; zero uses all remaining shards")
	traceStressStartBlock                = flag.Uint64("traceStressStartBlock", 0, "First inclusive trace block; zero starts at the first selected record")
	traceStressEndBlock                  = flag.Uint64("traceStressEndBlock", 0, "Last inclusive trace block; zero disables an explicit end boundary")
	traceStressBlocks                    = flag.Uint64("traceStressBlocks", 0, "Replay this many blocks from the first selected block; incompatible with traceStressEndBlock")
	traceStressAsyncPrune                = flag.Bool("traceStressAsyncPrune", true, "Overlap one shard prune with each operation batch")
	traceStressPruneEveryBatches         = flag.Int("traceStressPruneEveryBatches", 1, "Advance prune after this many batches; zero disables pruning")
	traceStressPruneEveryBlocks          = flag.Uint64("traceStressPruneEveryBlocks", 0, "Advance one prune shard per this many trace blocks; incompatible with traceStressPruneEveryBatches")
	traceStressAccessSampleEvery         = flag.Int64("traceStressAccessSampleEvery", 1000, "Sample logical hot/archive access state every N operations; zero disables")
	traceStressActivateArchivedKeyOnRead = flag.Bool("traceStressActivateArchivedKeyOnRead", false, "Activate an archived AMT key when a read finds it")
	traceStressShardDepthBits            = flag.Int("traceStressShardDepthBits", 16, "Logical archive domain width in bits for the MPT hot layer (domains = 2^this)")
	traceStressArchiveResidentEntries    = flag.Int("traceStressArchiveResidentEntries", 0, "MPT hot layer: cap on memory-resident archived entries across all buckets (0 = engine default)")
	traceStressTrieBackend               = flag.String("traceStressTrieBackend", "hashdb", `MPT hot-layer node store: "hashdb" (content-addressed) or "pathdb" (triedb.PathDatabase, path-addressed)`)
	traceStressPathCleanCacheMB          = flag.Int("traceStressPathCleanCacheMB", 64, "pathdb clean cache size in MiB")
	traceStressPathWriteBufferMB         = flag.Int("traceStressPathWriteBufferMB", 256, "pathdb dirty write buffer size in MiB (larger = fewer, fatter flushes)")
	traceStressPathFlushEveryBatches     = flag.Int("traceStressPathFlushEveryBatches", 0, "Force a pathdb flush to disk every N commits; 0 lets pathdb decide")
	traceStressCompactPerWindow          = flag.Bool("traceStressCompactPerWindow", true, "Run a full LevelDB compaction at each metrics-window boundary, BEFORE measuring State_Bytes, so storage reflects live data only (dead bytes reclaimed); compaction time lands in its own column, never in business timing")
	traceStressSplitArchiveDB            = flag.Bool("traceStressSplitArchiveDB", false, "Store MPTA archive payloads in archive_db and all other records in state_db; report both physical directory sizes per window")
	traceStressCuckooBuckets             = flag.Int("traceStressCuckooBuckets", 32, "Archive cuckoo filter bucket count")
	traceStressCuckooSlots               = flag.Int("traceStressCuckooSlots", 4, "Archive cuckoo filter slots per bucket")
	traceStressMPTVerifyAtCommit         = flag.Bool("traceStressMPTVerifyAtCommit", false, "Audit every distinct revived bucket before a block commit")
	traceStressMPTVerifyBucketWorkers    = flag.Int("traceStressMPTVerifyBucketWorkers", 8, "Concurrent bucket audit workers")
	traceStressMPTVerifyItemWorkers      = flag.Int("traceStressMPTVerifyItemWorkers", 2, "Concurrent item workers per bucket")
)

const traceStressHeader = "block_number,tx_index,seq,object_type,address,slot_or_chunk,operation,value_hash,value_len"

type traceStressKind byte

const (
	traceStressGet traceStressKind = iota
	traceStressPut
	traceStressDelete
)

type traceStressOp struct {
	key       []byte
	value     []byte
	kind      traceStressKind
	operation string
	block     uint64
}

type traceStressReader struct {
	files     []string
	fileIndex int
	file      *os.File
	gz        *gzip.Reader
	csv       *csv.Reader
	current   string
}

func newTraceStressReader(inputDir string, startFile, fileLimit int) (*traceStressReader, error) {
	files, err := filepath.Glob(filepath.Join(inputDir, "state_access_trace_*.csv.gz"))
	if err != nil {
		return nil, err
	}
	sort.Strings(files)
	if startFile < 0 || startFile >= len(files) {
		return nil, fmt.Errorf("traceStressStartFile=%d outside %d trace shards", startFile, len(files))
	}
	files = files[startFile:]
	if fileLimit > 0 && fileLimit < len(files) {
		files = files[:fileLimit]
	}
	return &traceStressReader{files: files}, nil
}

func (r *traceStressReader) closeCurrent() error {
	var errs []error
	if r.gz != nil {
		errs = append(errs, r.gz.Close())
	}
	if r.file != nil {
		errs = append(errs, r.file.Close())
	}
	r.file, r.gz, r.csv = nil, nil, nil
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *traceStressReader) Close() error {
	return r.closeCurrent()
}

func (r *traceStressReader) openNext() error {
	if err := r.closeCurrent(); err != nil {
		return err
	}
	if r.fileIndex >= len(r.files) {
		return io.EOF
	}
	path := r.files[r.fileIndex]
	r.fileIndex++
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	gz, err := gzip.NewReader(bufio.NewReaderSize(f, 1<<20))
	if err != nil {
		f.Close()
		return err
	}
	reader := csv.NewReader(bufio.NewReaderSize(gz, 1<<20))
	reader.ReuseRecord = true
	header, err := reader.Read()
	if err != nil {
		gz.Close()
		f.Close()
		return err
	}
	if strings.Join(header, ",") != traceStressHeader {
		gz.Close()
		f.Close()
		return fmt.Errorf("unexpected trace header in %s: %q", path, strings.Join(header, ","))
	}
	r.file, r.gz, r.csv, r.current = f, gz, reader, filepath.Base(path)
	return nil
}

func (r *traceStressReader) Read() (traceStressOp, error) {
	for {
		if r.csv == nil {
			if err := r.openNext(); err != nil {
				return traceStressOp{}, err
			}
		}
		record, err := r.csv.Read()
		if errors.Is(err, io.EOF) {
			if err := r.closeCurrent(); err != nil {
				return traceStressOp{}, err
			}
			continue
		}
		if err != nil {
			return traceStressOp{}, fmt.Errorf("read %s: %w", r.current, err)
		}
		op, err := parseTraceStressRecord(record)
		if err != nil {
			line, _ := r.csv.FieldPos(0)
			return traceStressOp{}, fmt.Errorf("parse %s line %d: %w", r.current, line, err)
		}
		return op, nil
	}
}

func decodeTraceStressHash(input string) ([]byte, error) {
	input = strings.TrimPrefix(input, "0x")
	if len(input) != common.HashLength*2 {
		return nil, fmt.Errorf("want 32-byte hash, got %d hex chars", len(input))
	}
	decoded, err := hex.DecodeString(input)
	if err != nil {
		return nil, err
	}
	return decoded, nil
}

var traceStressZeroStorageHash = crypto.Keccak256Hash(make([]byte, common.HashLength)).Hex()

func parseTraceStressRecord(record []string) (traceStressOp, error) {
	if len(record) != 9 {
		return traceStressOp{}, fmt.Errorf("want 9 fields, got %d", len(record))
	}
	block, err := strconv.ParseUint(record[0], 10, 64)
	if err != nil {
		return traceStressOp{}, fmt.Errorf("block: %w", err)
	}
	if !common.IsHexAddress(record[4]) {
		return traceStressOp{}, fmt.Errorf("invalid address %q", record[4])
	}
	address := common.HexToAddress(record[4])
	var key []byte
	switch record[3] {
	case "account":
		key = trieutils.BinaryTreeBasicDataKey(address)
	case "storage":
		slot, err := decodeTraceStressHash(record[5])
		if err != nil {
			return traceStressOp{}, fmt.Errorf("storage slot: %w", err)
		}
		key = trieutils.BinaryTreeStorageSlotKey(address, slot)
	case "code":
		chunk, err := strconv.ParseUint(record[5], 10, 64)
		if err != nil {
			return traceStressOp{}, fmt.Errorf("code chunk: %w", err)
		}
		key = trieutils.BinaryTreeCodeChunkKey(address, chunk)
	default:
		return traceStressOp{}, fmt.Errorf("unsupported object type %q", record[3])
	}
	op := traceStressOp{key: key, operation: record[6], block: block}
	switch record[6] {
	case "read", "touch":
		op.kind = traceStressGet
	case "delete":
		op.kind = traceStressDelete
	case "write", "create":
		if record[3] == "storage" && strings.EqualFold(record[7], traceStressZeroStorageHash) {
			op.kind = traceStressDelete
			break
		}
		op.kind = traceStressPut
		if record[7] != "" {
			op.value, err = decodeTraceStressHash(record[7])
			if err != nil {
				return traceStressOp{}, fmt.Errorf("value hash: %w", err)
			}
		} else {
			op.value = crypto.Keccak256(key, []byte(record[6]))
		}
	default:
		return traceStressOp{}, fmt.Errorf("unsupported operation %q", record[6])
	}
	return op, nil
}

type traceStressCounts struct {
	Reads   int64 `json:"reads"`
	Touches int64 `json:"touches"`
	Writes  int64 `json:"writes"`
	Creates int64 `json:"creates"`
	Deletes int64 `json:"deletes"`
}

func (c *traceStressCounts) add(operation string) {
	switch operation {
	case "read":
		c.Reads++
	case "touch":
		c.Touches++
	case "write":
		c.Writes++
	case "create":
		c.Creates++
	case "delete":
		c.Deletes++
	}
}

func (c traceStressCounts) total() int64 {
	return c.Reads + c.Touches + c.Writes + c.Creates + c.Deletes
}

type traceStressWindow struct {
	counts                traceStressCounts
	gets, puts, deletes   int64
	parse, operations     time.Duration
	prune, commit, write  time.Duration
	intermediate          time.Duration
	intermediateWrite     time.Duration
	finalCommitPhase      [4]int64
	stagedPruneBytes      int64
	stagedOpsBytes        int64
	pruneDiag             [4]int64
	batchWall             []time.Duration
	pruneWall             []time.Duration
	comparativeBatchWall  []time.Duration
	firstBlock, lastBlock uint64
	mptDiag               [8]int64
	access                traceStressAccessStats
	accessSample          time.Duration
	classificationFilter  traceStressFilterStats
	proofRedemptions      int64
	proofBytes            int64
	proofBlocks           []int64
	proofCounts           []int64
	proofVerifyCount      int64
	proofVerify           time.Duration
	bucketIO              MPTBucketReadIOStats
	blockRedemptions      []int64
	blockUniqueProofBytes []int64
	blockUniqueBuckets    int64
	proofSizes            []int64
	proofLatencies        []time.Duration
	filterProbeLatencies  []time.Duration
	bucketOccupancy       []int64
	bucketEmpty           int64
}

type traceStressFilterStats struct {
	Lookups        int64
	Negatives      int64
	Positives      int64
	FalsePositives int64
}

func proofReservoirAdd(res *[]int64, v, seen int64, cap int, rng *rand.Rand) {
	if len(*res) < cap {
		*res = append(*res, v)
		return
	}
	if j := rng.Int63n(seen); j < int64(cap) {
		(*res)[j] = v
	}
}

func percentileInt64(sorted []int64, q float64) int64 {
	if len(sorted) == 0 {
		return 0
	}
	return sorted[min(int(q*float64(len(sorted))), len(sorted)-1)]
}

func sortedInt64Copy(values []int64) []int64 {
	out := append([]int64(nil), values...)
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func blockProofColumns(counts, proofBytes []int64, uniqueBuckets int64) []string {
	positiveCounts := make([]int64, 0, len(counts))
	positiveBytes := make([]int64, 0, len(counts))
	var totalBytes int64
	for i, count := range counts {
		totalBytes += proofBytes[i]
		if count > 0 {
			positiveCounts = append(positiveCounts, count)
			positiveBytes = append(positiveBytes, proofBytes[i])
		}
	}
	stat := func(values []int64) []string {
		sorted := sortedInt64Copy(values)
		var sum int64
		for _, value := range values {
			sum += value
		}
		return []string{
			strconv.FormatFloat(traceStressRatio(sum, int64(len(values))), 'f', 6, 64),
			strconv.FormatInt(percentileInt64(sorted, .50), 10),
			strconv.FormatInt(percentileInt64(sorted, .95), 10),
			strconv.FormatInt(percentileInt64(sorted, .99), 10),
		}
	}
	columns := []string{strconv.Itoa(len(counts))}
	columns = append(columns, stat(counts)...)
	columns = append(columns, stat(proofBytes)...)
	columns = append(columns, strconv.Itoa(len(positiveCounts)))
	columns = append(columns, stat(positiveCounts)...)
	columns = append(columns, stat(positiveBytes)...)
	return append(columns, strconv.FormatInt(uniqueBuckets, 10), strconv.FormatInt(totalBytes, 10))
}

func bucketCapacityMetadata() any {
	if TraceMPTBucketCapacity > 0 {
		return TraceMPTBucketCapacity
	}
	return nil
}

func filterStatsFromDiagnostics(d UpdateDiagnostics) traceStressFilterStats {
	return traceStressFilterStats{
		Lookups:        d.ArchiveFilterLookups,
		Negatives:      d.ArchiveFilterNegatives,
		Positives:      d.ArchiveFilterPositives,
		FalsePositives: d.ArchiveFilterFalsePositives,
	}
}

func (s *traceStressFilterStats) add(other traceStressFilterStats) {
	s.Lookups += other.Lookups
	s.Negatives += other.Negatives
	s.Positives += other.Positives
	s.FalsePositives += other.FalsePositives
}

func (s traceStressFilterStats) sub(other traceStressFilterStats) traceStressFilterStats {
	return traceStressFilterStats{
		Lookups:        s.Lookups - other.Lookups,
		Negatives:      s.Negatives - other.Negatives,
		Positives:      s.Positives - other.Positives,
		FalsePositives: s.FalsePositives - other.FalsePositives,
	}
}

func traceStressRatio(numerator, denominator int64) float64 {
	if denominator == 0 {
		return 0
	}
	return float64(numerator) / float64(denominator)
}

type traceStressAccessStats struct {
	Samples             int64 `json:"samples"`
	ReadHot             int64 `json:"read_hot"`
	ReadHotNanos        int64 `json:"read_hot_nanos"`
	ReadArchived        int64 `json:"read_archived"`
	ReadArchivedNanos   int64 `json:"read_archived_nanos"`
	ReadMissing         int64 `json:"read_missing"`
	ReadMissingNanos    int64 `json:"read_missing_nanos"`
	WriteHot            int64 `json:"write_hot"`
	WriteHotNanos       int64 `json:"write_hot_nanos"`
	WriteArchived       int64 `json:"write_archived"`
	WriteArchivedNanos  int64 `json:"write_archived_nanos"`
	WriteMissing        int64 `json:"write_missing"`
	WriteMissingNanos   int64 `json:"write_missing_nanos"`
	DeleteHot           int64 `json:"delete_hot"`
	DeleteHotNanos      int64 `json:"delete_hot_nanos"`
	DeleteArchived      int64 `json:"delete_archived"`
	DeleteArchivedNanos int64 `json:"delete_archived_nanos"`
	DeleteMissing       int64 `json:"delete_missing"`
	DeleteMissingNanos  int64 `json:"delete_missing_nanos"`
	Errors              int64 `json:"errors"`
}

func (s *traceStressAccessStats) add(kind traceStressKind, fromArchive bool, sampleErr error, elapsed time.Duration) {
	s.Samples++
	if sampleErr != nil && !errors.Is(sampleErr, ErrNodeNotFound) {
		s.Errors++
		return
	}
	if errors.Is(sampleErr, ErrNodeNotFound) {
		switch kind {
		case traceStressGet:
			s.ReadMissing++
			s.ReadMissingNanos += elapsed.Nanoseconds()
		case traceStressPut:
			s.WriteMissing++
			s.WriteMissingNanos += elapsed.Nanoseconds()
		case traceStressDelete:
			s.DeleteMissing++
			s.DeleteMissingNanos += elapsed.Nanoseconds()
		}
		return
	}
	switch {
	case kind == traceStressGet && fromArchive:
		s.ReadArchived++
		s.ReadArchivedNanos += elapsed.Nanoseconds()
	case kind == traceStressGet:
		s.ReadHot++
		s.ReadHotNanos += elapsed.Nanoseconds()
	case kind == traceStressPut && fromArchive:
		s.WriteArchived++
		s.WriteArchivedNanos += elapsed.Nanoseconds()
	case kind == traceStressPut:
		s.WriteHot++
		s.WriteHotNanos += elapsed.Nanoseconds()
	case kind == traceStressDelete && fromArchive:
		s.DeleteArchived++
		s.DeleteArchivedNanos += elapsed.Nanoseconds()
	case kind == traceStressDelete:
		s.DeleteHot++
		s.DeleteHotNanos += elapsed.Nanoseconds()
	}
}

func classifyTraceStressAccess(hot TraceHotTrie, key []byte) (bool, error) {
	if len(key) != common.HashLength {
		return false, fmt.Errorf("trace key must be %d bytes", common.HashLength)
	}
	_, fromArchive, err := hot.GetValueRef(key)
	return fromArchive, err
}

type TraceHotTrie interface {
	Get(key []byte) ([]byte, error)
	Put(key, value []byte) error
	Delete(key []byte) error
	GetValueRef(key []byte) ([]byte, bool, error)
	PruneNextShard() error
	CommitToBatch(batch Batcher, destructive bool) ([]byte, error)
}

type TraceHotTrieSpec struct {
	DB                        KVStore
	ShardDepthBits            int
	CuckooBuckets             int
	CuckooSlots               int
	ActivateArchivedKeyOnRead bool
	Backend                   string
	CleanCacheBytes           int
	WriteBufferBytes          int
	FlushEveryBatches         int
	ArchiveResidentEntries    int
	AsyncPrune                bool
	VerifyAtCommit            bool
	VerifyBucketWorkers       int
	VerifyItemWorkers         int
}

var TraceHotTrieNew func(spec TraceHotTrieSpec) (TraceHotTrie, error)

func (s *traceStressAccessStats) merge(other traceStressAccessStats) {
	s.Samples += other.Samples
	s.ReadHot += other.ReadHot
	s.ReadHotNanos += other.ReadHotNanos
	s.ReadArchived += other.ReadArchived
	s.ReadArchivedNanos += other.ReadArchivedNanos
	s.ReadMissing += other.ReadMissing
	s.ReadMissingNanos += other.ReadMissingNanos
	s.WriteHot += other.WriteHot
	s.WriteHotNanos += other.WriteHotNanos
	s.WriteArchived += other.WriteArchived
	s.WriteArchivedNanos += other.WriteArchivedNanos
	s.WriteMissing += other.WriteMissing
	s.WriteMissingNanos += other.WriteMissingNanos
	s.DeleteHot += other.DeleteHot
	s.DeleteHotNanos += other.DeleteHotNanos
	s.DeleteArchived += other.DeleteArchived
	s.DeleteArchivedNanos += other.DeleteArchivedNanos
	s.DeleteMissing += other.DeleteMissing
	s.DeleteMissingNanos += other.DeleteMissingNanos
	s.Errors += other.Errors
}

type traceStressSelection struct {
	startBlock      uint64
	endBlock        uint64
	blockCount      uint64
	started         bool
	firstBlock      uint64
	lastBlock       uint64
	nextBlock       uint64
	pending         *traceStressOp
	reachedEOF      bool
	reachedEndBlock bool
}

func (s *traceStressSelection) collect(reader *traceStressReader, limit int, remaining int64) ([]traceStressOp, bool, error) {
	ops := make([]traceStressOp, 0, limit)
	if remaining > 0 && int64(limit) > remaining {
		limit = int(remaining)
	}
	for len(ops) < limit {
		op, err := reader.Read()
		if errors.Is(err, io.EOF) {
			s.reachedEOF = true
			if s.endBlock > 0 && s.lastBlock >= s.endBlock {
				s.reachedEndBlock = true
			}
			return ops, true, nil
		}
		if err != nil {
			return nil, false, err
		}
		if !s.started {
			if op.block < s.startBlock {
				continue
			}
			s.started = true
			s.firstBlock = op.block
			if s.blockCount > 0 {
				s.endBlock = op.block + s.blockCount - 1
			}
		}
		if s.endBlock > 0 && op.block > s.endBlock {
			s.reachedEndBlock = true
			return ops, true, nil
		}
		ops = append(ops, op)
		s.lastBlock = op.block
		if remaining > 0 && int64(len(ops)) >= remaining {
			return ops, true, nil
		}
	}
	return ops, false, nil
}

// collectBlock 每次返回一个区块的操作；轨迹缺失的区块也返回空批次。
func (s *traceStressSelection) collectBlock(reader *traceStressReader) (uint64, []traceStressOp, bool, error) {
	if !s.started {
		for {
			op, err := reader.Read()
			if err != nil {
				if errors.Is(err, io.EOF) {
					s.reachedEOF = true
					return 0, nil, true, nil
				}
				return 0, nil, false, err
			}
			if op.block < s.startBlock {
				continue
			}
			s.started = true
			s.firstBlock = op.block
			if s.startBlock > 0 {
				s.firstBlock = s.startBlock
			}
			s.nextBlock = s.firstBlock
			if s.blockCount > 0 {
				s.endBlock = s.firstBlock + s.blockCount - 1
			}
			s.pending = &op
			break
		}
	}
	block := s.nextBlock
	if s.endBlock > 0 && block > s.endBlock {
		s.reachedEndBlock = true
		return 0, nil, true, nil
	}
	var ops []traceStressOp
	for {
		var op traceStressOp
		if s.pending != nil {
			op = *s.pending
			s.pending = nil
		} else {
			var err error
			op, err = reader.Read()
			if err != nil {
				if errors.Is(err, io.EOF) {
					s.reachedEOF = true
					s.lastBlock = block
					s.nextBlock = block + 1
					if s.endBlock > 0 && block == s.endBlock {
						s.reachedEndBlock = true
					}
					return block, ops, true, nil
				}
				return 0, nil, false, err
			}
		}
		if op.block < block {
			return 0, nil, false, fmt.Errorf("out-of-order source block %d after block %d", op.block, block)
		}
		if op.block > block {
			s.pending = &op
			s.lastBlock = block
			s.nextBlock = block + 1
			if s.endBlock > 0 && block == s.endBlock {
				s.reachedEndBlock = true
				return block, ops, true, nil
			}
			return block, ops, false, nil
		}
		ops = append(ops, op)
	}
}

func durationMS(d time.Duration) string {
	return strconv.FormatFloat(float64(d)/float64(time.Millisecond), 'f', 3, 64)
}

func opsPerSecond(ops int64, elapsed time.Duration) float64 {
	if elapsed <= 0 {
		return 0
	}
	return float64(ops) / elapsed.Seconds()
}

func percentileDuration(values []time.Duration, p float64) time.Duration {
	if len(values) == 0 {
		return 0
	}
	ordered := append([]time.Duration(nil), values...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	return ordered[int(p*float64(len(ordered)-1))]
}

func writeTraceStressJSON(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(path, data, 0o644)
}

func TestParseTraceStressRecord(t *testing.T) {
	address := "0x0000000000000000000000000000000000001234"
	valueHash := "0x1111111111111111111111111111111111111111111111111111111111111111"
	tests := []struct {
		name   string
		record []string
		kind   traceStressKind
	}{
		{"account read", []string{"1", "0", "0", "account", address, "", "read", valueHash, "32"}, traceStressGet},
		{"storage zero delete", []string{"1", "0", "1", "storage", address, common.Hash{}.Hex(), "write", traceStressZeroStorageHash, "32"}, traceStressDelete},
		{"code write", []string{"1", "0", "2", "code", address, "7", "write", valueHash, "31"}, traceStressPut},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			op, err := parseTraceStressRecord(test.record)
			if err != nil {
				t.Fatal(err)
			}
			if op.kind != test.kind {
				t.Fatalf("kind %d, want %d", op.kind, test.kind)
			}
			if len(op.key) != common.HashLength {
				t.Fatalf("key length %d", len(op.key))
			}
			if op.kind == traceStressPut && len(op.value) != common.HashLength {
				t.Fatalf("value length %d", len(op.value))
			}
		})
	}
}

func TestTraceStressReader(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state_access_trace_000000001_000000001.csv.gz")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	gz := gzip.NewWriter(f)
	w := csv.NewWriter(gz)
	address := "0x0000000000000000000000000000000000001234"
	valueHash := "0x1111111111111111111111111111111111111111111111111111111111111111"
	for _, row := range [][]string{
		strings.Split(traceStressHeader, ","),
		{"1", "0", "0", "account", address, "", "read", valueHash, "32"},
		{"1", "0", "1", "code", address, "0", "write", valueHash, "31"},
	} {
		if err := w.Write(row); err != nil {
			t.Fatal(err)
		}
	}
	w.Flush()
	if err := w.Error(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := newTraceStressReader(dir, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	for i, want := range []traceStressKind{traceStressGet, traceStressPut} {
		op, err := r.Read()
		if err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		if op.kind != want {
			t.Fatalf("read %d kind %d, want %d", i, op.kind, want)
		}
	}
	if _, err := r.Read(); !errors.Is(err, io.EOF) {
		t.Fatalf("final read error %v, want EOF", err)
	}
}

func TestTraceStressSelection(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state_access_trace_000000001_000000003.csv.gz")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	gz := gzip.NewWriter(f)
	w := csv.NewWriter(gz)
	address := "0x0000000000000000000000000000000000001234"
	valueHash := "0x1111111111111111111111111111111111111111111111111111111111111111"
	if err := w.Write(strings.Split(traceStressHeader, ",")); err != nil {
		t.Fatal(err)
	}
	for _, block := range []string{"1", "2", "3"} {
		for seq, row := range [][]string{
			{"read", "", "32"},
			{"write", valueHash, "32"},
		} {
			record := []string{block, "0", strconv.Itoa(seq), "account", address, "", row[0], row[1], row[2]}
			if err := w.Write(record); err != nil {
				t.Fatal(err)
			}
		}
	}
	w.Flush()
	if err := w.Error(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := newTraceStressReader(dir, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	selection := traceStressSelection{startBlock: 2, endBlock: 2}
	ops, complete, err := selection.collect(r, 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !complete || len(ops) != 2 || selection.firstBlock != 2 || selection.lastBlock != 2 || !selection.reachedEndBlock {
		t.Fatalf("unexpected block selection: complete=%v ops=%d first=%d last=%d reachedEnd=%v", complete, len(ops), selection.firstBlock, selection.lastBlock, selection.reachedEndBlock)
	}

	r2, err := newTraceStressReader(dir, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer r2.Close()
	selection = traceStressSelection{blockCount: 2}
	ops, complete, err = selection.collect(r2, 2, 0)
	if err != nil {
		t.Fatal(err)
	}
	if complete || len(ops) != 2 || selection.firstBlock != 1 || selection.lastBlock != 1 {
		t.Fatalf("unexpected operation-limit selection: complete=%v ops=%d first=%d last=%d", complete, len(ops), selection.firstBlock, selection.lastBlock)
	}
	ops, complete, err = selection.collect(r2, 2, 0)
	if err != nil {
		t.Fatal(err)
	}
	if complete || len(ops) != 2 || selection.firstBlock != 1 || selection.lastBlock != 2 || selection.endBlock != 2 {
		t.Fatalf("unexpected block-count selection: complete=%v ops=%d first=%d last=%d end=%d", complete, len(ops), selection.firstBlock, selection.lastBlock, selection.endBlock)
	}
	ops, complete, err = selection.collect(r2, 2, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !complete || len(ops) != 0 {
		t.Fatalf("unexpected end-boundary read: complete=%v ops=%d", complete, len(ops))
	}
}

// TestAMTTraceStress 按原始顺序回放状态操作并记录论文所用指标。
func TestAMTTraceStress(t *testing.T) {
	if *traceStressInputDir == "" {
		t.Skip("set -traceStressInputDir to run the trace workload")
	}
	if *traceStressBaseDir == "" {
		t.Fatal("-traceStressBaseDir must name a fresh output directory")
	}
	blockBoundary := *traceStressEndBlock > 0 || *traceStressBlocks > 0
	if *traceStressOps <= 0 && !blockBoundary {
		t.Fatal("traceStressOps must be positive when no end block or block count is set")
	}
	if *traceStressBatchSize <= 0 || *traceStressMetricsBatches <= 0 {
		t.Fatal("traceStressBatchSize and traceStressMetricsBatches must be positive")
	}
	if *traceStressAccessSampleEvery < 0 {
		t.Fatal("traceStressAccessSampleEvery cannot be negative")
	}
	if blockBoundary && *traceStressOps != 0 {
		t.Fatal("use traceStressOps=0 with traceStressEndBlock or traceStressBlocks so a block range is never truncated")
	}
	if *traceStressBatchByBlock && !blockBoundary {
		t.Fatal("traceStressBatchByBlock requires traceStressEndBlock or traceStressBlocks")
	}
	if *traceStressMPTVerifyAtCommit && !*traceStressBatchByBlock {
		t.Fatal("commit verification requires block replay")
	}
	if *traceStressMPTVerifyAtCommit && (*traceStressMPTVerifyBucketWorkers < 1 || *traceStressMPTVerifyItemWorkers < 1) {
		t.Fatal("verification worker counts must be positive")
	}
	if *traceStressEndBlock > 0 && *traceStressBlocks > 0 {
		t.Fatal("traceStressEndBlock and traceStressBlocks are mutually exclusive")
	}
	if *traceStressStartBlock > 0 && *traceStressEndBlock > 0 && *traceStressStartBlock > *traceStressEndBlock {
		t.Fatal("traceStressStartBlock cannot exceed traceStressEndBlock")
	}
	if *traceStressShardDepthBits < 1 || *traceStressShardDepthBits > 30 {
		t.Fatalf("invalid traceStressShardDepthBits %d", *traceStressShardDepthBits)
	}
	if *traceStressCuckooBuckets <= 0 || *traceStressCuckooSlots <= 0 {
		t.Fatalf("traceStressCuckooBuckets and traceStressCuckooSlots must be positive")
	}
	if *traceStressPruneEveryBatches < 0 {
		t.Fatal("traceStressPruneEveryBatches cannot be negative")
	}
	if *traceStressPruneEveryBatches > 0 && *traceStressPruneEveryBlocks > 0 {
		t.Fatal("traceStressPruneEveryBatches and traceStressPruneEveryBlocks are mutually exclusive")
	}
	if *traceStressSplitArchiveDB && !*traceStressCompactPerWindow {
		t.Fatal("traceStressSplitArchiveDB requires traceStressCompactPerWindow=true")
	}
	if _, err := os.Stat(*traceStressBaseDir); !errors.Is(err, os.ErrNotExist) {
		if err == nil {
			t.Fatalf("traceStressBaseDir already exists: %s", *traceStressBaseDir)
		}
		t.Fatalf("stat traceStressBaseDir: %v", err)
	}
	stateDir := filepath.Join(*traceStressBaseDir, "state_db")
	archiveDir := filepath.Join(*traceStressBaseDir, "archive_db")
	resultsDir := filepath.Join(*traceStressBaseDir, "results")
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(resultsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if *traceStressSplitArchiveDB {
		if err := os.MkdirAll(archiveDir, 0o755); err != nil {
			t.Fatal(err)
		}
	}

	reader, err := newTraceStressReader(*traceStressInputDir, *traceStressStartFile, *traceStressFileLimit)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	archiveRoundDomains := int64(1) << uint(*traceStressShardDepthBits)
	pruneEveryBatches := int64(*traceStressPruneEveryBatches)
	if pruneEveryBatches < 1 {
		pruneEveryBatches = 1
	}
	archivePeriodOps := archiveRoundDomains * pruneEveryBatches * int64(*traceStressBatchSize)
	var batchSizeMetadata, archivePeriodMetadata any
	if !*traceStressBatchByBlock {
		batchSizeMetadata = *traceStressBatchSize
		archivePeriodMetadata = archivePeriodOps
	}
	metadata := map[string]any{
		"generated_at":                  time.Now().Format(time.RFC3339),
		"base_dir":                      *traceStressBaseDir,
		"input_dir":                     *traceStressInputDir,
		"source_files":                  reader.files,
		"start_file":                    *traceStressStartFile,
		"file_limit":                    *traceStressFileLimit,
		"operations":                    *traceStressOps,
		"start_block":                   *traceStressStartBlock,
		"requested_start_block":         *traceStressStartBlock,
		"effective_start_block":         nil,
		"end_block":                     *traceStressEndBlock,
		"block_count":                   *traceStressBlocks,
		"batch_size":                    batchSizeMetadata,
		"batch_by_block":                *traceStressBatchByBlock,
		"metrics_batches":               *traceStressMetricsBatches,
		"async_prune":                   *traceStressAsyncPrune,
		"prune_every_batches":           *traceStressPruneEveryBatches,
		"prune_every_blocks":            *traceStressPruneEveryBlocks,
		"shard_depth_bits":              *traceStressShardDepthBits,
		"shard_count":                   1 << uint(*traceStressShardDepthBits),
		"archive_bucket_capacity":       bucketCapacityMetadata(),
		"trie_backend":                  *traceStressTrieBackend,
		"clean_cache_mb":                *traceStressPathCleanCacheMB,
		"write_buffer_mb":               *traceStressPathWriteBufferMB,
		"flush_every_batches":           *traceStressPathFlushEveryBatches,
		"archive_resident_entries":      *traceStressArchiveResidentEntries,
		"archive_period_ops":            archivePeriodMetadata,
		"cuckoo_buckets":                *traceStressCuckooBuckets,
		"cuckoo_slots":                  *traceStressCuckooSlots,
		"mpt_verify_at_commit":          *traceStressMPTVerifyAtCommit,
		"mpt_verify_bucket_workers":     *traceStressMPTVerifyBucketWorkers,
		"mpt_verify_item_workers":       *traceStressMPTVerifyItemWorkers,
		"leveldb":                       map[string]any{"cache_mb": 512, "handles": 256},
		"split_archive_db":              *traceStressSplitArchiveDB,
		"archive_db_dir":                map[bool]string{true: archiveDir, false: ""}[*traceStressSplitArchiveDB],
		"access_sample_every":           *traceStressAccessSampleEvery,
		"activate_archived_key_on_read": *traceStressActivateArchivedKeyOnRead,
		"value_rule":                    "32-byte value_hash; deterministic key/op hash when absent",
		"storage_zero_hash_is_delete":   true,
		"timing_scope":                  "Operations_Per_Sec includes bucket KV reads and read promotion; excludes parse, prune, access sampling, final stats and commit verification",
		"command":                       strings.Join(os.Args, " "),
	}
	if err := writeTraceStressJSON(filepath.Join(*traceStressBaseDir, "metadata.json"), metadata); err != nil {
		t.Fatal(err)
	}
	if err := writeTraceStressJSON(filepath.Join(*traceStressBaseDir, "run_status.json"), map[string]any{"status": "running", "started_at": time.Now().Format(time.RFC3339)}); err != nil {
		t.Fatal(err)
	}

	db, err := leveldb.New(stateDir, 512, 256, "trace-stress", false)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var stateStore ethdb.KeyValueStore = db
	if *traceStressSplitArchiveDB {
		archiveDB, err := leveldb.New(archiveDir, 512, 256, "trace-stress-archive", false)
		if err != nil {
			t.Fatal(err)
		}
		defer archiveDB.Close()
		stateStore = &traceSplitArchiveStore{KeyValueStore: db, archive: archiveDB}
	}
	stateAdapter := &stressDBAdapter{stateStore}
	if TraceHotTrieNew == nil {
		t.Fatal("MPT hot-layer hook is not registered")
	}
	hotBackend, err := TraceHotTrieNew(TraceHotTrieSpec{
		DB:                        stateAdapter,
		ShardDepthBits:            *traceStressShardDepthBits,
		CuckooBuckets:             *traceStressCuckooBuckets,
		CuckooSlots:               *traceStressCuckooSlots,
		ActivateArchivedKeyOnRead: *traceStressActivateArchivedKeyOnRead,
		Backend:                   *traceStressTrieBackend,
		CleanCacheBytes:           *traceStressPathCleanCacheMB << 20,
		WriteBufferBytes:          *traceStressPathWriteBufferMB << 20,
		FlushEveryBatches:         *traceStressPathFlushEveryBatches,
		ArchiveResidentEntries:    *traceStressArchiveResidentEntries,
		AsyncPrune:                *traceStressAsyncPrune,
		VerifyAtCommit:            *traceStressMPTVerifyAtCommit,
		VerifyBucketWorkers:       *traceStressMPTVerifyBucketWorkers,
		VerifyItemWorkers:         *traceStressMPTVerifyItemWorkers,
	})
	if err != nil {
		t.Fatalf("create AMT hot layer: %v", err)
	}
	getValue := hotBackend.Get
	putValue := hotBackend.Put
	deleteValue := hotBackend.Delete
	classifyAccess := classifyTraceStressAccess
	batch := stateAdapter.NewBatch()
	defer batch.Reset()
	intermediateBatch := stateAdapter.NewBatch()
	defer intermediateBatch.Reset()

	metricsPath := filepath.Join(resultsDir, "amt_trace_stress.csv")
	metricsFile, err := os.Create(metricsPath)
	if err != nil {
		t.Fatal(err)
	}
	defer metricsFile.Close()
	metrics := csv.NewWriter(metricsFile)
	defer metrics.Flush()

	header := []string{
		"Window_Start_Batch", "Window_End_Batch", "Total_Batches", "Archive_Round_Completed", "Archive_Round_Progress", "Total_Operations", "First_Block", "Last_Block",
		"Window_Operations", "Reads", "Touches", "Writes", "Creates", "Deletes",
		"Executed_Gets", "Executed_Puts", "Executed_Deletes",
		"Access_Samples", "Sampled_Read_Hot", "Sampled_Read_Archived", "Sampled_Read_Missing",
		"Sampled_Write_Hot", "Sampled_Write_Archived", "Sampled_Write_Missing",
		"Sampled_Delete_Hot", "Sampled_Delete_Archived", "Sampled_Delete_Missing",
		"Sampled_Read_Hot_ms", "Sampled_Read_Archived_ms", "Sampled_Read_Missing_ms",
		"Sampled_Write_Hot_ms", "Sampled_Write_Archived_ms", "Sampled_Write_Missing_ms",
		"Sampled_Delete_Hot_ms", "Sampled_Delete_Archived_ms", "Sampled_Delete_Missing_ms",
		"Sampled_Access_Errors", "Hot_Read_Hit_Rate", "Access_Sample_ms",
		"Archive_Filter_Lookups", "Archive_Filter_Negatives", "Archive_Filter_Positives", "Archive_Filter_False_Positives", "Runtime_Filter_FPR",
		"Archive_Read_Promotion_Calls", "Archive_Read_Promotion_Hits", "Archive_Read_Promotion_ms",
		"Parse_ms", "Operations_ms", "Prune_Launch_ms", "Commit_ms", "DB_Write_ms", "Measured_Wall_ms",
		"Comparative_Wall_ms", "Operations_Per_Sec", "Batch_P50_ms", "Batch_P95_ms", "Batch_P99_ms", "Batch_Max_ms",
		"Comparative_Batch_P50_ms", "Comparative_Batch_P95_ms", "Comparative_Batch_P99_ms", "Comparative_Batch_Max_ms",
		"State_Bytes", "RSS_Bytes", "Heap_Bytes",
		"MPT_Staged_HotNode_Bytes", "MPT_Staged_Archive_Bytes", "MPT_Staged_Index_Bytes",
		"MPT_NodeCache_Gets", "MPT_NodeCache_Hits",
		"Archive_Maint_ms", "Intermediate_Root_ms",
		"Prune_Extracted", "Prune_Resolved_Nodes", "Prune_Extract_ms", "Prune_Mount_ms",
		"Commit_Hx_ms", "Commit_Stage_ms", "Commit_TdbCommit_ms", "Commit_ArchiveLoop_ms",
		"Staged_Prune_Bytes", "Staged_Ops_Bytes",
		"Archive_Bytes", "System_Total_Bytes", "Live_Hot_Leaves", "Live_Buckets",
		"MPT_Staged_StubMeta_Bytes", "MPT_StubMeta_Fetches", "MPT_StubMeta_Fetch_Bytes",
		"GC_Pause_ms", "GC_Num",
		"Proof_Redemptions", "Proof_Bytes",
		"Proof_Block_P50_Bytes", "Proof_Block_P95_Bytes", "Proof_Block_P99_Bytes", "Proof_Block_Max_Bytes",
		"Proof_Verify_Count", "Proof_Verify_ms",
		"Prune_P50_ms", "Prune_P95_ms", "Prune_P99_ms", "Prune_Max_ms", "Prune_Spike_Rate",
		"Resurrection_Proof_P50_Bytes", "Resurrection_Proof_P95_Bytes", "Resurrection_Proof_P99_Bytes", "Resurrection_Proof_Max_Bytes",
		"Resurrection_Proof_P50_ms", "Resurrection_Proof_P95_ms", "Resurrection_Proof_P99_ms", "Resurrection_Proof_Max_ms",
		"Bucket_Occupancy_P50", "Bucket_Occupancy_P95", "Bucket_Occupancy_P99", "Bucket_Occupancy_Max", "Bucket_Empty_Count",
		"Filter_Probe_P50_ms", "Filter_Probe_P95_ms", "Filter_Probe_P99_ms", "Filter_Probe_Max_ms",
		"Resurrection_Block_P50", "Resurrection_Block_P95", "Resurrection_Block_P99",
		"Full_Node_DB_Bytes", "Archive_DB_Bytes", "Path_Buffered_Bytes", "Path_Diff_Bytes",
		"Block_All_N", "Block_All_Redemptions_Mean", "Block_All_Redemptions_P50", "Block_All_Redemptions_P95", "Block_All_Redemptions_P99",
		"Block_All_Unique_Proof_Mean_Bytes", "Block_All_Unique_Proof_P50_Bytes", "Block_All_Unique_Proof_P95_Bytes", "Block_All_Unique_Proof_P99_Bytes",
		"Block_With_Redemption_N", "Block_With_Redemption_Redemptions_Mean", "Block_With_Redemption_Redemptions_P50", "Block_With_Redemption_Redemptions_P95", "Block_With_Redemption_Redemptions_P99",
		"Block_With_Redemption_Unique_Proof_Mean_Bytes", "Block_With_Redemption_Unique_Proof_P50_Bytes", "Block_With_Redemption_Unique_Proof_P95_Bytes", "Block_With_Redemption_Unique_Proof_P99_Bytes",
		"Block_Unique_Buckets_Total", "Block_Unique_Proof_Bytes_Total",
		"MPT_Bucket_StubMeta_DB_Reads", "MPT_Bucket_StubMeta_DB_Get_ms", "MPT_Bucket_Payload_DB_Reads", "MPT_Bucket_Payload_DB_Get_ms",
		"Operations_Excl_Bucket_IO_Per_Sec", "Operations_Excl_Bucket_IO_And_Read_Promotion_Per_Sec",
	}
	if err := metrics.Write(header); err != nil {
		t.Fatal(err)
	}
	metrics.Flush()

	proc, _ := process.NewProcess(int32(os.Getpid()))
	ResetCommitDiagnostics()
	selection := traceStressSelection{
		startBlock: *traceStressStartBlock,
		endBlock:   *traceStressEndBlock,
		blockCount: *traceStressBlocks,
	}
	var (
		totalCounts               traceStressCounts
		window                    traceStressWindow
		totalParse                time.Duration
		totalOpsDur               time.Duration
		totalPrune                time.Duration
		totalCommit               time.Duration
		totalWrite                time.Duration
		totalGets                 int64
		totalPuts                 int64
		totalDeletes              int64
		totalAccess               traceStressAccessStats
		totalAccessSample         time.Duration
		totalClassificationFilter traceStressFilterStats
		lastRoot                  []byte
		initialUpdateDiag         = LastUpdateDiagnostics()
		prevUpdateDiag            = initialUpdateDiag
		initialBucketIO           = CurrentMPTBucketReadIOStats()
		prevBucketIO              = initialBucketIO
		prevMPTDiag               [8]int64
		prevPruneDiag             [4]int64
		prevGCPauseNs             uint64
		prevGCNum                 uint32
		started                   = time.Now()
		totalBatches              int64
		prunesDone                int64
		nextPruneBlock            uint64
		executedOperations        int64
		proofLastCount            int64
		proofLastBytes            int64
		proofBlockNum             uint64
		proofBlockAcc             int64
		proofCountAcc             int64
		proofBlockOpen            bool
		proofBlockTotal           int64
		proofBlockMax             int64
		proofBlockReservoir       []int64
		proofRedemptionSeen       int64
		proofRedemptionReservoir  []int64
		proofVerifyLastCount      int64
		proofVerifyLastNanos      int64
		proofRng                  = rand.New(rand.NewSource(20261004))
		totalBlockRedemptions     int64
		totalUniqueProofBytes     int64
		totalSourceBlocks         int64
		totalUniqueBuckets        int64
	)
	pruneOne := func() (time.Duration, error) {
		pruneStart := time.Now()
		if err := hotBackend.PruneNextShard(); err != nil {
			return 0, err
		}
		prunesDone++
		return time.Since(pruneStart), nil
	}

	flushWindow := func(batchNumber int64) {
		currentBucketIO := CurrentMPTBucketReadIOStats()
		window.bucketIO = currentBucketIO.Sub(prevBucketIO)
		prevBucketIO = currentBucketIO
		vc, vn := MPTRedemptionVerifyStats()
		window.proofVerifyCount = vc - proofVerifyLastCount
		window.proofVerify = time.Duration(vn - proofVerifyLastNanos)
		proofVerifyLastCount, proofVerifyLastNanos = vc, vn
		measured := window.operations + window.prune + window.intermediate + window.intermediateWrite + window.commit + window.write
		comparative := window.operations + window.commit + window.write
		if *traceStressMPTVerifyAtCommit {
			comparative -= window.proofVerify
		}
		archiveMaint := window.prune + window.intermediate + window.intermediateWrite
		opsPerSec := float64(0)
		if comparative > 0 {
			opsPerSec = float64(window.counts.total()) / comparative.Seconds()
		}
		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)
		var rss uint64
		if proc != nil {
			if info, err := proc.MemoryInfo(); err == nil {
				rss = info.RSS
			}
		}
		// 测量磁盘占用前先压缩 LevelDB，排除可回收的旧版本数据。
		if *traceStressCompactPerWindow {
			// 分离存储时先持久化 pathdb 的缓冲区和差分层。
			if *traceStressSplitArchiveDB {
				flusher, ok := hotBackend.(interface{ FlushDurable() error })
				if !ok {
					t.Fatal("split archive DB requires a durable hot-layer flush")
				}
				if err := flusher.FlushDurable(); err != nil {
					t.Fatalf("per-window durable hot-layer flush: %v", err)
				}
			} else if flusher, ok := hotBackend.(interface{ Flush() error }); ok {
				if err := flusher.Flush(); err != nil {
					t.Fatalf("per-window hot-layer flush: %v", err)
				}
			}
			compactStart := time.Now()
			if err := stateAdapter.Compact(); err != nil {
				t.Fatalf("per-window compaction: %v", err)
			}
			windowCompactDur := time.Since(compactStart)
			fmt.Printf("[TRACE_STRESS] batch=%d compact_ms=%d\n", batchNumber, windowCompactDur.Milliseconds())
		}
		diag := LastCommitDiagnostics()
		fullNodeDBBytes := getDirSize(stateDir)
		archiveDBBytes := int64(0)
		if *traceStressSplitArchiveDB {
			archiveDBBytes = getDirSize(archiveDir)
		}
		stateBytes := fullNodeDBBytes + archiveDBBytes
		var pathBufferedBytes, pathDiffBytes int64
		if ps, ok := hotBackend.(interface {
			PathStats() (int64, int64, int64, int64)
		}); ok {
			_, _, pathBufferedBytes, pathDiffBytes = ps.PathStats()
			if *traceStressSplitArchiveDB && *traceStressCompactPerWindow && (pathBufferedBytes != 0 || pathDiffBytes != 0) {
				t.Fatalf("per-window durable flush left pathdb bytes: buffer=%d diff=%d", pathBufferedBytes, pathDiffBytes)
			}
		}
		currentUpdateDiag := LastUpdateDiagnostics()
		updateDiag := currentUpdateDiag.Sub(prevUpdateDiag)
		prevUpdateDiag = currentUpdateDiag
		withoutBucketIO := comparative - time.Duration(window.bucketIO.TotalNanos())
		withoutBucketIOAndPromotion := withoutBucketIO - time.Duration(updateDiag.ArchiveReadPromotionNanos)
		curMPTDiag := [8]int64{diag.MPTStagedHotNodeBytes, diag.MPTStagedArchiveBytes, diag.MPTStagedIndexBytes, diag.MPTNodeCacheGets, diag.MPTNodeCacheHits, diag.MPTStagedStubMetaBytes, diag.MPTStubMetaFetches, diag.MPTStubMetaFetchBytes}
		for i := range curMPTDiag {
			window.mptDiag[i] = curMPTDiag[i] - prevMPTDiag[i]
		}
		prevMPTDiag = curMPTDiag
		curPruneDiag := [4]int64{diag.MPTPruneExtracted, diag.MPTPruneResolvedNodes, diag.MPTPruneExtractNanos, diag.MPTPruneMountNanos}
		for i := range curPruneDiag {
			window.pruneDiag[i] = curPruneDiag[i] - prevPruneDiag[i]
		}
		prevPruneDiag = curPruneDiag
		sampledExistingReads := window.access.ReadHot + window.access.ReadArchived
		workloadFilter := filterStatsFromDiagnostics(updateDiag).sub(window.classificationFilter)
		hotReadRate := traceStressRatio(window.access.ReadHot, sampledExistingReads)
		runtimeFilterFPR := traceStressRatio(workloadFilter.FalsePositives, workloadFilter.Negatives)
		archiveRoundCompleted := prunesDone / archiveRoundDomains
		archiveRoundProgress := prunesDone % archiveRoundDomains
		proofBlocksSorted := sortedInt64Copy(window.proofBlocks)
		// 窗口结束时取出全部证明和过滤器样本，并记录桶占用分布。
		psizes, platns, _, _, _ := TakeMPTRedemptionProofHistogram()
		window.proofSizes = psizes
		window.proofLatencies = make([]time.Duration, len(platns))
		for i, ns := range platns {
			window.proofLatencies[i] = time.Duration(ns)
		}
		fplatns, _, _ := TakeMPTFilterProbeLatencyHistogram()
		window.filterProbeLatencies = make([]time.Duration, len(fplatns))
		for i, ns := range fplatns {
			window.filterProbeLatencies[i] = time.Duration(ns)
		}
		if snapper, ok := hotBackend.(interface {
			BucketOccupancy() ([]int64, int64)
		}); ok {
			window.bucketOccupancy, window.bucketEmpty = snapper.BucketOccupancy()
		}
		pruneP50 := percentileDuration(window.pruneWall, .50)
		pruneSpikeRate := float64(0)
		if pruneP50 >= 100*time.Microsecond {
			pruneSpikeThreshold := 5 * pruneP50
			pruneSpikes := 0
			for _, d := range window.pruneWall {
				if d > pruneSpikeThreshold {
					pruneSpikes++
				}
			}
			pruneSpikeRate = float64(pruneSpikes) / float64(len(window.pruneWall))
		}
		row := []string{
			strconv.FormatInt(batchNumber-int64(len(window.batchWall))+1, 10), strconv.FormatInt(batchNumber, 10), strconv.FormatInt(batchNumber, 10), strconv.FormatInt(archiveRoundCompleted, 10), strconv.FormatInt(archiveRoundProgress, 10), strconv.FormatInt(totalCounts.total(), 10),
			strconv.FormatUint(window.firstBlock, 10), strconv.FormatUint(window.lastBlock, 10), strconv.FormatInt(window.counts.total(), 10),
			strconv.FormatInt(window.counts.Reads, 10), strconv.FormatInt(window.counts.Touches, 10), strconv.FormatInt(window.counts.Writes, 10), strconv.FormatInt(window.counts.Creates, 10), strconv.FormatInt(window.counts.Deletes, 10),
			strconv.FormatInt(window.gets, 10), strconv.FormatInt(window.puts, 10), strconv.FormatInt(window.deletes, 10),
			strconv.FormatInt(window.access.Samples, 10), strconv.FormatInt(window.access.ReadHot, 10), strconv.FormatInt(window.access.ReadArchived, 10), strconv.FormatInt(window.access.ReadMissing, 10),
			strconv.FormatInt(window.access.WriteHot, 10), strconv.FormatInt(window.access.WriteArchived, 10), strconv.FormatInt(window.access.WriteMissing, 10),
			strconv.FormatInt(window.access.DeleteHot, 10), strconv.FormatInt(window.access.DeleteArchived, 10), strconv.FormatInt(window.access.DeleteMissing, 10),
			durationMS(time.Duration(window.access.ReadHotNanos)), durationMS(time.Duration(window.access.ReadArchivedNanos)), durationMS(time.Duration(window.access.ReadMissingNanos)),
			durationMS(time.Duration(window.access.WriteHotNanos)), durationMS(time.Duration(window.access.WriteArchivedNanos)), durationMS(time.Duration(window.access.WriteMissingNanos)),
			durationMS(time.Duration(window.access.DeleteHotNanos)), durationMS(time.Duration(window.access.DeleteArchivedNanos)), durationMS(time.Duration(window.access.DeleteMissingNanos)),
			strconv.FormatInt(window.access.Errors, 10), strconv.FormatFloat(hotReadRate*100, 'f', 6, 64), durationMS(window.accessSample),
			strconv.FormatInt(workloadFilter.Lookups, 10), strconv.FormatInt(workloadFilter.Negatives, 10),
			strconv.FormatInt(workloadFilter.Positives, 10), strconv.FormatInt(workloadFilter.FalsePositives, 10),
			strconv.FormatFloat(runtimeFilterFPR*100, 'f', 6, 64),
			strconv.FormatInt(updateDiag.ArchiveReadPromotionCalls, 10), strconv.FormatInt(updateDiag.ArchiveReadPromotionHits, 10), durationMS(time.Duration(updateDiag.ArchiveReadPromotionNanos)),
			durationMS(window.parse), durationMS(window.operations), durationMS(window.prune), durationMS(window.commit), durationMS(window.write), durationMS(measured),
			durationMS(comparative),
			strconv.FormatFloat(opsPerSec, 'f', 2, 64), durationMS(percentileDuration(window.batchWall, .50)), durationMS(percentileDuration(window.batchWall, .95)), durationMS(percentileDuration(window.batchWall, .99)), durationMS(percentileDuration(window.batchWall, 1)),
			durationMS(percentileDuration(window.comparativeBatchWall, .50)), durationMS(percentileDuration(window.comparativeBatchWall, .95)), durationMS(percentileDuration(window.comparativeBatchWall, .99)), durationMS(percentileDuration(window.comparativeBatchWall, 1)),
			strconv.FormatInt(stateBytes, 10), strconv.FormatUint(rss, 10), strconv.FormatUint(mem.HeapAlloc, 10),
			strconv.FormatInt(window.mptDiag[0], 10), strconv.FormatInt(window.mptDiag[1], 10), strconv.FormatInt(window.mptDiag[2], 10),
			strconv.FormatInt(window.mptDiag[3], 10), strconv.FormatInt(window.mptDiag[4], 10),
			strconv.FormatInt(archiveMaint.Milliseconds(), 10), strconv.FormatInt((window.intermediate + window.intermediateWrite).Milliseconds(), 10),
			strconv.FormatInt(window.pruneDiag[0], 10), strconv.FormatInt(window.pruneDiag[1], 10),
			strconv.FormatInt(window.pruneDiag[2]/1e6, 10), strconv.FormatInt(window.pruneDiag[3]/1e6, 10),
			strconv.FormatInt(window.finalCommitPhase[0]/1e6, 10), strconv.FormatInt(window.finalCommitPhase[1]/1e6, 10),
			strconv.FormatInt(window.finalCommitPhase[2]/1e6, 10), strconv.FormatInt(window.finalCommitPhase[3]/1e6, 10),
			strconv.FormatInt(window.stagedPruneBytes, 10), strconv.FormatInt(window.stagedOpsBytes, 10),
			strconv.FormatInt(diag.MPTArchivePayloadBytes, 10), strconv.FormatInt(stateBytes, 10),
			strconv.FormatInt(diag.MPTLiveHotLeaves, 10), strconv.FormatInt(diag.MPTLiveBuckets, 10),
			strconv.FormatInt(window.mptDiag[5], 10), strconv.FormatInt(window.mptDiag[6], 10), strconv.FormatInt(window.mptDiag[7], 10),
			strconv.FormatUint((mem.PauseTotalNs-prevGCPauseNs)/uint64(time.Millisecond), 10), strconv.FormatUint(uint64(mem.NumGC-prevGCNum), 10),
			strconv.FormatInt(window.proofRedemptions, 10), strconv.FormatInt(window.proofBytes, 10),
			strconv.FormatInt(percentileInt64(proofBlocksSorted, .50), 10), strconv.FormatInt(percentileInt64(proofBlocksSorted, .95), 10),
			strconv.FormatInt(percentileInt64(proofBlocksSorted, .99), 10), strconv.FormatInt(percentileInt64(proofBlocksSorted, 1), 10),
			strconv.FormatInt(window.proofVerifyCount, 10), durationMS(window.proofVerify),
			durationMS(pruneP50), durationMS(percentileDuration(window.pruneWall, .95)), durationMS(percentileDuration(window.pruneWall, .99)), durationMS(percentileDuration(window.pruneWall, 1)),
			strconv.FormatFloat(pruneSpikeRate, 'f', 6, 64),
			strconv.FormatInt(percentileInt64(sortedInt64Copy(window.proofSizes), .50), 10),
			strconv.FormatInt(percentileInt64(sortedInt64Copy(window.proofSizes), .95), 10),
			strconv.FormatInt(percentileInt64(sortedInt64Copy(window.proofSizes), .99), 10),
			strconv.FormatInt(percentileInt64(sortedInt64Copy(window.proofSizes), 1), 10),
			durationMS(percentileDuration(window.proofLatencies, .50)),
			durationMS(percentileDuration(window.proofLatencies, .95)),
			durationMS(percentileDuration(window.proofLatencies, .99)),
			durationMS(percentileDuration(window.proofLatencies, 1)),
			strconv.FormatInt(percentileInt64(sortedInt64Copy(window.bucketOccupancy), .50), 10),
			strconv.FormatInt(percentileInt64(sortedInt64Copy(window.bucketOccupancy), .95), 10),
			strconv.FormatInt(percentileInt64(sortedInt64Copy(window.bucketOccupancy), .99), 10),
			strconv.FormatInt(percentileInt64(sortedInt64Copy(window.bucketOccupancy), 1), 10),
			strconv.FormatInt(window.bucketEmpty, 10),
			durationMS(percentileDuration(window.filterProbeLatencies, .50)),
			durationMS(percentileDuration(window.filterProbeLatencies, .95)),
			durationMS(percentileDuration(window.filterProbeLatencies, .99)),
			durationMS(percentileDuration(window.filterProbeLatencies, 1)),
			strconv.FormatInt(percentileInt64(sortedInt64Copy(window.proofCounts), .50), 10),
			strconv.FormatInt(percentileInt64(sortedInt64Copy(window.proofCounts), .95), 10),
			strconv.FormatInt(percentileInt64(sortedInt64Copy(window.proofCounts), .99), 10),
			strconv.FormatInt(fullNodeDBBytes, 10), strconv.FormatInt(archiveDBBytes, 10),
			strconv.FormatInt(pathBufferedBytes, 10), strconv.FormatInt(pathDiffBytes, 10),
		}
		if *traceStressMPTVerifyAtCommit {
			row = append(row, blockProofColumns(window.blockRedemptions, window.blockUniqueProofBytes, window.blockUniqueBuckets)...)
		} else {
			row = append(row, make([]string, 20)...)
		}
		row = append(row,
			strconv.FormatInt(window.bucketIO.MetaReads, 10), durationMS(time.Duration(window.bucketIO.MetaNanos)),
			strconv.FormatInt(window.bucketIO.PayloadReads, 10), durationMS(time.Duration(window.bucketIO.PayloadNanos)),
			strconv.FormatFloat(opsPerSecond(window.counts.total(), withoutBucketIO), 'f', 2, 64),
			strconv.FormatFloat(opsPerSecond(window.counts.total(), withoutBucketIOAndPromotion), 'f', 2, 64),
		)
		prevGCPauseNs = mem.PauseTotalNs
		prevGCNum = mem.NumGC
		if err := metrics.Write(row); err != nil {
			t.Fatal(err)
		}
		metrics.Flush()
		if err := metrics.Error(); err != nil {
			t.Fatal(err)
		}
		fmt.Printf("[TRACE_STRESS] batches=%d/%d ops=%d block=%d full_db=%d archive_db=%d rate=%.0f ops/s\n",
			batchNumber, totalBatches, totalCounts.total(), window.lastBlock, fullNodeDBBytes, archiveDBBytes, opsPerSec)
		totalAccess.merge(window.access)
		totalAccessSample += window.accessSample
		totalClassificationFilter.add(window.classificationFilter)
		window = traceStressWindow{}
	}

	for batchNumber := int64(1); ; batchNumber++ {
		parseStart := time.Now()
		remaining := *traceStressOps - totalCounts.total()
		if *traceStressOps == 0 {
			remaining = 0
		}
		var ops []traceStressOp
		var blockNumber uint64
		var reachedBoundary bool
		var err error
		if *traceStressBatchByBlock {
			blockNumber, ops, reachedBoundary, err = selection.collectBlock(reader)
		} else {
			ops, reachedBoundary, err = selection.collect(reader, *traceStressBatchSize, remaining)
			if len(ops) > 0 {
				blockNumber = ops[0].block
			}
		}
		if err != nil {
			t.Fatalf("batch %d parse: %v", batchNumber, err)
		}
		if blockNumber == 0 {
			if !selection.started {
				t.Fatalf("no trace records at or after traceStressStartBlock=%d", *traceStressStartBlock)
			}
			break
		}
		totalBatches = batchNumber
		if metadata["effective_start_block"] == nil {
			metadata["effective_start_block"] = blockNumber
			metadata["start_block"] = blockNumber
			if err := writeTraceStressJSON(filepath.Join(*traceStressBaseDir, "metadata.json"), metadata); err != nil {
				t.Fatal(err)
			}
		}
		if *traceStressBatchByBlock {
			if window.firstBlock == 0 {
				window.firstBlock = blockNumber
			}
			window.lastBlock = blockNumber
		} else {
			window.firstBlock = ops[0].block
			window.lastBlock = ops[len(ops)-1].block
		}
		for _, op := range ops {
			window.counts.add(op.operation)
			totalCounts.add(op.operation)
		}
		parseDur := time.Since(parseStart)
		window.parse += parseDur
		totalParse += parseDur
		batchStart := time.Now()
		var pruneDur time.Duration
		prunedThisBatch := false
		if *traceStressPruneEveryBatches > 0 && (batchNumber-1)%int64(*traceStressPruneEveryBatches) == 0 {
			dur, err := pruneOne()
			if err != nil {
				t.Fatalf("batch %d prune: %v", batchNumber, err)
			}
			pruneDur = dur
			prunedThisBatch = true
		}
		if *traceStressPruneEveryBlocks > 0 {
			if batchNumber == 1 {
				dur, err := pruneOne()
				if err != nil {
					t.Fatalf("batch %d prune at block %d: %v", batchNumber, window.lastBlock, err)
				}
				pruneDur += dur
				prunedThisBatch = true
				nextPruneBlock = window.firstBlock + *traceStressPruneEveryBlocks
			}
			for window.lastBlock >= nextPruneBlock {
				dur, err := pruneOne()
				if err != nil {
					t.Fatalf("batch %d prune at block %d: %v", batchNumber, window.lastBlock, err)
				}
				pruneDur += dur
				prunedThisBatch = true
				nextPruneBlock += *traceStressPruneEveryBlocks
			}
		}
		snapBeforeCommit := LastCommitDiagnostics()
		var intermediateDur, intermediateWriteDur time.Duration
		// 先落盘剪枝结果，再执行本批状态操作，分别记录两阶段开销。
		if prunedThisBatch {
			intermediateStart := time.Now()
			if _, err := hotBackend.CommitToBatch(intermediateBatch, true); err != nil {
				t.Fatalf("batch %d intermediate root commit: %v", batchNumber, err)
			}
			intermediateDur = time.Since(intermediateStart)
			intermediateWriteStart := time.Now()
			if err := intermediateBatch.Write(); err != nil {
				t.Fatalf("batch %d intermediate root write: %v", batchNumber, err)
			}
			intermediateWriteDur = time.Since(intermediateWriteStart)
			intermediateBatch.Reset()
		}
		snapMidCommit := LastCommitDiagnostics()
		blockProofStart, _, _, _ := MPTRedemptionProofStats()
		opStart := time.Now()
		batchAccessSampleDur := time.Duration(0)
		for _, op := range ops {
			sample := *traceStressAccessSampleEvery > 0 &&
				executedOperations%*traceStressAccessSampleEvery == 0
			var fromArchive bool
			var sampleErr error
			if sample {
				beforeSampleDiag := LastUpdateDiagnostics()
				sampleStart := time.Now()
				fromArchive, sampleErr = classifyAccess(hotBackend, op.key)
				sampleDur := time.Since(sampleStart)
				window.classificationFilter.add(filterStatsFromDiagnostics(LastUpdateDiagnostics()).sub(filterStatsFromDiagnostics(beforeSampleDiag)))
				window.accessSample += sampleDur
				batchAccessSampleDur += sampleDur
			}
			executionStart := time.Now()
			switch op.kind {
			case traceStressGet:
				window.gets++
				totalGets++
				_, err := getValue(op.key)
				executionDur := time.Since(executionStart)
				if sample {
					window.access.add(op.kind, fromArchive, sampleErr, executionDur)
				}
				if err != nil && !errors.Is(err, ErrNodeNotFound) {
					t.Fatalf("batch %d get: %v", batchNumber, err)
				}
			case traceStressPut:
				window.puts++
				totalPuts++
				err := putValue(op.key, op.value)
				executionDur := time.Since(executionStart)
				if sample {
					window.access.add(op.kind, fromArchive, sampleErr, executionDur)
				}
				if err != nil {
					t.Fatalf("batch %d put: %v", batchNumber, err)
				}
			case traceStressDelete:
				window.deletes++
				totalDeletes++
				err := deleteValue(op.key)
				executionDur := time.Since(executionStart)
				if sample {
					window.access.add(op.kind, fromArchive, sampleErr, executionDur)
				}
				if err != nil {
					t.Fatalf("batch %d delete: %v", batchNumber, err)
				}
			}
			executedOperations++
			if pc, pb, _, plast := MPTRedemptionProofStats(); pc != proofLastCount {
				d := pb - proofLastBytes
				window.proofRedemptions += pc - proofLastCount
				window.proofBytes += d
				if !proofBlockOpen || proofBlockNum != op.block {
					if proofBlockOpen {
						window.proofBlocks = append(window.proofBlocks, proofBlockAcc)
						window.proofCounts = append(window.proofCounts, proofCountAcc)
						proofBlockTotal++
						if proofBlockAcc > proofBlockMax {
							proofBlockMax = proofBlockAcc
						}
						proofReservoirAdd(&proofBlockReservoir, proofBlockAcc, proofBlockTotal, 1<<19, proofRng)
					}
					proofBlockNum, proofBlockAcc, proofCountAcc, proofBlockOpen = op.block, 0, 0, true
				}
				proofBlockAcc += d
				proofCountAcc += pc - proofLastCount
				for i := int64(0); i < pc-proofLastCount; i++ {
					proofRedemptionSeen++
					proofReservoirAdd(&proofRedemptionReservoir, plast, proofRedemptionSeen, 1<<18, proofRng)
				}
				proofLastCount, proofLastBytes = pc, pb
			}
		}
		opDur := time.Since(opStart) - batchAccessSampleDur
		if *traceStressMPTVerifyAtCommit {
			blockProofEnd, _, _, _ := MPTRedemptionProofStats()
			redemptions := blockProofEnd - blockProofStart
			counter, ok := hotBackend.(interface{ PendingProofStats() (int64, int64) })
			if !ok {
				t.Fatal("MPT block verification has no distinct-proof counter")
			}
			buckets, uniqueBytes := counter.PendingProofStats()
			if (redemptions == 0) != (buckets == 0) {
				t.Fatalf("block %d: %d redemptions, %d distinct buckets", blockNumber, redemptions, buckets)
			}
			window.blockRedemptions = append(window.blockRedemptions, redemptions)
			window.blockUniqueProofBytes = append(window.blockUniqueProofBytes, uniqueBytes)
			window.blockUniqueBuckets += buckets
			totalBlockRedemptions += redemptions
			totalUniqueProofBytes += uniqueBytes
			totalSourceBlocks++
			totalUniqueBuckets += buckets
		}
		_, verifyBeforeNanos := MPTRedemptionVerifyStats()
		commitStart := time.Now()
		lastRoot, err = hotBackend.CommitToBatch(batch, true)
		if err != nil {
			t.Fatalf("batch %d commit: %v", batchNumber, err)
		}
		commitDur := time.Since(commitStart)
		verifyBlockDur := time.Duration(0)
		if *traceStressMPTVerifyAtCommit {
			_, verifyAfterNanos := MPTRedemptionVerifyStats()
			verifyBlockDur = time.Duration(verifyAfterNanos - verifyBeforeNanos)
		}
		diag := LastCommitDiagnostics()
		window.stagedPruneBytes += snapMidCommit.MPTStagedHotNodeBytes - snapBeforeCommit.MPTStagedHotNodeBytes
		window.stagedOpsBytes += diag.MPTStagedHotNodeBytes - snapMidCommit.MPTStagedHotNodeBytes
		window.finalCommitPhase[0] += diag.MPTCommitHxNanos - snapMidCommit.MPTCommitHxNanos
		window.finalCommitPhase[1] += diag.MPTCommitStageNanos - snapMidCommit.MPTCommitStageNanos
		window.finalCommitPhase[2] += diag.MPTCommitTdbCommitNanos - snapMidCommit.MPTCommitTdbCommitNanos
		window.finalCommitPhase[3] += diag.MPTCommitArchiveLoopNanos - snapMidCommit.MPTCommitArchiveLoopNanos
		writeStart := time.Now()
		if err := batch.Write(); err != nil {
			t.Fatalf("batch %d write: %v", batchNumber, err)
		}
		writeDur := time.Since(writeStart)
		batch.Reset()
		window.operations += opDur
		window.prune += pruneDur
		if prunedThisBatch {
			window.pruneWall = append(window.pruneWall, pruneDur)
		}
		window.intermediate += intermediateDur
		window.intermediateWrite += intermediateWriteDur
		window.commit += commitDur
		window.write += writeDur
		window.batchWall = append(window.batchWall, time.Since(batchStart))
		window.comparativeBatchWall = append(window.comparativeBatchWall, opDur+commitDur+writeDur-verifyBlockDur)
		totalOpsDur += opDur
		totalPrune += pruneDur
		totalCommit += commitDur
		totalWrite += writeDur

		if batchNumber%int64(*traceStressMetricsBatches) == 0 || reachedBoundary {
			flushWindow(batchNumber)
		}
		if reachedBoundary {
			break
		}
	}
	if selection.endBlock > 0 && !selection.reachedEndBlock {
		t.Fatalf("trace ended at block %d before requested end block %d", selection.lastBlock, selection.endBlock)
	}

	if *traceStressSplitArchiveDB {
		if err := hotBackend.(interface{ FlushDurable() error }).FlushDurable(); err != nil {
			t.Fatalf("final durable hot-layer flush: %v", err)
		}
	} else if flusher, ok := hotBackend.(interface{ Flush() error }); ok {
		if err := flusher.Flush(); err != nil {
			t.Fatalf("final hot-layer flush: %v", err)
		}
	}
	finalFullNodeDBBytes := getDirSize(stateDir)
	finalArchiveDBBytes := int64(0)
	if *traceStressSplitArchiveDB {
		finalArchiveDBBytes = getDirSize(archiveDir)
	}
	finalSystemBytes := finalFullNodeDBBytes + finalArchiveDBBytes
	actualBlockCount := uint64(0)
	if selection.endBlock >= selection.firstBlock && selection.firstBlock > 0 {
		actualBlockCount = selection.endBlock - selection.firstBlock + 1
	}
	updateDiag := LastUpdateDiagnostics().Sub(initialUpdateDiag)
	sampledExistingReads := totalAccess.ReadHot + totalAccess.ReadArchived
	workloadFilter := filterStatsFromDiagnostics(updateDiag).sub(totalClassificationFilter)
	hotReadRate := traceStressRatio(totalAccess.ReadHot, sampledExistingReads)
	readArchiveShare := traceStressRatio(totalAccess.ReadArchived, sampledExistingReads)
	runtimeFilterFPR := traceStressRatio(workloadFilter.FalsePositives, workloadFilter.Negatives)
	average := func(total, count int64) float64 {
		if count == 0 {
			return 0
		}
		return float64(total) / float64(count)
	}
	accessAverageNanos := map[string]float64{
		"read_hot":        average(totalAccess.ReadHotNanos, totalAccess.ReadHot),
		"read_archived":   average(totalAccess.ReadArchivedNanos, totalAccess.ReadArchived),
		"read_missing":    average(totalAccess.ReadMissingNanos, totalAccess.ReadMissing),
		"write_hot":       average(totalAccess.WriteHotNanos, totalAccess.WriteHot),
		"write_archived":  average(totalAccess.WriteArchivedNanos, totalAccess.WriteArchived),
		"write_missing":   average(totalAccess.WriteMissingNanos, totalAccess.WriteMissing),
		"delete_hot":      average(totalAccess.DeleteHotNanos, totalAccess.DeleteHot),
		"delete_archived": average(totalAccess.DeleteArchivedNanos, totalAccess.DeleteArchived),
		"delete_missing":  average(totalAccess.DeleteMissingNanos, totalAccess.DeleteMissing),
	}
	if proofBlockOpen {
		proofBlockTotal++
		if proofBlockAcc > proofBlockMax {
			proofBlockMax = proofBlockAcc
		}
		proofReservoirAdd(&proofBlockReservoir, proofBlockAcc, proofBlockTotal, 1<<19, proofRng)
		proofBlockOpen = false
	}
	// 最后一个窗口已写入 CSV；汇总只补齐尚未结束的区块统计。
	proofRedemptionSorted := sortedInt64Copy(proofRedemptionReservoir)
	proofBlockGlobalSorted := sortedInt64Copy(proofBlockReservoir)
	_, _, proofRedemptionMaxExact, _ := MPTRedemptionProofStats()
	proofVerifyTotalCount, proofVerifyTotalNanos := MPTRedemptionVerifyStats()
	bucketIO := CurrentMPTBucketReadIOStats().Sub(initialBucketIO)
	verificationToExclude := time.Duration(0)
	if *traceStressMPTVerifyAtCommit {
		verificationToExclude = time.Duration(proofVerifyTotalNanos)
	}
	summary := map[string]any{
		"status":                                 "completed",
		"finished_at":                            time.Now().Format(time.RFC3339),
		"elapsed_ms":                             float64(time.Since(started)) / float64(time.Millisecond),
		"operations":                             totalCounts.total(),
		"batches":                                totalBatches,
		"prunes_done":                            prunesDone,
		"archive_round_domains":                  archiveRoundDomains,
		"archive_round_completed":                prunesDone / archiveRoundDomains,
		"archive_round_progress":                 prunesDone % archiveRoundDomains,
		"requested_operations":                   *traceStressOps,
		"first_block":                            selection.firstBlock,
		"last_block":                             selection.lastBlock,
		"block_count":                            actualBlockCount,
		"counts":                                 totalCounts,
		"executed_gets":                          totalGets,
		"executed_puts":                          totalPuts,
		"executed_deletes":                       totalDeletes,
		"parse_ms":                               float64(totalParse) / float64(time.Millisecond),
		"operations_ms":                          float64(totalOpsDur) / float64(time.Millisecond),
		"prune_launch_ms":                        float64(totalPrune) / float64(time.Millisecond),
		"commit_ms":                              float64(totalCommit) / float64(time.Millisecond),
		"db_write_ms":                            float64(totalWrite) / float64(time.Millisecond),
		"measured_ops_per_s":                     opsPerSecond(totalCounts.total(), totalOpsDur+totalCommit+totalWrite-verificationToExclude),
		"measured_ops_excluding_bucket_io_per_s": opsPerSecond(totalCounts.total(), totalOpsDur+totalCommit+totalWrite-verificationToExclude-time.Duration(bucketIO.TotalNanos())),
		"measured_ops_excluding_bucket_io_and_read_promotion_per_s": opsPerSecond(totalCounts.total(), totalOpsDur+totalCommit+totalWrite-verificationToExclude-time.Duration(bucketIO.TotalNanos())-time.Duration(updateDiag.ArchiveReadPromotionNanos)),
		"bucket_read_io":      map[string]any{"meta_reads": bucketIO.MetaReads, "meta_ms": float64(bucketIO.MetaNanos) / float64(time.Millisecond), "payload_reads": bucketIO.PayloadReads, "payload_ms": float64(bucketIO.PayloadNanos) / float64(time.Millisecond)},
		"block_proof":         map[string]any{"blocks": totalSourceBlocks, "unique_buckets": totalUniqueBuckets, "redemptions": totalBlockRedemptions, "unique_proof_bytes": totalUniqueProofBytes},
		"last_root":           common.BytesToHash(lastRoot).Hex(),
		"state_bytes":         finalSystemBytes,
		"full_node_db_bytes":  finalFullNodeDBBytes,
		"archive_db_bytes":    finalArchiveDBBytes,
		"archive_bytes":       LastCommitDiagnostics().MPTArchivePayloadBytes,
		"system_total_bytes":  finalSystemBytes,
		"access_sample":       totalAccess,
		"access_sample_every": *traceStressAccessSampleEvery,
		"access_sample_ms":    float64(totalAccessSample) / float64(time.Millisecond),
		"access_average_ns":   accessAverageNanos,
		"hot_read_hit_rate":   hotReadRate,
		"read_archive_share":  readArchiveShare,
		"archive_filter_runtime": map[string]any{
			"lookups":         workloadFilter.Lookups,
			"negatives":       workloadFilter.Negatives,
			"positives":       workloadFilter.Positives,
			"false_positives": workloadFilter.FalsePositives,
			"fpr":             runtimeFilterFPR,
		},
		"archive_read_promotion": map[string]any{
			"calls":         updateDiag.ArchiveReadPromotionCalls,
			"hits":          updateDiag.ArchiveReadPromotionHits,
			"time_ms":       float64(time.Duration(updateDiag.ArchiveReadPromotionNanos)) / float64(time.Millisecond),
			"in_operations": true,
			"in_commit":     false,
		},
		"redemption_proof": map[string]any{
			"definition":                "bucket payload record + stub side record (ECMH audit payload)",
			"redemptions":               proofLastCount,
			"bytes_total":               proofLastBytes,
			"redemption_p50_bytes":      percentileInt64(proofRedemptionSorted, .50),
			"redemption_p95_bytes":      percentileInt64(proofRedemptionSorted, .95),
			"redemption_p99_bytes":      percentileInt64(proofRedemptionSorted, .99),
			"redemption_max_bytes":      proofRedemptionMaxExact,
			"blocks_with_redemptions":   proofBlockTotal,
			"block_p50_bytes":           percentileInt64(proofBlockGlobalSorted, .50),
			"block_p95_bytes":           percentileInt64(proofBlockGlobalSorted, .95),
			"block_p99_bytes":           percentileInt64(proofBlockGlobalSorted, .99),
			"block_max_bytes":           proofBlockMax,
			"verified_distinct_buckets": proofVerifyTotalCount,
			"verify_ms_total":           float64(proofVerifyTotalNanos) / float64(time.Millisecond),
			"verify_time_scope":         map[bool]string{true: "per-block parallel audit wall time", false: "in-read verification time"}[*traceStressMPTVerifyAtCommit],
		},
		"stub_side_records": map[string]any{
			"staged_bytes": LastCommitDiagnostics().MPTStagedStubMetaBytes,
			"fetches":      LastCommitDiagnostics().MPTStubMetaFetches,
			"fetch_bytes":  LastCommitDiagnostics().MPTStubMetaFetchBytes,
		},
		"stub_node_stubs_max":         LastCommitDiagnostics().MPTStubNodeMax,
		"stub_node_stubs_avg":         traceStressRatio(LastCommitDiagnostics().MPTStubNodeSum, LastCommitDiagnostics().MPTStubNodeEvents),
		"stub_node_events":            LastCommitDiagnostics().MPTStubNodeEvents,
		"mpt_duplicate_archived_keys": MPTDuplicateArchivedKeys(),
	}
	// pathdb 的缓冲区和差分层尚未全部落盘，计入活跃层大小。
	if ps, ok := hotBackend.(interface {
		PathStats() (written, writes, buffered, diff int64)
	}); ok {
		written, writes, buffered, diff := ps.PathStats()
		summary["path_hot_written_bytes"] = written
		summary["path_hot_writes"] = writes
		summary["path_hot_buffered_bytes"] = buffered
		summary["path_hot_diff_bytes"] = diff
		summary["active_layer_bytes_total"] = written + buffered + diff
	}
	if err := writeTraceStressJSON(filepath.Join(resultsDir, "summary.json"), summary); err != nil {
		t.Fatal(err)
	}
	if err := writeTraceStressJSON(filepath.Join(*traceStressBaseDir, "run_status.json"), map[string]any{"status": "completed", "finished_at": time.Now().Format(time.RFC3339), "operations": totalCounts.total()}); err != nil {
		t.Fatal(err)
	}
	fmt.Printf("[TRACE_STRESS] DONE ops=%d batches=%d root=%s full_db=%d archive_db=%d elapsed=%s\n", totalCounts.total(), totalBatches, common.BytesToHash(lastRoot).Hex(), finalFullNodeDBBytes, finalArchiveDBBytes, time.Since(started))
}
