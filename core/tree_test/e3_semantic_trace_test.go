package tree

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/binary"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/ethdb/leveldb"
	"github.com/ethereum/go-ethereum/trie"
	archivetrie "github.com/ethereum/go-ethereum/trie/archive"
	archivempt "github.com/ethereum/go-ethereum/trie/archive/mpt"
	"github.com/ethereum/go-ethereum/trie/trienode"
	"github.com/ethereum/go-ethereum/triedb"
)

var (
	e3InputDir        = flag.String("e3InputDir", "", "Directory of ordered state_access_trace_*.csv or .csv.gz files")
	e3OutputDir       = flag.String("e3OutputDir", "", "Fresh directory for E3 databases and result.json")
	e3Ops             = flag.Int64("e3Ops", 0, "Maximum trace operations; zero replays all selected files")
	e3StartBlock      = flag.Uint64("e3StartBlock", 0, "First source block, inclusive")
	e3EndBlock        = flag.Uint64("e3EndBlock", 0, "Last source block, inclusive; zero means unbounded")
	e3BatchSize       = flag.Int("e3BatchSize", 4000, "Ordered operations per prune/commit batch")
	e3Depth           = flag.Int("e3Depth", 19, "AMT logical domain depth D")
	e3ProgressBatches = flag.Int64("e3ProgressBatches", 250, "Print and save progress every N batches")
)

type e3Store struct{ ethdb.KeyValueStore }

func (s *e3Store) NewBatch() archivetrie.Batcher { return s.KeyValueStore.NewBatch() }

type e3Reader struct {
	files []string
	index int
	file  *os.File
	gz    *gzip.Reader
	csv   *csv.Reader
}

func newE3Reader(dir string) (*e3Reader, error) {
	plain, err := filepath.Glob(filepath.Join(dir, "state_access_trace_*.csv"))
	if err != nil {
		return nil, err
	}
	compressed, err := filepath.Glob(filepath.Join(dir, "state_access_trace_*.csv.gz"))
	if err != nil {
		return nil, err
	}
	files := append(plain, compressed...)
	sort.Strings(files)
	if len(files) == 0 {
		return nil, fmt.Errorf("no state access traces in %s", dir)
	}
	return &e3Reader{files: files}, nil
}

func (r *e3Reader) Close() error {
	var err error
	if r.gz != nil {
		err = errors.Join(err, r.gz.Close())
		r.gz = nil
	}
	if r.file != nil {
		err = errors.Join(err, r.file.Close())
		r.file = nil
	}
	r.csv = nil
	return err
}

func (r *e3Reader) nextFile() error {
	if err := r.Close(); err != nil {
		return err
	}
	if r.index == len(r.files) {
		return io.EOF
	}
	name := r.files[r.index]
	r.index++
	f, err := os.Open(name)
	if err != nil {
		return err
	}
	r.file = f
	var source io.Reader = bufio.NewReaderSize(f, 1<<20)
	if strings.HasSuffix(name, ".gz") {
		r.gz, err = gzip.NewReader(source)
		if err != nil {
			r.Close()
			return err
		}
		source = r.gz
	}
	r.csv = csv.NewReader(bufio.NewReaderSize(source, 1<<20))
	r.csv.ReuseRecord = true
	header, err := r.csv.Read()
	if err != nil {
		r.Close()
		return err
	}
	if strings.Join(header, ",") != traceCompareHeader {
		r.Close()
		return fmt.Errorf("%s: unexpected header %q", name, strings.Join(header, ","))
	}
	return nil
}

func (r *e3Reader) Read() (traceCompareOp, error) {
	for {
		if r.csv == nil {
			if err := r.nextFile(); err != nil {
				return traceCompareOp{}, err
			}
		}
		record, err := r.csv.Read()
		if errors.Is(err, io.EOF) {
			if err := r.Close(); err != nil {
				return traceCompareOp{}, err
			}
			continue
		}
		if err != nil {
			return traceCompareOp{}, fmt.Errorf("%s: %w", r.files[r.index-1], err)
		}
		op, err := parseTraceCompareRecord(record)
		if err != nil {
			return traceCompareOp{}, fmt.Errorf("%s: %w", r.files[r.index-1], err)
		}
		return op, nil
	}
}

type e3Result struct {
	Status             string   `json:"status"`
	StartedAt          string   `json:"started_at"`
	UpdatedAt          string   `json:"updated_at"`
	InputDir           string   `json:"input_dir"`
	SourceFiles        []string `json:"source_files"`
	BatchSize          int      `json:"batch_size"`
	Depth              int      `json:"depth"`
	ArchivePeriodOps   int64    `json:"archive_period_ops"`
	FirstBlock         uint64   `json:"first_block"`
	LastBlock          uint64   `json:"last_block"`
	Batches            int64    `json:"batches"`
	Operations         int64    `json:"operations"`
	Reads              int64    `json:"reads"`
	Writes             int64    `json:"writes"`
	Deletes            int64    `json:"deletes"`
	Mismatches         int64    `json:"mismatches"`
	ArchivedEntries    int64    `json:"archived_entries"`
	LiveArchiveBuckets int64    `json:"live_archive_buckets"`
	Revivals           int64    `json:"revivals"`
	FirstArchiveBlock  uint64   `json:"first_archive_block,omitempty"`
	FirstArchiveBatch  int64    `json:"first_archive_batch,omitempty"`
	FirstRevivalBlock  uint64   `json:"first_revival_block,omitempty"`
	FirstRevivalBatch  int64    `json:"first_revival_batch,omitempty"`
	MPTReadHash        string   `json:"mpt_read_hash"`
	AMTReadHash        string   `json:"amt_read_hash"`
	MPTWriteHash       string   `json:"mpt_write_hash"`
	AMTWriteHash       string   `json:"amt_write_hash"`
	MPTFinalHash       string   `json:"mpt_final_hash,omitempty"`
	AMTFinalHash       string   `json:"amt_final_hash,omitempty"`
	FinalEntries       int64    `json:"final_entries,omitempty"`
	Error              string   `json:"error,omitempty"`
}

func e3HashEvent(h hash.Hash, key, value []byte, kind traceCompareKind) {
	var length [8]byte
	h.Write([]byte{byte(kind)})
	binary.BigEndian.PutUint64(length[:], uint64(len(key)))
	h.Write(length[:])
	h.Write(key)
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	h.Write(length[:])
	h.Write(value)
}

func e3LogicalGet(get func([]byte) ([]byte, error), key []byte) ([]byte, error) {
	value, err := get(key)
	if errors.Is(err, archivempt.ErrNotFound) {
		return nil, nil
	}
	return value, err
}

func e3SaveResult(path string, result *e3Result) error {
	result.UpdatedAt = time.Now().Format(time.RFC3339)
	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(path+".tmp", append(data, '\n'), 0o644); err != nil {
		return err
	}
	return os.Rename(path+".tmp", path)
}

// TestE3StateSemanticEquivalence 对同一轨迹逐批比较 AMT 与原生 MPT 的状态语义。
func TestE3StateSemanticEquivalence(t *testing.T) {
	if *e3InputDir == "" {
		t.Skip("set -e3InputDir to run E3")
	}
	if *e3OutputDir == "" {
		t.Fatal("set -e3OutputDir to a fresh directory")
	}
	if *e3BatchSize <= 0 || *e3Depth < 1 || *e3Depth > 30 || *e3Ops < 0 || *e3ProgressBatches <= 0 {
		t.Fatal("invalid E3 batch size, depth, operation limit, or progress interval")
	}
	if *e3EndBlock > 0 && *e3StartBlock > *e3EndBlock {
		t.Fatal("e3StartBlock exceeds e3EndBlock")
	}
	if _, err := os.Stat(*e3OutputDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("E3 output directory must not exist: %s (%v)", *e3OutputDir, err)
	}
	if err := os.MkdirAll(*e3OutputDir, 0o755); err != nil {
		t.Fatal(err)
	}
	reader, err := newE3Reader(*e3InputDir)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	result := &e3Result{Status: "running", StartedAt: time.Now().Format(time.RFC3339), InputDir: *e3InputDir, SourceFiles: reader.files, BatchSize: *e3BatchSize, Depth: *e3Depth, ArchivePeriodOps: int64(1) << uint(*e3Depth) * int64(*e3BatchSize)}
	resultPath := filepath.Join(*e3OutputDir, "result.json")
	defer func() {
		if t.Failed() && result.Status == "running" {
			result.Status = "failed"
		}
		if err := e3SaveResult(resultPath, result); err != nil {
			t.Errorf("save E3 result: %v", err)
		}
	}()

	mptKV, err := leveldb.New(filepath.Join(*e3OutputDir, "mpt_db"), 512, 256, "e3-mpt", false)
	if err != nil {
		t.Fatal(err)
	}
	defer mptKV.Close()
	mptDB := rawdb.NewDatabase(mptKV)
	cache := core.DefaultCacheConfigWithScheme(rawdb.PathScheme)
	cache.SnapshotLimit = 0
	previousLayers := common.VerkleLayerCount
	common.VerkleLayerCount = 0
	defer func() { common.VerkleLayerCount = previousLayers }()
	trieDB := triedb.NewDatabase(mptDB, cache.TriedbConfig(false))
	defer trieDB.Close()
	root := types.EmptyRootHash
	baseline, err := trie.New(trie.TrieID(root), trieDB)
	if err != nil {
		t.Fatal(err)
	}
	amtKV, err := leveldb.New(filepath.Join(*e3OutputDir, "amt_db"), 512, 256, "e3-amt", false)
	if err != nil {
		t.Fatal(err)
	}
	defer amtKV.Close()
	amt, err := archivempt.New(nil, &e3Store{amtKV}, &archivempt.Config{ShardDepthBits: *e3Depth, Backend: archivempt.BackendPath, ActivateArchivedKeyOnRead: true})
	if err != nil {
		t.Fatal(err)
	}
	archivetrie.ResetCommitDiagnostics()
	proofBase, _, _, _ := archivetrie.MPTRedemptionProofStats()
	readMPT, readAMT, writeMPT, writeAMT := sha256.New(), sha256.New(), sha256.New(), sha256.New()
	defer func() {
		result.MPTReadHash = hex.EncodeToString(readMPT.Sum(nil))
		result.AMTReadHash = hex.EncodeToString(readAMT.Sum(nil))
		result.MPTWriteHash = hex.EncodeToString(writeMPT.Sum(nil))
		result.AMTWriteHash = hex.EncodeToString(writeAMT.Sum(nil))
	}()
	for batch := int64(1); ; batch++ {
		ops := make([]traceCompareOp, 0, *e3BatchSize)
		end := false
		for len(ops) < *e3BatchSize {
			if *e3Ops > 0 && result.Operations+int64(len(ops)) >= *e3Ops {
				end = true
				break
			}
			op, readErr := reader.Read()
			if errors.Is(readErr, io.EOF) {
				end = true
				break
			}
			if readErr != nil {
				result.Error = readErr.Error()
				t.Fatal(readErr)
			}
			if op.block < *e3StartBlock {
				continue
			}
			if *e3EndBlock > 0 && op.block > *e3EndBlock {
				end = true
				break
			}
			ops = append(ops, op)
		}
		if len(ops) == 0 {
			break
		}
		if result.Operations == 0 {
			result.FirstBlock = ops[0].block
		}
		result.LastBlock = ops[len(ops)-1].block
		result.Batches = batch
		// 每批先推进剪枝，再逐项比对状态读取结果。
		if err := amt.PruneNextShard(); err != nil {
			result.Error = err.Error()
			t.Fatalf("batch %d prune: %v", batch, err)
		}
		if _, err := amt.Commit(); err != nil {
			result.Error = err.Error()
			t.Fatalf("batch %d intermediate AMT commit: %v", batch, err)
		}
		pruned := archivetrie.LastCommitDiagnostics().MPTPruneExtracted
		if pruned > result.ArchivedEntries {
			if result.FirstArchiveBatch == 0 {
				result.FirstArchiveBatch = batch
				result.FirstArchiveBlock = ops[0].block
			}
			result.ArchivedEntries = pruned
		}
		for _, op := range ops {
			switch op.kind {
			case traceCompareGet:
				mv, me := baseline.Get(op.key)
				av, ae := e3LogicalGet(amt.Get, op.key)
				if me != nil || ae != nil {
					result.Error = fmt.Sprintf("read errors mpt=%v amt=%v", me, ae)
					t.Fatalf("batch %d block %d: %s", batch, op.block, result.Error)
				}
				e3HashEvent(readMPT, op.key, mv, op.kind)
				e3HashEvent(readAMT, op.key, av, op.kind)
				result.Reads++
				if !bytes.Equal(mv, av) {
					result.Mismatches++
					result.Error = fmt.Sprintf("read mismatch block=%d key=%x mpt=%x amt=%x", op.block, op.key, mv, av)
					t.Fatal(result.Error)
				}
			case traceComparePut, traceCompareDelete:
				if op.kind == traceComparePut {
					err = baseline.Update(op.key, op.value)
				} else {
					err = baseline.Delete(op.key)
				}
				if err != nil {
					result.Error = err.Error()
					t.Fatal(err)
				}
				if op.kind == traceComparePut {
					err = amt.Put(op.key, op.value)
					result.Writes++
				} else {
					err = amt.Delete(op.key)
					result.Deletes++
				}
				if err != nil {
					result.Error = err.Error()
					t.Fatal(err)
				}
				amt.SetActivateArchivedKeyOnRead(false)
				mv, me := baseline.Get(op.key)
				av, ae := e3LogicalGet(amt.Get, op.key)
				amt.SetActivateArchivedKeyOnRead(true)
				if me != nil || ae != nil {
					result.Error = fmt.Sprintf("post-write errors mpt=%v amt=%v", me, ae)
					t.Fatal(result.Error)
				}
				e3HashEvent(writeMPT, op.key, mv, op.kind)
				e3HashEvent(writeAMT, op.key, av, op.kind)
				if !bytes.Equal(mv, av) {
					result.Mismatches++
					result.Error = fmt.Sprintf("write mismatch block=%d key=%x mpt=%x amt=%x", op.block, op.key, mv, av)
					t.Fatal(result.Error)
				}
			}
			result.Operations++
			proofs, _, _, _ := archivetrie.MPTRedemptionProofStats()
			if proofs-proofBase > result.Revivals {
				if result.FirstRevivalBatch == 0 {
					result.FirstRevivalBatch = batch
					result.FirstRevivalBlock = op.block
				}
				result.Revivals = proofs - proofBase
			}
		}
		previousRoot := root
		var nodes *trienode.NodeSet
		root, nodes = baseline.Commit(false)
		if nodes != nil && root != previousRoot {
			if err := trieDB.Update(root, previousRoot, uint64(batch), trienode.NewWithNodeSet(nodes), triedb.NewStateSet()); err != nil {
				result.Error = fmt.Sprintf("batch %d root %x parent %x: %v", batch, root, previousRoot, err)
				t.Fatal(result.Error)
			}
			if err := trieDB.Commit(root, false); err != nil {
				result.Error = err.Error()
				t.Fatal(err)
			}
		}
		baseline, err = trie.New(trie.TrieID(root), trieDB)
		if err != nil {
			result.Error = err.Error()
			t.Fatal(err)
		}
		if _, err = amt.Commit(); err != nil {
			result.Error = err.Error()
			t.Fatal(err)
		}
		result.LiveArchiveBuckets = archivetrie.LastCommitDiagnostics().MPTLiveBuckets
		if batch%*e3ProgressBatches == 0 || end {
			if err := e3SaveResult(resultPath, result); err != nil {
				t.Fatal(err)
			}
			fmt.Printf("[E3] batches=%d ops=%d block=%d archived=%d revived=%d mismatches=%d\n", batch, result.Operations, result.LastBlock, result.ArchivedEntries, result.Revivals, result.Mismatches)
		}
		if end {
			break
		}
	}
	if result.Operations == 0 {
		t.Fatal("no trace operations selected")
	}
	if *e3Ops > 0 && result.Operations != *e3Ops {
		result.Error = fmt.Sprintf("trace ended after %d operations; requested %d", result.Operations, *e3Ops)
		t.Fatal(result.Error)
	}
	it, err := baseline.NodeIterator(nil)
	if err != nil {
		result.Error = err.Error()
		t.Fatal(err)
	}
	entries := trie.NewIterator(it)
	mh, ah := sha256.New(), sha256.New()
	var mismatch string
	var mismatchKey []byte
	if err := amt.ForEach(func(key, value []byte) bool {
		if !entries.Next() {
			mismatch = fmt.Sprintf("AMT has extra key %x", key)
			return false
		}
		e3HashEvent(mh, entries.Key, entries.Value, traceComparePut)
		e3HashEvent(ah, key, value, traceComparePut)
		result.FinalEntries++
		if !bytes.Equal(entries.Key, key) || !bytes.Equal(entries.Value, value) {
			mismatchKey = bytes.Clone(key)
			mismatch = fmt.Sprintf("final mismatch at entry %d: mpt %x=%x amt %x=%x", result.FinalEntries, entries.Key, entries.Value, key, value)
			return false
		}
		return true
	}); err != nil {
		result.Error = err.Error()
		t.Fatal(err)
	}
	if mismatch == "" && entries.Next() {
		mismatch = fmt.Sprintf("MPT has extra key %x", entries.Key)
	}
	if entries.Err != nil {
		mismatch = fmt.Sprintf("MPT iterator: %v", entries.Err)
	}
	result.MPTFinalHash = hex.EncodeToString(mh.Sum(nil))
	result.AMTFinalHash = hex.EncodeToString(ah.Sum(nil))
	if mismatch != "" {
		if mismatchKey != nil {
			amt.SetActivateArchivedKeyOnRead(false)
			logical, getErr := e3LogicalGet(amt.Get, mismatchKey)
			mismatch += fmt.Sprintf("; AMT.Get=%x get_err=%v", logical, getErr)
		}
		result.Mismatches++
		result.Error = mismatch
		t.Fatal(mismatch)
	}
	result.Status = "passed"
}
