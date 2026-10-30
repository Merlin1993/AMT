package archive_test

import (
	"errors"

	"github.com/ethereum/go-ethereum/trie/archive"
	"github.com/ethereum/go-ethereum/trie/archive/mpt"
)

type mptHotLayer struct {
	trie *mpt.Trie
}

func (h mptHotLayer) Get(key []byte) ([]byte, error) {
	value, err := h.trie.Get(key)
	if errors.Is(err, mpt.ErrNotFound) {
		return nil, archive.ErrNodeNotFound
	}
	return value, err
}

func (h mptHotLayer) Put(key, value []byte) error { return h.trie.Put(key, value) }

func (h mptHotLayer) Delete(key []byte) error { return h.trie.Delete(key) }

func (h mptHotLayer) GetValueRef(key []byte) ([]byte, bool, error) {
	ref, fromArchive, err := h.trie.GetValueRef(key)
	if errors.Is(err, mpt.ErrNotFound) {
		return nil, false, archive.ErrNodeNotFound
	}
	return ref, fromArchive, err
}

func (h mptHotLayer) PruneNextShard() error { return h.trie.PruneNextShard() }

func (h mptHotLayer) CommitToBatch(batch archive.Batcher, destructive bool) ([]byte, error) {
	return h.trie.CommitToBatch(batch, destructive)
}

func (h mptHotLayer) Flush() error { return h.trie.Flush() }

func (h mptHotLayer) FlushDurable() error { return h.trie.FlushDurable() }

func (h mptHotLayer) PathStats() (written, writes, buffered, diff int64) {
	return h.trie.PathStats()
}

func (h mptHotLayer) BucketOccupancy() ([]int64, int64) {
	return h.trie.BucketOccupancy()
}

func (h mptHotLayer) PendingProofStats() (int64, int64) {
	return h.trie.PendingProofStats()
}

func init() {
	var _ interface {
		BucketOccupancy() ([]int64, int64)
	} = mptHotLayer{}
	archive.TraceMPTBucketCapacity = mpt.DefaultBucketCapacity
	archive.TraceHotTrieNew = func(spec archive.TraceHotTrieSpec) (archive.TraceHotTrie, error) {
		trie, err := mpt.New(nil, spec.DB, &mpt.Config{
			ShardDepthBits:            spec.ShardDepthBits,
			CuckooBuckets:             spec.CuckooBuckets,
			CuckooSlots:               spec.CuckooSlots,
			ActivateArchivedKeyOnRead: spec.ActivateArchivedKeyOnRead,
			Backend:                   spec.Backend,
			CleanCacheBytes:           spec.CleanCacheBytes,
			WriteBufferBytes:          spec.WriteBufferBytes,
			ForceCommitEveryBatches:   spec.FlushEveryBatches,
			ArchiveResidentEntries:    spec.ArchiveResidentEntries,
			AsyncPrune:                spec.AsyncPrune,
			VerifyAtCommit:            spec.VerifyAtCommit,
			VerifyBucketWorkers:       spec.VerifyBucketWorkers,
			VerifyItemWorkers:         spec.VerifyItemWorkers,
		})
		if err != nil {
			return nil, err
		}
		return mptHotLayer{trie: trie}, nil
	}
}
