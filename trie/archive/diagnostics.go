package archive

import (
	"sync/atomic"
	"time"
)

type CommitDiagnostics struct {
	MPTStagedHotNodeBytes     int64
	MPTStagedArchiveBytes     int64
	MPTStagedIndexBytes       int64
	MPTStagedStubMetaBytes    int64
	MPTStubMetaFetches        int64
	MPTStubMetaFetchBytes     int64
	MPTStubNodeMax            int64
	MPTStubNodeSum            int64
	MPTStubNodeEvents         int64
	MPTNodeCacheGets          int64
	MPTNodeCacheHits          int64
	MPTPruneExtracted         int64
	MPTPruneResolvedNodes     int64
	MPTPruneExtractNanos      int64
	MPTPruneMountNanos        int64
	MPTArchivePayloadBytes    int64
	MPTLiveHotLeaves          int64
	MPTLiveBuckets            int64
	MPTCommitHxNanos          int64
	MPTCommitStageNanos       int64
	MPTCommitTdbCommitNanos   int64
	MPTCommitArchiveLoopNanos int64
}

type UpdateDiagnostics struct {
	ArchiveReadPromotionCalls   int64
	ArchiveReadPromotionHits    int64
	ArchiveReadPromotionNanos   int64
	ArchiveFilterLookups        int64
	ArchiveFilterNegatives      int64
	ArchiveFilterPositives      int64
	ArchiveFilterFalsePositives int64
}

func (d UpdateDiagnostics) Sub(previous UpdateDiagnostics) UpdateDiagnostics {
	return UpdateDiagnostics{
		ArchiveReadPromotionCalls:   d.ArchiveReadPromotionCalls - previous.ArchiveReadPromotionCalls,
		ArchiveReadPromotionHits:    d.ArchiveReadPromotionHits - previous.ArchiveReadPromotionHits,
		ArchiveReadPromotionNanos:   d.ArchiveReadPromotionNanos - previous.ArchiveReadPromotionNanos,
		ArchiveFilterLookups:        d.ArchiveFilterLookups - previous.ArchiveFilterLookups,
		ArchiveFilterNegatives:      d.ArchiveFilterNegatives - previous.ArchiveFilterNegatives,
		ArchiveFilterPositives:      d.ArchiveFilterPositives - previous.ArchiveFilterPositives,
		ArchiveFilterFalsePositives: d.ArchiveFilterFalsePositives - previous.ArchiveFilterFalsePositives,
	}
}

var (
	updateArchiveReadPromotionCalls     int64
	updateArchiveReadPromotionHits      int64
	updateArchiveReadPromotionNanos     int64
	updateArchiveFilterLookups          int64
	updateArchiveFilterNegatives        int64
	updateArchiveFilterPositives        int64
	updateArchiveFilterFalsePositives   int64
	commitDiagMPTStagedHotNodeBytes     int64
	commitDiagMPTStagedArchiveBytes     int64
	commitDiagMPTStagedIndexBytes       int64
	commitDiagMPTStagedStubMetaBytes    int64
	commitDiagMPTStubMetaFetches        int64
	commitDiagMPTStubMetaFetchBytes     int64
	commitDiagMPTStubNodeMax            int64
	commitDiagMPTStubNodeSum            int64
	commitDiagMPTStubNodeEvents         int64
	commitDiagMPTNodeCacheGets          int64
	commitDiagMPTNodeCacheHits          int64
	commitDiagMPTPruneExtracted         int64
	commitDiagMPTPruneResolvedNodes     int64
	commitDiagMPTPruneExtractNanos      int64
	commitDiagMPTPruneMountNanos        int64
	commitDiagMPTArchivePayloadBytes    int64
	commitDiagMPTLiveHotLeaves          int64
	commitDiagMPTLiveBuckets            int64
	commitDiagMPTCommitHxNanos          int64
	commitDiagMPTCommitStageNanos       int64
	commitDiagMPTCommitTdbCommitNanos   int64
	commitDiagMPTCommitArchiveLoopNanos int64
)

func LastUpdateDiagnostics() UpdateDiagnostics {
	return UpdateDiagnostics{
		ArchiveReadPromotionCalls:   atomic.LoadInt64(&updateArchiveReadPromotionCalls),
		ArchiveReadPromotionHits:    atomic.LoadInt64(&updateArchiveReadPromotionHits),
		ArchiveReadPromotionNanos:   atomic.LoadInt64(&updateArchiveReadPromotionNanos),
		ArchiveFilterLookups:        atomic.LoadInt64(&updateArchiveFilterLookups),
		ArchiveFilterNegatives:      atomic.LoadInt64(&updateArchiveFilterNegatives),
		ArchiveFilterPositives:      atomic.LoadInt64(&updateArchiveFilterPositives),
		ArchiveFilterFalsePositives: atomic.LoadInt64(&updateArchiveFilterFalsePositives),
	}
}

func ResetCommitDiagnostics() {
	atomic.StoreInt64(&commitDiagMPTStagedHotNodeBytes, 0)
	atomic.StoreInt64(&commitDiagMPTStagedArchiveBytes, 0)
	atomic.StoreInt64(&commitDiagMPTStagedIndexBytes, 0)
	atomic.StoreInt64(&commitDiagMPTStagedStubMetaBytes, 0)
	atomic.StoreInt64(&commitDiagMPTStubMetaFetches, 0)
	atomic.StoreInt64(&commitDiagMPTStubMetaFetchBytes, 0)
	atomic.StoreInt64(&commitDiagMPTStubNodeMax, 0)
	atomic.StoreInt64(&commitDiagMPTStubNodeSum, 0)
	atomic.StoreInt64(&commitDiagMPTStubNodeEvents, 0)
	atomic.StoreInt64(&commitDiagMPTNodeCacheGets, 0)
	atomic.StoreInt64(&commitDiagMPTNodeCacheHits, 0)
	atomic.StoreInt64(&commitDiagMPTPruneExtracted, 0)
	atomic.StoreInt64(&commitDiagMPTPruneResolvedNodes, 0)
	atomic.StoreInt64(&commitDiagMPTPruneExtractNanos, 0)
	atomic.StoreInt64(&commitDiagMPTPruneMountNanos, 0)
	atomic.StoreInt64(&commitDiagMPTArchivePayloadBytes, 0)
	atomic.StoreInt64(&commitDiagMPTLiveHotLeaves, 0)
	atomic.StoreInt64(&commitDiagMPTLiveBuckets, 0)
	atomic.StoreInt64(&commitDiagMPTCommitHxNanos, 0)
	atomic.StoreInt64(&commitDiagMPTCommitStageNanos, 0)
	atomic.StoreInt64(&commitDiagMPTCommitTdbCommitNanos, 0)
	atomic.StoreInt64(&commitDiagMPTCommitArchiveLoopNanos, 0)
}

func LastCommitDiagnostics() CommitDiagnostics {
	return CommitDiagnostics{
		MPTStagedHotNodeBytes:     atomic.LoadInt64(&commitDiagMPTStagedHotNodeBytes),
		MPTStagedArchiveBytes:     atomic.LoadInt64(&commitDiagMPTStagedArchiveBytes),
		MPTStagedIndexBytes:       atomic.LoadInt64(&commitDiagMPTStagedIndexBytes),
		MPTStagedStubMetaBytes:    atomic.LoadInt64(&commitDiagMPTStagedStubMetaBytes),
		MPTStubMetaFetches:        atomic.LoadInt64(&commitDiagMPTStubMetaFetches),
		MPTStubMetaFetchBytes:     atomic.LoadInt64(&commitDiagMPTStubMetaFetchBytes),
		MPTStubNodeMax:            atomic.LoadInt64(&commitDiagMPTStubNodeMax),
		MPTStubNodeSum:            atomic.LoadInt64(&commitDiagMPTStubNodeSum),
		MPTStubNodeEvents:         atomic.LoadInt64(&commitDiagMPTStubNodeEvents),
		MPTNodeCacheGets:          atomic.LoadInt64(&commitDiagMPTNodeCacheGets),
		MPTNodeCacheHits:          atomic.LoadInt64(&commitDiagMPTNodeCacheHits),
		MPTPruneExtracted:         atomic.LoadInt64(&commitDiagMPTPruneExtracted),
		MPTPruneResolvedNodes:     atomic.LoadInt64(&commitDiagMPTPruneResolvedNodes),
		MPTPruneExtractNanos:      atomic.LoadInt64(&commitDiagMPTPruneExtractNanos),
		MPTPruneMountNanos:        atomic.LoadInt64(&commitDiagMPTPruneMountNanos),
		MPTArchivePayloadBytes:    atomic.LoadInt64(&commitDiagMPTArchivePayloadBytes),
		MPTLiveHotLeaves:          atomic.LoadInt64(&commitDiagMPTLiveHotLeaves),
		MPTLiveBuckets:            atomic.LoadInt64(&commitDiagMPTLiveBuckets),
		MPTCommitHxNanos:          atomic.LoadInt64(&commitDiagMPTCommitHxNanos),
		MPTCommitStageNanos:       atomic.LoadInt64(&commitDiagMPTCommitStageNanos),
		MPTCommitTdbCommitNanos:   atomic.LoadInt64(&commitDiagMPTCommitTdbCommitNanos),
		MPTCommitArchiveLoopNanos: atomic.LoadInt64(&commitDiagMPTCommitArchiveLoopNanos),
	}
}

func SetMPTCommitStagedBytes(hotNode, archive, index, stubMeta int64) {
	atomic.AddInt64(&commitDiagMPTStagedHotNodeBytes, hotNode)
	atomic.AddInt64(&commitDiagMPTStagedArchiveBytes, archive)
	atomic.AddInt64(&commitDiagMPTStagedIndexBytes, index)
	atomic.AddInt64(&commitDiagMPTStagedStubMetaBytes, stubMeta)
}

func RecordMPTStubMetaFetch(recordBytes int64) {
	atomic.AddInt64(&commitDiagMPTStubMetaFetches, 1)
	atomic.AddInt64(&commitDiagMPTStubMetaFetchBytes, recordBytes)
}

func RecordMPTNodeStubCount(count int) {
	atomic.AddInt64(&commitDiagMPTStubNodeEvents, 1)
	atomic.AddInt64(&commitDiagMPTStubNodeSum, int64(count))
	for {
		cur := atomic.LoadInt64(&commitDiagMPTStubNodeMax)
		if int64(count) <= cur || atomic.CompareAndSwapInt64(&commitDiagMPTStubNodeMax, cur, int64(count)) {
			return
		}
	}
}

func SetMPTPruneDiagnostics(extracted, resolvedNodes, extractNanos, mountNanos int64) {
	atomic.AddInt64(&commitDiagMPTPruneExtracted, extracted)
	atomic.AddInt64(&commitDiagMPTPruneResolvedNodes, resolvedNodes)
	atomic.AddInt64(&commitDiagMPTPruneExtractNanos, extractNanos)
	atomic.AddInt64(&commitDiagMPTPruneMountNanos, mountNanos)
}

func SetMPTArchivePayloadBytes(v int64) {
	atomic.StoreInt64(&commitDiagMPTArchivePayloadBytes, v)
}

func SetMPTLiveGauges(liveHotLeaves, liveBuckets int64) {
	atomic.StoreInt64(&commitDiagMPTLiveHotLeaves, liveHotLeaves)
	atomic.StoreInt64(&commitDiagMPTLiveBuckets, liveBuckets)
}

func SetMPTCommitPhaseNanos(hxCommit, stageNodes, tdbCommit, archiveLoop int64) {
	atomic.AddInt64(&commitDiagMPTCommitHxNanos, hxCommit)
	atomic.AddInt64(&commitDiagMPTCommitStageNanos, stageNodes)
	atomic.AddInt64(&commitDiagMPTCommitTdbCommitNanos, tdbCommit)
	atomic.AddInt64(&commitDiagMPTCommitArchiveLoopNanos, archiveLoop)
}

func SetMPTNodeCacheStats(gets, hits int64) {
	atomic.StoreInt64(&commitDiagMPTNodeCacheGets, gets)
	atomic.StoreInt64(&commitDiagMPTNodeCacheHits, hits)
}

var TraceMPTBucketCapacity int

func RecordMPTReadPromotion(elapsed time.Duration, hit bool) {
	atomic.AddInt64(&updateArchiveReadPromotionCalls, 1)
	atomic.AddInt64(&updateArchiveReadPromotionNanos, elapsed.Nanoseconds())
	if hit {
		atomic.AddInt64(&updateArchiveReadPromotionHits, 1)
	}
}

// MPTBucketReadIOStats 统计归档查询中的桶元数据和载荷读取耗时。
type MPTBucketReadIOStats struct {
	MetaReads, MetaNanos       int64
	PayloadReads, PayloadNanos int64
}

func (s MPTBucketReadIOStats) Sub(previous MPTBucketReadIOStats) MPTBucketReadIOStats {
	return MPTBucketReadIOStats{
		MetaReads: s.MetaReads - previous.MetaReads, MetaNanos: s.MetaNanos - previous.MetaNanos,
		PayloadReads: s.PayloadReads - previous.PayloadReads, PayloadNanos: s.PayloadNanos - previous.PayloadNanos,
	}
}

func (s MPTBucketReadIOStats) TotalNanos() int64 { return s.MetaNanos + s.PayloadNanos }

var (
	mptBucketMetaReads, mptBucketMetaNanos       int64
	mptBucketPayloadReads, mptBucketPayloadNanos int64
)

func RecordMPTBucketReadIO(metaReads, metaNanos, payloadReads, payloadNanos int64) {
	if metaReads != 0 {
		atomic.AddInt64(&mptBucketMetaReads, metaReads)
		atomic.AddInt64(&mptBucketMetaNanos, metaNanos)
	}
	if payloadReads != 0 {
		atomic.AddInt64(&mptBucketPayloadReads, payloadReads)
		atomic.AddInt64(&mptBucketPayloadNanos, payloadNanos)
	}
}

func CurrentMPTBucketReadIOStats() MPTBucketReadIOStats {
	return MPTBucketReadIOStats{
		MetaReads: atomic.LoadInt64(&mptBucketMetaReads), MetaNanos: atomic.LoadInt64(&mptBucketMetaNanos),
		PayloadReads: atomic.LoadInt64(&mptBucketPayloadReads), PayloadNanos: atomic.LoadInt64(&mptBucketPayloadNanos),
	}
}

var (
	redemptionProofCount  int64
	redemptionProofBytes  int64
	redemptionProofMax    int64
	redemptionProofLast   int64
	redemptionVerifyCount int64
	redemptionVerifyNanos int64
)

func RecordMPTRedemptionProof(proofBytes int64) {
	atomic.StoreInt64(&redemptionProofLast, proofBytes)
	atomic.AddInt64(&redemptionProofCount, 1)
	atomic.AddInt64(&redemptionProofBytes, proofBytes)
	for {
		cur := atomic.LoadInt64(&redemptionProofMax)
		if proofBytes <= cur || atomic.CompareAndSwapInt64(&redemptionProofMax, cur, proofBytes) {
			return
		}
	}
}

func MPTRedemptionProofStats() (count, bytes, max, last int64) {
	return atomic.LoadInt64(&redemptionProofCount),
		atomic.LoadInt64(&redemptionProofBytes),
		atomic.LoadInt64(&redemptionProofMax),
		atomic.LoadInt64(&redemptionProofLast)
}

func RecordMPTRedemptionVerify(elapsed time.Duration) {
	atomic.AddInt64(&redemptionVerifyCount, 1)
	atomic.AddInt64(&redemptionVerifyNanos, elapsed.Nanoseconds())
}

// RecordMPTRedemptionVerifyBatch 记录提交前验证的不同归档桶数量与总耗时。
func RecordMPTRedemptionVerifyBatch(count int64, elapsed time.Duration) {
	atomic.AddInt64(&redemptionVerifyCount, count)
	atomic.AddInt64(&redemptionVerifyNanos, elapsed.Nanoseconds())
}

func MPTRedemptionVerifyStats() (count, nanos int64) {
	return atomic.LoadInt64(&redemptionVerifyCount), atomic.LoadInt64(&redemptionVerifyNanos)
}

func RecordMPTFilterFalsePositive() {
	atomic.AddInt64(&updateArchiveFilterFalsePositives, 1)
}

func RecordMPTFilterLookup(positive bool) {
	atomic.AddInt64(&updateArchiveFilterLookups, 1)
	if positive {
		atomic.AddInt64(&updateArchiveFilterPositives, 1)
	} else {
		atomic.AddInt64(&updateArchiveFilterNegatives, 1)
	}
}

var (
	resurrectionProofSizeHisto     []int64
	resurrectionProofLatencyHisto  []int64
	resurrectionProofHistoSeq      uint64
	resurrectionProofHistoMaxSize  int64
	resurrectionProofHistoMaxNanos int64
	filterProbeLatencyHisto        []int64
	filterProbeHistoSeq            uint64
	filterProbeHistoMaxNanos       int64
)

func RecordMPTRedemptionProofHistogram(proofBytes, latencyNanos int64) {
	resurrectionProofSizeHisto = append(resurrectionProofSizeHisto, proofBytes)
	resurrectionProofLatencyHisto = append(resurrectionProofLatencyHisto, latencyNanos)
	atomic.AddUint64(&resurrectionProofHistoSeq, 1)
	if proofBytes > atomic.LoadInt64(&resurrectionProofHistoMaxSize) {
		atomic.StoreInt64(&resurrectionProofHistoMaxSize, proofBytes)
	}
	if latencyNanos > atomic.LoadInt64(&resurrectionProofHistoMaxNanos) {
		atomic.StoreInt64(&resurrectionProofHistoMaxNanos, latencyNanos)
	}
}

func TakeMPTRedemptionProofHistogram() (sizes, latencies []int64, maxSize, maxNanos int64, seq uint64) {
	sizes, latencies = resurrectionProofSizeHisto, resurrectionProofLatencyHisto
	resurrectionProofSizeHisto, resurrectionProofLatencyHisto = nil, nil
	return sizes, latencies,
		atomic.LoadInt64(&resurrectionProofHistoMaxSize),
		atomic.LoadInt64(&resurrectionProofHistoMaxNanos),
		atomic.LoadUint64(&resurrectionProofHistoSeq)
}

func RecordMPTFilterProbeLatency(nanos int64) {
	filterProbeLatencyHisto = append(filterProbeLatencyHisto, nanos)
	atomic.AddUint64(&filterProbeHistoSeq, 1)
	if nanos > atomic.LoadInt64(&filterProbeHistoMaxNanos) {
		atomic.StoreInt64(&filterProbeHistoMaxNanos, nanos)
	}
}

func TakeMPTFilterProbeLatencyHistogram() (latencies []int64, maxNanos int64, seq uint64) {
	latencies = filterProbeLatencyHisto
	filterProbeLatencyHisto = nil
	return latencies, atomic.LoadInt64(&filterProbeHistoMaxNanos), atomic.LoadUint64(&filterProbeHistoSeq)
}

var mptDuplicateArchivedKeys int64

func RecordMPTDuplicateArchivedKey() {
	atomic.AddInt64(&mptDuplicateArchivedKeys, 1)
}

func MPTDuplicateArchivedKeys() int64 {
	return atomic.LoadInt64(&mptDuplicateArchivedKeys)
}
