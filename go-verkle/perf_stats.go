package verkle

import (
	"fmt"
	"sync"
	"time"
)

// PerfStats 性能统计结构
type PerfStats struct {
	// 总体统计
	TotalInsertCalls int64
	TotalTime        time.Duration

	// 各操作统计
	ResolveNodeTime       time.Duration
	ResolveNodeCount      int64
	NodeTypeCheckTime     time.Duration
	NodeTypeCheckCount    int64
	CreateInternalTime    time.Duration
	CreateInternalCount   int64
	CreateLeafTime        time.Duration
	CreateLeafCount       int64
	InsertMultipleTime    time.Duration
	InsertMultipleCount   int64
	UpdateMultipleTime    time.Duration
	UpdateMultipleCount   int64
	RecursiveCallTime     time.Duration
	RecursiveCallCount    int64
	CowOperationTime      time.Duration
	CowOperationCount     int64
	CommitmentUpdateTime  time.Duration
	CommitmentUpdateCount int64

	// NewLeafNode 相关统计
	NewLeafNodeTime       time.Duration
	NewLeafNodeCount      int64
	FillSuffixTreeTime    time.Duration
	FillSuffixTreeCount   int64
	CommitToPolyTime      time.Duration
	CommitToPolyCount     int64
	BatchMapToScalarTime  time.Duration
	BatchMapToScalarCount int64
	StemProcessingTime    time.Duration
	StemProcessingCount   int64
}

var (
	// 全局性能统计实例
	globalPerfStats PerfStats
	// 保护统计数据的互斥锁
	statsMutex sync.RWMutex
)

// ResetPerfStats 重置所有性能统计数据
func ResetPerfStats() {
	statsMutex.Lock()
	defer statsMutex.Unlock()

	globalPerfStats = PerfStats{}
}

// PrintPerfStats 打印性能统计日志
func PrintPerfStats() {
	statsMutex.RLock()
	defer statsMutex.RUnlock()

	if globalPerfStats.TotalInsertCalls == 0 {
		fmt.Println("=== Verkle Tree Insert Performance Stats ===")
		fmt.Println("No insert operations recorded.")
		return
	}

	totalTime := globalPerfStats.ResolveNodeTime +
		globalPerfStats.NodeTypeCheckTime +
		globalPerfStats.CreateInternalTime +
		globalPerfStats.CreateLeafTime +
		globalPerfStats.InsertMultipleTime +
		globalPerfStats.UpdateMultipleTime +
		globalPerfStats.RecursiveCallTime +
		globalPerfStats.CowOperationTime +
		globalPerfStats.CommitmentUpdateTime

	fmt.Println("=== Verkle Tree Insert Performance Stats ===")
	fmt.Printf("Total Insert Calls: %d\n", globalPerfStats.TotalInsertCalls)
	fmt.Printf("Total Time: %v\n", totalTime)
	fmt.Printf("Average Time per Insert: %v\n", totalTime/time.Duration(globalPerfStats.TotalInsertCalls))
	fmt.Println()

	fmt.Println("Operation Breakdown:")
	printOperationStats("Resolve Node", globalPerfStats.ResolveNodeTime, globalPerfStats.ResolveNodeCount, totalTime)
	printOperationStats("Node Type Check", globalPerfStats.NodeTypeCheckTime, globalPerfStats.NodeTypeCheckCount, totalTime)
	printOperationStats("Create Internal", globalPerfStats.CreateInternalTime, globalPerfStats.CreateInternalCount, totalTime)
	printOperationStats("Create Leaf", globalPerfStats.CreateLeafTime, globalPerfStats.CreateLeafCount, totalTime)
	printOperationStats("Insert Multiple", globalPerfStats.InsertMultipleTime, globalPerfStats.InsertMultipleCount, totalTime)
	printOperationStats("Update Multiple", globalPerfStats.UpdateMultipleTime, globalPerfStats.UpdateMultipleCount, totalTime)
	printOperationStats("Recursive Call", globalPerfStats.RecursiveCallTime, globalPerfStats.RecursiveCallCount, totalTime)
	printOperationStats("CoW Operation", globalPerfStats.CowOperationTime, globalPerfStats.CowOperationCount, totalTime)
	printOperationStats("Commitment Update", globalPerfStats.CommitmentUpdateTime, globalPerfStats.CommitmentUpdateCount, totalTime)

	// NewLeafNode 相关统计
	if globalPerfStats.NewLeafNodeCount > 0 {
		fmt.Println("\n--- NewLeafNode Breakdown ---")
		printOperationStats("NewLeafNode Total", globalPerfStats.NewLeafNodeTime, globalPerfStats.NewLeafNodeCount, totalTime)
		printOperationStats("FillSuffixTree", globalPerfStats.FillSuffixTreeTime, globalPerfStats.FillSuffixTreeCount, totalTime)
		printOperationStats("CommitToPoly", globalPerfStats.CommitToPolyTime, globalPerfStats.CommitToPolyCount, totalTime)
		printOperationStats("BatchMapToScalar", globalPerfStats.BatchMapToScalarTime, globalPerfStats.BatchMapToScalarCount, totalTime)
		printOperationStats("StemProcessing", globalPerfStats.StemProcessingTime, globalPerfStats.StemProcessingCount, totalTime)
	}

	fmt.Println("==========================================")
}

// printOperationStats 打印单个操作的统计信息
func printOperationStats(name string, duration time.Duration, count int64, totalTime time.Duration) {
	if count == 0 {
		fmt.Printf("%-18s: %8s (%5.1f%%) | Count: %6d | Avg: %8s\n",
			name, "0s", 0.0, count, "0s")
		return
	}

	percentage := float64(duration) / float64(totalTime) * 100
	avgTime := duration / time.Duration(count)

	fmt.Printf("%-18s: %8s (%5.1f%%) | Count: %6d | Avg: %8s\n",
		name, duration.Round(time.Microsecond), percentage, count, avgTime.Round(time.Microsecond))
}

// 内部辅助函数，用于记录各种操作的耗时

func recordResolveNode(duration time.Duration) {
	statsMutex.Lock()
	defer statsMutex.Unlock()
	globalPerfStats.ResolveNodeTime += duration
	globalPerfStats.ResolveNodeCount++
}

func recordNodeTypeCheck(duration time.Duration) {
	statsMutex.Lock()
	defer statsMutex.Unlock()
	globalPerfStats.NodeTypeCheckTime += duration
	globalPerfStats.NodeTypeCheckCount++
}

func recordCreateInternal(duration time.Duration) {
	statsMutex.Lock()
	defer statsMutex.Unlock()
	globalPerfStats.CreateInternalTime += duration
	globalPerfStats.CreateInternalCount++
}

func recordCreateLeaf(duration time.Duration) {
	statsMutex.Lock()
	defer statsMutex.Unlock()
	globalPerfStats.CreateLeafTime += duration
	globalPerfStats.CreateLeafCount++
}

func recordInsertMultiple(duration time.Duration) {
	statsMutex.Lock()
	defer statsMutex.Unlock()
	globalPerfStats.InsertMultipleTime += duration
	globalPerfStats.InsertMultipleCount++
}

func recordUpdateMultiple(duration time.Duration) {
	statsMutex.Lock()
	defer statsMutex.Unlock()
	globalPerfStats.UpdateMultipleTime += duration
	globalPerfStats.UpdateMultipleCount++
}

func recordRecursiveCall(duration time.Duration) {
	statsMutex.Lock()
	defer statsMutex.Unlock()
	globalPerfStats.RecursiveCallTime += duration
	globalPerfStats.RecursiveCallCount++
}

func recordCowOperation(duration time.Duration) {
	statsMutex.Lock()
	defer statsMutex.Unlock()
	globalPerfStats.CowOperationTime += duration
	globalPerfStats.CowOperationCount++
}

func recordCommitmentUpdate(duration time.Duration) {
	statsMutex.Lock()
	defer statsMutex.Unlock()
	globalPerfStats.CommitmentUpdateTime += duration
	globalPerfStats.CommitmentUpdateCount++
}

// 记录 NewLeafNode 总耗时
func recordNewLeafNode(duration time.Duration) {
	statsMutex.Lock()
	defer statsMutex.Unlock()
	globalPerfStats.NewLeafNodeTime += duration
	globalPerfStats.NewLeafNodeCount++
}

// 记录填充后缀树多项式耗时
func recordFillSuffixTree(duration time.Duration) {
	statsMutex.Lock()
	defer statsMutex.Unlock()
	globalPerfStats.FillSuffixTreeTime += duration
	globalPerfStats.FillSuffixTreeCount++
}

// 记录提交到多项式耗时
func recordCommitToPoly(duration time.Duration) {
	statsMutex.Lock()
	defer statsMutex.Unlock()
	globalPerfStats.CommitToPolyTime += duration
	globalPerfStats.CommitToPolyCount++
}

// 记录批量映射到标量域耗时
func recordBatchMapToScalar(duration time.Duration) {
	statsMutex.Lock()
	defer statsMutex.Unlock()
	globalPerfStats.BatchMapToScalarTime += duration
	globalPerfStats.BatchMapToScalarCount++
}

// 记录 Stem 处理耗时
func recordStemProcessing(duration time.Duration) {
	statsMutex.Lock()
	defer statsMutex.Unlock()
	globalPerfStats.StemProcessingTime += duration
	globalPerfStats.StemProcessingCount++
}

func recordTotalInsertCall() {
	statsMutex.Lock()
	defer statsMutex.Unlock()
	globalPerfStats.TotalInsertCalls++
}

// GetPerfStats 获取当前性能统计数据的副本
func GetPerfStats() PerfStats {
	statsMutex.Lock()
	defer statsMutex.Unlock()
	return globalPerfStats
}
