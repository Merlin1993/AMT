package verkle

import (
	"crypto/rand"
	"fmt"
	"testing"
	"time"
)

// TestPerfStats 测试性能统计功能
func TestPerfStats(t *testing.T) {
	// 重置统计
	ResetPerfStats()

	// 创建一个新的 Verkle 树
	root := New()

	// 生成一些测试数据
	numInserts := 100
	for i := 0; i < numInserts; i++ {
		key := make([]byte, 32)
		value := make([]byte, 32)

		// 生成随机的 key 和 value
		rand.Read(key)
		rand.Read(value)

		// 插入到树中
		err := root.Insert(key, value, nil)
		if err != nil {
			t.Fatalf("插入失败: %v", err)
		}
	}

	// 打印性能统计
	fmt.Println("\n=== 性能统计测试结果 ===")
	PrintPerfStats()

	// 验证统计数据
	if globalPerfStats.TotalInsertCalls == 0 {
		t.Error("总插入调用次数应该大于 0")
	}

	// 重置统计并验证
	ResetPerfStats()
	if globalPerfStats.TotalInsertCalls != 0 {
		t.Error("重置后总插入调用次数应该为 0")
	}

	fmt.Println("\n=== 重置后的统计 ===")
	PrintPerfStats()
}

// TestPerfStatsWithBatchInsert 测试批量插入的性能统计
func TestPerfStatsWithBatchInsert(t *testing.T) {
	// 重置统计
	ResetPerfStats()

	// 创建一个新的 Verkle 树
	root := New()

	// 生成相同 stem 的多个值进行批量插入
	stem := make([]byte, 31)
	rand.Read(stem)

	values := make([][]byte, 256)
	for i := 0; i < 256; i++ {
		values[i] = make([]byte, 32)
		rand.Read(values[i])
	}

	// 执行批量插入
	if internalRoot, ok := root.(*InternalNode); ok {
		err := internalRoot.InsertValuesAtStem(stem, values, nil)
		if err != nil {
			t.Fatalf("批量插入失败: %v", err)
		}
	}

	// 打印性能统计
	fmt.Println("\n=== 批量插入性能统计 ===")
	PrintPerfStats()
}

// BenchmarkInsertWithStats 带统计的插入性能基准测试
func BenchmarkInsertWithStats(b *testing.B) {
	// 重置统计
	ResetPerfStats()

	root := New()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := make([]byte, 32)
		value := make([]byte, 32)

		// 生成随机数据
		rand.Read(key)
		rand.Read(value)

		// 插入
		err := root.Insert(key, value, nil)
		if err != nil {
			b.Fatalf("插入失败: %v", err)
		}
	}
	b.StopTimer()

	// 打印统计结果
	fmt.Printf("\n=== 基准测试统计 (N=%d) ===\n", b.N)
	PrintPerfStats()
}

// TestNewLeafNodePerfStats 测试 NewLeafNode 的性能统计功能
func TestNewLeafNodePerfStats(t *testing.T) {
	ResetPerfStats()

	// 创建测试数据
	stem := make([]byte, 31)
	for i := 0; i < 31; i++ {
		stem[i] = byte(i)
	}

	values := make([][]byte, NodeWidth)
	for i := 0; i < NodeWidth; i++ {
		values[i] = make([]byte, 32)
		for j := 0; j < 32; j++ {
			values[i][j] = byte(i + j)
		}
	}

	// 创建多个 LeafNode 来测试统计
	for i := 0; i < 10; i++ {
		// 修改 stem 使每次创建不同的节点
		stem[30] = byte(i)

		_, err := NewLeafNode(stem, values)
		if err != nil {
			t.Fatalf("NewLeafNode failed: %v", err)
		}
	}

	// 验证统计数据
	stats := GetPerfStats()
	if stats.NewLeafNodeCount != 10 {
		t.Errorf("Expected 10 NewLeafNode calls, got %d", stats.NewLeafNodeCount)
	}

	if stats.NewLeafNodeTime == 0 {
		t.Error("Expected non-zero NewLeafNode time")
	}

	// 验证子操作统计
	if stats.FillSuffixTreeCount == 0 {
		t.Error("Expected non-zero FillSuffixTree count")
	}

	if stats.CommitToPolyCount == 0 {
		t.Error("Expected non-zero CommitToPoly count")
	}

	if stats.BatchMapToScalarCount == 0 {
		t.Error("Expected non-zero BatchMapToScalar count")
	}

	if stats.StemProcessingCount == 0 {
		t.Error("Expected non-zero StemProcessing count")
	}

	// 手动打印 NewLeafNode 统计信息
	fmt.Printf("\n=== NewLeafNode Performance Stats ===\n")
	fmt.Printf("NewLeafNode Total : %v | Count: %d | Avg: %v\n",
		stats.NewLeafNodeTime,
		stats.NewLeafNodeCount,
		stats.NewLeafNodeTime/time.Duration(stats.NewLeafNodeCount))

	fmt.Printf("FillSuffixTree    : %v | Count: %d | Avg: %v\n",
		stats.FillSuffixTreeTime,
		stats.FillSuffixTreeCount,
		stats.FillSuffixTreeTime/time.Duration(stats.FillSuffixTreeCount))

	fmt.Printf("CommitToPoly      : %v | Count: %d | Avg: %v\n",
		stats.CommitToPolyTime,
		stats.CommitToPolyCount,
		stats.CommitToPolyTime/time.Duration(stats.CommitToPolyCount))

	fmt.Printf("BatchMapToScalar  : %v | Count: %d | Avg: %v\n",
		stats.BatchMapToScalarTime,
		stats.BatchMapToScalarCount,
		stats.BatchMapToScalarTime/time.Duration(stats.BatchMapToScalarCount))

	fmt.Printf("StemProcessing    : %v | Count: %d | Avg: %v\n",
		stats.StemProcessingTime,
		stats.StemProcessingCount,
		stats.StemProcessingTime/time.Duration(stats.StemProcessingCount))

	fmt.Println("==========================================")
}
