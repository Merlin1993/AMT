package verkle

import (
	"fmt"
	"testing"
	"time"
)

func TestSimpleCommitToPolyTiming(t *testing.T) {
	// 重置计时器
	ResetCommitToPolyTotalTime()

	// 验证初始状态
	initialTime := GetCommitToPolyTotalTime()
	fmt.Printf("Initial time: %v\n", initialTime)

	// 创建配置
	cfg := GetConfig()

	// 创建测试多项式
	poly := make([]Fr, NodeWidth)
	for i := range poly {
		poly[i].SetUint64(uint64(i + 1))
	}

	// 执行多次 CommitToPoly 操作以确保能测量到时间
	numOps := 100
	fmt.Printf("Executing %d CommitToPoly operations...\n", numOps)
	start := time.Now()
	for i := 0; i < numOps; i++ {
		result := cfg.CommitToPoly(poly, 0)
		if result == nil {
			t.Errorf("CommitToPoly returned nil")
			return
		}
	}
	elapsed := time.Since(start)
	fmt.Printf("Direct measurement for %d operations: %v\n", numOps, elapsed)
	fmt.Printf("Average per operation (direct): %v\n", elapsed/time.Duration(numOps))

	// 获取统计的总耗时
	totalTime := GetCommitToPolyTotalTime()
	fmt.Printf("Tracked total time: %v\n", totalTime)
	fmt.Printf("Average per operation (tracked): %v\n", totalTime/time.Duration(numOps))

	if totalTime <= 0 {
		t.Errorf("Expected total time to be greater than 0, got %v", totalTime)
	} else {
		t.Logf("SUCCESS: CommitToPoly timing is working correctly")

		// 验证统计时间与直接测量时间的合理性
		ratio := float64(totalTime) / float64(elapsed)
		fmt.Printf("Tracked/Direct ratio: %.2f\n", ratio)
		if ratio < 0.5 || ratio > 2.0 {
			t.Logf("Warning: Tracked time ratio seems unusual, but timing may vary")
		}
	}
}
