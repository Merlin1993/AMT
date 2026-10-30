package verkle

import (
	"testing"
	"time"
)

// TestMockModeBasic 测试基本的mock模式功能
func TestMockModeBasic(t *testing.T) {
	// 确保mock模式开启
	SetMockMode(true)
	SetMockUseFixedPoint(true)

	cfg := GetConfig()

	// 测试生成点不会出错
	var poly [4]Fr
	poly[0].SetUint64(1)
	poly[1].SetUint64(2)
	poly[2].SetUint64(3)
	poly[3].SetUint64(4)

	// 这应该不会报错
	point := cfg.CommitToPoly(poly[:], 0)
	if point == nil {
		t.Fatal("CommitToPoly returned nil in mock mode")
	}

	// 测试多次调用应该返回相同的点（固定点模式）
	point2 := cfg.CommitToPoly(poly[:], 0)
	if !point.Equal(point2) {
		t.Error("Fixed point mode should return same point for same input")
	}
}

// TestMockModePointPool 测试点池模式
func TestMockModePointPool(t *testing.T) {
	// 设置为点池模式
	SetMockMode(true)
	SetMockUseFixedPoint(false)

	cfg := GetConfig()

	var poly [4]Fr
	poly[0].SetUint64(1)
	poly[1].SetUint64(2)
	poly[2].SetUint64(3)
	poly[3].SetUint64(4)

	// 生成多个点
	points := make([]*Point, 10)
	for i := 0; i < 10; i++ {
		points[i] = cfg.CommitToPoly(poly[:], 0)
		if points[i] == nil {
			t.Fatal("CommitToPoly returned nil in mock mode")
		}
	}

	// 在点池模式下，点可能不同（因为随机选择）
	// 但都应该是有效的点
	for i := 0; i < 10; i++ {
		if points[i] == nil {
			t.Errorf("Point %d is nil", i)
		}
	}
}

// TestMockModePerformance 测试mock模式的性能
func TestMockModePerformance(t *testing.T) {
	// 测试固定点模式的性能
	SetMockMode(true)
	SetMockUseFixedPoint(true)

	cfg := GetConfig()

	var poly [4]Fr
	poly[0].SetUint64(1)
	poly[1].SetUint64(2)
	poly[2].SetUint64(3)
	poly[3].SetUint64(4)

	start := time.Now()
	for i := 0; i < 1000; i++ {
		point := cfg.CommitToPoly(poly[:], 0)
		if point == nil {
			t.Fatal("CommitToPoly returned nil")
		}
	}
	fixedPointDuration := time.Since(start)

	// 测试点池模式的性能
	SetMockUseFixedPoint(false)

	start = time.Now()
	for i := 0; i < 1000; i++ {
		point := cfg.CommitToPoly(poly[:], 0)
		if point == nil {
			t.Fatal("CommitToPoly returned nil")
		}
	}
	pointPoolDuration := time.Since(start)

	t.Logf("Fixed point mode: %v", fixedPointDuration)
	t.Logf("Point pool mode: %v", pointPoolDuration)

	// 固定点模式应该更快
	if fixedPointDuration > pointPoolDuration {
		t.Log("Warning: Fixed point mode is slower than point pool mode")
	}
}

// TestMockModeToggle 测试mock模式开关
func TestMockModeToggle(t *testing.T) {
	// 测试获取和设置mock模式状态
	originalMode := GetMockMode()
	originalFixedPoint := GetMockUseFixedPoint()

	// 测试设置和获取
	SetMockMode(false)
	if GetMockMode() != false {
		t.Error("SetMockMode(false) failed")
	}

	SetMockMode(true)
	if GetMockMode() != true {
		t.Error("SetMockMode(true) failed")
	}

	SetMockUseFixedPoint(false)
	if GetMockUseFixedPoint() != false {
		t.Error("SetMockUseFixedPoint(false) failed")
	}

	SetMockUseFixedPoint(true)
	if GetMockUseFixedPoint() != true {
		t.Error("SetMockUseFixedPoint(true) failed")
	}

	// 恢复原始状态
	SetMockMode(originalMode)
	SetMockUseFixedPoint(originalFixedPoint)
}
