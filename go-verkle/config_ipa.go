// This is free and unencumbered software released into the public domain.
//
// Anyone is free to copy, modify, publish, use, compile, sell, or
// distribute this software, either in source code form or as a compiled
// binary, for any purpose, commercial or non-commercial, and by any
// means.
//
// In jurisdictions that recognize copyright laws, the author or authors
// of this software dedicate any and all copyright interest in the
// software to the public domain. We make this dedication for the benefit
// of the public at large and to the detriment of our heirs and
// successors. We intend this dedication to be an overt act of
// relinquishment in perpetuity of all present and future rights to this
// software under copyright law.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
// IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
// OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
// ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
// OTHER DEALINGS IN THE SOFTWARE.
//
// For more information, please refer to <https://unlicense.org>

package verkle

import (
	"sync"
	"time"

	"github.com/crate-crypto/go-ipa/ipa"
)

// EmptyCodeHashPoint is a cached point that is used to represent an empty code hash.
// This value is initialized once in GetConfig().
var (
	EmptyCodeHashPoint           Point
	EmptyCodeHashFirstHalfValue  Fr
	EmptyCodeHashSecondHalfValue Fr
)

const (
	CodeHashVectorPosition     = 3 // Defined by the spec.
	EmptyCodeHashFirstHalfIdx  = CodeHashVectorPosition * 2
	EmptyCodeHashSecondHalfIdx = EmptyCodeHashFirstHalfIdx + 1
)

var (
	FrZero Fr
	FrOne  Fr

	cfg     *Config
	onceCfg sync.Once

	// CommitToPoly 总耗时统计
	commitToPolyTotalTime time.Duration
	commitToPolyMutex     sync.RWMutex

	// BatchMapToScalarField statistics
	batchMapTotalTime time.Duration
	batchMapMutex     sync.RWMutex

	// Commit statistics
	commitTotalTime time.Duration
	commitMutex     sync.RWMutex

	// Mock mode - 硬编码控制，设置为true时poly计算不耗时
	MockMode = false

	// Mock模式的变体：
	// - true: 使用固定点，最快但所有commitment相同
	// - false: 使用点池，稍慢但commitment不同，简单递增提供随机性
	mockUseFixedPoint = true

	// Mock模式使用的固定点值
	mockFixedPoint Point

	// Mock模式使用的预计算点池，包含10万个不同的点，简单递增使用
	mockPointPool       [100000]Point
	mockPoolInitialized bool
	mockPointIndex      uint32 // 当前使用的点索引，简单递增
)

func init() {
	FrZero.SetZero()
	FrOne.SetOne()

	// 初始化mock固定点值为一个有效的固定点
	// 使用一个简单的多项式来生成一个有效的点
	var mockPoly [1]Fr
	mockPoly[0].SetUint64(42) // 使用固定值42

	// 延迟初始化，在GetConfig被调用后进行
}

type IPAConfig struct {
	conf *ipa.IPAConfig
}

type Config = IPAConfig

func GetConfig() *Config {
	onceCfg.Do(func() {
		conf, err := ipa.NewIPASettings()
		if err != nil {
			panic(err)
		}
		cfg = &IPAConfig{conf: conf}

		// Initialize the empty code cached values.
		values := make([][]byte, NodeWidth)
		values[CodeHashVectorPosition] = EmptyCodeHash
		var c1poly [NodeWidth]Fr
		if _, err := fillSuffixTreePoly(c1poly[:], values[:NodeWidth/2]); err != nil {
			panic(err)
		}
		EmptyCodeHashPoint = *cfg.CommitToPoly(c1poly[:], 0)
		EmptyCodeHashFirstHalfValue = c1poly[EmptyCodeHashFirstHalfIdx]
		EmptyCodeHashSecondHalfValue = c1poly[EmptyCodeHashSecondHalfIdx]

		// 初始化mock固定点值
		var mockPoly [1]Fr
		mockPoly[0].SetUint64(42) // 使用固定值42
		mockFixedPoint = cfg.conf.Commit(mockPoly[:])

		// 初始化mock点池
		initMockPointPool(cfg)
	})
	return cfg
}

func (conf *IPAConfig) CommitToPoly(poly []Fr, _ int) *Point {
	if MockMode {
		// Mock模式：生成随机点值，跳过所有计算
		return generateRandomPoint(conf)
	}

	// 正常模式：执行真正的poly计算并统计耗时
	start := time.Now()
	ret := conf.conf.Commit(poly)
	duration := time.Since(start)

	commitToPolyMutex.Lock()
	commitToPolyTotalTime += duration
	commitToPolyMutex.Unlock()

	return &ret
}

// GetCommitToPolyTotalTime 获取 CommitToPoly 方法的总耗时
func GetCommitToPolyTotalTime() time.Duration {
	commitToPolyMutex.RLock()
	defer commitToPolyMutex.RUnlock()
	return commitToPolyTotalTime
}

// ResetCommitToPolyTotalTime 清零 CommitToPoly 方法的总耗时统计
func ResetCommitToPolyTotalTime() {
	commitToPolyMutex.Lock()
	defer commitToPolyMutex.Unlock()
	commitToPolyTotalTime = 0
}

// GetBatchMapTotalTime 获取 BatchMapToScalarField 的总耗时
func GetBatchMapTotalTime() time.Duration {
	batchMapMutex.RLock()
	defer batchMapMutex.RUnlock()
	return batchMapTotalTime
}

// ResetBatchMapTotalTime 重置 BatchMapToScalarField 的总耗时
func ResetBatchMapTotalTime() {
	batchMapMutex.Lock()
	defer batchMapMutex.Unlock()
	batchMapTotalTime = 0
}

// GetCommitTotalTime returns the total time spent in Commit operations
func GetCommitTotalTime() time.Duration {
	commitMutex.RLock()
	defer commitMutex.RUnlock()
	return commitTotalTime
}

// ResetCommitTotalTime resets the total time spent in Commit operations
func ResetCommitTotalTime() {
	commitMutex.Lock()
	defer commitMutex.Unlock()
	commitTotalTime = 0
}

// SetMockMode 设置mock模式的开关
func SetMockMode(enabled bool) {
	MockMode = enabled
}

// SetMockUseFixedPoint 设置mock模式是否使用固定点
// true: 使用固定点，最快但所有commitment相同
// false: 使用点池，稍慢但commitment不同
func SetMockUseFixedPoint(useFixed bool) {
	mockUseFixedPoint = useFixed
}

// GetMockMode 获取当前mock模式状态
func GetMockMode() bool {
	return MockMode
}

// GetMockUseFixedPoint 获取当前mock固定点模式状态
func GetMockUseFixedPoint() bool {
	return mockUseFixedPoint
}

// initMockPointPool 初始化mock模式使用的点池
func initMockPointPool(conf *IPAConfig) {
	if mockPoolInitialized {
		return
	}

	// 预计算100000个不同的点，使用不同的标量值
	for i := 0; i < 100000; i++ {
		var scalar Fr
		scalar.SetUint64(uint64(i + 1)) // 使用1-100000的值，避免零点
		mockPointPool[i].ScalarMul(&conf.conf.SRS[0], &scalar)
	}
	mockPoolInitialized = true
}

// generateRandomPoint generates a point for mock mode with good randomness
func generateRandomPoint(conf *IPAConfig) *Point {
	// 如果使用固定点模式，直接返回固定点（最快）
	if mockUseFixedPoint {
		return &mockFixedPoint
	}

	// 否则使用点池模式（提供不同的点）
	// 确保点池已初始化
	if !mockPoolInitialized {
		initMockPointPool(conf)
	}

	// 简单递增计数器，提供基本的随机性
	mockPointIndex++
	index := mockPointIndex % 100000
	return &mockPointPool[index]
}
