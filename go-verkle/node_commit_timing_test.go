package verkle

import (
	"testing"
	"time"
)

func TestInternalNodeCommitBasicStats(t *testing.T) {
	// Reset statistics before test
	ResetCommitTotalTime()

	// Verify initial state
	if totalTime := GetCommitTotalTime(); totalTime != 0 {
		t.Errorf("Expected initial total time to be 0, got %v", totalTime)
	}

	// Create a tree with multiple values to ensure we have InternalNodes
	tree := New()

	// Insert multiple values to create internal nodes and trigger cow
	for i := 0; i < 20; i++ {
		key := make([]byte, 32) // Key must be 32 bytes (StemSize + 1)
		copy(key, []byte("test-key-"))
		key[9] = byte(i) // Make keys different at different positions
		key[10] = byte(i % 256)
		value := []byte("test-value")

		err := tree.Insert(key, value, nil)
		if err != nil {
			t.Fatalf("Failed to insert key %d: %v", i, err)
		}
	}

	// Perform commit operation - this will trigger InternalNode.Commit() timing
	tree.Commit()

	// Insert more values to trigger cow changes
	for i := 20; i < 30; i++ {
		key := make([]byte, 32)
		copy(key, []byte("test-key-"))
		key[9] = byte(i)
		key[10] = byte(i % 256)
		value := []byte("test-value-modified")

		err := tree.Insert(key, value, nil)
		if err != nil {
			t.Fatalf("Failed to insert key %d: %v", i, err)
		}
	}

	// Perform another commit operation to trigger cow-based commit
	tree.Commit()

	// Check that total time has increased (only InternalNode commits are timed)
	totalTime := GetCommitTotalTime()
	if totalTime <= 0 {
		t.Errorf("Expected total time to be greater than 0 after commit, got %v", totalTime)
	}

	t.Logf("Total InternalNode commit time: %v", totalTime)
}

func TestInternalNodeCommitReset(t *testing.T) {
	// Reset statistics to start fresh
	ResetCommitTotalTime()

	// Verify initial state
	if totalTime := GetCommitTotalTime(); totalTime != 0 {
		t.Errorf("Expected initial total time to be 0, got %v", totalTime)
	}

	// Create a tree with multiple values to ensure we have InternalNodes
	tree := New()

	// Insert multiple values to create internal nodes
	for i := 0; i < 15; i++ {
		key := make([]byte, 32) // Key must be 32 bytes (StemSize + 1)
		copy(key, []byte("test-key-"))
		key[9] = byte(i) // Make keys different
		key[10] = byte(i % 256)
		value := []byte("test-value")

		err := tree.Insert(key, value, nil)
		if err != nil {
			t.Fatalf("Failed to insert key %d: %v", i, err)
		}
	}

	// Perform commit operation - this will trigger InternalNode.Commit() timing
	tree.Commit()

	// Insert more values to trigger cow changes
	for i := 15; i < 25; i++ {
		key := make([]byte, 32)
		copy(key, []byte("test-key-"))
		key[9] = byte(i)
		key[10] = byte(i % 256)
		value := []byte("test-value-modified")

		err := tree.Insert(key, value, nil)
		if err != nil {
			t.Fatalf("Failed to insert key %d: %v", i, err)
		}
	}

	// Perform another commit to trigger cow-based commit
	tree.Commit()

	// Insert even more values to ensure we trigger cow again
	for i := 25; i < 35; i++ {
		key := make([]byte, 32)
		copy(key, []byte("test-key-"))
		key[9] = byte(i)
		key[10] = byte(i % 256)
		value := []byte("test-value-final")

		err := tree.Insert(key, value, nil)
		if err != nil {
			t.Fatalf("Failed to insert key %d: %v", i, err)
		}
	}

	// Perform final commit to ensure we have timing data
	tree.Commit()

	// Verify that time was recorded (only InternalNode commits are timed)
	totalTime := GetCommitTotalTime()
	if totalTime <= 0 {
		t.Errorf("Expected total time to be greater than 0, got %v", totalTime)
	}
	t.Logf("Total time before reset: %v", totalTime)

	// Reset and verify
	ResetCommitTotalTime()
	resetTime := GetCommitTotalTime()
	if resetTime != 0 {
		t.Errorf("Expected total time to be 0 after reset, got %v", resetTime)
	}
	t.Logf("Total time after reset: %v", resetTime)
}

func TestInternalNodeCommitAccumulation(t *testing.T) {
	// Reset statistics
	ResetCommitTotalTime()

	tree := New()

	// Perform multiple commit operations with different keys to create internal nodes
	for i := 0; i < 5; i++ {
		// Insert multiple keys in each iteration to ensure internal nodes
		for j := 0; j < 3; j++ {
			key := make([]byte, 32) // Key must be 32 bytes (StemSize + 1)
			copy(key, []byte("test-key-"))
			key[9] = byte(i)
			key[10] = byte(j)
			value := []byte("test-value")

			err := tree.Insert(key, value, nil)
			if err != nil {
				t.Fatalf("Failed to insert at iteration %d-%d: %v", i, j, err)
			}
		}

		// Commit after each batch - this will trigger InternalNode.Commit() timing
		tree.Commit()
	}

	// Check that total time has accumulated (only InternalNode commits are timed)
	totalTime := GetCommitTotalTime()
	if totalTime <= 0 {
		t.Errorf("Expected accumulated total time to be greater than 0, got %v", totalTime)
	}

	t.Logf("Accumulated InternalNode commit time for 5 operations: %v", totalTime)
}

func BenchmarkInternalNodeCommitStats(b *testing.B) {
	// Reset statistics before benchmark
	ResetCommitTotalTime()

	tree := New()

	// Insert multiple values to create internal nodes
	for i := 0; i < 10; i++ {
		key := make([]byte, 32) // Key must be 32 bytes (StemSize + 1)
		copy(key, []byte("benchmark-key-"))
		key[14] = byte(i) // Make keys different
		value := []byte("benchmark-value")

		err := tree.Insert(key, value, nil)
		if err != nil {
			b.Fatalf("Failed to insert key %d: %v", i, err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree.Commit() // This will trigger InternalNode.Commit() timing
	}
	b.StopTimer()

	// Verify that statistics were collected (only InternalNode commits are timed)
	totalTime := GetCommitTotalTime()
	if totalTime <= 0 {
		b.Errorf("Expected total time to be greater than 0, got %v", totalTime)
	}

	b.Logf("Total InternalNode commit time for %d operations: %v", b.N, totalTime)
	if b.N > 0 {
		b.Logf("Average time per operation: %v", totalTime/time.Duration(b.N))
	}
}
