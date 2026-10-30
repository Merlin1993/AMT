package archive

import (
	"reflect"
	"testing"
)

func TestBlockProofColumnsIncludesEmptyBlocks(t *testing.T) {
	got := blockProofColumns([]int64{0, 0, 2, 4}, []int64{0, 0, 100, 300}, 3)
	want := []string{
		"4", "1.500000", "2", "4", "4",
		"100.000000", "100", "300", "300",
		"2", "3.000000", "4", "4", "4",
		"200.000000", "300", "300", "300",
		"3", "400",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("block columns = %v, want %v", got, want)
	}
}
