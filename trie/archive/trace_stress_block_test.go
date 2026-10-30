package archive

import (
	"compress/gzip"
	"encoding/csv"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

func TestTraceStressCollectBlock(t *testing.T) {
	dir := t.TempDir()
	file, err := os.Create(filepath.Join(dir, "state_access_trace_000000001_000000004.csv.gz"))
	if err != nil {
		t.Fatal(err)
	}
	gz := gzip.NewWriter(file)
	writer := csv.NewWriter(gz)
	if err := writer.Write(strings.Split(traceStressHeader, ",")); err != nil {
		t.Fatal(err)
	}
	for _, block := range []uint64{1, 2, 4} {
		count := 1
		if block == 2 {
			count = 4101
		}
		for i := 0; i < count; i++ {
			if err := writer.Write([]string{strconv.FormatUint(block, 10), "0", strconv.Itoa(i), "account", "0x0000000000000000000000000000000000001234", "", "read", common.Hash{}.Hex(), "32"}); err != nil {
				t.Fatal(err)
			}
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	reader, err := newTraceStressReader(dir, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	selection := traceStressSelection{startBlock: 1, endBlock: 4}
	for _, want := range []struct {
		block uint64
		count int
		end   bool
	}{{1, 1, false}, {2, 4101, false}, {3, 0, false}, {4, 1, true}} {
		block, ops, end, err := selection.collectBlock(reader)
		if err != nil || block != want.block || len(ops) != want.count || end != want.end {
			t.Fatalf("block=%d ops=%d end=%t err=%v; want %+v", block, len(ops), end, err, want)
		}
	}
	if !selection.reachedEndBlock {
		t.Fatal("end block was not reached")
	}
}
