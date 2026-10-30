package tree

import (
	"encoding/csv"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestE3SemanticTraceSmoke(t *testing.T) {
	input := t.TempDir()
	file, err := os.Create(filepath.Join(input, "state_access_trace_000.csv"))
	if err != nil {
		t.Fatal(err)
	}
	writer := csv.NewWriter(file)
	address := "0x" + strings.Repeat("11", 20)
	other := "0x" + strings.Repeat("44", 20)
	slot := "0x" + strings.Repeat("22", 32)
	value := "0x" + strings.Repeat("33", 32)
	rows := [][]string{
		strings.Split(traceCompareHeader, ","),
		{"1", "0", "0", "account", address, "", "read", "", "0"},
		{"1", "0", "1", "account", address, "", "create", value, "32"},
		{"2", "0", "0", "storage", address, slot, "write", value, "32"},
	}
	for i := 0; i < 40; i++ {
		rows = append(rows, []string{"3", "0", "0", "account", other, "", "read", "", "0"})
	}
	rows = append(rows,
		[]string{"4", "0", "0", "account", address, "", "read", "", "0"},
		[]string{"4", "0", "1", "storage", address, slot, "read", "", "0"},
		[]string{"4", "0", "2", "storage", address, slot, "delete", "", "0"},
		[]string{"4", "0", "3", "account", address, "", "delete", "", "0"},
		[]string{"4", "0", "4", "account", address, "", "read", "", "0"},
	)
	if err := writer.WriteAll(rows); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	oldInput, oldOutput := *e3InputDir, *e3OutputDir
	oldOps, oldBatch, oldDepth := *e3Ops, *e3BatchSize, *e3Depth
	oldProgress := *e3ProgressBatches
	t.Cleanup(func() {
		*e3InputDir, *e3OutputDir = oldInput, oldOutput
		*e3Ops, *e3BatchSize, *e3Depth = oldOps, oldBatch, oldDepth
		*e3ProgressBatches = oldProgress
	})
	*e3InputDir = input
	*e3OutputDir = filepath.Join(t.TempDir(), "output")
	*e3Ops, *e3BatchSize, *e3Depth = 0, 2, 1
	*e3ProgressBatches = 1
	t.Run("replay", TestE3StateSemanticEquivalence)
	data, err := os.ReadFile(filepath.Join(*e3OutputDir, "result.json"))
	if err != nil {
		t.Fatal(err)
	}
	var result e3Result
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatal(err)
	}
	if result.Status != "passed" || result.ArchivedEntries == 0 || result.Revivals == 0 {
		t.Fatalf("E3 did not exercise archive and revival: status=%s archived=%d revived=%d", result.Status, result.ArchivedEntries, result.Revivals)
	}
}
