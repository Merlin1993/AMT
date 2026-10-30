package tree

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	trieutils "github.com/ethereum/go-ethereum/trie/utils"
)

const traceCompareHeader = "block_number,tx_index,seq,object_type,address,slot_or_chunk,operation,value_hash,value_len"

type traceCompareKind byte

const (
	traceCompareGet traceCompareKind = iota
	traceComparePut
	traceCompareDelete
)

type traceCompareOp struct {
	key       []byte
	value     []byte
	kind      traceCompareKind
	operation string
	block     uint64
}

var traceCompareZeroStorageHash = crypto.Keccak256Hash(make([]byte, common.HashLength)).Hex()

func parseTraceCompareRecord(record []string) (traceCompareOp, error) {
	if len(record) != 9 {
		return traceCompareOp{}, fmt.Errorf("want 9 fields, got %d", len(record))
	}
	block, err := strconv.ParseUint(record[0], 10, 64)
	if err != nil {
		return traceCompareOp{}, fmt.Errorf("block: %w", err)
	}
	if !common.IsHexAddress(record[4]) {
		return traceCompareOp{}, fmt.Errorf("invalid address %q", record[4])
	}
	address := common.HexToAddress(record[4])
	var key []byte
	switch record[3] {
	case "account":
		key = trieutils.BinaryTreeBasicDataKey(address)
	case "storage":
		slot, err := decodeTraceCompareHash(record[5])
		if err != nil {
			return traceCompareOp{}, fmt.Errorf("storage slot: %w", err)
		}
		key = trieutils.BinaryTreeStorageSlotKey(address, slot)
	case "code":
		chunk, err := strconv.ParseUint(record[5], 10, 64)
		if err != nil {
			return traceCompareOp{}, fmt.Errorf("code chunk: %w", err)
		}
		key = trieutils.BinaryTreeCodeChunkKey(address, chunk)
	default:
		return traceCompareOp{}, fmt.Errorf("unsupported object type %q", record[3])
	}
	op := traceCompareOp{key: key, operation: record[6], block: block}
	switch record[6] {
	case "read", "touch":
		op.kind = traceCompareGet
	case "delete":
		op.kind = traceCompareDelete
	case "write", "create":
		if record[3] == "storage" && strings.EqualFold(record[7], traceCompareZeroStorageHash) {
			op.kind = traceCompareDelete
			break
		}
		op.kind = traceComparePut
		if record[7] != "" {
			op.value, err = decodeTraceCompareHash(record[7])
			if err != nil {
				return traceCompareOp{}, fmt.Errorf("value hash: %w", err)
			}
		} else {
			op.value = crypto.Keccak256(key, []byte(record[6]))
		}
	default:
		return traceCompareOp{}, fmt.Errorf("unsupported operation %q", record[6])
	}
	return op, nil
}

func decodeTraceCompareHash(input string) ([]byte, error) {
	input = strings.TrimPrefix(input, "0x")
	if len(input) != common.HashLength*2 {
		return nil, fmt.Errorf("want 32-byte hash, got %d hex chars", len(input))
	}
	return hex.DecodeString(input)
}
