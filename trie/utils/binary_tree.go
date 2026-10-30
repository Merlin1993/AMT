package utils

import (
	"bytes"
	"crypto/sha256"

	"github.com/ethereum/go-ethereum/common"
	"github.com/holiman/uint256"
)

const (
	BinaryBasicDataLeafKey = byte(0)
	binaryCodeOffset       = uint64(128)
)

func BinaryTreeKey(addr common.Address, offset []byte) []byte {
	return binaryTreeKey(addr, offset, false)
}

func binaryTreeKey(addr common.Address, offset []byte, overflow bool) []byte {
	var input [64]byte
	copy(input[12:32], addr[:])
	copy(input[33:64], offset[:31])
	if overflow {
		input[32] = 1
	}
	hash := sha256.Sum256(input[:])
	hash[31] = offset[31]
	return hash[:]
}

func BinaryTreeBasicDataKey(addr common.Address) []byte {
	var offset [32]byte
	offset[31] = BinaryBasicDataLeafKey
	return BinaryTreeKey(addr, offset[:])
}

func BinaryTreeStorageSlotKey(addr common.Address, slot []byte) []byte {
	var (
		offset [32]byte
		normal = common.BytesToHash(slot)
		zero   common.Hash
	)
	if bytes.Equal(normal[:31], zero[:31]) && normal[31] < 64 {
		offset[31] = 64 + normal[31]
		return BinaryTreeKey(addr, offset[:])
	}
	overflow := normal[0] == 0xff
	copy(offset[:], normal[:])
	offset[0]++
	return binaryTreeKey(addr, offset[:], overflow)
}

func BinaryTreeCodeChunkKey(addr common.Address, chunk uint64) []byte {
	offset := new(uint256.Int).SetUint64(binaryCodeOffset + chunk).Bytes32()
	return BinaryTreeKey(addr, offset[:])
}

func ChunkifyBinaryCode(code []byte) []byte {
	const (
		chunkDataSize = 31
		chunkSize     = 32
		push1         = byte(0x60)
		push32        = byte(0x7f)
	)
	var (
		chunkOffset = 0
		chunkCount  = len(code) / chunkDataSize
		codeOffset  = 0
	)
	if len(code)%chunkDataSize != 0 {
		chunkCount++
	}
	chunks := make([]byte, chunkCount*chunkSize)
	for i := 0; i < chunkCount; i++ {
		end := min(len(code), chunkDataSize*(i+1))
		copy(chunks[i*chunkSize+1:], code[chunkDataSize*i:end])
		if chunkOffset > chunkDataSize {
			chunks[i*chunkSize] = chunkDataSize
			chunkOffset = 1
			continue
		}
		chunks[chunkSize*i] = byte(chunkOffset)
		chunkOffset = 0
		for ; codeOffset < end; codeOffset++ {
			if code[codeOffset] >= push1 && code[codeOffset] <= push32 {
				codeOffset += int(code[codeOffset]-push1) + 1
				if codeOffset+1 >= chunkDataSize*(i+1) {
					codeOffset++
					chunkOffset = codeOffset - chunkDataSize*(i+1)
					break
				}
			}
		}
	}
	return chunks
}
