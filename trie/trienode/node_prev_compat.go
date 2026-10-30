package trienode

import "github.com/ethereum/go-ethereum/common"

func NewNodeWithPrev(hash common.Hash, blob []byte, _ []byte) *Node {
	return New(hash, blob)
}

func NewDeletedWithPrev(_ []byte) *Node {
	return NewDeleted()
}
