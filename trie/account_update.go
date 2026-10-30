package trie

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

type AccountUpdate struct {
	Address common.Address
	Account *types.StateAccount
	CodeLen int
}
