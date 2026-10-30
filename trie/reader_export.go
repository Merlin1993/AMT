package trie

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/triedb/database"
)

type Reader struct {
	reader *trieReader
}

func NewReader(stateRoot, owner common.Hash, db database.NodeDatabase) (*Reader, error) {
	reader, err := newTrieReader(stateRoot, owner, db)
	if err != nil {
		return nil, err
	}
	return &Reader{reader: reader}, nil
}

func (r *Reader) Node(path []byte, hash common.Hash) ([]byte, error) {
	return r.reader.node(path, hash)
}
