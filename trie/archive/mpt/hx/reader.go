// Copyright 2022 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License as
// published by the Free Software Foundation, version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package hx

import (
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/triedb/database"
)

var ErrCommitted = errors.New("hx: trie already committed")

type MissingNodeError struct {
	Owner    common.Hash
	NodeHash common.Hash
	Path     []byte
	err      error
}

func (e *MissingNodeError) Error() string {
	return fmt.Sprintf("hx: missing trie node %x (owner %x, path %x) (%v)", e.NodeHash, e.Owner, e.Path, e.err)
}

func (e *MissingNodeError) Unwrap() error { return e.err }

type trieReader struct {
	owner  common.Hash
	reader database.NodeReader
}

func newTrieReader(stateRoot, owner common.Hash, db database.NodeDatabase) (*trieReader, error) {
	if stateRoot == (common.Hash{}) || stateRoot == types.EmptyRootHash {
		return &trieReader{owner: owner}, nil
	}
	reader, err := db.NodeReader(stateRoot)
	if err != nil {
		return nil, &MissingNodeError{Owner: owner, NodeHash: stateRoot, err: err}
	}
	return &trieReader{owner: owner, reader: reader}, nil
}

func (r *trieReader) node(path []byte, hash common.Hash) ([]byte, error) {
	if r.reader == nil {
		return nil, &MissingNodeError{Owner: r.owner, NodeHash: hash, Path: path, err: errors.New("hx: no node reader (empty trie)")}
	}
	blob, err := r.reader.Node(r.owner, path, hash)
	if err != nil {
		return nil, &MissingNodeError{Owner: r.owner, NodeHash: hash, Path: path, err: err}
	}
	return blob, nil
}
