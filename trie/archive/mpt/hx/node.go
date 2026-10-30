// Copyright 2014 The go-ethereum Authors
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

// Package hx 为十六叉树节点增加 AMT 元数据。
// 叶节点记录纪元；内部节点汇总子树纪元并挂载档案桩，根哈希覆盖这些元数据。
package hx

import (
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rlp"
)

type node interface {
	cache() (hashNode, bool)
	encode(w rlp.EncoderBuffer)
	fstring(string) string
}

type (
	fullNode struct {
		Children [17]node
		EpochAgg uint32
		Stubs    []*Stub
		flags    nodeFlag
	}
	shortNode struct {
		Key   []byte
		Val   node
		Epoch byte
		Agg   uint8
		Stubs []*Stub
		flags nodeFlag
	}
	hashNode  []byte
	valueNode []byte
)

const (
	aggEmpty = uint8(0)
	aggZero  = uint8(1)
	aggOne   = uint8(2)
	aggMixed = uint8(3)
)

func epochToAgg(epoch byte) uint8 {
	if epoch == 0 {
		return aggZero
	}
	return aggOne
}

func (n *fullNode) getAgg(i int) uint8 {
	return uint8((n.EpochAgg >> uint(2*i)) & 0x3)
}

func (n *fullNode) setAgg(i int, a uint8) {
	n.EpochAgg &^= 0x3 << uint(2*i)
	n.EpochAgg |= uint32(a&0x3) << uint(2*i)
}

func combineAgg(agg uint8, seen bool, next uint8) (uint8, bool) {
	if next == aggEmpty {
		return agg, seen
	}
	if !seen {
		return next, true
	}
	if agg == next {
		return agg, true
	}
	return aggMixed, true
}

func subtreeAggOf(n node) uint8 {
	switch n := n.(type) {
	case nil:
		return aggEmpty
	case *shortNode:
		if _, isVal := n.Val.(valueNode); isVal {
			return epochToAgg(n.Epoch)
		}
		return n.Agg
	case *fullNode:
		agg, seen := uint8(0), false
		for i := 0; i < 16; i++ {
			agg, seen = combineAgg(agg, seen, n.getAgg(i))
		}
		if !seen {
			return aggEmpty
		}
		return agg
	case hashNode:
		return aggMixed
	default:
		return aggMixed
	}
}

func (n *fullNode) refreshAgg(i int) {
	n.setAgg(i, subtreeAggOf(n.Children[i]))
}

type Stub struct {
	Path        []byte
	ContentHash [32]byte
}

func (s *Stub) copy() *Stub {
	c := *s
	return &c
}

type nodeFlag struct {
	hash  hashNode
	dirty bool
}

func (n *fullNode) cache() (hashNode, bool)  { return n.flags.hash, n.flags.dirty }
func (n *shortNode) cache() (hashNode, bool) { return n.flags.hash, n.flags.dirty }
func (n hashNode) cache() (hashNode, bool)   { return nil, true }
func (n valueNode) cache() (hashNode, bool)  { return nil, true }

func (n *fullNode) copy() *fullNode {
	c := *n
	if n.Stubs != nil {
		// 节点复制列表，列表中的存根描述在更新时保持不可变。
		c.Stubs = append([]*Stub(nil), n.Stubs...)
	}
	return &c
}
func (n *shortNode) copy() *shortNode {
	cpy := *n
	if n.Stubs != nil {
		cpy.Stubs = append([]*Stub(nil), n.Stubs...)
	}
	return &cpy
}

var indices = []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c", "d", "e", "f", "[17]"}

func (n *fullNode) String() string  { return n.fstring("") }
func (n *shortNode) String() string { return n.fstring("") }
func (n hashNode) String() string   { return n.fstring("") }
func (n valueNode) String() string  { return n.fstring("") }

func (n *fullNode) fstring(ind string) string {
	resp := fmt.Sprintf("[agg=%08x stubs=%d\n%s  ", n.EpochAgg, len(n.Stubs), ind)
	for i, node := range &n.Children {
		if node == nil {
			resp += fmt.Sprintf("%s: <nil> ", indices[i])
		} else {
			resp += fmt.Sprintf("%s: %v", indices[i], node.fstring(ind+"  "))
		}
	}
	return resp + fmt.Sprintf("\n%s] ", ind)
}

func (n *shortNode) fstring(ind string) string {
	return fmt.Sprintf("{%x: epoch=%d agg=%d %v} ", n.Key, n.Epoch, n.Agg, n.Val.fstring(ind+"  "))
}
func (n hashNode) fstring(ind string) string  { return fmt.Sprintf("<%x> ", []byte(n)) }
func (n valueNode) fstring(ind string) string { return fmt.Sprintf("%x ", []byte(n)) }

const hashLen = len(common.Hash{})
