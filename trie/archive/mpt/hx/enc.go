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
	"bytes"
	"fmt"
	"io"
	"strings"

	"github.com/ethereum/go-ethereum/rlp"
)

func nodeToBytes(n node) []byte {
	w := rlp.NewEncoderBuffer(nil)
	n.encode(w)
	result := w.ToBytes()
	w.Flush()
	return result
}

func encodeStubs(w rlp.EncoderBuffer, stubs []*Stub) {
	off := w.List()
	for _, s := range stubs {
		soff := w.List()
		w.WriteBytes(s.Path)
		w.WriteBytes(s.ContentHash[:])
		w.ListEnd(soff)
	}
	w.ListEnd(off)
}

func hexToCompactNoTerm(hex []byte) []byte {
	buf := make([]byte, len(hex)/2+1)
	if len(hex)&1 == 1 {
		buf[0] |= 1 << 4
		buf[0] |= hex[0]
		hex = hex[1:]
	}
	decodeNibbles(hex, buf[1:])
	return buf
}

func compactToHexNoTerm(compact []byte) []byte {
	if len(compact) == 0 {
		return nil
	}
	base := keybytesToHex(compact)
	base = base[:len(base)-1]
	if compact[0]&0x10 != 0 {
		return base[1:]
	}
	return base[2:]
}

func (n *fullNode) encode(w rlp.EncoderBuffer) {
	offset := w.List()
	for _, c := range n.Children {
		if c != nil {
			c.encode(w)
		} else {
			w.Write(rlp.EmptyString)
		}
	}
	w.WriteUint64(uint64(n.EpochAgg))
	encodeStubs(w, n.Stubs)
	w.ListEnd(offset)
}

type fullnodeEncoder struct {
	Children [17][]byte
	EpochAgg uint32
	Stubs    []*Stub
}

func (n *fullnodeEncoder) encode(w rlp.EncoderBuffer) {
	offset := w.List()
	for _, c := range n.Children {
		if c == nil {
			w.Write(rlp.EmptyString)
		} else if len(c) < 32 {
			w.Write(c)
		} else {
			w.WriteBytes(c)
		}
	}
	w.WriteUint64(uint64(n.EpochAgg))
	encodeStubs(w, n.Stubs)
	w.ListEnd(offset)
}

func (n *shortNode) encode(w rlp.EncoderBuffer) {
	offset := w.List()
	w.WriteBytes(n.Key)
	if n.Val != nil {
		n.Val.encode(w)
	} else {
		w.Write(rlp.EmptyString)
	}
	if hasTerm(compactToHex(n.Key)) {
		w.WriteUint64(uint64(n.Epoch))
	} else {
		w.WriteUint64(uint64(n.Agg))
	}
	if len(n.Stubs) > 0 {
		encodeStubs(w, n.Stubs)
	}
	w.ListEnd(offset)
}

type shortNodeEncoder struct {
	Key  []byte
	Val  []byte
	Meta uint64
}

func (n *shortNodeEncoder) encode(w rlp.EncoderBuffer) {
	offset := w.List()
	w.WriteBytes(n.Key)
	if n.Val == nil {
		w.Write(rlp.EmptyString)
	} else if len(n.Val) < 32 {
		w.Write(n.Val)
	} else {
		w.WriteBytes(n.Val)
	}
	w.WriteUint64(n.Meta)
	w.ListEnd(offset)
}

func (n hashNode) encode(w rlp.EncoderBuffer) {
	w.WriteBytes(n)
}

func (n valueNode) encode(w rlp.EncoderBuffer) {
	w.WriteBytes(n)
}

func decodeNode(hash, buf []byte) (node, error) {
	if len(buf) == 0 {
		return nil, io.ErrUnexpectedEOF
	}
	elems, _, err := rlp.SplitList(buf)
	if err != nil {
		return nil, fmt.Errorf("hx decode error: %v", err)
	}
	switch c, _ := rlp.CountValues(elems); c {
	case 3, 4:
		n, err := decodeShort(hash, elems)
		return n, wrapError(err, "short")
	case 19:
		n, err := decodeFull(hash, elems)
		return n, wrapError(err, "full")
	default:
		return nil, fmt.Errorf("hx: invalid number of list elements: %v", c)
	}
}

func decodeShort(hash, elems []byte) (node, error) {
	kbuf, rest, err := rlp.SplitString(elems)
	if err != nil {
		return nil, err
	}
	key := compactToHex(kbuf)
	flag := nodeFlag{hash: hash}
	var sn *shortNode
	if hasTerm(key) {
		val, r, err := rlp.SplitString(rest)
		if err != nil {
			return nil, fmt.Errorf("hx: invalid value node: %v", err)
		}
		rest = r
		meta, r, err := rlp.SplitUint64(rest)
		if err != nil {
			return nil, fmt.Errorf("hx: invalid leaf epoch: %v", err)
		}
		rest = r
		sn = &shortNode{Key: key, Val: valueNode(val), Epoch: byte(meta), flags: flag}
	} else {
		ref, r, err := decodeRef(rest)
		if err != nil {
			return nil, wrapError(err, "val")
		}
		rest = r
		meta, r, err := rlp.SplitUint64(rest)
		if err != nil {
			return nil, fmt.Errorf("hx: invalid extension aggregate: %v", err)
		}
		rest = r
		sn = &shortNode{Key: key, Val: ref, Agg: uint8(meta), flags: flag}
	}
	if len(rest) > 0 {
		stubs, r, err := rlp.SplitList(rest)
		if err != nil {
			return nil, fmt.Errorf("hx: invalid short stub list: %v", err)
		}
		rest = r
		for len(stubs) > 0 {
			var raw []byte
			raw, stubs, err = rlp.SplitList(stubs)
			if err != nil {
				return nil, fmt.Errorf("hx: invalid stub entry: %v", err)
			}
			stub, err := decodeStub(raw)
			if err != nil {
				return nil, err
			}
			sn.Stubs = append(sn.Stubs, stub)
		}
	}
	if len(rest) != 0 {
		return nil, fmt.Errorf("hx: trailing short node bytes")
	}
	return sn, nil
}

func decodeFull(hash, elems []byte) (*fullNode, error) {
	n := &fullNode{flags: nodeFlag{hash: hash}}
	var err error
	for i := 0; i < 16; i++ {
		var cld node
		cld, elems, err = decodeRef(elems)
		if err != nil {
			return n, wrapError(err, fmt.Sprintf("[%d]", i))
		}
		n.Children[i] = cld
	}
	val, rest, err := rlp.SplitString(elems)
	if err != nil {
		return n, err
	}
	if len(val) > 0 {
		n.Children[16] = valueNode(val)
	}
	agg, rest, err := rlp.SplitUint64(rest)
	if err != nil {
		return n, fmt.Errorf("hx: invalid epoch aggregate: %v", err)
	}
	n.EpochAgg = uint32(agg)
	stubs, _, err := rlp.SplitList(rest)
	if err != nil {
		return n, fmt.Errorf("hx: invalid stub list: %v", err)
	}
	for len(stubs) > 0 {
		var raw []byte
		raw, stubs, err = rlp.SplitList(stubs)
		if err != nil {
			return n, fmt.Errorf("hx: invalid stub entry: %v", err)
		}
		stub, err := decodeStub(raw)
		if err != nil {
			return n, err
		}
		n.Stubs = append(n.Stubs, stub)
	}
	return n, nil
}

func decodeStub(elems []byte) (*Stub, error) {
	path, rest, err := rlp.SplitString(elems)
	if err != nil {
		return nil, fmt.Errorf("hx: invalid stub path: %v", err)
	}
	contentHash, rest, err := rlp.SplitString(rest)
	if err != nil {
		return nil, fmt.Errorf("hx: invalid stub content hash: %v", err)
	}
	if len(contentHash) != 32 {
		return nil, fmt.Errorf("hx: stub content hash length %d, want 32", len(contentHash))
	}
	if len(rest) != 0 {
		return nil, fmt.Errorf("hx: trailing stub bytes")
	}
	s := &Stub{
		Path: bytes.Clone(path),
	}
	copy(s.ContentHash[:], contentHash)
	return s, nil
}

func decodeRef(buf []byte) (node, []byte, error) {
	kind, val, rest, err := rlp.Split(buf)
	if err != nil {
		return nil, buf, err
	}
	switch {
	case kind == rlp.List:
		if size := len(buf) - len(rest); size > hashLen {
			err := fmt.Errorf("hx: oversized embedded node (size is %d bytes, want size < %d)", size, hashLen)
			return nil, buf, err
		}
		n, err := decodeNode(nil, buf)
		return n, rest, err
	case kind == rlp.String && len(val) == 0:
		return nil, rest, nil
	case kind == rlp.String && len(val) == 32:
		return hashNode(val), rest, nil
	default:
		return nil, nil, fmt.Errorf("hx: invalid RLP string size %d (want 0 or 32)", len(val))
	}
}

type decodeError struct {
	what  error
	stack []string
}

func wrapError(err error, ctx string) error {
	if err == nil {
		return nil
	}
	if decErr, ok := err.(*decodeError); ok {
		decErr.stack = append(decErr.stack, ctx)
		return decErr
	}
	return &decodeError{err, []string{ctx}}
}

func (err *decodeError) Error() string {
	return fmt.Sprintf("%v (decode path: %s)", err.what, strings.Join(err.stack, "<-"))
}
