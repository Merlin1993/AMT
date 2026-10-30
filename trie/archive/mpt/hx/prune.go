package hx

import (
	"bytes"
)

type ExtractedEntry struct {
	Key   []byte
	Value []byte
}

func aggExcludesEpoch(agg uint8, evict byte) bool {
	if evict == 0 {
		return agg == aggOne
	}
	return agg == aggZero
}

// ExtractDomain 按前缀定位分片，并依据时期位摘除过期叶子。
func (t *Trie) ExtractDomain(prefix []byte, evictEpoch byte, inDomain func(key []byte) bool) ([]ExtractedEntry, error) {
	if t.committed {
		return nil, ErrCommitted
	}
	var out []ExtractedEntry
	n, _, err := t.extract(t.root, nil, prefix, evictEpoch, inDomain, &out)
	if err != nil {
		return nil, err
	}
	t.root = n
	return out, nil
}

func (t *Trie) extract(n node, path []byte, domain []byte, evict byte, inDomain func([]byte) bool, out *[]ExtractedEntry) (node, bool, error) {
	inside := len(path) >= len(domain)
	switch n := n.(type) {
	case nil:
		return nil, false, nil

	case valueNode:
		return n, false, nil

	case hashNode:
		resolved, err := t.resolveAndTrack(n, path)
		if err != nil {
			return nil, false, err
		}
		nn, changed, err := t.extract(resolved, path, domain, evict, inDomain, out)
		if err != nil {
			return nil, false, err
		}
		if !changed {
			return n, false, nil
		}
		if nn == nil {
			t.tracer.onDelete(path)
			return nil, true, nil
		}
		t.tracer.onDelete(path)
		t.tracer.onInsert(path)
		return nn, true, nil

	case *shortNode:
		if !inside {
			rest := domain[len(path):]
			if len(rest) >= len(n.Key) {
				if !bytes.Equal(rest[:len(n.Key)], n.Key) {
					return n, false, nil
				}
			} else {
				if !bytes.Equal(n.Key[:len(rest)], rest) {
					return n, false, nil
				}
				if _, isLeaf := n.Val.(valueNode); isLeaf {
					return t.extractLeaf(n, path, evict, inDomain, out)
				}
				if aggExcludesEpoch(n.Agg, evict) {
					return n, false, nil
				}
				return t.extractShortChild(n, path, domain, evict, inDomain, out)
			}
			if _, isLeaf := n.Val.(valueNode); isLeaf {
				return t.extractLeaf(n, path, evict, inDomain, out)
			}
			return t.extractShortChild(n, path, domain, evict, inDomain, out)
		}
		if _, isLeaf := n.Val.(valueNode); isLeaf {
			return t.extractLeaf(n, path, evict, inDomain, out)
		}
		if aggExcludesEpoch(n.Agg, evict) {
			return n, false, nil
		}
		return t.extractShortChild(n, path, domain, evict, inDomain, out)

	case *fullNode:
		if !inside {
			idx := domain[len(path)]
			child := n.Children[idx]
			if child == nil {
				return n, false, nil
			}
			nc, changed, err := t.extract(child, concat(path, idx), domain, evict, inDomain, out)
			if err != nil {
				return nil, false, err
			}
			if !changed {
				return n, false, nil
			}
			cpy := n.copy()
			cpy.flags = t.newFlag()
			cpy.Children[idx] = nc
			if nc == nil {
				t.tracer.onDelete(concat(path, idx))
				cpy.refreshAgg(int(idx))
				return t.shrinkFull(cpy, path)
			}
			cpy.refreshAgg(int(idx))
			return cpy, true, nil
		}
		cpy := n
		changed := false
		for i := 0; i < 16; i++ {
			child := n.Children[i]
			if child == nil {
				continue
			}
			if aggExcludesEpoch(n.getAgg(i), evict) {
				continue
			}
			nc, childChanged, err := t.extract(child, concat(path, byte(i)), domain, evict, inDomain, out)
			if err != nil {
				return nil, false, err
			}
			if !childChanged {
				continue
			}
			if !changed {
				cpy = n.copy()
				changed = true
			}
			cpy.Children[i] = nc
			if nc == nil {
				t.tracer.onDelete(concat(path, byte(i)))
			}
			cpy.refreshAgg(i)
		}
		if !changed {
			return n, false, nil
		}
		cpy.flags = t.newFlag()
		return t.shrinkFull(cpy, path)

	default:
		panic("hx: invalid node type in extract")
	}
}

func (t *Trie) extractLeaf(n *shortNode, path []byte, evict byte, inDomain func([]byte) bool, out *[]ExtractedEntry) (node, bool, error) {
	if n.Epoch != evict {
		return n, false, nil
	}
	full := concat(path, n.Key...)
	key := hexToKeybytes(full)
	if inDomain != nil && !inDomain(key) {
		return n, false, nil
	}
	*out = append(*out, ExtractedEntry{
		Key:   key,
		Value: append([]byte(nil), n.Val.(valueNode)...),
	})
	t.tracer.onDelete(path)
	return nil, true, nil
}

func (t *Trie) extractShortChild(n *shortNode, path []byte, domain []byte, evict byte, inDomain func([]byte) bool, out *[]ExtractedEntry) (node, bool, error) {
	childPath := concat(path, n.Key...)
	nc, changed, err := t.extract(n.Val, childPath, domain, evict, inDomain, out)
	if err != nil {
		return nil, false, err
	}
	if !changed {
		return n, false, nil
	}
	if nc == nil {
		if len(n.Stubs) > 0 {
			t.tracer.onDelete(childPath)
			t.tracer.onInsert(path)
			return &fullNode{Stubs: n.Stubs, flags: t.newFlag()}, true, nil
		}
		t.tracer.onDelete(path)
		return nil, true, nil
	}
	if child, ok := nc.(*shortNode); ok {
		if _, isVal := child.Val.(valueNode); isVal && len(n.Stubs) > 0 {
			fresh := &shortNode{Key: n.Key, Val: nc, Agg: subtreeAggOf(nc), Stubs: n.Stubs, flags: t.newFlag()}
			t.tracer.onInsert(path)
			return fresh, true, nil
		}
		t.tracer.onDelete(path)
		t.tracer.onDelete(childPath)
		t.tracer.onInsert(path)
		return t.foldShortChain(&shortNode{
			Key:   concat(n.Key, child.Key...),
			Val:   child.Val,
			Epoch: child.Epoch,
			Agg:   child.Agg,
			Stubs: mergeStubs(n.Stubs, child.Stubs),
			flags: t.newFlag(),
		}, concat(childPath, child.Key...)), true, nil
	}
	fresh := &shortNode{Key: n.Key, Val: nc, Agg: subtreeAggOf(nc), Stubs: n.Stubs, flags: t.newFlag()}
	t.tracer.onInsert(path)
	return fresh, true, nil
}

func (t *Trie) shrinkFull(n *fullNode, path []byte) (node, bool, error) {
	pos := -1
	count := 0
	for i, child := range n.Children {
		if child != nil {
			pos = i
			count++
		}
	}
	if count >= 2 {
		n.flags = t.newFlag()
		return n, true, nil
	}
	if count == 0 {
		if len(n.Stubs) > 0 {
			n.flags = t.newFlag()
			return n, true, nil
		}
		t.tracer.onDelete(path)
		return nil, true, nil
	}
	if pos == 16 {
		n.flags = t.newFlag()
		return n, true, nil
	}
	child := n.Children[pos]
	childPath := concat(path, byte(pos))
	if hn, ok := child.(hashNode); ok {
		resolved, err := t.resolveAndTrack(hn, childPath)
		if err != nil {
			return nil, false, err
		}
		child = resolved
	}
	if short, ok := child.(*shortNode); ok {
		if _, isVal := short.Val.(valueNode); isVal && len(n.Stubs) > 0 {
			n.flags = t.newFlag()
			return n, true, nil
		}
		t.tracer.onDelete(path)
		t.tracer.onDelete(childPath)
		t.tracer.onInsert(path)
		return t.foldShortChain(&shortNode{
			Key:   concat([]byte{byte(pos)}, short.Key...),
			Val:   short.Val,
			Epoch: short.Epoch,
			Agg:   short.Agg,
			Stubs: mergeStubs(n.Stubs, short.Stubs),
			flags: t.newFlag(),
		}, concat(childPath, short.Key...)), true, nil
	}
	return &shortNode{Key: []byte{byte(pos)}, Val: child, Agg: subtreeAggOf(child), Stubs: n.Stubs, flags: t.newFlag()}, true, nil
}

func (t *Trie) foldShortChain(merged *shortNode, innerPath []byte) *shortNode {
	for {
		inner, ok := merged.Val.(*shortNode)
		if !ok {
			return merged
		}
		t.tracer.onDelete(innerPath)
		merged = &shortNode{
			Key:   concat(merged.Key, inner.Key...),
			Val:   inner.Val,
			Epoch: inner.Epoch,
			Agg:   inner.Agg,
			Stubs: mergeStubs(merged.Stubs, inner.Stubs),
			flags: t.newFlag(),
		}
		innerPath = concat(innerPath, inner.Key...)
	}
}
