package hx

import (
	"bytes"
	"fmt"
)

func (t *Trie) MountPoint(prefix []byte) ([]byte, error) {
	_, mp, err := t.mountPointAt(t.root, nil, prefix)
	return mp, err
}

func (t *Trie) StubsAt(path []byte) ([]*Stub, error) {
	n, err := t.nodeAt(t.root, nil, path)
	if err != nil {
		return nil, err
	}
	switch n := n.(type) {
	case *fullNode:
		return n.Stubs, nil
	case *shortNode:
		return n.Stubs, nil
	default:
		return nil, nil
	}
}

func (t *Trie) nodeAt(n node, path, target []byte) (node, error) {
	switch n := n.(type) {
	case nil:
		return nil, nil
	case hashNode:
		resolved, err := t.resolve(n, path)
		if err != nil {
			return nil, err
		}
		return t.nodeAt(resolved, path, target)
	case *shortNode:
		if bytes.Equal(path, target) {
			return n, nil
		}
		rest := target[len(path):]
		if len(rest) < len(n.Key) || !bytes.Equal(rest[:len(n.Key)], n.Key) {
			return nil, nil
		}
		if _, isVal := n.Val.(valueNode); isVal {
			return nil, nil
		}
		return t.nodeAt(n.Val, concat(path, n.Key...), target)
	case *fullNode:
		if bytes.Equal(path, target) {
			return n, nil
		}
		if len(path) >= len(target) {
			return nil, nil
		}
		return t.nodeAt(n.Children[target[len(path)]], concat(path, target[len(path)]), target)
	default:
		return nil, fmt.Errorf("hx: nodeAt on invalid node type %T", n)
	}
}

func (t *Trie) mountPointAt(n node, path, prefix []byte) (node, []byte, error) {
	switch n := n.(type) {
	case nil:
		return nil, path, nil
	case hashNode:
		resolved, err := t.resolve(n, path)
		if err != nil {
			return nil, nil, err
		}
		return t.mountPointAt(resolved, path, prefix)
	case *shortNode:
		if _, isVal := n.Val.(valueNode); isVal {
			return n, path, nil
		}
		rest := prefix[len(path):]
		if len(rest) >= len(n.Key) && bytes.Equal(rest[:len(n.Key)], n.Key) {
			leaf, err := t.isLeaf(n.Val, concat(path, n.Key...))
			if err != nil {
				return nil, nil, err
			}
			if !leaf {
				return t.mountPointAt(n.Val, concat(path, n.Key...), prefix)
			}
		}
		return n, path, nil
	case *fullNode:
		if len(path) < len(prefix) {
			if child := n.Children[prefix[len(path)]]; child != nil {
				leaf, err := t.isLeaf(child, concat(path, prefix[len(path)]))
				if err != nil {
					return nil, nil, err
				}
				if !leaf {
					return t.mountPointAt(child, concat(path, prefix[len(path)]), prefix)
				}
			}
		}
		return n, path, nil
	default:
		return nil, nil, fmt.Errorf("hx: mountPoint on invalid node type %T", n)
	}
}

func mergeStubs(a, b []*Stub) []*Stub {
	if len(a) == 0 {
		return b
	}
	if len(b) == 0 {
		return a
	}
	out := make([]*Stub, 0, len(a)+len(b))
	return append(append(out, a...), b...)
}

func (t *Trie) MountStub(prefix []byte, stub *Stub) ([]byte, error) {
	if t.committed {
		return nil, ErrCommitted
	}
	if t.root == nil {
		t.root = &fullNode{Stubs: []*Stub{stub}, flags: t.newFlag()}
		return nil, nil
	}
	n, mp, err := t.mountStubAt(t.root, nil, prefix, stub)
	if err != nil {
		return nil, err
	}
	t.root = n
	return mp, nil
}

func (t *Trie) mountStubAt(n node, path, prefix []byte, stub *Stub) (node, []byte, error) {
	switch n := n.(type) {
	case hashNode:
		resolved, err := t.resolve(n, path)
		if err != nil {
			return nil, nil, err
		}
		nn, mp, err := t.mountStubAt(resolved, path, prefix, stub)
		if err != nil {
			return nil, nil, err
		}
		return nn, mp, nil
	case *shortNode:
		if _, isVal := n.Val.(valueNode); isVal {
			return nil, nil, fmt.Errorf("hx: mountStub reached leaf at %x", path)
		}
		rest := prefix[len(path):]
		if len(rest) >= len(n.Key) && bytes.Equal(rest[:len(n.Key)], n.Key) {
			leaf, err := t.isLeaf(n.Val, concat(path, n.Key...))
			if err != nil {
				return nil, nil, err
			}
			if !leaf {
				nn, mp, err := t.mountStubAt(n.Val, concat(path, n.Key...), prefix, stub)
				if err != nil {
					return nil, nil, err
				}
				cpy := n.copy()
				cpy.Val = nn
				cpy.flags = t.newFlag()
				return cpy, mp, nil
			}
		}
		cpy := n.copy()
		cpy.Stubs = append(cpy.Stubs, stub)
		cpy.flags = t.newFlag()
		return cpy, path, nil
	case *fullNode:
		if len(path) < len(prefix) {
			if child := n.Children[prefix[len(path)]]; child != nil {
				leaf, err := t.isLeaf(child, concat(path, prefix[len(path)]))
				if err != nil {
					return nil, nil, err
				}
				if !leaf {
					cpy := n.copy()
					nn, mp, err := t.mountStubAt(child, concat(path, prefix[len(path)]), prefix, stub)
					if err != nil {
						return nil, nil, err
					}
					cpy.Children[prefix[len(path)]] = nn
					cpy.flags = t.newFlag()
					return cpy, mp, nil
				}
			}
		}
		cpy := n.copy()
		cpy.Stubs = append(cpy.Stubs, stub)
		cpy.flags = t.newFlag()
		return cpy, path, nil
	default:
		return nil, nil, fmt.Errorf("hx: mountStub on invalid node type %T", n)
	}
}

func (t *Trie) isLeaf(n node, path []byte) (bool, error) {
	if hn, ok := n.(hashNode); ok {
		resolved, err := t.resolve(hn, path)
		if err != nil {
			return false, err
		}
		n = resolved
	}
	sn, ok := n.(*shortNode)
	if !ok {
		return false, nil
	}
	_, isVal := sn.Val.(valueNode)
	return isVal, nil
}

func (t *Trie) StubsOnPath(key []byte, fn func(nodePath []byte, stub *Stub) bool) error {
	hexKey := keybytesToHex(key)
	found, err := t.stubsOnPath(t.root, nil, hexKey, fn)
	_ = found
	return err
}

func (t *Trie) stubsOnPath(n node, path, hexKey []byte, fn func([]byte, *Stub) bool) (bool, error) {
	switch n := n.(type) {
	case nil:
		return false, nil
	case hashNode:
		resolved, err := t.resolve(n, path)
		if err != nil {
			return false, err
		}
		return t.stubsOnPath(resolved, path, hexKey, fn)
	case *shortNode:
		for _, s := range n.Stubs {
			if fn(path, s) {
				return true, nil
			}
		}
		rest := hexKey[len(path):]
		if len(rest) < len(n.Key) || !bytes.Equal(rest[:len(n.Key)], n.Key) {
			return false, nil
		}
		if _, isVal := n.Val.(valueNode); isVal {
			return false, nil
		}
		return t.stubsOnPath(n.Val, concat(path, n.Key...), hexKey, fn)
	case *fullNode:
		for _, s := range n.Stubs {
			if fn(path, s) {
				return true, nil
			}
		}
		if len(path) >= len(hexKey)-1 {
			return false, nil
		}
		return t.stubsOnPath(n.Children[hexKey[len(path)]], concat(path, hexKey[len(path)]), hexKey, fn)
	default:
		return false, fmt.Errorf("hx: stubsOnPath on invalid node type %T", n)
	}
}

func (t *Trie) ReplaceStub(route, stubPath []byte, repl *Stub) error {
	if t.committed {
		return ErrCommitted
	}
	n, err := t.replaceStubAt(t.root, nil, route, stubPath, repl)
	if err != nil {
		return err
	}
	t.root = n
	return nil
}

func (t *Trie) replaceStubAt(n node, path, route, stubPath []byte, repl *Stub) (node, error) {
	notFound := func() (node, error) {
		return nil, fmt.Errorf("hx: stub %x not found on route %x", stubPath, route)
	}
	switch n := n.(type) {
	case nil:
		return notFound()
	case hashNode:
		resolved, err := t.resolve(n, path)
		if err != nil {
			return nil, err
		}
		return t.replaceStubAt(resolved, path, route, stubPath, repl)
	case *shortNode:
		if stubListHas(n.Stubs, stubPath) {
			cpy := n.copy()
			cpy.Stubs = replaceStubIn(cpy.Stubs, stubPath, repl)
			cpy.flags = t.newFlag()
			return cpy, nil
		}
		if _, isVal := n.Val.(valueNode); isVal {
			return notFound()
		}
		rest := route[len(path):]
		if len(rest) < len(n.Key) || !bytes.Equal(rest[:len(n.Key)], n.Key) {
			return notFound()
		}
		nn, err := t.replaceStubAt(n.Val, concat(path, n.Key...), route, stubPath, repl)
		if err != nil {
			return nil, err
		}
		cpy := n.copy()
		cpy.Val = nn
		cpy.flags = t.newFlag()
		return cpy, nil
	case *fullNode:
		if stubListHas(n.Stubs, stubPath) {
			cpy := n.copy()
			cpy.Stubs = replaceStubIn(cpy.Stubs, stubPath, repl)
			cpy.flags = t.newFlag()
			return cpy, nil
		}
		if len(path) >= len(route) {
			return notFound()
		}
		slot := route[len(path)]
		if n.Children[slot] == nil {
			return notFound()
		}
		nn, err := t.replaceStubAt(n.Children[slot], concat(path, slot), route, stubPath, repl)
		if err != nil {
			return nil, err
		}
		cpy := n.copy()
		cpy.Children[slot] = nn
		cpy.flags = t.newFlag()
		return cpy, nil
	default:
		return nil, fmt.Errorf("hx: replaceStub on invalid node type %T", n)
	}
}

func stubListHas(stubs []*Stub, stubPath []byte) bool {
	for _, s := range stubs {
		if bytes.Equal(s.Path, stubPath) {
			return true
		}
	}
	return false
}

func replaceStubIn(stubs []*Stub, stubPath []byte, repl *Stub) []*Stub {
	for i, s := range stubs {
		if bytes.Equal(s.Path, stubPath) {
			if repl == nil {
				return append(stubs[:i], stubs[i+1:]...)
			}
			stubs[i] = repl
			return stubs
		}
	}
	return stubs
}

func (t *Trie) AllStubs(fn func(nodePath []byte, stub *Stub)) error {
	_, err := t.allStubs(t.root, nil, fn)
	return err
}

func (t *Trie) allStubs(n node, path []byte, fn func([]byte, *Stub)) (bool, error) {
	switch n := n.(type) {
	case nil:
		return true, nil
	case hashNode:
		resolved, err := t.resolve(n, path)
		if err != nil {
			return false, err
		}
		return t.allStubs(resolved, path, fn)
	case *shortNode:
		for _, s := range n.Stubs {
			fn(path, s)
		}
		if _, isVal := n.Val.(valueNode); isVal {
			return true, nil
		}
		return t.allStubs(n.Val, concat(path, n.Key...), fn)
	case *fullNode:
		for _, s := range n.Stubs {
			fn(path, s)
		}
		for i := 0; i < 16; i++ {
			if n.Children[i] == nil {
				continue
			}
			if _, err := t.allStubs(n.Children[i], concat(path, byte(i)), fn); err != nil {
				return false, err
			}
		}
		return true, nil
	default:
		return false, fmt.Errorf("hx: allStubs on invalid node type %T", n)
	}
}
