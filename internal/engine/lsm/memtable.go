package lsm

import (
	"bytes"
	"sync"
	"unsafe"
)

type color uint8

const (
	black color = iota
	red
)

type memNode struct {
	key, value          []byte
	left, right, parent *memNode
	color               color
	tombstone           bool
}

type memtable struct {
	mu   sync.RWMutex
	root *memNode
	leaf *memNode
	size int
}

func newMemtable() *memtable {
	leaf := &memNode{color: black}
	return &memtable{root: leaf, leaf: leaf}
}

// TODO
// Get looks up key. found reports whether the memtable has an entry for key;
// deleted reports whether that entry is a tombstone. When found is false,
// the caller should check older memtables and SSTables.
func (m *memtable) Get(key []byte) (value []byte, deleted bool, found bool) {
	return nil, false, false
}

func (m *memtable) Put(key, value []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.insertOrDelete(key, value, false)
}

func (m *memtable) Delete(key []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.insertOrDelete(key, nil, true)
}

// Size returns the approximate number of bytes held by the memtable.
func (m *memtable) Size() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.size
}

// TODO
// NewIterator returns an iterator over all entries, tombstones included,
// in ascending key order.
func (m *memtable) NewIterator() *memIterator {
	return &memIterator{}
}

// TODO
// memIterator walks a memtable's entries in ascending key order.
// Typical use:
//
//	it := m.NewIterator()
//	for it.Next() {
//		k, v, tomb := it.Key(), it.Value(), it.Tombstone()
//	}
type memIterator struct{}

// TODO
// Next advances to the next entry and reports whether there is one.
// It must be called before the first Key/Value/Tombstone call.
func (it *memIterator) Next() bool {
	return false
}

// TODO
func (it *memIterator) Key() []byte {
	return nil
}

// TODO
func (it *memIterator) Value() []byte {
	return nil
}

// TODO
func (it *memIterator) Tombstone() bool {
	return false
}

// the caller must hold m.mu
func (m *memtable) rotateLeft(x *memNode) {
	// set y
	y := x.right
	// turn y's left subtree into x's right subtree
	x.right = y.left
	if y.left != m.leaf {
		y.left.parent = x
	}
	// link x's parent to y
	y.parent = x.parent
	if x.parent == m.leaf {
		m.root = y
	} else if x == x.parent.left {
		x.parent.left = y
	} else {
		x.parent.right = y
	}
	// put x on y's left
	y.left = x
	x.parent = y
}

// the caller must hold m.mu
func (m *memtable) rotateRight(x *memNode) {
	// set y
	y := x.left
	// turn y's right subtree into x's left subtree
	x.left = y.right
	if y.right != m.leaf {
		y.right.parent = x
	}
	// link x's parent to y
	y.parent = x.parent
	if x.parent == m.leaf {
		m.root = y
	} else if x == x.parent.right {
		x.parent.right = y
	} else {
		x.parent.left = y
	}
	// put x on y's right
	y.right = x
	x.parent = y
}

// the caller must hold m.mu
func (m *memtable) insertFixup(z *memNode) {
	for z.parent.color == red {
		if z.parent == z.parent.parent.left {
			y := z.parent.parent.right
			if y.color == red {
				z.parent.color = black
				y.color = black
				z.parent.parent.color = red
				z = z.parent.parent
			} else {
				if z == z.parent.right {
					z = z.parent
					m.rotateLeft(z)
				}
				z.parent.color = black
				z.parent.parent.color = red
				m.rotateRight(z.parent.parent)
			}
		} else {
			y := z.parent.parent.left
			if y.color == red {
				z.parent.color = black
				y.color = black
				z.parent.parent.color = red
				z = z.parent.parent
			} else {
				if z == z.parent.left {
					z = z.parent
					m.rotateRight(z)
				}
				z.parent.color = black
				z.parent.parent.color = red
				m.rotateLeft(z.parent.parent)
			}
		}
	}
	m.root.color = black
}

// insertOrDelete inserts an entry for key, or overwrites the existing one.
// A tombstone marks the key as deleted. The memtable stores copies of key
// and value. The caller must hold m.mu.
func (m *memtable) insertOrDelete(key []byte, value []byte, tombstone bool) {
	var compared int
	y := m.leaf
	x := m.root
	for x != m.leaf {
		y = x
		compared = bytes.Compare(key, x.key)
		// If the keys are the same then all we
		// need to do is update the value
		// and the tombstone and return early
		// since we're just modifying an existing node
		if compared == 0 {
			m.size += memNodeSize(key, value) - memNodeSize(key, x.value)
			x.tombstone = tombstone
			x.value = bytes.Clone(value)
			return
		}
		if compared < 0 {
			x = x.left
		} else {
			x = x.right
		}
	}
	n := &memNode{key: bytes.Clone(key), value: bytes.Clone(value), tombstone: tombstone}
	n.parent = y
	if y == m.leaf {
		m.root = n
	} else if compared < 0 {
		y.left = n
	} else {
		y.right = n
	}
	n.left = m.leaf
	n.right = m.leaf
	n.color = red
	m.size += memNodeSize(n.key, n.value)
	m.insertFixup(n)
}

const memNodeOverhead = int(unsafe.Sizeof(memNode{}))

// memNodeSize gets the approximate amount of memory allocated for the node
func memNodeSize(key, value []byte) int {
	return memNodeOverhead + len(key) + len(value)
}
