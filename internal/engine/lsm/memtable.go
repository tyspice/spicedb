package lsm

import "sync"

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

// Get looks up key. found reports whether the memtable has an entry for key;
// deleted reports whether that entry is a tombstone. When found is false,
// the caller should check older memtables and SSTables.
func (m *memtable) Get(key []byte) (value []byte, deleted bool, found bool) {
	return nil, false, false
}

func (m *memtable) Put(key, value []byte) {

}

func (m *memtable) Delete(key []byte) {

}

// Size returns the approximate number of bytes held by the memtable.
func (m *memtable) Size() int {
	return 0
}

// NewIterator returns an iterator over all entries, tombstones included,
// in ascending key order.
func (m *memtable) NewIterator() *memIterator {
	return &memIterator{}
}

// memIterator walks a memtable's entries in ascending key order.
// Typical use:
//
//	it := m.NewIterator()
//	for it.Next() {
//		k, v, tomb := it.Key(), it.Value(), it.Tombstone()
//	}
type memIterator struct{}

// Next advances to the next entry and reports whether there is one.
// It must be called before the first Key/Value/Tombstone call.
func (it *memIterator) Next() bool {
	return false
}

func (it *memIterator) Key() []byte {
	return nil
}

func (it *memIterator) Value() []byte {
	return nil
}

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
