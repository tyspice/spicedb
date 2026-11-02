package lsm

import (
	"bytes"
	"container/heap"
)

type mergeSource struct {
	it  entryIterator
	idx int // position in sources; lower = newer
}

type sourceHeap []*mergeSource

func (h *sourceHeap) Len() int {
	return len(*h)
}

func (h *sourceHeap) Swap(i, j int) {
	(*h)[i], (*h)[j] = (*h)[j], (*h)[i]
}

func (h *sourceHeap) Less(i, j int) bool {
	if c := bytes.Compare((*h)[i].it.Key(), (*h)[j].it.Key()); c != 0 {
		return c < 0
	}
	return (*h)[i].idx < (*h)[j].idx
}

func (h *sourceHeap) Push(x any) {
	*h = append(*h, x.(*mergeSource))
}

func (h *sourceHeap) Pop() any {
	old := *h
	s := old[len(old)-1]
	*h = old[:len(old)-1]
	return s
}

// must not be called on an empty heap
func (h *sourceHeap) peek() *mergeSource {
	return (*h)[0]
}

// Only sources that have a current entry are in the heap. An exhausted source just isn't pushed back.
// The iterator. pending holds the sources used by the last Next(): the winner plus any shadowed duplicates. They aren't advanced until the next call.

type mergeIterator struct {
	h         sourceHeap
	pending   []*mergeSource
	key       []byte
	value     []byte
	tombstone bool
	err       error
}

func newMergeIterator(sources []entryIterator) *mergeIterator {
	it := &mergeIterator{}
	for i, src := range sources {
		it.advance(&mergeSource{it: src, idx: i}) // prime: call Next() once, push if it has an entry
	}
	return it
}

// advance moves s forward and puts it back in the heap if it has an entry.
func (it *mergeIterator) advance(s *mergeSource) {
	if s.it.Next() {
		heap.Push(&it.h, s)
	} else if err := s.it.Err(); err != nil {
		it.err = err
	}
}

func (it *mergeIterator) Next() bool {
	if it.err != nil {
		return false
	}
	// 1. Advance everything consumed last time.
	for _, s := range it.pending {
		it.advance(s)
	}
	it.pending = it.pending[:0]
	if it.err != nil || it.h.Len() == 0 {
		return false
	}
	// 2. Smallest key, newest source wins.
	winner := heap.Pop(&it.h).(*mergeSource)
	it.pending = append(it.pending, winner)
	// 3. Pull out older versions of the same key so they're skipped.
	for it.h.Len() > 0 && bytes.Equal(it.h.peek().it.Key(), winner.it.Key()) {
		it.pending = append(it.pending, heap.Pop(&it.h).(*mergeSource))
	}
	it.key, it.value, it.tombstone = winner.it.Key(), winner.it.Value(), winner.it.Tombstone()
	return true
}

func (it *mergeIterator) Key() []byte {
	return it.key
}

func (it *mergeIterator) Value() []byte {
	return it.value
}

func (it *mergeIterator) Tombstone() bool {
	return it.tombstone
}

func (it *mergeIterator) Err() error {
	return it.err
}
