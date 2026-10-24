package lsm

// Outline for the merge iterator
type mergeIterator struct{}

func newMergeIterator(sources []entryIterator) *mergeIterator
func (it *mergeIterator) Next() bool
func (it *mergeIterator) Key() []byte
func (it *mergeIterator) Value() []byte
func (it *mergeIterator) Tombstone() bool
func (it *mergeIterator) Err() error
