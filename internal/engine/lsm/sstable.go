package lsm

import "io"

func readSSTableKey(r io.ReaderAt, size int64, key []byte) (value []byte, deleted bool, found bool, err error) {
	return nil, false, false, nil
}

func writeSSTable(w io.Writer, iter entryIterator) error {
	return nil
}

type sstIterator struct{}

func newSSTableIterator(r io.ReaderAt, size int64) (*sstIterator, error)
func (it *sstIterator) Next() bool
func (it *sstIterator) Key() []byte
func (it *sstIterator) Value() []byte
func (it *sstIterator) Tombstone() bool
func (it *sstIterator) Err() error
