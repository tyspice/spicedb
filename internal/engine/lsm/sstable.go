package lsm

import (
	"encoding/binary"
	"errors"
	"io"
)

const (
	// TODO: move this out of code after development
	// 8 byte magic number
	sstMagicNumber = "SPICEDB!"
	footerSize     = 16
)

var byteOrder = binary.LittleEndian

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

func checkFileAndRetrieveIndexOffset(r io.ReaderAt, size int64) (offset int64, err error) {
	if size < footerSize {
		return 0, errors.New("lsm: sstable too small")
	}

	footer := make([]byte, footerSize)
	if _, err := r.ReadAt(footer, size-footerSize); err != nil {
		return 0, err
	}

	// first check the file integrity
	if string(footer[8:16]) != sstMagicNumber {
		return 0, errors.New("lsm: not sstable (bad magic)")
	}

	offset = int64(byteOrder.Uint64(footer[0:8]))
	if offset < 0 || offset > size-footerSize {
		return 0, errors.New("lsm: corrupt sstable footer (bad index offset)")
	}

	return offset, nil
}
