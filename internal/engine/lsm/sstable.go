// SSTable file format (all fixed-width integers little-endian):
//
//   data block:  repeated { uvarint keyLen | uvarint valLen | byte flags | key | value }
//   index block: repeated { uvarint keyLen | key | uint64 offset }
//   footer:      uint64 indexOffset | [8]byte magic "SPICEDB!"

package lsm

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
)

const (
	sstMagicNumber = "SPICEDB!"

	footerIndexOffsetPos       = 0
	footerMagicPos             = 8
	footerSize           int64 = 16
)

// All fixed-width integers in sstable files use this order. Never call
// binary.BigEndian/LittleEndian directly elsewhere in this file.
var byteOrder = binary.LittleEndian

type lookupResult int

const (
	notFound lookupResult = iota
	found
	foundTombstone
)

type indexEntry struct {
	key    []byte
	offset int64
	length int64
}

type sstReader struct {
	r     io.ReaderAt
	c     io.Closer // nil if caller manages the lifetime
	size  int64
	index []indexEntry
}

func openSSTable(r io.ReaderAt, size int64) (*sstReader, error) {
	if size < footerSize {
		return nil, errors.New("lsm: sstable too small")
	}

	footerOffset := size - footerSize

	footerBytes := make([]byte, footerSize)
	if _, err := r.ReadAt(footerBytes, footerOffset); err != nil {
		return nil, err
	}

	indexOffset, err := decodeFooter(footerBytes)
	if err != nil {
		return nil, err
	}

	indexSize := size - indexOffset - footerSize

	indexBytes := make([]byte, indexSize)
	if _, err := r.ReadAt(indexBytes, indexOffset); err != nil {
		return nil, err
	}

	var indexEntries []indexEntry
	var buf []byte
	for pos := 0; pos < int(indexSize); {
		keyLen, n := binary.Uvarint(buf[pos:])
		if n <= 0 {
			return nil, errors.New("lsm: corrupt index (keyLen)")
		}
		pos += n
		if uint64(len(buf)-pos) < keyLen+8 {
			return nil, errors.New("lsm: corrupt index (truncated)")
		}
		key := buf[pos : pos+int(keyLen)]
		pos += int(keyLen)
		offset := int64(byteOrder.Uint64(buf[pos:]))
		pos += 8
		indexEntries = append(indexEntries, indexEntry{key: key, offset: offset})
	}

	return &sstReader{r: r, c: nil, size: size, index: indexEntries}, nil
}

// openSSTableFile opens path and takes ownership of the file.
func openSSTableFile(path string) (*sstReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	t, err := openSSTable(f, fi.Size())
	if err != nil {
		t.c.Close()
		return nil, err
	}
	t.c = f
	return t, nil
}

// TODO
func (t *sstReader) get(key []byte) ([]byte, lookupResult, error)

// TODO
func writeSSTable(w io.Writer, iter entryIterator) error {
	return nil
}

// TODO
type sstIterator struct{}

// TODO
func newSSTableIterator(r io.ReaderAt, size int64) (*sstIterator, error)
func (it *sstIterator) Next() bool
func (it *sstIterator) Key() []byte
func (it *sstIterator) Value() []byte
func (it *sstIterator) Tombstone() bool
func (it *sstIterator) Err() error

func encodeFooter(indexOffset int64) [footerSize]byte {
	var buf [footerSize]byte
	byteOrder.PutUint64(buf[footerIndexOffsetPos:], uint64(indexOffset))
	copy(buf[footerMagicPos:], []byte(sstMagicNumber))
	return buf
}

func decodeFooter(buf []byte) (indexOffset int64, err error) {
	// first check the file integrity
	if string(buf[8:16]) != sstMagicNumber {
		return 0, errors.New("lsm: not sstable (bad magic)")
	}
	indexOffset = int64(byteOrder.Uint64(buf[0:8]))
	return indexOffset, nil
}
