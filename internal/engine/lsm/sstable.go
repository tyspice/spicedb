// SSTable file format (all fixed-width integers little-endian):
//
//   data block:  repeated { uvarint keyLen | uvarint valLen | flags | key | value }
//   index block: repeated { uvarint keyLen | key | uint64 offset }
//   footer:      uint64 indexOffset | [8]byte magic "SPICEDB!"

package lsm

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sort"
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

type flag int8

const (
	delete flag = iota
	put
)

type indexEntry struct {
	key    []byte
	offset int64
}

type sstReader struct {
	r           io.ReaderAt
	c           io.Closer // nil if caller manages the lifetime
	size        int64
	indexOffset int64
	index       []indexEntry
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
	for pos := 0; pos < int(indexSize); {
		keyLen, n := binary.Uvarint(indexBytes[pos:])
		if n <= 0 {
			return nil, errors.New("lsm: corrupt index (keyLen)")
		}
		pos += n
		if uint64(len(indexBytes)-pos) < keyLen+8 {
			return nil, errors.New("lsm: corrupt index (truncated)")
		}
		key := indexBytes[pos : pos+int(keyLen)]
		pos += int(keyLen)
		offset := int64(byteOrder.Uint64(indexBytes[pos:]))
		pos += 8
		indexEntries = append(indexEntries, indexEntry{key: key, offset: offset})
	}

	return &sstReader{r: r, c: nil, size: size, indexOffset: indexOffset, index: indexEntries}, nil
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

func (t *sstReader) get(key []byte) ([]byte, lookupResult, error) {
	i := sort.Search(len(t.index), func(i int) bool {
		return bytes.Compare(t.index[i].key, key) > 0
	})
	if i == 0 {
		return nil, notFound, nil
	}

	start := t.index[i-1].offset
	var end int64
	if i < len(t.index) {
		end = t.index[i].offset
	} else {
		end = t.indexOffset
	}

	dataBlock := make([]byte, end-start)
	if _, err := t.r.ReadAt(dataBlock, start); err != nil {
		return nil, notFound, err
	}

	for pos := 0; pos < len(dataBlock); {
		keyLen, n := binary.Uvarint(dataBlock[pos:])
		if n <= 0 {
			return nil, notFound, errors.New("lsm: corrupt data block (keyLen)")
		}
		pos += n

		valLen, n := binary.Uvarint(dataBlock[pos:])
		if n <= 0 {
			return nil, notFound, errors.New("lsm: corrupt data block (valLen)")
		}
		pos += n

		if pos >= len(dataBlock) {
			return nil, notFound, errors.New("lsm: corrupt data block (flags)")
		}
		flag := dataBlock[pos]
		pos++

		if uint64(len(dataBlock)-pos) < keyLen+valLen {
			return nil, notFound, errors.New("lsm: corrupt data block (truncated)")
		}

		k := dataBlock[pos : pos+int(keyLen)]
		pos += int(keyLen)

		switch c := bytes.Compare(k, key); {
		case c == 0:
			if flag == byte(delete) {
				return nil, foundTombstone, nil
			}
			return dataBlock[pos : pos+int(valLen)], found, nil
		case c > 0:
			return nil, notFound, nil
		}
		pos += int(valLen)
	}

	return nil, notFound, nil
}

func (t *sstReader) close() error {
	if t.c == nil {
		return nil
	}
	return t.c.Close()
}

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
