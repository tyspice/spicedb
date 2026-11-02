// SSTable file format (all fixed-width integers little-endian):
//
//   data block:  repeated data entry (see encoding.go)
//   index block: repeated { uvarint keyLen | key | uint64 offset }
//   filter block : TBD
//   footer:      uint64 indexOffset | uint64 filterOffset | [8]byte magic "SPICEDB!"

package lsm

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sort"
)

const (
	sstMagicNumber             = "SPICEDB!"
	footerIndexOffsetPos       = 0
	footerFilterPos            = 8
	footerMagicPos             = 16
	footerSize           int64 = 24
	blockSize                  = 4096 // 4KB
)

type lookupResult int

const (
	notFound lookupResult = iota
	found
	foundTombstone
)

type indexEntry struct {
	key    []byte
	offset int64
	size   int64
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

	indexOffset, filterOffset, err := decodeFooter(footerBytes)
	if err != nil {
		return nil, err
	}

	if indexOffset < 0 || filterOffset < indexOffset || filterOffset > size-footerSize {
		return nil, errors.New("lsm: corrupt footer")
	}

	indexSize := filterOffset - indexOffset

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

	for i := range indexEntries {
		end := indexOffset
		if i+1 < len(indexEntries) {
			end = indexEntries[i+1].offset
		}
		indexEntries[i].size = end - indexEntries[i].offset
		if indexEntries[i].offset < 0 || indexEntries[i].size < 0 {
			return nil, errors.New("lsm: corrupt index (offsets)")
		}
	}

	// TODO: generate an in memory filter from the filter section

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
		f.Close()
		return nil, err
	}
	t.c = f
	return t, nil
}

func (t *sstReader) Get(key []byte) ([]byte, lookupResult, error) {
	i := sort.Search(len(t.index), func(i int) bool {
		return bytes.Compare(t.index[i].key, key) > 0
	})
	if i == 0 {
		return nil, notFound, nil
	}

	e := t.index[i-1]
	dataBlock := make([]byte, e.size)
	if _, err := t.r.ReadAt(dataBlock, e.offset); err != nil {
		return nil, notFound, err
	}

	for pos := 0; pos < len(dataBlock); {
		k, v, kind, n, err := decodeDataEntry(dataBlock[pos:])
		if err != nil {
			return nil, notFound, err
		}

		switch c := bytes.Compare(k, key); {
		case c == 0:
			if kind == kindDelete {
				return nil, foundTombstone, nil
			}
			return v, found, nil
		case c > 0:
			return nil, notFound, nil
		}
		pos += n
	}

	return nil, notFound, nil
}

func (t *sstReader) Close() error {
	if t.c == nil {
		return nil
	}
	return t.c.Close()
}

// This function doesn't sync the resultant file so the
// caller is responsible for ensuring file durability before closing
func writeSSTable(w io.Writer, iter entryIterator) error {
	bw := bufio.NewWriter(w)
	indexEntries := []indexEntry{}
	offset := 0
	currentBlock := 0

	var buf []byte
	for iter.Next() {
		key, value, tombstone := iter.Key(), iter.Value(), iter.Tombstone()
		// TODO add key to bloom filter
		if offset == 0 || currentBlock >= blockSize {
			indexEntries = append(indexEntries, indexEntry{key: bytes.Clone(key), offset: int64(offset)})
			currentBlock = 0
		}

		kind := kindPut
		if tombstone {
			kind = kindDelete
		}

		buf = appendDataEntry(buf[:0], key, value, kind)
		if _, err := bw.Write(buf); err != nil {
			return err
		}

		offset += len(buf)
		currentBlock += len(buf)
	}
	if err := iter.Err(); err != nil {
		return err
	}
	indexOffset := uint64(offset)

	for _, e := range indexEntries {
		buf = binary.AppendUvarint(buf[:0], uint64(len(e.key)))
		buf = append(buf, e.key...)
		buf = byteOrder.AppendUint64(buf, uint64(e.offset))
		bw.Write(buf)
		offset += len(buf)
	}
	filterOffset := uint64(offset)

	// TODO: append m, k, and bits of bloom filter

	bw.Write(appendFooter(buf[:0], indexOffset, filterOffset))
	return bw.Flush()
}

type sstIterator struct {
	sstr         *sstReader
	nextIndexPos int
	dataBuff     []byte
	offset       int
	key          []byte
	value        []byte
	tombstone    bool
	err          error
}

func (t *sstReader) NewIterator() *sstIterator {
	return &sstIterator{sstr: t}
}

func (it *sstIterator) Next() bool {
	if it.err != nil {
		return false
	}

	// if we're at the end of the data buffer then load the next block
	if it.offset >= len(it.dataBuff) {
		if it.nextIndexPos >= len(it.sstr.index) {
			return false
		}
		e := it.sstr.index[it.nextIndexPos]
		// This adds an extra allocation per index entry so there's an opportunity to optimize
		buf := make([]byte, e.size)
		if _, err := it.sstr.r.ReadAt(buf, e.offset); err != nil {
			it.err = err
			return false
		}
		it.dataBuff = buf
		it.offset = 0
		it.nextIndexPos += 1
	}

	k, v, kind, n, err := decodeDataEntry(it.dataBuff[it.offset:])
	if err != nil {
		it.err = err
		return false
	}

	it.key = k
	it.value = v
	it.tombstone = kind == kindDelete
	it.offset += n

	return true
}

func (it *sstIterator) Key() []byte {
	return it.key
}

func (it *sstIterator) Value() []byte {
	return it.value
}

func (it *sstIterator) Tombstone() bool {
	return it.tombstone
}

func (it *sstIterator) Err() error {
	return it.err
}

func appendFooter(b []byte, indexOffset uint64, filterOffset uint64) []byte {
	b = byteOrder.AppendUint64(b, indexOffset)
	b = byteOrder.AppendUint64(b, filterOffset)
	return append(b, sstMagicNumber...)
}

func decodeFooter(buf []byte) (indexOffset int64, filterOffset int64, err error) {
	// first check the file integrity
	if string(buf[footerMagicPos:footerSize]) != sstMagicNumber {
		return 0, 0, errors.New("lsm: not sstable (bad magic)")
	}
	indexOffset = int64(byteOrder.Uint64(buf[footerIndexOffsetPos:footerFilterPos]))
	filterOffset = int64(byteOrder.Uint64(buf[footerFilterPos:footerMagicPos]))
	return indexOffset, filterOffset, nil
}
