// crc     : uint32   # CRC-32C of length + payload
// length  : uint32   # number of payload bytes
// payload : []byte   # one data entry

package lsm

import (
	"hash/crc32"
	"io"
	"sync"
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

const (
	walHeaderSize     = 8
	maxRetainedWalBuf = 64 << 10 // 64 KiB
)

type walFile interface {
	io.Writer
	Sync() error
	Close() error
}

type walWriter struct {
	mu sync.Mutex
	f  walFile
	b  []byte
}

func newWalWriter(f walFile) *walWriter {
	return &walWriter{f: f}
}

func (w *walWriter) AddEntrySync(key, val []byte, kind entryKind) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	b := append(w.b[:0], make([]byte, walHeaderSize)...)
	b = appendDataEntry(b, key, val, kind)
	byteOrder.PutUint32(b[4:8], uint32(len(b)-walHeaderSize))
	byteOrder.PutUint32(b[0:4], crc32.Checksum(b[4:], crcTable))
	if cap(b) <= maxRetainedWalBuf {
		w.b = b
	} else {
		w.b = nil
	}

	if _, err := w.f.Write(b); err != nil {
		return err
	}

	return w.f.Sync()
}

func (w *walWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.f.Close()
}
