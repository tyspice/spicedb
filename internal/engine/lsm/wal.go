// crc     : uint32   # CRC-32C of length + payload
// length  : uint32   # number of payload bytes
// payload : []byte   # one data entry

package lsm

import (
	"bufio"
	"errors"
	"hash/crc32"
	"io"
	"sync"
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

const (
	walHeaderSize     = 8
	maxRetainedWalBuf = 64 << 10 // 64 KiB
	maxWalPayload     = 16 << 20 // 16 MiB
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
	if len(b)-walHeaderSize > maxWalPayload {
		return errors.New("lsm: payload exceeds max value (wal)")
	}

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

func recoverWal(r io.Reader) (*memtable, int64, error) {
	mt := newMemtable()
	br := bufio.NewReader(r)
	var end int64 = 0

	hdr := [walHeaderSize]byte{}
	for {
		if _, err := io.ReadFull(br, hdr[:]); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				// torn record or just EOF
				break
			}
			return nil, 0, err
		}

		want := byteOrder.Uint32(hdr[0:4]) // checksum
		length := byteOrder.Uint32(hdr[4:8])
		if length > maxWalPayload {
			// torn record
			break
		}

		payload := make([]byte, length)
		if _, err := io.ReadFull(br, payload); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				// torn record
				break
			}
			return nil, 0, err
		}

		got := crc32.Checksum(hdr[4:8], crcTable)
		got = crc32.Update(got, crcTable, payload)
		if want != got {
			// torn record
			break
		}

		k, v, kind, _, err := decodeDataEntry(payload)
		if err != nil {
			return nil, 0, err
		}

		switch kind {
		case kindPut:
			mt.Put(k, v)
		case kindDelete:
			mt.Delete(k)
		default:
			return nil, 0, errors.New("lsm: bad wal value (kind)")
		}
		end += walHeaderSize + int64(length)
	}
	return mt, end, nil
}
