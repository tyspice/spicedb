// Data entry encoding, shared by sstable data blocks and WAL records:
//
//   kind | uvarint keyLen | uvarint valLen | key | value

package lsm

import (
	"encoding/binary"
	"errors"
)

// All fixed-width integers use this order. Never call
// binary.BigEndian/LittleEndian directly elsewhere in this package.
var byteOrder = binary.LittleEndian

type entryKind byte

const (
	kindDelete entryKind = iota
	kindPut
	numEntryKinds // must be last
)

func appendDataEntry(b, key, value []byte, kind entryKind) []byte {
	b = append(b, byte(kind))
	b = binary.AppendUvarint(b, uint64(len(key)))
	b = binary.AppendUvarint(b, uint64(len(value)))
	b = append(b, key...)
	return append(b, value...)
}

func decodeDataEntry(dataBlock []byte) (key []byte, value []byte, kind entryKind, bytesRead int, err error) {
	if len(dataBlock) == 0 {
		return nil, nil, kindDelete, 0, errors.New("lsm: corrupt data block (empty)")
	}

	kind = entryKind(dataBlock[0])
	if kind >= numEntryKinds {
		return nil, nil, kindDelete, 0, errors.New("lsm: corrupt data block (kind)")
	}
	pos := 1

	keyLen, n := binary.Uvarint(dataBlock[pos:])
	if n <= 0 {
		return nil, nil, kindDelete, 0, errors.New("lsm: corrupt data block (keyLen)")
	}
	pos += n

	valLen, n := binary.Uvarint(dataBlock[pos:])
	if n <= 0 {
		return nil, nil, kindDelete, 0, errors.New("lsm: corrupt data block (valLen)")
	}
	pos += n

	rem := uint64(len(dataBlock) - pos)
	if keyLen > rem || valLen > rem-keyLen {
		return nil, nil, kindDelete, 0, errors.New("lsm: corrupt data block (truncated)")
	}

	key = dataBlock[pos : pos+int(keyLen)]
	pos += int(keyLen)

	value = dataBlock[pos : pos+int(valLen)]
	pos += int(valLen)

	return key, value, kind, pos, nil
}
