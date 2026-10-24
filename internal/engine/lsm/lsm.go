// lsm implementation
package lsm

import "github.com/tyspice/spicedb/internal/engine"

type entryIterator interface {
	Next() bool
	Key() []byte
	Value() []byte
	Tombstone() bool
	Err() error
}

// Compile-time check that *lsm implements engine.Engine.
var _ engine.Engine = (*lsm)(nil)

func New() engine.Engine {
	return &lsm{}
}

type lsm struct{}

func (l *lsm) Get(key []byte) ([]byte, error) {
	return nil, nil
}

func (l *lsm) Put(key, value []byte) error {
	return nil
}

func (l *lsm) Delete(key []byte) error {
	return nil
}

func (l *lsm) Close() error {
	return nil
}
