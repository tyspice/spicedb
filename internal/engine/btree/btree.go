// btree implementation
package btree

import "github.com/tyspice/spicedb/internal/engine"

// Compile-time check that *btree implements engine.Engine.
var _ engine.Engine = (*btree)(nil)

func New() engine.Engine {
	return &btree{}
}

type btree struct{}

func (b *btree) Get(key []byte) ([]byte, error) {
	return nil, nil
}

func (b *btree) Put(key, value []byte) error {
	return nil
}

func (b *btree) Delete(key []byte) error {
	return nil
}

func (b *btree) Close() error {
	return nil
}
