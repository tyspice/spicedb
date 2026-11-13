// lsm implementation
package lsm

import (
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"github.com/tyspice/spicedb/internal/engine"
)

const (
	sstExt       = ".sst"
	walExt       = ".wal"
	tmpExt       = ".tmp"
	lockFileName = "LOCK"
)

type entryIterator interface {
	Next() bool
	Key() []byte
	Value() []byte
	Tombstone() bool
	Err() error
}

type lsmWriter struct {
	mu     sync.RWMutex
	n      uint64
	dir    string
	frozen bool
	mem    *memtable
	wal    *walWriter
}

func newLSMWriter(dir string, f walFile, n uint64) *lsmWriter {
	return &lsmWriter{n: n, dir: dir, mem: newMemtable(), wal: newWalWriter(f)}
}

func (lsmW *lsmWriter) Get(key []byte) ([]byte, lookupResult) {
	lsmW.mu.RLock()
	defer lsmW.mu.RUnlock()
	return lsmW.mem.Get(key)
}

func (lsmW *lsmWriter) Put(key, value []byte) error {
	return lsmW.addEntry(key, value, kindPut)
}

func (lsmW *lsmWriter) Delete(key []byte) error {
	return lsmW.addEntry(key, nil, kindDelete)
}

func (lsmW *lsmWriter) Freeze() {
	lsmW.mu.Lock()
	defer lsmW.mu.Unlock()
	lsmW.frozen = true
}

func (lsmW *lsmWriter) Close() error {
	lsmW.mu.Lock()
	defer lsmW.mu.Unlock()
	lsmW.wal.Close()
	return nil
}

func (lsmW *lsmWriter) Flush() error {
	lsmW.mu.Lock()
	defer lsmW.mu.Unlock()

	if !lsmW.frozen {
		lsmW.frozen = true
	}

	if err := flushMemtableToSSTable(lsmW.dir, lsmW.mem, lsmW.n); err != nil {
		return err
	}

	if err := lsmW.wal.Close(); err != nil {
		return err
	}

	walFilePath := filepath.Join(lsmW.dir, dataFileName(lsmW.n, walExt))
	if err := os.Remove(walFilePath); err != nil {
		return err
	}

	return nil
}

func (lsmW *lsmWriter) addEntry(key, value []byte, kind entryKind) error {
	lsmW.mu.Lock()
	defer lsmW.mu.Unlock()

	if lsmW.frozen {
		return errors.New("lsm: writer is frozen")
	}

	if err := lsmW.wal.AddEntrySync(key, value, kind); err != nil {
		return err
	}

	if kind == kindDelete {
		lsmW.mem.Delete(key)
	} else {
		lsmW.mem.Put(key, value)
	}

	return nil
}

// Compile-time check that *lsm implements engine.Engine.
var _ engine.Engine = (*lsm)(nil)

type Options struct{}

type lsm struct {
	lockFile    *os.File
	dataDir     string
	nextFileNum uint64
	sstmu       sync.RWMutex
	sstReaders  []*sstReader
}

// TODO - [x] add something that cleans up left over tmp files
// TODO - [x] if there's already an sst file with the same name as the number as the wal just delete the wal
// TODO - [x] open an sstReader for every sst file (including ones created by wal replay), ordered highest number first
// TODO - [ ] create the active wal (nextFileNum), its walWriter, and an empty memtable and store them on l
// TODO - [ ] add a start compaction process step before returning (Close must stop it and wait for it)
// TODO - [ ] break up function so that file close handling on error is somewhat sane
func Open(dir string, opts *Options) (_ engine.Engine, err error) {
	l := &lsm{}
	defer func() {
		if err != nil {
			l.Close()
		}
	}()

	// take control of lock file and set up directory

	absDataDirPath, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}

	if err = os.MkdirAll(absDataDirPath, 0o755); err != nil {
		return nil, err
	}

	lockFile, err := os.OpenFile(filepath.Join(absDataDirPath, lockFileName), os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	l.lockFile = lockFile

	if err = syscall.Flock(int(lockFile.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return nil, err
	}

	dataFiles, err := os.ReadDir(absDataDirPath)
	if err != nil {
		return nil, err
	}

	var nextFileNumber uint64 = 0
	var walNumbers []uint64
	sstMap := make(map[uint64]bool)

	// get the highest file number and all of the current
	// wal files to that we can rebuild their mem tables
	for _, f := range dataFiles {
		if f.Name() == lockFileName {
			continue
		}

		num, ext, err := parseDataFileName(f.Name())
		if err != nil {
			return nil, err
		}

		if ext == sstExt || ext == walExt {
			nextFileNumber = max(nextFileNumber, num)

			if ext == walExt {
				walNumbers = append(walNumbers, num)
			}

			if ext == sstExt {
				sstMap[num] = true
			}
		}

		// remove any tmp files that may have been left over from a crash or improper shutdown
		if ext == tmpExt {
			// best effort. we don't care that much if there was an error on this cleanup pass
			_ = os.Remove(filepath.Join(absDataDirPath, dataFileName(num, ext)))
		}
	}
	nextFileNumber += 1
	slices.Sort(walNumbers)

	for _, n := range walNumbers {
		walFilePath := filepath.Join(absDataDirPath, dataFileName(n, walExt))

		// if there's already an sst file with the same number as the wal, just delete the wal
		// since the sst file is the result of a successful wal replay, we don't need to do it again.
		if sstMap[n] {
			if err := os.Remove(walFilePath); err != nil {
				return nil, err
			}
			continue
		}

		walFile, err := os.Open(walFilePath)
		if err != nil {
			return nil, err
		}

		mt, _, err := recoverWal(walFile)
		if err != nil {
			return nil, err
		}

		if err = flushMemtableToSSTable(absDataDirPath, mt, n); err != nil {
			return nil, err
		}

		if err = walFile.Close(); err != nil {
			return nil, err
		}

		if err := os.Remove(walFilePath); err != nil {
			return nil, err
		}
	}

	sstNumbers := slices.Sorted(maps.Keys(sstMap))
	for _, n := range slices.Backward(sstNumbers) {
		path := filepath.Join(absDataDirPath, dataFileName(n, sstExt))
		r, err := openSSTableFile(path)
		if err != nil {
			return nil, err
		}
		l.sstReaders = append(l.sstReaders, r)
	}

	l.dataDir = absDataDirPath
	l.nextFileNum = nextFileNumber

	return l, nil
}

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
	defer l.lockFile.Close()

	l.sstmu.Lock()
	defer l.sstmu.Unlock()
	for _, r := range l.sstReaders {
		r.Close()
	}
	l.sstReaders = nil

	return nil
}

func flushMemtableToSSTable(dir string, mt *memtable, n uint64) error {
	tmpSSTPath := filepath.Join(dir, dataFileName(n, tmpExt))
	sstFile, err := os.Create(tmpSSTPath)
	if err != nil {
		return err
	}

	dataDir, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer dataDir.Close()

	if err := writeSSTable(sstFile, mt.NewIterator()); err != nil {
		sstFile.Close()
		return err
	}
	if err := sstFile.Sync(); err != nil {
		sstFile.Close()
		return err
	}
	if err := sstFile.Close(); err != nil {
		return err
	}

	if err := os.Rename(tmpSSTPath, filepath.Join(dir, dataFileName(n, sstExt))); err != nil {
		return err
	}
	if err := dataDir.Sync(); err != nil {
		return err
	}

	return nil
}

func parseDataFileName(fileName string) (uint64, string, error) {
	base := filepath.Base(fileName)
	ext := filepath.Ext(base)
	naked := strings.TrimSuffix(base, ext)
	num, err := strconv.ParseUint(naked, 10, 64)
	if err != nil {
		return 0, "", fmt.Errorf("lsm: malformed data file %q: %w", base, err)
	}
	if base != dataFileName(num, ext) {
		return 0, "", fmt.Errorf("lsm: malformed data file %q: non-canonical name", base)
	}
	return num, ext, nil
}

func dataFileName(n uint64, ext string) string {
	return fmt.Sprintf("%06d%s", n, ext)
}
