// lsm implementation
package lsm

import (
	"context"
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
	"time"

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
	mu      sync.RWMutex
	n       uint64
	dir     string
	frozen  bool
	flushed bool
	mem     *memtable
	wal     *walWriter
}

func newLSMWriter(dir string, f walFile, n uint64) *lsmWriter {
	return &lsmWriter{n: n, dir: dir, mem: newMemtable(), wal: newWalWriter(f)}
}

func (lsmW *lsmWriter) Get(key []byte) ([]byte, lookupResult) {
	lsmW.mu.RLock()
	defer lsmW.mu.RUnlock()
	return lsmW.mem.Get(key)
}

func (lsmW *lsmWriter) Put(key, value []byte) (mSize int, _ error) {
	return lsmW.addEntry(key, value, kindPut)
}

func (lsmW *lsmWriter) Delete(key []byte) (mSize int, _ error) {
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

func (lsmW *lsmWriter) Flush() (sstFilePath string, err error) {
	// closeAndDeleteWal is idempotent: a WAL that is already closed or
	// already deleted counts as success, and the remove is attempted
	// even if the close failed, so a retry can finish a partial cleanup.
	closeAndDeleteWal := func() error {
		closeErr := lsmW.wal.Close()
		if errors.Is(closeErr, os.ErrClosed) {
			closeErr = nil
		}

		walFilePath := filepath.Join(lsmW.dir, dataFileName(lsmW.n, walExt))
		removeErr := os.Remove(walFilePath)
		if errors.Is(removeErr, os.ErrNotExist) {
			removeErr = nil
		}

		return errors.Join(closeErr, removeErr)
	}

	lsmW.mu.Lock()
	defer lsmW.mu.Unlock()

	if !lsmW.frozen {
		lsmW.frozen = true
	}

	// once the sstable is durable the memtable never needs to be
	// written again; retries only need to finish the WAL cleanup
	if !lsmW.flushed {
		if _, err = flushMemtableToSSTable(lsmW.dir, lsmW.mem, lsmW.n); err != nil {
			return "", err
		}
		lsmW.flushed = true
	}

	if err = closeAndDeleteWal(); err != nil {
		return "", err
	}

	return filepath.Join(lsmW.dir, dataFileName(lsmW.n, sstExt)), nil
}

func (lsmW *lsmWriter) addEntry(key, value []byte, kind entryKind) (mSize int, _ error) {
	lsmW.mu.Lock()
	defer lsmW.mu.Unlock()

	if lsmW.frozen {
		return 0, errors.New("lsm: writer is frozen")
	}

	if err := lsmW.wal.AddEntrySync(key, value, kind); err != nil {
		return 0, err
	}

	if kind == kindDelete {
		lsmW.mem.Delete(key)
	} else {
		lsmW.mem.Put(key, value)
	}

	return lsmW.mem.Size(), nil
}

// Compile-time check that *lsm implements engine.Engine.
var _ engine.Engine = (*lsm)(nil)

type Options struct {
	CompactionInterval time.Duration
	maxMemtableSize    int
}

type lsm struct {
	mu               sync.RWMutex
	lockFile         *os.File
	dataDir          string
	nextFileNum      uint64
	sstReaders       []*sstReader
	frozenWriters    []*lsmWriter
	activeWriter     *lsmWriter
	cancelCompaction context.CancelFunc
	wg               sync.WaitGroup
	opts             Options
}

// TODO: break up function so that file close handling on error is somewhat sane
func Open(dir string, opts Options) (_ engine.Engine, err error) {
	l := &lsm{}
	defer func() {
		if err != nil {
			l.Close()
		}
	}()

	// set default options if not provided
	if opts.CompactionInterval <= 0 {
		opts.CompactionInterval = 5 * time.Minute
	}

	if opts.maxMemtableSize <= 0 {
		opts.maxMemtableSize = 4 << 20 // 4 MiB
	}

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

		if _, err = flushMemtableToSSTable(absDataDirPath, mt, n); err != nil {
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

	walFile, err := createWalFile(absDataDirPath, nextFileNumber)
	if err != nil {
		return nil, err
	}

	l.activeWriter = newLSMWriter(absDataDirPath, walFile, nextFileNumber)
	nextFileNumber += 1

	l.dataDir = absDataDirPath
	l.nextFileNum = nextFileNumber
	l.opts = opts

	ctx, cancel := context.WithCancel(context.Background())
	l.cancelCompaction = cancel
	l.wg.Add(1)
	go func() {
		ticker := time.NewTicker(opts.CompactionInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				l.wg.Done()
				return
			case <-ticker.C:
				// TODO: implement compaction logic here
			}
		}
	}()

	return l, nil
}

func (l *lsm) Get(key []byte) ([]byte, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	result := func(v []byte, res lookupResult, err error) ([]byte, error) {
		if err != nil || res == foundTombstone {
			return nil, err
		}
		return v, err
	}

	// first check the active writer
	if v, res := l.activeWriter.Get(key); res != notFound {
		return result(v, res, nil)
	}

	// then check the frozen writers back to front
	for _, w := range slices.Backward(l.frozenWriters) {
		if v, res := w.Get(key); res != notFound {
			return result(v, res, nil)
		}
	}

	// then check the sstables back to front
	for _, r := range slices.Backward(l.sstReaders) {
		if r.MayContain(key) {
			if v, res, err := r.Get(key); res != notFound || err != nil {
				return result(v, res, err)
			}
		}
	}
	return nil, nil
}

func (l *lsm) Put(key, value []byte) error {
	return l.addEntry(key, value, kindPut)
}

func (l *lsm) Delete(key []byte) error {
	return l.addEntry(key, nil, kindDelete)
}

func (l *lsm) addEntry(key, value []byte, kind entryKind) (err error) {
	// activeWriter has its own mutex to lock writes
	// we use an RLock here because the top level
	// lsm mutex should lock when actual field
	// objects on the lsm change
	l.mu.RLock()
	w := l.activeWriter
	var mSize int
	switch kind {
	case kindPut:
		mSize, err = w.Put(key, value)
	case kindDelete:
		mSize, err = w.Delete(key)
	}
	l.mu.RUnlock()

	if err != nil {
		return err
	}

	// if the memtable has exceeded the maxMemtableSize
	// then we rotate it out and spin up a new memtable
	if mSize >= l.opts.maxMemtableSize {
		l.mu.Lock()
		// if the instances are different it means that another
		// goroutine already rotated the table out so noothing to do
		if w == l.activeWriter {
			n := l.nextFileNum
			f, err := createWalFile(l.dataDir, n)
			if err != nil {
				l.mu.Unlock()
				return fmt.Errorf("lsm: rotate memtable: %w", err)
			}
			w.Freeze()
			l.frozenWriters = append(l.frozenWriters, w)
			l.activeWriter = newLSMWriter(l.dataDir, f, n)
			l.nextFileNum += 1
		}
		l.mu.Unlock()
		// after rotation we flush the frozen memtables
		// in the background so that doesn't hold up the
		// response
		// FIXME: race: this can technically still add to wg during a wg.Wait() on close.
		l.wg.Go(func() {
			// TODO: capture the error return and log it once we have a logger
			l.flushFrozenWriters()
		})
	}
	return nil
}

func (l *lsm) flushFrozenWriters() error {
	formattedErr := func(err error) error {
		return fmt.Errorf("lsm: flush frozen writers: %w", err)
	}

	// TODO: This locks for the whole flush process. Break up the locks
	// so it's onoly holding up the engine for the actual swaps that it
	// needs to do.
	l.mu.Lock()
	defer l.mu.Unlock()

	for len(l.frozenWriters) > 0 {
		w := l.frozenWriters[0]
		sstFilePath, err := w.Flush()
		if err != nil {
			return formattedErr(err)
		}

		sstReader, err := openSSTableFile(sstFilePath)
		if err != nil {
			return formattedErr(err)
		}

		l.sstReaders = append(l.sstReaders, sstReader)
		l.frozenWriters[0] = nil
		l.frozenWriters = l.frozenWriters[1:]
	}
	return nil
}

func (l *lsm) Close() error {
	defer l.lockFile.Close()

	l.mu.Lock()
	cancel := l.cancelCompaction
	l.cancelCompaction = nil
	l.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	l.wg.Wait()

	l.mu.Lock()
	defer l.mu.Unlock()

	for _, r := range l.sstReaders {
		r.Close()
	}
	l.sstReaders = nil

	// TODO: close the activeWriter and any frozenWriters that were never flushed
	// so their WAL files aren't left open.

	return nil
}

func flushMemtableToSSTable(dir string, mt *memtable, n uint64) (sstFilePath string, err error) {
	tmpSSTPath := filepath.Join(dir, dataFileName(n, tmpExt))
	sstFile, err := os.Create(tmpSSTPath)
	if err != nil {
		return "", err
	}

	dataDir, err := os.Open(dir)
	if err != nil {
		return "", err
	}
	defer dataDir.Close()

	if err := writeSSTable(sstFile, mt.NewIterator()); err != nil {
		sstFile.Close()
		return "", err
	}
	if err := sstFile.Sync(); err != nil {
		sstFile.Close()
		return "", err
	}
	if err := sstFile.Close(); err != nil {
		return "", err
	}

	sstFilePath = filepath.Join(dir, dataFileName(n, sstExt))
	if err := os.Rename(tmpSSTPath, sstFilePath); err != nil {
		return "", err
	}

	if err := dataDir.Sync(); err != nil {
		return "", err
	}

	return sstFilePath, nil
}

func createWalFile(dir string, n uint64) (*os.File, error) {
	path := filepath.Join(dir, dataFileName(n, walExt))
	_, err := os.Stat(path)
	if !errors.Is(err, os.ErrNotExist) {
		return nil, errors.New("lsm: wal file already exists")
	}

	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	return f, nil
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
