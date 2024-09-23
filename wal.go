package wal

import (
	"encoding/binary"
	errs "errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/go-stdlog/stdlog"
	"github.com/shirou/gopsutil/v3/process"

	"github.com/heyvito/wal/errors"
	"github.com/heyvito/wal/internal"
	"github.com/heyvito/wal/internal/flock"
	"github.com/heyvito/wal/internal/procutils"
)

type WAL interface {
	// WriteObject writes a given object to the WAL. Returns an error in case
	// the write operation fails.
	WriteObject(data []byte) error

	// ReadObject attempts to read a previously stored object under a given
	// id. Either returns an io.Reader for the object's data, or an error. In
	// case the object has been marked for removal, a NotFoundError is returned.
	ReadObject(id int64) (io.Reader, error)

	// ReadObjects returns a new Cursor pointing to either the object under the
	// specified id, or its right sibling, depending on the value of the
	// inclusive flag.
	ReadObjects(id int64, inclusive bool) Cursor

	// Close flushes all data to disk, and safely closes underlying facilities
	// of the WAL.
	Close() error

	// VacuumRecords marks and removes all records from the storage medium.
	// Whether the provided id is also included in the vacuuming is defined by
	// the inclusive flag.
	VacuumRecords(id int64, inclusive bool) error

	// CountObjects returns the amount of objects after a given id. In case the
	// inclusive flag is set, the object itself is also accounted in the
	// returned total.
	CountObjects(id int64, inclusive bool) int64

	// CurrentRecordID returns the id of the latest written object
	CurrentRecordID() int64

	// IsEmpty returns whether the WAL contains any items. Returns true in case
	// no item is currently stored, otherwise returns false.
	IsEmpty() bool
}

func New(config Config) (WAL, error) {
	if config.IndexSegmentSize == 0 {
		config.IndexSegmentSize = int64(internal.NearestMultiple(64*1024*1024, internal.IndexRecordSize))
	}

	if config.DataSegmentSize == 0 {
		config.DataSegmentSize = 128 * 1024 * 1024 // 128MiB
	}

	if config.WorkDir == "" {
		return nil, fmt.Errorf("cannot initialize WAL without WorkDir")
	}

	log := config.GetLogger()
	log.Info("WAL is initializing",
		"IndexSegmentSize", config.IndexSegmentSize,
		"DataSegmentSize", config.DataSegmentSize,
		"WorkDir", config.WorkDir,
	)

	stat, err := os.Stat(config.WorkDir)
	if err != nil {
		if os.IsNotExist(err) {
			if err = os.Mkdir(config.WorkDir, 0755); err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	} else if !stat.IsDir() {
		return nil, fmt.Errorf("%s: exists and is not a directory", config.WorkDir)
	}

	w := &wal{
		config: &config,
		log:    log,
	}

	if err := w.initialize(); err != nil {
		return nil, err
	}

	return w, nil
}

type wal struct {
	config *Config
	log    stdlog.Logger
	index  *internal.Index
	flock  flock.Flock
}

func (w *wal) initialize() error {
	w.log.Info("Lock initialization in progress")
	pid, err := w.initializeLock()
	if err != nil {
		return err
	}
	if pid != -1 {
		return errors.CannotAcquireWALLockError{PID: pid}
	}

	indexInitStart := time.Now()
	idx, err := internal.NewIndex(w.config)
	if err != nil {
		w.tearDownLock()
		w.log.Error(err, "WAL startup failed")
		return err
	}
	w.log.Debug("Index initialization completed", "elapsed", time.Since(indexInitStart).String())
	w.index = idx
	return nil
}

func (w *wal) initializeLock() (int, error) {
	lockPath := filepath.Join(w.config.WorkDir, "lock")
	var err error
	w.flock, err = flock.New(lockPath)
	if err != nil {
		return -1, err
	}
	if err = w.flock.Lock(); err != nil {
		return -1, err
	}
	data := make([]byte, 16)
	l, err := w.flock.Read(data)
	if err != nil && err != io.EOF {
		err = fmt.Errorf("failed reading lock file: %w", err)
		if unlockErr := w.flock.Unlock(); unlockErr != nil {
			return -1, errs.Join(err, unlockErr)
		}
		return -1, err
	}

	if l == 0 {
		return w.writePidToLock()
	}

	pid := binary.BigEndian.Uint64(data)
	proc, err := process.NewProcess(int32(pid))
	if err != nil && errs.Is(err, process.ErrorProcessNotRunning) {
		return w.writePidToLock()
	} else if err != nil {
		err = fmt.Errorf("failed querying pid %d: %w", pid, err)
		if unlockErr := w.flock.Unlock(); unlockErr != nil {
			return -1, errs.Join(err, unlockErr)
		}
		return -1, err
	}

	running, err := proc.IsRunning()
	if err != nil {
		err = fmt.Errorf("failed querying pid %d status: %w", pid, err)
		if unlockErr := w.flock.Unlock(); unlockErr != nil {
			return -1, errs.Join(err, unlockErr)
		}
		return -1, err
	}
	if !running {
		return w.writePidToLock()
	}

	cmd, err := proc.CmdlineSlice()
	if err != nil && !errs.Is(err, syscall.EINVAL) {
		err = fmt.Errorf("failed querying pid %d cmdline: %w", pid, err)
		if unlockErr := w.flock.Unlock(); unlockErr != nil {
			return -1, errs.Join(err, unlockErr)
		}
		return -1, err
	}

	// Here, if we have a cmdslice with zero length, this may indicate a zombie
	// process. It's a huge edge case, but could be observed on a
	// non-virtualised environment.
	if len(cmd) == 0 {
		var state procutils.ProcessState
		state, err = procutils.GetPIDState(int(pid))
		if err != nil {
			// At this point we can't continue for sure. Let's bail as we can't
			// guarantee system consistency.
			err = fmt.Errorf("failed querying pid %d state: %w. System consistency cannot be guaranteed", pid, err)
			if unlockErr := w.flock.Unlock(); unlockErr != nil {
				return -1, errs.Join(err, unlockErr)
			}
			return -1, err
		}

		if state&procutils.StateDefunct == procutils.StateDefunct {
			return w.writePidToLock()
		}

		err = fmt.Errorf("lock is being held by a possible zombie process %d with no zombie flag set", pid)
		return -1, err
	}

	// At this point, there's a process, and although it  has not a lease on the
	// lockfile, its PID is registered. Just make sure it is not the same
	// process as ours.
	currentExec, err := os.Executable()
	if err != nil {
		err = fmt.Errorf("failed querying current executable path: %w", err)
		err = fmt.Errorf("failed querying pid %d: %w", pid, err)
		if unlockErr := w.flock.Unlock(); unlockErr != nil {
			return -1, errs.Join(err, unlockErr)
		}
		return -1, err
	}

	if cmd[0] == currentExec {
		// There's one last thing to check: whether we are not the PID that has
		// locked the file. This may happen in cases such as virtualization
		// environments like containers. At this point, we are sure we are
		// virtually the same process that obtained the lock, but we may always
		// have a static PID such as 1.
		if int(pid) != os.Getpid() {

			// It's not the case. The file belongs to some other process.
			return int(pid), nil
		}
	}

	// Otherwise, the process that owned the lock has died, and another process
	// already took its PID. It's safe to override.

	return w.writePidToLock()
}

func (w *wal) tearDownLock() { _ = w.flock.Remove() }

func (w *wal) writePidToLock() (int, error) {
	currentPid := os.Getpid()
	data := make([]byte, 8)
	binary.BigEndian.PutUint64(data, uint64(currentPid))
	err := w.flock.Write(data)
	if err != nil {
		if unlockErr := w.flock.Unlock(); unlockErr != nil {
			return -1, errs.Join(fmt.Errorf("failed writing current pid to lockfile: %w", err), unlockErr)
		}
		return -1, err
	}
	return -1, nil
}

func (w *wal) WriteObject(data []byte) error {
	rec := &internal.IndexRecord{}
	return w.index.Append(data, rec)
}

func (w *wal) ReadObject(id int64) (io.Reader, error) {
	rec := &internal.IndexRecord{}
	if err := w.index.LookupMeta(id, rec); err != nil {
		return nil, err
	}
	if rec.Purged {
		return nil, errors.NotFound{RecordID: id}
	}
	return w.index.ReadRecord(rec)
}

func (w *wal) ReadObjects(id int64, inclusive bool) Cursor {
	return w.index.ReadObjects(id, inclusive)
}

func (w *wal) Close() error {
	if err := w.index.Close(); err != nil {
		return err
	}
	w.tearDownLock()
	return nil
}

func (w *wal) VacuumRecords(id int64, inclusive bool) error {
	return w.index.VacuumObjects(id, inclusive)
}

func (w *wal) CountObjects(id int64, inclusive bool) int64 {
	return w.index.CountObjects(id, inclusive)
}

func (w *wal) CurrentRecordID() int64 {
	return max(w.index.MaxRecord.Load(), 0)
}

func (w *wal) IsEmpty() bool {
	return w.index.IsEmpty()
}
