// Package flock implements a small wrapper around the flock(2) Kernel API in
// order to provide advisory locks through the filesystem. It may be important
// to notice that flock is an advisory lock, meaning processes are free to
// ignore the lock altogether.
//
// For more information and documentation about the exposed API, see
// [flock.go](flock.go).
package flock

// A word about conventions: Flock exposes the public interface intended for
// user usage. Methods implemented by the interface must be safe and rely on
// the internal mutex before any operation takes place. Unexported methods
// implemented by the flock struct are intended for internal usage and must
// not use the internal mutex in order to allow reentrancy. Unexported methods
// must be used with care, and the lock is expected to be held before those
// are called (which should happen in every "entry" methods, exposed to the
// user).

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"syscall"
)

var (
	AlreadyLockedErr = fmt.Errorf("flock is already locked")
	NotLockedErr     = fmt.Errorf("flock is not locked")
	ClosedErr        = fmt.Errorf("underlying file descriptor has already been closed")
	CannotLockErr    = fmt.Errorf("could not obtain lock")
)

type Flock interface {
	// Lock attempts to lock the file managed by this instance.
	// Returns AlreadyLockedErr if the lock has already been acquired, ClosedErr
	// in case Close has already been called on this instance, or CannotLockErr
	// in case the lock cannot be acquired.
	Lock() error

	// Unlock releases the lock acquired by calling Lock. Returns NotLockedErr
	// in case the lock is not currently held, or ClosedErr in case Close has
	// already been called on this instance.
	Unlock() error

	// Close automatically releases the lock (in case it is currently being held
	// by this instance), and closes the underlying file descriptor. After
	// calling this method, no further operations can be done against the
	// instance; To reacquire the lock, create a new Flock instance by calling
	// New.
	Close() error

	// Remove automatically releases the lock (in case it is currently being
	// held by this instance), closes the underlying file descriptor, and
	// removes it from the filesystem. After calling this method, no further
	// operations can be done against the instance; To recreate the filesystem
	// node and reacquire the lock, create a new Flock instance by calling New.
	Remove() error

	// Write replaces the contents of the lock file with the provided buffer.
	// Returns an error in case writing to the underlying file fails.
	Write(data []byte) error

	// Read reads the contents of the lock file into the provided buffer.
	// Returns the amount of data read, or an error in case reading fails.
	Read(data []byte) (int, error)
}

// New returns a new Flock instance for a file at a given path. This method will
// not lock the file until Lock is called.
// Returns an error in case the file cannot be open or created.
func New(path string) (Flock, error) {
	oldMask := syscall.Umask(0)
	defer syscall.Umask(oldMask)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	return &flock{file: f, fd: f.Fd(), name: path}, nil
}

type flock struct {
	mu     sync.Mutex
	file   *os.File
	fd     uintptr
	locked bool
	closed bool
	name   string
}

func (f *flock) Lock() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	switch {
	case f.closed:
		return ClosedErr
	case f.locked:
		return AlreadyLockedErr
	}

	err := syscall.Flock(int(f.fd), syscall.LOCK_EX|syscall.LOCK_NB)
	if err == nil {
		f.locked = true
	} else {
		err = errors.Join(CannotLockErr, err)
	}
	return err
}

func (f *flock) Unlock() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	switch {
	case f.closed:
		return ClosedErr
	case !f.locked:
		return NotLockedErr
	}

	return f.unlock()
}

func (f *flock) unlock() error {
	switch {
	case f.closed, !f.locked:
		return nil
	}

	err := syscall.Flock(int(f.fd), syscall.LOCK_UN)
	if err == nil {
		f.locked = false
	}
	return err
}

func (f *flock) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.close()
}

func (f *flock) close() error {
	if f.closed {
		return ClosedErr
	}

	if err := f.unlock(); err != nil {
		return err
	}
	if err := f.file.Close(); err != nil {
		return err
	}
	f.closed = true
	return nil
}

func (f *flock) Remove() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.close(); err != nil && !errors.Is(err, os.ErrClosed) {
		return err
	}

	if err := os.Remove(f.name); err != nil && !os.IsNotExist(err) {
		return err
	}

	return nil
}

func (f *flock) Write(data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, err := f.file.WriteAt(data, 0); err != nil {
		return err
	}
	return f.file.Sync()
}

func (f *flock) Read(data []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.file.ReadAt(data, 0)
}
