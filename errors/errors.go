package errors

import "fmt"

// CannotAcquireWALLockError indicates that the WAL Lock could not be obtained
// since it is in use by another process. The process holding the lock is
// present in the PID field of this error.
type CannotAcquireWALLockError struct {
	PID int
}

func (c CannotAcquireWALLockError) Error() string {
	return fmt.Sprintf("cannot acquire WAL lock, as it is being held by process %d", c.PID)
}

// NotFound indicates that a record could not be located by its ID, or it has
// already been vacuumed.
type NotFound struct {
	RecordID int64
}

func (n NotFound) Error() string {
	return fmt.Sprintf("record %d not found", n.RecordID)
}
