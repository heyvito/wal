package flock

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func makePath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "lock")
}

func makeLock(t *testing.T) Flock {
	path := makePath(t)
	f, err := New(path)
	require.NoError(t, err)
	require.NotNil(t, f)
	return f
}

func makeLockAt(t *testing.T, path string) Flock {
	f, err := New(path)
	require.NoError(t, err)
	require.NotNil(t, f)
	return f
}

func TestFlock(t *testing.T) {
	t.Run("Open, Lock, Unlock, Close", func(t *testing.T) {
		path := makePath(t)
		f, err := New(path)
		require.NoError(t, err)
		require.NotNil(t, f)

		err = f.Lock()
		require.NoError(t, err)

		err = f.Unlock()
		require.NoError(t, err)

		err = f.Close()
		require.NoError(t, err)
	})

	t.Run("Concurrent lock", func(t *testing.T) {
		path := makePath(t)
		f := makeLockAt(t, path)
		f2 := makeLockAt(t, path)

		err := f.Lock()
		require.NoError(t, err)

		err = f2.Lock()
		require.Error(t, err)

		err = f.Close()
		require.NoError(t, err)

		err = f2.Close()
		require.NoError(t, err)
	})

	t.Run("Unlock when not locked", func(t *testing.T) {
		f := makeLock(t)
		err := f.Unlock()
		require.ErrorIs(t, err, NotLockedErr)
	})

	t.Run("Double lock", func(t *testing.T) {
		f := makeLock(t)
		err := f.Lock()
		require.NoError(t, err)
		err = f.Lock()
		require.ErrorIs(t, err, AlreadyLockedErr)

		err = f.Close()
		require.NoError(t, err)
	})

	t.Run("Double unlock", func(t *testing.T) {
		f := makeLock(t)
		err := f.Lock()
		require.NoError(t, err)

		err = f.Unlock()
		require.NoError(t, err)

		err = f.Unlock()
		require.ErrorIs(t, err, NotLockedErr)

		err = f.Close()
		require.NoError(t, err)
	})

	t.Run("Lock after close", func(t *testing.T) {
		f := makeLock(t)
		err := f.Close()
		require.NoError(t, err)

		err = f.Lock()
		require.ErrorIs(t, err, ClosedErr)
	})

	t.Run("Unlock after close", func(t *testing.T) {
		f := makeLock(t)
		err := f.Lock()
		require.NoError(t, err)

		err = f.Close()
		require.NoError(t, err)

		err = f.Unlock()
		require.ErrorIs(t, err, ClosedErr)
	})

	t.Run("Recreate after close", func(t *testing.T) {
		p := makePath(t)
		f := makeLockAt(t, p)
		err := f.Lock()
		require.NoError(t, err)
		err = f.Close()
		require.NoError(t, err)

		f = makeLockAt(t, p)
		err = f.Lock()
		require.NoError(t, err)
		err = f.Close()
		require.NoError(t, err)
	})
}
