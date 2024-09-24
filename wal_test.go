package wal

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/go-stdlog/stdlog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/heyvito/wal/internal"
)

// TestWALWriteReadSingle tests a simple create -> write -> close -> open ->
// read to make sure the system is able to perform simple operations.
func TestWALWriteReadSingle(t *testing.T) {
	d := t.TempDir()
	l := stdlog.Discard
	conf := Config{
		DataSegmentSize:  90,
		IndexSegmentSize: 42,
		WorkDir:          d,
		Logger:           l,
	}
	w, err := New(conf)
	require.NoError(t, err)
	require.NotNil(t, w)

	err = w.WriteObject([]byte("Hello, World!"))
	require.NoError(t, err)

	err = w.Close()
	require.NoError(t, err)

	w, err = New(conf)
	require.NoError(t, err)
	require.NotNil(t, w)

	cur := w.ReadObjects(0, true)
	require.True(t, cur.Next())
	r, err := cur.Read()
	require.NoError(t, err)
	data, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, []byte("Hello, World!"), data)

	require.False(t, cur.Next())
	require.False(t, cur.Next())
}

// TestWALWriteReadMultiple exercises the split-index mechanism, making sure we
// are able to write and read data across different index segments.
func TestWALWriteReadMultiple(t *testing.T) {
	dir := t.TempDir()
	l := stdlog.Discard
	conf := Config{
		DataSegmentSize: 1024,

		// This will split the index into 50 files, each containing a single
		// entry. This is specially interesting for testing RightSibling, as
		// used by ReadObjects.
		IndexSegmentSize: internal.IndexRecordSize + 3,
		WorkDir:          dir,
		Logger:           l,
	}
	w, err := New(conf)
	require.NoError(t, err)
	require.NotNil(t, w)

	for i := 0; i < 50; i++ {
		err = w.WriteObject([]byte("Hello, World!"))
		require.NoError(t, err)
	}

	err = w.Close()
	require.NoError(t, err)

	w, err = New(conf)
	require.NoError(t, err)
	require.NotNil(t, w)

	cur := w.ReadObjects(0, true)
	for i := 0; i < 50; i++ {
		require.Truef(t, cur.Next(), "cur.Next should return true for index %d", i)
		r, err := cur.Read()
		require.NoError(t, err)
		obj, err := io.ReadAll(r)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello, World!"), obj)
	}

	require.False(t, cur.Next())
	require.False(t, cur.Next())
}

// TestWALVacuum creates 50 index segments, and attempts to purge half of those
// records through WAL.VacuumRecords. It simply asserts that the first data
// segment is marked as purged.
func TestWALVacuum(t *testing.T) {
	dir := t.TempDir()
	l := stdlog.Discard
	conf := Config{
		DataSegmentSize:  43,
		IndexSegmentSize: internal.IndexRecordSize + 3,
		WorkDir:          dir,
		Logger:           l,
	}
	w, err := New(conf)
	require.NoError(t, err)
	require.NotNil(t, w)

	for i := 0; i < 50; i++ {
		err = w.WriteObject([]byte("Hello, World!"))
		require.NoError(t, err)
	}

	err = w.VacuumRecords(25, true)
	require.NoError(t, err)

	assert.NoFileExists(t, filepath.Join(conf.WorkDir, "data0000"))
}

// TestWALVacuumSegmented exercises the mechanism responsible for keeping
// segments intact in case objects spans more than a single segment. The test is
// sequential, and performed operations and expectations are described in each
// inner test case. It is important to notice that the maximum data segment size
// is 128 bytes, and the maximum index size is 1024 bytes. The first record
// takes 298 bytes, spanning more than one segment. The other records take the
// rest of the segments, yielding a total of 3 segments. This case also makes
// sure the WAL instance is still working as it should after the vacuum
// operation is completed.
func TestWALVacuumSegmented(t *testing.T) {
	dir := t.TempDir()
	l := stdlog.Discard

	conf := Config{
		DataSegmentSize:  128,
		IndexSegmentSize: 1024,
		WorkDir:          dir,
		Logger:           l,
	}
	w, err := New(conf)
	require.NoError(t, err)

	rec0Data := make([]byte, 298)
	rec1Data := make([]byte, 25)
	rec2Data := make([]byte, 25)
	for _, buf := range [][]byte{rec0Data, rec1Data, rec2Data} {
		_, err = rand.Read(buf)
		require.NoError(t, err)
		err = w.WriteObject(buf)
		require.NoError(t, err)
	}

	t.Run("Remove Absolute Offset 0", func(t *testing.T) {
		// Remove Absolute Offset 0.
		// Segments 0 and 1 MUST be marked as purged.
		// Segment 2 MUST NOT be marked as purged.
		err := w.VacuumRecords(0, true)
		require.NoError(t, err)

		assert.NoFileExistsf(t, filepath.Join(dir, "data0000"), "Segment 0 should have been purged")
		assert.NoFileExistsf(t, filepath.Join(dir, "data0001"), "Segment 1 should have been purged")
		assert.FileExistsf(t, filepath.Join(dir, "data0002"), "Segment 2 should not have been purged")
	})

	t.Run("Remove Absolute Offset 1", func(t *testing.T) {
		// Remove Absolute Offset 1
		// Segment 2 MUST NOT be marked as purged.

		err := w.VacuumRecords(1, true)
		require.NoError(t, err)

		assert.FileExistsf(t, filepath.Join(dir, "data0002"), "Segment 2 should not have been purged")
	})

	t.Run("Remove Absolute Offset 2", func(t *testing.T) {
		// Remove Absolute Offset 2
		// Segment 2 MUST be marked as purged.

		err := w.VacuumRecords(2, true)
		require.NoError(t, err)

		assert.NoFileExistsf(t, filepath.Join(dir, "data0002"), "Segment 2 should have been purged")
	})

	t.Run("Use WAL after vacuum", func(t *testing.T) {
		err := w.WriteObject(rec1Data)
		require.NoError(t, err)
	})
}

// TestWALCursorContiguous ensures that a cursor obtained from
// WAL.ReadObjectsFrom is able to stop when reaching the end of the WAL, and
// continuing without being recycled after a new record is added.
func TestWALCursorContiguous(t *testing.T) {
	d := t.TempDir()
	l := stdlog.Discard
	conf := Config{
		DataSegmentSize:  90,
		IndexSegmentSize: 90,
		WorkDir:          d,
		Logger:           l,
	}
	w, err := New(conf)
	require.NoError(t, err)
	require.NotNil(t, w)

	off := w.CurrentRecordID()
	cur := w.ReadObjects(off, true)
	assert.False(t, cur.Next(), "expected cursor.Next to return false")
	assert.False(t, cur.Next(), "expected cursor.Next to return false")

	err = w.WriteObject([]byte("Hello, World!"))
	require.NoError(t, err)

	require.True(t, cur.Next(), "expected cursor.Next to return true after a record is added")
	r, err := cur.Read()
	require.NoError(t, err)
	obj, err := io.ReadAll(r)
	require.NoError(t, err)
	assert.Equal(t, []byte("Hello, World!"), obj)

	assert.False(t, cur.Next(), "expected cursor.Next to return false after the only record is read")

	err = w.WriteObject([]byte("Hello, World! Again!"))
	require.NoError(t, err)

	require.True(t, cur.Next(), "expected cursor.Next to return true after the second record is added")
	r, err = cur.Read()
	require.NoError(t, err)
	obj, err = io.ReadAll(r)
	require.NoError(t, err)
	assert.Equal(t, []byte("Hello, World! Again!"), obj)
}

// TestWALCursorNonInitialSince continues exercising the same component as
// TestWALCursorContiguous, but in a different way. Whilst
// TestWALCursorContiguous starts from the beginning and moves forward, this
// case does not start at the beginning of the WAL. Instead, if starts from
// the fifth element (out of ten), and should obtain all five elements before
// reaching EOF. Only records 5, 6, 7, 8 and 9 must be returned. Instead of
// using static values like other tests, UUIDs are generated to ensure element
// ordering and read values.
func TestWALCursorNonInitialSince(t *testing.T) {
	uuids := make([]string, 10)
	for i := range uuids {
		buf := make([]byte, 16)
		_, err := rand.Read(buf)
		require.NoError(t, err)
		uuids[i] = hex.EncodeToString(buf)
	}
	d := t.TempDir()
	l := stdlog.Discard
	conf := Config{
		DataSegmentSize:  1024,
		IndexSegmentSize: 1024,
		WorkDir:          d,
		Logger:           l,
	}
	w, err := New(conf)
	require.NoError(t, err)
	require.NotNil(t, w)

	for _, v := range uuids {
		err = w.WriteObject([]byte(v))
		require.NoError(t, err)
	}

	cursor := w.ReadObjects(5, true)
	i := 5
	for cursor.Next() {
		curr := cursor.Offset()
		t.Logf("Current offset: %d, expected offset: %d", curr, i)
		assert.Equalf(t, int64(i), curr, "expected cursor.Next to return %d, got %d instead", i, curr)
		reader, err := cursor.Read()
		require.NoError(t, err)
		data, err := io.ReadAll(reader)
		require.NoError(t, err)
		t.Logf("Current object: %s, expected object: %s", string(data), uuids[i])
		assert.Equal(t, []byte(uuids[i]), data)
		i++
	}
	assert.Equal(t, 10, i, "Expected counter to go up to 10")
}

// TestWALCursorNonInitialAfter performs the same test as
// TestWALCursorNonInitialSince, but uses WAL.ReadObjectsAfter instead of
// WAL.ReadObjectsFrom. The difference here is that starting at index 5 will
// not include the object at index 5, but the next record from it.
func TestWALCursorNonInitialAfter(t *testing.T) {
	uuids := make([]string, 10)
	for i := range uuids {
		buf := make([]byte, 16)
		_, err := rand.Read(buf)
		require.NoError(t, err)
		uuids[i] = hex.EncodeToString(buf)
	}
	d := t.TempDir()
	l := stdlog.Discard
	conf := Config{
		DataSegmentSize:  1024,
		IndexSegmentSize: 1024,
		WorkDir:          d,
		Logger:           l,
	}
	w, err := New(conf)
	require.NoError(t, err)
	require.NotNil(t, w)

	for _, v := range uuids {
		err = w.WriteObject([]byte(v))
		require.NoError(t, err)
	}

	cursor := w.ReadObjects(5, false)
	i := 6
	for cursor.Next() {
		curr := cursor.Offset()
		t.Logf("Current offset: %d, expected offset: %d", curr, i)
		assert.Equalf(t, int64(i), curr, "expected cursor.Next to return %d, got %d instead", i, curr)
		reader, err := cursor.Read()
		require.NoError(t, err)
		data, err := io.ReadAll(reader)
		require.NoError(t, err)
		t.Logf("Current object: %s, expected object: %s", string(data), uuids[i])
		assert.Equal(t, []byte(uuids[i]), data)
		i++
	}
	assert.Equal(t, 10, i, "Expected counter to go up to 10")
}

// DataSegment's readMetadataUnsafe was incorrectly assuming that relative
// offsets >= to the segment size were out of bound. That's incorrect, since the
// relative offset may lie outside the file size, once it must take into account
// the segment's header. That case was found when writing specific sizes to a
// given WAL instance, and may be also reproducible in other conditions, but I'm
// leaving this regression test with the same parameters of when I encountered
// this issue.
func TestWALSpuriousOffsetOutOfRange(t *testing.T) {
	conf := Config{
		DataSegmentSize:  4096,
		IndexSegmentSize: 4096,
		WorkDir:          t.TempDir(),
		Logger:           stdlog.Discard,
	}
	w, err := New(conf)
	require.NoError(t, err)
	for i := range 1000 {
		err = w.WriteObject([]byte("object " + strconv.Itoa(i)))
		require.NoError(t, err)
	}

	// The cursor should be able to travel from 0 to 999
	cursor := w.ReadObjects(0, true)
	read := 0
	for cursor.Next() {
		read++
		require.NoError(t, err)
	}
	assert.Equal(t, 1000, read)
}

// TestWALCountObjects tests both WAL.CountObjectsAfter and WAL.CountObjectsFrom
func TestWALCountObjects(t *testing.T) {
	conf := Config{
		DataSegmentSize:  4096,
		IndexSegmentSize: 4096,
		WorkDir:          t.TempDir(),
		Logger:           stdlog.Discard,
	}
	w, err := New(conf)
	require.NoError(t, err)
	for i := range 50 {
		err = w.WriteObject([]byte("object " + strconv.Itoa(i)))
		require.NoError(t, err)
	}

	countFrom := w.CountObjects(0, true)
	assert.Equal(t, int64(50), countFrom)
	countAfter := w.CountObjects(0, false)
	assert.Equal(t, int64(49), countAfter)
}

// TestWALDifferentCursors tests both WAL.ReadObjectsAfter and
// WAL.ReadObjectsFrom
// Implemented as part of the investigation of https://linear.app/whaudit/issue/WHA-74/agent-fails-to-deliver-payloads-after-first-run
func TestWALDifferentCursors(t *testing.T) {
	conf := Config{
		DataSegmentSize:  4096,
		IndexSegmentSize: 4096,
		WorkDir:          t.TempDir(),
		Logger:           stdlog.Discard,
	}
	w, err := New(conf)
	require.NoError(t, err)
	for i := range 100 {
		err = w.WriteObject([]byte("object " + strconv.Itoa(i)))
		require.NoError(t, err)
	}

	lastOffset := int64(0)
	reader := w.ReadObjects(0, true)
	for reader.Next() {
		lastOffset = reader.Offset()
	}
	assert.Equal(t, int64(99), lastOffset)

	for i := range 100 {
		err = w.WriteObject([]byte("object " + strconv.Itoa(i)))
		require.NoError(t, err)
	}

	reader = w.ReadObjects(lastOffset, false)
	objectsRead := 0
	for reader.Next() {
		lastOffset = reader.Offset()
		objectsRead++
	}

	assert.Equal(t, 100, objectsRead)
	assert.Equal(t, int64(199), lastOffset)
}

func TestWALIsEmpty(t *testing.T) {
	conf := Config{
		DataSegmentSize:  4096,
		IndexSegmentSize: 4096,
		WorkDir:          t.TempDir(),
		Logger:           stdlog.Discard,
	}
	w, err := New(conf)
	require.NoError(t, err)

	assert.True(t, w.IsEmpty(), "expected new WAL to be empty")

	for i := range 100 {
		err = w.WriteObject([]byte("object " + strconv.Itoa(i)))
		require.NoError(t, err)
	}

	assert.False(t, w.IsEmpty(), "expected WAL not to be empty after items are added")

	err = w.VacuumRecords(w.CurrentRecordID(), true)
	require.NoError(t, err)

	assert.True(t, w.IsEmpty(), "expected WAL to be empty after complete vacuum")
}

// TestWALCountObjectsFrom tests the CountObjectsFrom method
func TestWALCountObjectsFrom(t *testing.T) {
	conf := Config{
		DataSegmentSize:  4096,
		IndexSegmentSize: 4096,
		WorkDir:          t.TempDir(),
		Logger:           stdlog.Discard,
	}
	w, err := New(conf)
	require.NoError(t, err)

	assert.True(t, w.IsEmpty(), "expected new WAL to be empty")

	for i := range 10000 {
		err = w.WriteObject([]byte("object " + strconv.Itoa(i)))
		require.NoError(t, err)
	}

	c := w.CountObjects(49, false)
	assert.Equal(t, int64(9950), c)
}

func TestWALLoopingCursor(t *testing.T) {
	conf := Config{
		DataSegmentSize:  4096,
		IndexSegmentSize: 4096,
		WorkDir:          t.TempDir(),
		Logger:           stdlog.Discard,
	}
	w, err := New(conf)
	require.NoError(t, err)
	for i := range 1000 {
		err = w.WriteObject([]byte("object " + strconv.Itoa(i)))
		require.NoError(t, err)
	}

	objsRead := 0
	cursor := w.ReadObjects(0, true)
	for cursor.Next() {
		reader, err := cursor.Read()
		require.NoError(t, err)
		data, err := io.ReadAll(reader)
		require.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("object %d", objsRead), string(data))
		objsRead++
	}
	assert.Equal(t, 1000, objsRead)

	objsRead = 0
	cursor = w.ReadObjects(100, false)
	for cursor.Next() {
		off := cursor.Offset()
		require.Equal(t, int64(101+objsRead), off)
		reader, err := cursor.Read()
		require.NoError(t, err)
		data, err := io.ReadAll(reader)
		require.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("object %d", 101+objsRead), string(data))
		objsRead++
	}

	assert.Equal(t, 899, objsRead)
}

// TestWALOperationPartialVacuum tests if the WAL continues operating normally
// after a partial vacuum operation
func TestWALOperationPartialVacuum(t *testing.T) {
	conf := Config{
		DataSegmentSize:  64,
		IndexSegmentSize: 92,
		WorkDir:          t.TempDir(),
		Logger:           stdlog.NewStd(os.Stdout),
	}
	w, err := New(conf)
	require.NoError(t, err)

	assert.True(t, w.IsEmpty(), "expected new WAL to be empty")

	for i := range 1000 {
		err = w.WriteObject([]byte("object " + strconv.Itoa(i)))
		require.NoError(t, err)
	}

	err = w.VacuumRecords(20, true)
	require.NoError(t, err)

	err = w.VacuumRecords(30, true)
	require.NoError(t, err)

	err = w.VacuumRecords(50, true)
	require.NoError(t, err)

	err = w.VacuumRecords(100, true)
	require.NoError(t, err)

	assert.Equal(t, int64(999), w.CurrentRecordID())

	err = w.Close()
	require.NoError(t, err)

	w, err = New(conf)
	require.NoError(t, err)

	err = w.VacuumRecords(500, true)
	require.NoError(t, err)

	assert.Equal(t, int64(999), w.CurrentRecordID())

	err = w.Close()
	require.NoError(t, err)
}
