package internal

import (
	"io"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIndexNew(t *testing.T) {
	conf := NewDummyConfig(t)
	idx, err := NewIndex(conf)
	require.NoError(t, err)

	assert.True(t, idx.IsEmpty())

	seg, ok := idx.SegmentForID(0)
	require.False(t, ok)
	require.Nil(t, seg)

	err = idx.LookupMeta(0, nil)
	assert.ErrorContains(t, err, "not found")

	c := idx.CountObjects(0, true)
	assert.Zero(t, c)

	r := idx.ReadObjects(0, true)
	assert.False(t, r.Next())

	err = idx.Close()
	require.NoError(t, err)

	require.FileExists(t, filepath.Join(conf.WorkDir, "data0000"))
	require.FileExists(t, filepath.Join(conf.WorkDir, "index0000"))
}

func TestIndexAppend(t *testing.T) {
	conf := NewDummyConfig(t)
	idx, err := NewIndex(conf)
	require.NoError(t, err)

	rData := randomData(t, 32)
	rec := &IndexRecord{}
	err = idx.Append(rData, rec)
	require.NoError(t, err)

	assert.Equal(t, int64(32), rec.Size)
	assert.Equal(t, int64(0), rec.DataSegmentOffset)
	assert.Equal(t, int64(0), rec.DataSegmentStartID)
	assert.Equal(t, int64(0), rec.DataSegmentEndID)
	assert.Equal(t, int64(0), rec.RecordID)

	r, err := idx.ReadRecord(rec)
	require.NoError(t, err)
	data, err := io.ReadAll(r)
	require.NoError(t, err)
	assert.Equal(t, rData, data)

	assert.False(t, idx.IsEmpty())
	assert.Equal(t, int64(1), idx.CountObjects(0, true))
	assert.Equal(t, int64(0), idx.CountObjects(0, false))

	reader := idx.ReadObjects(0, false)
	assert.False(t, reader.Next())

	reader = idx.ReadObjects(0, true)
	assert.True(t, reader.Next())

	err = idx.VacuumObjects(0, false)
	require.NoError(t, err)

	err = idx.VacuumObjects(0, true)
	require.NoError(t, err)

	err = idx.Close()
	require.NoError(t, err)
}

func TestIndexRead(t *testing.T) {
	conf := NewDummyConfig(t)
	idx, err := NewIndex(conf)
	require.NoError(t, err)

	rData := randomData(t, 32)
	rec := &IndexRecord{}
	err = idx.Append(rData, rec)
	require.NoError(t, err)

	reader, err := idx.ReadRecord(rec)
	require.NoError(t, err)
	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, rData, data)

	cur := idx.ReadObjects(0, false)
	assert.False(t, cur.Next())

	rData = randomData(t, 32)
	rec = &IndexRecord{}
	err = idx.Append(rData, rec)
	require.NoError(t, err)

	assert.True(t, cur.Next())
	assert.Equal(t, rec.RecordID, cur.Offset())
}
