package internal

import (
	"io"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDataManagerNew(t *testing.T) {
	conf := NewDummyConfig(t)
	dm, err := NewDataManager(conf)
	require.NoError(t, err)
	assert.Zero(t, dm.MinSegment)
	assert.Zero(t, dm.MaxSegment.Load())
	assert.NotNil(t, dm.CurrentSegment)
	err = dm.Close()
	require.NoError(t, err)
}

func TestDataManagerGetSegment(t *testing.T) {
	conf := NewDummyConfig(t)
	dm, err := NewDataManager(conf)
	require.NoError(t, err)
	seg, ok := dm.Segments.Load(0)
	require.True(t, ok)
	assert.NotNil(t, seg)
	assert.Equal(t, int64(0), seg.SegmentID)
	assert.Equal(t, conf.DataSegmentSize, seg.Size)
}

func TestDataManagerWriteSmall(t *testing.T) {
	conf := NewDummyConfig(t)
	dm, err := NewDataManager(conf)
	require.NoError(t, err)
	rec := &IndexRecord{
		RecordID:           0,
		DataSegmentStartID: 100,
		DataSegmentEndID:   200,
		DataSegmentOffset:  300,
		Size:               32,
		Purged:             false,
	}
	data1 := randomData(t, 32)
	err = dm.Write(data1, rec)
	require.NoError(t, err)
	assert.Zero(t, rec.DataSegmentOffset)
	assert.Zero(t, rec.DataSegmentStartID)
	assert.Zero(t, rec.DataSegmentEndID)

	data2 := randomData(t, 32)
	err = dm.Write(data2, rec)
	require.NoError(t, err)
	assert.Equal(t, int64(32), rec.DataSegmentOffset)
	assert.Zero(t, rec.DataSegmentStartID)
	assert.Zero(t, rec.DataSegmentEndID)

	err = dm.Close()
	require.NoError(t, err)
	require.NoFileExists(t, filepath.Join(conf.WorkDir, "data0001"))
}

func TestDataManagerReadSmall(t *testing.T) {
	conf := NewDummyConfig(t)
	dm, err := NewDataManager(conf)
	require.NoError(t, err)
	recA := &IndexRecord{
		Size: 32,
	}
	data1 := randomData(t, 32)
	err = dm.Write(data1, recA)
	require.NoError(t, err)

	recB := &IndexRecord{
		Size: 32,
	}
	data2 := randomData(t, 32)
	err = dm.Write(data2, recB)
	require.NoError(t, err)

	err = dm.Close()
	require.NoError(t, err)

	dm, err = NewDataManager(conf)
	require.NoError(t, err)

	r1, err := dm.Read(recA)
	require.NoError(t, err)
	data, err := io.ReadAll(r1)
	require.NoError(t, err)
	assert.Equal(t, data1, data)

	r2, err := dm.Read(recB)
	require.NoError(t, err)
	data, err = io.ReadAll(r2)
	require.NoError(t, err)
	assert.Equal(t, data2, data)
}

func TestDataManagerWriteBig(t *testing.T) {
	conf := NewDummyConfig(t)
	dm, err := NewDataManager(conf)
	require.NoError(t, err)
	rec := &IndexRecord{
		RecordID:           0,
		DataSegmentStartID: 100,
		DataSegmentEndID:   200,
		DataSegmentOffset:  300,
		Size:               256,
		Purged:             false,
	}
	data1 := randomData(t, 256)
	err = dm.Write(data1, rec)
	require.NoError(t, err)
	assert.Zero(t, rec.DataSegmentOffset)
	assert.Zero(t, rec.DataSegmentStartID)
	assert.Equal(t, int64(3), rec.DataSegmentEndID)

	require.FileExists(t, filepath.Join(conf.WorkDir, "data0000"))
	require.FileExists(t, filepath.Join(conf.WorkDir, "data0001"))
	require.FileExists(t, filepath.Join(conf.WorkDir, "data0002"))
	require.FileExists(t, filepath.Join(conf.WorkDir, "data0003"))

	err = dm.Close()
	require.NoError(t, err)
	require.NoFileExists(t, filepath.Join(conf.WorkDir, "data0004"))

	dm, err = NewDataManager(conf)
	require.NoError(t, err)
	assert.Zero(t, dm.MinSegment)
	assert.Equal(t, int64(3), dm.MaxSegment.Load())
	err = dm.Close()
	require.NoError(t, err)
}

func TestDataManagerReadBig(t *testing.T) {
	conf := NewDummyConfig(t)
	dm, err := NewDataManager(conf)
	require.NoError(t, err)
	rec := &IndexRecord{
		RecordID:           0,
		DataSegmentStartID: 100,
		DataSegmentEndID:   200,
		DataSegmentOffset:  300,
		Size:               256,
		Purged:             false,
	}
	data1 := randomData(t, 256)
	err = dm.Write(data1, rec)
	require.NoError(t, err)
	err = dm.Close()

	dm, err = NewDataManager(conf)
	require.NoError(t, err)

	r, err := dm.Read(rec)
	require.NoError(t, err)
	data, err := io.ReadAll(r)
	require.NoError(t, err)
	assert.Equal(t, data1, data)

	err = dm.Close()
	require.NoError(t, err)
}

func TestDataManagerRace(t *testing.T) {
	conf := NewDummyConfig(t)
	dm, err := NewDataManager(conf)
	require.NoError(t, err)

	rec := &IndexRecord{
		RecordID:           0,
		DataSegmentStartID: 100,
		DataSegmentEndID:   200,
		DataSegmentOffset:  300,
		Size:               256,
		Purged:             false,
	}
	data1 := randomData(t, 256)
	err = dm.Write(data1, rec)
	require.NoError(t, err)

	var toRead []byte
	toWrite := randomData(t, 256)
	start := make(chan struct{})
	wg := sync.WaitGroup{}
	wg.Add(2)

	var readErr error
	go func() {
		defer wg.Done()
		<-start
		r, err := dm.Read(rec)
		if err != nil {
			readErr = err
			return
		}
		toRead, readErr = io.ReadAll(r)
	}()

	rec2 := &IndexRecord{
		Size: 256,
	}
	var writeErr error
	go func() {
		defer wg.Done()
		<-start
		writeErr = dm.Write(toWrite, rec2)
	}()

	close(start)
	wg.Wait()
	assert.NoError(t, readErr)
	assert.NoError(t, writeErr)

	assert.Equal(t, data1, toRead)

	err = dm.Close()
	require.NoError(t, err)
}
