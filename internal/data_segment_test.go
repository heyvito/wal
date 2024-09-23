package internal

import (
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDataSegmentNew(t *testing.T) {
	cfg := NewDummyConfig(t)
	seg, err := NewDataSegment(0, cfg)
	require.NoError(t, err)
	err = seg.Close()
	require.NoError(t, err)
	data, err := os.ReadFile(cfg.GetWorkdir() + "/data0000")
	require.NoError(t, err)
	expected := mustByesFromHex("00000000 00000000 00000000 00000040 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000")
	assert.Equal(t, expected, data)
}

func TestDataSegmentOpen(t *testing.T) {
	// data := mustByesFromHex("00000000 0001E240 00000000 00001ED2 00000000 00000000 00000000 000181CD 00000000 000181CE 01000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00")
	// TODO: The above is an INDEX SEGMENT FOR FUCK'S SAKE
	data := mustByesFromHex("00000000 0001E240 00000000 00001ED2 00000000 00000315")
	cfg := NewDummyConfig(t)
	err := os.WriteFile(cfg.GetWorkdir()+"/data0000", data, 0644)
	require.NoError(t, err)

	seg, err := NewDataSegment(0, cfg)
	require.NoError(t, err)
	expected := &DataSegment{
		SegmentID: 123456,
		Size:      7890,
		Cursor:    atomic.Int64{},
	}
	expected.Cursor.Store(789)

	assert.Equal(t, expected.SegmentID, seg.SegmentID)
	assert.Equal(t, expected.Size, seg.Size)
	assert.Equal(t, expected.Cursor.Load(), seg.Cursor.Load())
}

func TestDataSegmentReadWrite(t *testing.T) {
	cfg := NewDummyConfig(t)

	seg, err := NewDataSegment(0, cfg)
	require.NoError(t, err)
	d1 := randomData(t, 32)
	require.True(t, seg.Available())
	require.Equal(t, int64(64), seg.AvailableSize())

	offset, written := seg.Write(d1)
	require.Equal(t, int64(0), offset)
	require.Equal(t, int64(32), written)
	assert.Equal(t, int64(32), seg.Cursor.Load())

	toRead := make([]byte, 32)
	r := seg.Read(toRead, 0)
	require.Equal(t, int64(32), r)
	assert.Equal(t, d1, toRead)

	d2 := randomData(t, 32)
	require.True(t, seg.Available())
	require.Equal(t, int64(32), seg.AvailableSize())

	offset, written = seg.Write(d2)
	require.Equal(t, int64(32), offset)
	require.Equal(t, int64(32), written)
	assert.Equal(t, int64(64), seg.Cursor.Load())

	r = seg.Read(toRead, 32)
	require.Equal(t, int64(32), r)
	assert.Equal(t, d2, toRead)

	require.False(t, seg.Available())
	require.Equal(t, int64(0), seg.AvailableSize())

	err = seg.Close()
	require.NoError(t, err)

	seg, err = NewDataSegment(0, cfg)
	require.NoError(t, err)

	require.False(t, seg.Available())
	require.Equal(t, int64(0), seg.AvailableSize())

	r = seg.Read(toRead, 0)
	require.Equal(t, int64(32), r)
	assert.Equal(t, d1, toRead)

	r = seg.Read(toRead, 32)
	require.Equal(t, int64(32), r)
	assert.Equal(t, d2, toRead)
}

func TestDataSegmentRace(t *testing.T) {
	cfg := NewDummyConfig(t)

	seg, err := NewDataSegment(0, cfg)
	require.NoError(t, err)
	d1 := randomData(t, 32)
	require.True(t, seg.Available())
	require.Equal(t, int64(64), seg.AvailableSize())

	seg.Write(d1)

	wg := sync.WaitGroup{}
	wg.Add(2)
	start := make(chan struct{})

	read := make([]byte, 32)
	var r int64
	go func() {
		defer wg.Done()
		<-start
		r = seg.Read(read, 0)
	}()

	write := randomData(t, 32)
	go func() {
		defer wg.Done()
		<-start
		seg.Write(write)
	}()

	close(start)
	wg.Wait()

	assert.Equal(t, int64(32), r)
	assert.Equal(t, d1, read)
	assert.False(t, seg.Available())
}
