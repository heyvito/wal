package internal

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/heyvito/gommap"
)

const dataSegmentMetadataSize = 8 * 3

type DataSegment struct {
	Path string
	File *os.File

	SegmentID int64
	Size      int64
	Cursor    atomic.Int64

	RawData  gommap.MMap
	Metadata gommap.MMap
	Records  gommap.MMap
	writeMu  sync.Mutex
}

func NewDataSegment(id int64, config Config) (*DataSegment, error) {
	path := filepath.Join(config.GetWorkdir(), fmt.Sprintf("data%04d", id))
	var fd *os.File
	stat, err := os.Stat(path)
	isNew := false
	switch {
	case os.IsNotExist(err):
		fd, err = os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_EXCL|os.O_SYNC, 0644)
		isNew = true
	case err != nil:
		return nil, err
	case stat.IsDir():
		return nil, fmt.Errorf("%s: is a directory", path)
	default:
		fd, err = os.OpenFile(path, os.O_RDWR|os.O_EXCL|os.O_SYNC, 0644)
	}
	if err != nil {
		return nil, err
	}

	if isNew {
		if err = fd.Truncate(config.GetDataSegmentSize() + dataSegmentMetadataSize); err != nil {
			_ = fd.Close()
			return nil, err
		}
	}

	mapped, err := gommap.Map(fd.Fd(), gommap.PROT_READ|gommap.PROT_WRITE, gommap.MAP_SHARED)
	if err != nil {
		_ = fd.Close()
		return nil, err
	}

	seg := &DataSegment{
		Path:      path,
		File:      fd,
		SegmentID: id,
		Size:      config.GetDataSegmentSize(),
		RawData:   mapped,
		Metadata:  mapped[:dataSegmentMetadataSize],
		Records:   mapped[dataSegmentMetadataSize:],
	}

	if isNew {
		seg.FlushMetadata()
	} else {
		seg.LoadMetadata()
	}

	return seg, nil
}

func (s *DataSegment) FlushMetadata() {
	be.PutUint64(s.Metadata[dataSegmentOffsets.SegmentID:], uint64(s.SegmentID))
	be.PutUint64(s.Metadata[dataSegmentOffsets.Size:], uint64(s.Size))
	be.PutUint64(s.Metadata[dataSegmentOffsets.Cursor:], uint64(s.Cursor.Load()))
}

func (s *DataSegment) LoadMetadata() {
	s.SegmentID = int64(be.Uint64(s.Metadata[dataSegmentOffsets.SegmentID:]))
	s.Size = int64(be.Uint64(s.Metadata[dataSegmentOffsets.Size:]))
	s.Cursor.Store(int64(be.Uint64(s.Metadata[dataSegmentOffsets.Cursor:])))
}

func (s *DataSegment) Read(into []byte, offset int64) int64 {
	if offset >= s.Cursor.Load() {
		return 0
	}
	return int64(copy(into, s.Records[offset:offset+int64(len(into))]))
}

func (s *DataSegment) Reader(offset, size int64) (io.Reader, int64) {
	if offset+size > s.Size {
		size = s.Size - offset
	}

	if size > s.Size {
		size = s.Size
	}

	return bytes.NewReader(s.Records[offset : offset+size]), size
}

func (s *DataSegment) Write(data []byte) (offset int64, written int64) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	offset = s.Cursor.Load()
	written = int64(copy(s.Records[offset:], data))
	s.Cursor.Add(written)
	return
}

func (s *DataSegment) Close() error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.FlushMetadata()
	if err := s.RawData.Sync(gommap.MS_SYNC); err != nil {
		return err
	}
	return s.File.Close()
}

func (s *DataSegment) AvailableSize() int64 { return s.Size - s.Cursor.Load() }

func (s *DataSegment) Available() bool {
	return s.AvailableSize() > 0
}

func (s *DataSegment) Unlink() error {
	if err := s.Close(); err != nil {
		return err
	}
	return os.Remove(s.Path)
}
