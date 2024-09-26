package internal

import (
	"fmt"
	"github.com/heyvito/wal/internal/metrics"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/heyvito/gommap"
)

const IndexSegmentMetadataSize = 6*8 + 1

type IndexSegment struct {
	Path      string
	File      *os.File
	SegmentID int64
	Size      int64

	LowerRecord  atomic.Int64
	UpperRecord  atomic.Int64
	RecordsCount atomic.Int64
	Cursor       atomic.Int64
	Purged       bool

	RawData  gommap.MMap
	Metadata gommap.MMap
	Records  gommap.MMap

	writeMu sync.Mutex
}

func NewIndexSegment(id int64, config Config) (*IndexSegment, error) {
	path := filepath.Join(config.GetWorkdir(), fmt.Sprintf("index%04d", id))
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
		if err = fd.Truncate(config.GetIndexSegmentSize() + IndexSegmentMetadataSize); err != nil {
			_ = fd.Close()
			return nil, err
		}
	}

	mapped, err := gommap.Map(fd.Fd(), gommap.PROT_READ|gommap.PROT_WRITE, gommap.MAP_SHARED)
	if err != nil {
		_ = fd.Close()
		return nil, err
	}

	seg := &IndexSegment{
		Path:      path,
		File:      fd,
		SegmentID: id,
		Size:      config.GetIndexSegmentSize(),
		RawData:   mapped,
		Metadata:  mapped[:IndexSegmentMetadataSize],
		Records:   mapped[IndexSegmentMetadataSize:],
	}

	if isNew {
		seg.FlushMetadata()
	} else {
		seg.LoadMetadata()
	}

	return seg, nil
}

func (s *IndexSegment) LoadMetadata() {
	s.SegmentID = int64(be.Uint64(s.Metadata[indexSegmentOffsets.SegmentID:]))
	s.Size = int64(be.Uint64(s.Metadata[indexSegmentOffsets.Size:]))
	s.LowerRecord.Store(int64(be.Uint64(s.Metadata[indexSegmentOffsets.LowerRecord:])))
	s.UpperRecord.Store(int64(be.Uint64(s.Metadata[indexSegmentOffsets.UpperRecord:])))
	s.RecordsCount.Store(int64(be.Uint64(s.Metadata[indexSegmentOffsets.RecordsCount:])))
	s.Cursor.Store(int64(be.Uint64(s.Metadata[indexSegmentOffsets.Cursor:])))
	flags := s.Metadata[indexSegmentOffsets.Flags]
	s.Purged = flags&(0x01<<0) != 0

}

func (s *IndexSegment) FlushMetadata() {
	metrics.Simple(metrics.IndexSegmentFlushMetaCalls, 0)
	defer metrics.Measure(metrics.IndexSegmentFlushMetaLatency)()

	be.PutUint64(s.Metadata[indexSegmentOffsets.SegmentID:], uint64(s.SegmentID))
	be.PutUint64(s.Metadata[indexSegmentOffsets.Size:], uint64(s.Size))
	be.PutUint64(s.Metadata[indexSegmentOffsets.LowerRecord:], uint64(s.LowerRecord.Load()))
	be.PutUint64(s.Metadata[indexSegmentOffsets.UpperRecord:], uint64(s.UpperRecord.Load()))
	be.PutUint64(s.Metadata[indexSegmentOffsets.RecordsCount:], uint64(s.RecordsCount.Load()))
	be.PutUint64(s.Metadata[indexSegmentOffsets.Cursor:], uint64(s.Cursor.Load()))
	flags := byte(0x00)
	if s.Purged {
		flags |= 0x01 << 0
	}
	s.Metadata[indexSegmentOffsets.Flags] = flags
}

func (s *IndexSegment) ContainsRecord(id int64) bool {
	if s.RecordsCount.Load() == 0 {
		return false
	}

	l, u := s.LowerRecord.Load(), s.UpperRecord.Load()
	return id >= l && id <= u
}

func (s *IndexSegment) LoadRecord(id int64, rec *IndexRecord) bool {
	defer metrics.Measure(metrics.IndexSegmentLoadRecordLatency)()
	if !s.ContainsRecord(id) {
		return false
	}

	offset := (id - s.LowerRecord.Load()) * IndexRecordSize
	rec.Read(s.Records[offset:])
	return true
}

func (s *IndexSegment) FitsRecord() bool {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.Cursor.Load()+IndexRecordSize <= s.Size
}

func (s *IndexSegment) WriteRecord(rec *IndexRecord) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	defer metrics.Measure(metrics.IndexSegmentWriteRecordLatency)()

	cur := s.Cursor.Load()
	rec.Write(s.Records[cur:])
	if cur == 0 {
		s.LowerRecord.Store(rec.RecordID)
	}
	s.Cursor.Add(IndexRecordSize)
	s.UpperRecord.Store(rec.RecordID)
	s.RecordsCount.Add(1)
	s.FlushMetadata()
}

func (s *IndexSegment) Close() error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.RawData.Sync(gommap.MS_SYNC); err != nil {
		return err
	}
	return s.File.Close()
}

func (s *IndexSegment) PurgeFrom(id int64) {
	defer metrics.Measure(metrics.IndexSegmentPurgeFromLatency)()
	if !s.FitsRecord() && id == s.UpperRecord.Load() {
		s.Purged = true
		s.FlushMetadata()
		return
	}

	lr := s.LowerRecord.Load()
	cur := 0
	for i := lr; i <= id; i++ {
		SetIndexRecordPurged(s.Records[cur*IndexRecordSize:])
		cur++
	}

	count := 0
	rec := &IndexRecord{}
	for i := lr; i <= s.UpperRecord.Load(); i++ {
		s.LoadRecord(i, rec)
		if !rec.Purged {
			count++
		}
	}
	s.RecordsCount.Store(int64(count))
	if count == 0 && !s.Purged {
		s.Purged = true
		s.FlushMetadata()
	}

	if s.Purged {
		s.LowerRecord.Store(-1)
	} else {
		cur = 0
		for i := s.LowerRecord.Load(); i <= s.UpperRecord.Load(); i++ {
			if !IsIndexRecordPurged(s.Records[cur*IndexRecordSize:]) {
				s.LowerRecord.Store(i)
				break
			}
			cur++
		}
	}

}

func (s *IndexSegment) Unlink() error {
	if err := s.Close(); err != nil {
		return err
	}
	return os.Remove(s.Path)
}
