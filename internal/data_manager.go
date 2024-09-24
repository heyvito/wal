package internal

import (
	"fmt"
	"github.com/heyvito/wal/internal/metrics"
	"io"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/go-stdlog/stdlog"
)

type DataManager struct {
	Config         Config
	Workdir        string
	MinSegment     int64
	MaxSegment     atomic.Int64
	Segments       AtomicMap[int64, *DataSegment]
	LoadedSegments atomic.Int32
	CurrentSegment *DataSegment
	log            stdlog.Logger

	writeMu sync.Mutex
}

func NewDataManager(config Config) (*DataManager, error) {
	wd := config.GetWorkdir()
	stat, err := os.Stat(wd)
	if os.IsNotExist(err) {
		if err = os.MkdirAll(wd, 0755); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	} else if !stat.IsDir() {
		return nil, fmt.Errorf("%s: exists and is not a directory", wd)
	}

	log := config.GetLogger().Named("data_manager")

	var segmentsToLoad []int64
	entries, err := os.ReadDir(wd)
	if err != nil {
		return nil, err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if !strings.HasPrefix(entry.Name(), "data") {
			continue
		}
		id, err := strconv.ParseInt(entry.Name()[4:], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("%s: invalid data segment file name: %w", entry.Name(), err)
		}
		segmentsToLoad = append(segmentsToLoad, id)
	}

	log.Info("Loading data segments", "size", len(segmentsToLoad))
	slices.Sort(segmentsToLoad)

	d := &DataManager{
		Config:         config,
		Workdir:        wd,
		CurrentSegment: nil,
		MinSegment:     -1,
		log:            log,
	}

	d.MaxSegment.Store(-1)

	for _, id := range segmentsToLoad {
		segment, err := NewDataSegment(id, config)
		if err != nil {
			_ = d.Close()
			log.Error(err, "Failed loading data segment", "id", id)
			return nil, err
		}
		if d.MinSegment == -1 || id < d.MinSegment {
			d.MinSegment = id
		}
		d.Segments.Store(id, segment)
		d.LoadedSegments.Add(1)
		if d.CurrentSegment == nil || id > d.CurrentSegment.SegmentID {
			d.CurrentSegment = segment
			d.MaxSegment.Store(id)
		}
		if id > d.CurrentSegment.SegmentID {
			d.MaxSegment.Store(id)
		}
	}

	if len(segmentsToLoad) == 0 {
		return d, d.Rotate()
	}

	return d, nil
}

func (m *DataManager) Close() error {
	for id, seg := range m.Segments.Range() {
		if err := seg.Close(); err != nil {
			m.log.Error(err, "Failed closing segment", "id", id)
			return err
		}
	}
	return nil
}

func (m *DataManager) Rotate() error {
	var seg *DataSegment
	var err error

	if m.CurrentSegment == nil {
		seg, err = NewDataSegment(0, m.Config)
		if err != nil {
			return err
		}
		m.MinSegment = 0
		m.MaxSegment.Store(0)
		m.Segments.Store(0, seg)
		m.CurrentSegment = seg
	} else {
		seg, err = NewDataSegment(m.CurrentSegment.SegmentID+1, m.Config)
		if err != nil {
			return err
		}
		m.CurrentSegment = seg
		m.Segments.Store(seg.SegmentID, seg)
		m.MaxSegment.Store(seg.SegmentID)
	}
	m.LoadedSegments.Add(1)
	return nil
}

func (m *DataManager) Write(data []byte, rec *IndexRecord) error {
	m.writeMu.Lock()
	defer m.writeMu.Unlock()

	defer metrics.Measure(metrics.DataManagerWriteLatency)()
	metrics.Simple(metrics.DataManagerWriteCalls, 0)

	if !m.CurrentSegment.Available() {
		if err := m.Rotate(); err != nil {
			return err
		}
	}

	rec.DataSegmentStartID = m.CurrentSegment.SegmentID
	var written int64
	dataLen := int64(len(data))
	var wr int64

	for written < dataLen {
		if !m.CurrentSegment.Available() {
			if err := m.Rotate(); err != nil {
				return err
			}
		}
		if written == 0 {
			rec.DataSegmentOffset, written = m.CurrentSegment.Write(data)
		} else {
			_, wr = m.CurrentSegment.Write(data[written:])
			written += wr
		}
	}

	rec.DataSegmentEndID = m.CurrentSegment.SegmentID

	return nil
}

func (m *DataManager) Read(rec *IndexRecord) (io.Reader, error) {
	defer metrics.Measure(metrics.DataManagerReadLatency)()
	metrics.Simple(metrics.DataManagerReadCalls, 0)

	var readers []io.Reader

	size := rec.Size
	segID := rec.DataSegmentStartID
	offset := rec.DataSegmentOffset

	for size > 0 {
		seg, ok := m.Segments.Load(segID)
		if !ok {
			return nil, fmt.Errorf("segment %d not found", segID)
		}
		reader, limit := seg.Reader(offset, size)
		offset = 0
		readers = append(readers, reader)
		size -= limit
		segID++
	}

	return io.MultiReader(readers...), nil
}

func (m *DataManager) VacuumDataSegments(idsInUse []int64) error {
	m.writeMu.Lock()
	defer m.writeMu.Unlock()

	defer metrics.Measure(metrics.IndexVacuumObjectsLatency)()
	metrics.Simple(metrics.DataManagerVacuumCalls, 0)

	m.log.Info("Vacuuming data segments", "ids_in_use", idsInUse)
	defer m.log.Info("Finished vacuuming data segments")

	inUse := map[int64]struct{}{}
	for _, id := range idsInUse {
		inUse[id] = struct{}{}
	}

	for k, v := range m.Segments.Range() {
		_, ok := inUse[k]
		if ok {
			continue
		}
		m.log.Debug("Unlinking segment", "segment_id", k)
		if err := v.Unlink(); err != nil {
			m.log.Error(err, "Failed unlinking segment", "segment_id", k)
			return err
		}
		m.Segments.Delete(k)
		m.LoadedSegments.Add(-1)
	}

	if m.LoadedSegments.Load() == 0 {
		m.log.Debug("All segments were cleared during vacuum. Recreating initial segment...")
		if err := m.Rotate(); err != nil {
			m.log.Error(err, "Failed creating initial segment")
			return err
		}
		m.MaxSegment.Store(0)
	}

	maxSeg := int64(0)
	for k, _ := range m.Segments.Range() {
		if k > maxSeg {
			maxSeg = k
		}
	}
	m.MaxSegment.Store(maxSeg)

	if m.CurrentSegment == nil {
		m.CurrentSegment, _ = m.Segments.Load(m.MaxSegment.Load())
	}

	return nil
}
