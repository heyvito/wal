package internal

import (
	"fmt"
	"github.com/heyvito/wal/internal/metrics"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-stdlog/stdlog"
	"github.com/heyvito/wal/errors"
)

type Index struct {
	Config         Config
	Workdir        string
	MinSegment     int64
	MaxSegment     atomic.Int64
	MaxRecord      atomic.Int64
	Segments       AtomicMap[int64, *IndexSegment]
	LoadedSegments atomic.Int32
	CurrentSegment *IndexSegment
	log            stdlog.Logger

	dm *DataManager

	writeMu sync.Mutex

	measureUsageTimer *time.Ticker
}

func NewIndex(config Config) (*Index, error) {
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

	log := config.GetLogger().Named("index")

	done := metrics.Measure(metrics.CommonDataManagerInitializationTiming)
	dm, err := NewDataManager(config)
	if err != nil {
		metrics.Simple(metrics.CommonDataManagerInitializationFailures, 0)
		return nil, fmt.Errorf("failed starting data manager: %w", err)
	}
	done()

	var segmentsToLoad []int64
	entries, err := os.ReadDir(wd)
	if err != nil {
		return nil, err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if !strings.HasPrefix(entry.Name(), "index") {
			continue
		}
		id, err := strconv.ParseInt(entry.Name()[5:], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("%s: invalid index file name: %w", entry.Name(), err)
		}
		segmentsToLoad = append(segmentsToLoad, id)
	}

	log.Info("Loading index segments", "size", len(segmentsToLoad))
	slices.Sort(segmentsToLoad)

	i := &Index{
		Config:         config,
		Workdir:        wd,
		CurrentSegment: nil,
		MinSegment:     -1,
		log:            log,
		dm:             dm,
	}

	i.MaxSegment.Store(-1)

	for _, id := range segmentsToLoad {
		segment, err := NewIndexSegment(id, config)
		if err != nil {
			_ = i.Close()
			log.Error(err, "Failed loading index segment", "id", id)
			return nil, err
		}
		if id > i.MinSegment || i.MinSegment == -1 {
			i.MinSegment = id
		}
		i.Segments.Store(id, segment)
		i.LoadedSegments.Add(1)
		if i.CurrentSegment == nil || id > i.CurrentSegment.SegmentID {
			i.CurrentSegment = segment
			i.MaxRecord.Store(segment.UpperRecord.Load())
			i.MaxSegment.Store(segment.SegmentID)
		}
	}

	if len(segmentsToLoad) == 0 {
		return i, i.Rotate()
	}

	i.measureUsageTimer = time.NewTicker(10 * time.Second)
	go i.measureUsage()
	return i, nil
}

func (i *Index) Close() error {
	i.writeMu.Lock()
	defer i.writeMu.Unlock()

	if i.measureUsageTimer != nil {
		i.measureUsageTimer.Stop()
	}

	if i.dm != nil {
		done := metrics.Measure(metrics.CommonCloseDataManagerTiming)
		if err := i.dm.Close(); err != nil {
			metrics.Simple(metrics.CommonCloseDataManagerFailures, 0)
			i.log.Error(err, "Failed closing data manager")
			return err
		}
		done()
	}

	for id, segment := range i.Segments.Range() {
		if err := segment.Close(); err != nil {
			i.log.Error(err, "Failed closing segment", "segment_id", id)
			return err
		}
	}

	return nil
}

func (i *Index) Rotate() error {
	var seg *IndexSegment
	var err error
	if i.CurrentSegment == nil {
		seg, err = NewIndexSegment(0, i.Config)
		if err != nil {
			return err
		}
		i.MinSegment = 0
		i.MaxSegment.Store(0)
		i.Segments.Store(0, seg)
		i.MaxRecord.Store(-1)
		i.CurrentSegment = seg
	} else {
		seg, err = NewIndexSegment(i.CurrentSegment.SegmentID+1, i.Config)
		if err != nil {
			return err
		}
		i.CurrentSegment = seg
		i.Segments.Store(seg.SegmentID, seg)
		i.MaxSegment.Store(seg.SegmentID)
	}
	i.LoadedSegments.Add(1)
	return nil
}

func (i *Index) Append(data []byte, rec *IndexRecord) error {
	i.writeMu.Lock()
	defer i.writeMu.Unlock()
	defer metrics.Measure(metrics.IndexAppendLatency)()
	metrics.Simple(metrics.IndexAppendCalls, 0)

	if !i.CurrentSegment.FitsRecord() {
		if err := i.Rotate(); err != nil {
			return err
		}
	}

	recID := i.MaxRecord.Load() + 1
	rec.RecordID = recID
	rec.Size = int64(len(data))
	rec.Purged = false

	if err := i.dm.Write(data, rec); err != nil {
		return err
	}

	i.MaxRecord.Store(recID)

	i.CurrentSegment.WriteRecord(rec)
	return nil
}

func (i *Index) SegmentForID(id int64) (*IndexSegment, bool) {
	for _, seg := range i.Segments.Range() {
		if seg.ContainsRecord(id) {
			return seg, true
		}
	}
	return nil, false
}

func (i *Index) LookupMeta(id int64, rec *IndexRecord) error {
	defer metrics.Measure(metrics.IndexLookupLatency)()
	seg, ok := i.SegmentForID(id)
	if !ok {
		return errors.NotFound{RecordID: id}
	}
	seg.LoadRecord(id, rec)
	return nil
}

func (i *Index) ReadRecord(rec *IndexRecord) (io.Reader, error) {
	return i.dm.Read(rec)
}

func (i *Index) IsEmpty() bool {
	if i.MinSegment == 0 && i.MaxSegment.Load() == 0 {
		return i.CurrentSegment.RecordsCount.Load() == 0
	}

	for _, seg := range i.Segments.Range() {
		if !seg.Purged {
			return false
		}
	}

	return true
}

func (i *Index) CountObjects(id int64, inclusive bool) int64 {
	defer metrics.Measure(metrics.IndexCountObjectsLatency)()
	seg, ok := i.SegmentForID(id)
	if !ok {
		return 0
	}
	total := seg.UpperRecord.Load() - id

	for {
		seg, ok = i.Segments.Load(seg.SegmentID + 1)
		if !ok {
			break
		}
		total += seg.RecordsCount.Load()
	}
	if inclusive {
		total += 1
	}
	return total
}

func (i *Index) ReadObjects(id int64, inclusive bool) IndexCursor {
	if !inclusive {
		id += 1
	}
	return &indexCursor{
		index: i,
		wants: id,
	}
}

func (i *Index) VacuumObjects(id int64, inclusive bool) error {
	i.writeMu.Lock()
	defer i.writeMu.Unlock()
	defer metrics.Measure(metrics.IndexVacuumObjectsLatency)()

	if !inclusive {
		id = id - 1
	}

	if id < 0 {
		return nil
	}

	i.log.Info("Vacuum starting", "id", id)
	defer i.log.Info("Vacuum finished")

	seg, ok := i.SegmentForID(id)
	if !ok {
		i.log.Warning("Attempt to vacuum from non-existing object", "id", id)
		return nil
	}

	var segsToRemove []int64

	i.log.Debug("Vacuum starting at segment", "id", seg.SegmentID)
	seg.PurgeFrom(id)
	if seg.Purged {
		segsToRemove = append(segsToRemove, seg.SegmentID)
		i.log.Debug("Vacuum purged segment as start offset was its last possible item", "id", seg.SegmentID)
	}

	i.log.Debug("Marking previous segments as purged...")
	segID := seg.SegmentID - 1
	for {
		seg, ok := i.Segments.Load(segID)
		if !ok {
			break
		}
		seg.Purged = true
		seg.FlushMetadata()
		i.log.Debug("Marking segment as purged", "id", segID)
		segsToRemove = append(segsToRemove, segID)
		segID--
	}

	drsInUse := map[int64]bool{}
	for _, seg := range i.Segments.Range() {
		if seg.Purged {
			continue
		}
		minID, maxID := seg.LowerRecord.Load(), seg.UpperRecord.Load()
		rec := &IndexRecord{}
		for i := minID; i <= maxID; i++ {
			seg.LoadRecord(i, rec)
			if rec.Purged {
				continue
			}
			for i := rec.DataSegmentStartID; i <= rec.DataSegmentEndID; i++ {
				drsInUse[i] = true
			}
		}
	}

	dataInUse := make([]int64, 0, len(drsInUse))
	for k := range drsInUse {
		dataInUse = append(dataInUse, k)
	}

	if err := i.dm.VacuumDataSegments(dataInUse); err != nil {
		return err
	}

	for _, v := range segsToRemove {
		seg, ok := i.Segments.Load(v)
		if !ok {
			i.log.Warning("Could not find segment marked for removal", "segment_id", v)
			continue
		}
		i.log.Debug("Unlinking segment", "segment_id", v)
		if err := seg.Unlink(); err != nil {
			i.log.Error(err, "Failed unlinking segment", "segment_id", v)
			return err
		}
		i.Segments.Delete(v)
		i.LoadedSegments.Add(-1)
		if i.CurrentSegment != nil && v == i.CurrentSegment.SegmentID {
			i.CurrentSegment = nil
		}
	}

	if i.LoadedSegments.Load() == 0 {
		i.log.Debug("All segments were cleared during vacuum. Recreating initial segment...")
		if err := i.Rotate(); err != nil {
			i.log.Error(err, "Failed recreating initial segment", "segment_id", i.CurrentSegment.SegmentID)
			return err
		}
		i.MaxSegment.Store(0)
	}

	minSeg := int64(math.MaxInt64)
	maxSeg := int64(0)
	for k, _ := range i.Segments.Range() {
		if k >= maxSeg {
			maxSeg = k
		}
		if k <= minSeg {
			minSeg = k
		}
	}
	i.MaxSegment.Store(maxSeg)
	i.MinSegment = minSeg

	if i.CurrentSegment == nil {
		i.CurrentSegment, _ = i.Segments.Load(i.MaxSegment.Load())
	}

	if rec := i.CurrentSegment.RecordsCount.Load(); rec == 0 {
		i.MaxRecord.Store(-1)
	} else {
		i.MaxRecord.Store(rec)
	}

	return nil
}

func (i *Index) measureUsage() {
	wd := i.Config.GetWorkdir()
loop:
	for range i.measureUsageTimer.C {
		totalIndexSize := int64(0)
		totalDataSize := int64(0)
		indexFiles := 0
		dataFiles := 0

		entries, err := os.ReadDir(wd)
		if err != nil {
			i.log.Error(err, "Failed reading directory for usage measurement", "path", wd)
			continue loop
		}
		for _, v := range entries {
			if v.IsDir() {
				continue
			}
			stat, err := os.Stat(filepath.Join(wd, v.Name()))
			if err != nil {
				i.log.Error(err, "Failed calling stat for directory entry", "name", v.Name())
				continue loop
			}
			switch {
			case strings.HasPrefix(v.Name(), "data"):
				totalDataSize += stat.Size()
				dataFiles++
			case strings.HasPrefix(v.Name(), "index"):
				totalIndexSize += stat.Size()
				indexFiles++
			}
		}

		metrics.Simple(metrics.CommonTotalIndexSize, float64(totalIndexSize))
		metrics.Simple(metrics.CommonTotalDataSize, float64(totalDataSize))
		metrics.Simple(metrics.CommonIndexSegmentsCount, float64(indexFiles))
		metrics.Simple(metrics.CommonDataSegmentsCount, float64(dataFiles))
	}
}
