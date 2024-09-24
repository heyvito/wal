package metrics

import (
	"github.com/heyvito/wal/internal/metrics"
	"sync/atomic"
)

var hasDelegate atomic.Bool

func InstallDelegate(del *Delegates) {
	if hasDelegate.Swap(true) {
		return
	}
	go metrics.Dispatch(del)
}

type Delegates struct {
	Main         MainInstrumentationDelegate
	Index        IndexInstrumentationDelegate
	DataManager  DataManagerInstrumentationDelegate
	IndexSegment IndexSegmentInstrumentationDelegate
}

func (d *Delegates) Dispatch(kind metrics.MetricKind, value float64) {
	switch kind {
	case metrics.CommonWriteObjectCalls:
		d.Main.WriteObjectCalls(value)
	case metrics.CommonWriteObjectLatency:
		d.Main.WriteObjectLatency(value)
	case metrics.CommonWriteObjectFailures:
		d.Main.WriteObjectFailures(value)
	case metrics.CommonReadObjectCalls:
		d.Main.ReadObjectCalls(value)
	case metrics.CommonReadObjectLatency:
		d.Main.ReadObjectLatency(value)
	case metrics.CommonReadObjectFailures:
		d.Main.ReadObjectFailures(value)
	case metrics.CommonIndexInitializationTiming:
		d.Main.IndexInitializationTiming(value)
	case metrics.CommonIndexInitializationFailures:
		d.Main.IndexInitializationFailures(value)
	case metrics.CommonCloseIndexFailures:
		d.Main.CloseIndexFailures(value)
	case metrics.CommonCloseIndexTiming:
		d.Main.CloseIndexTiming(value)
	case metrics.CommonDataManagerInitializationTiming:
		d.Main.DataManagerInitializationTiming(value)
	case metrics.CommonDataManagerInitializationFailures:
		d.Main.DataManagerInitializationFailures(value)
	case metrics.CommonCloseDataManagerTiming:
		d.Main.CloseDataManagerTiming(value)
	case metrics.CommonCloseDataManagerFailures:
		d.Main.CloseDataManagerFailures(value)
	case metrics.CommonCountObjectsTiming:
		d.Main.CountObjectsTiming(value)
	case metrics.CommonTotalIndexSize:
		d.Main.TotalIndexSize(value)
	case metrics.CommonTotalDataSize:
		d.Main.TotalDataSize(value)
	case metrics.CommonIndexSegmentsCount:
		d.Main.IndexSegmentsCount(value)
	case metrics.CommonDataSegmentsCount:
		d.Main.DataSegmentsCount(value)
	case metrics.IndexAppendLatency:
		d.Index.AppendLatency(value)
	case metrics.IndexAppendCalls:
		d.Index.AppendCalls(value)
	case metrics.IndexLookupLatency:
		d.Index.LookupLatency(value)
	case metrics.IndexCountObjectsLatency:
		d.Index.CountObjectsLatency(value)
	case metrics.IndexVacuumObjectsLatency:
		d.Index.VacuumObjectsLatency(value)
	case metrics.DataManagerWriteLatency:
		d.DataManager.WriteLatency(value)
	case metrics.DataManagerWriteCalls:
		d.DataManager.WriteCalls(value)
	case metrics.DataManagerReadLatency:
		d.DataManager.ReadLatency(value)
	case metrics.DataManagerReadCalls:
		d.DataManager.ReadCalls(value)
	case metrics.DataManagerVacuumCalls:
		d.DataManager.VacuumCalls(value)
	case metrics.DataManagerVacuumLatency:
		d.DataManager.VacuumLatency(value)
	case metrics.IndexSegmentFlushMetaCalls:
		d.IndexSegment.FlushMetaCalls(value)
	case metrics.IndexSegmentFlushMetaLatency:
		d.IndexSegment.FlushMetaLatency(value)
	case metrics.IndexSegmentPurgeFromLatency:
		d.IndexSegment.PurgeFromLatency(value)
	case metrics.IndexSegmentWriteRecordLatency:
		d.IndexSegment.WriteRecordLatency(value)
	case metrics.IndexSegmentLoadRecordLatency:
		d.IndexSegment.LoadRecordLatency(value)
	}
}

type MainInstrumentationDelegate interface {
	WriteObjectCalls(float64)
	WriteObjectLatency(float64)
	WriteObjectFailures(float64)

	ReadObjectCalls(float64)
	ReadObjectLatency(float64)
	ReadObjectFailures(float64)

	IndexInitializationTiming(float64)
	IndexInitializationFailures(float64)
	CloseIndexFailures(float64)
	CloseIndexTiming(float64)

	DataManagerInitializationTiming(float64)
	DataManagerInitializationFailures(float64)
	CloseDataManagerTiming(float64)
	CloseDataManagerFailures(float64)

	CountObjectsTiming(float64)

	TotalIndexSize(float64)
	TotalDataSize(float64)
	IndexSegmentsCount(float64)
	DataSegmentsCount(float64)
}

type IndexInstrumentationDelegate interface {
	AppendLatency(float64)
	AppendCalls(float64)
	LookupLatency(float64)
	CountObjectsLatency(float64)
	VacuumObjectsLatency(float64)
}

type DataManagerInstrumentationDelegate interface {
	WriteLatency(float64)
	WriteCalls(float64)

	ReadLatency(float64)
	ReadCalls(float64)

	VacuumCalls(float64)
	VacuumLatency(float64)
}

type IndexSegmentInstrumentationDelegate interface {
	FlushMetaCalls(float64)
	FlushMetaLatency(float64)
	PurgeFromLatency(float64)
	WriteRecordLatency(float64)
	LoadRecordLatency(float64)
}
