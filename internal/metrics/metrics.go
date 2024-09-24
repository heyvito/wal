package metrics

import (
	"sync"
	"sync/atomic"
	"time"
)

var metricsCh = make(chan *metricReading, 1024)
var readingsPool = sync.Pool{
	New: func() interface{} {
		return &metricReading{}
	},
}
var dispatching atomic.Bool

func Simple(kind MetricKind, value float64) {
	if !dispatching.Load() {
		return
	}
	r := readingsPool.Get().(*metricReading)
	r.Kind = kind
	r.Value = value
	metricsCh <- r
}

func Measure(kind MetricKind) func() {
	start := time.Now()
	return func() {
		Simple(kind, float64(time.Since(start).Microseconds()))
	}
}

type metricReading struct {
	Kind  MetricKind
	Value float64
}

type delegate interface {
	Dispatch(kind MetricKind, value float64)
}

func Dispatch(del delegate) {
	for msg := range metricsCh {
		del.Dispatch(msg.Kind, msg.Value)
		readingsPool.Put(msg)
	}
}
