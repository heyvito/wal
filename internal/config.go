package internal

import "github.com/go-stdlog/stdlog"

type Config interface {
	GetIndexSegmentSize() int64
	GetDataSegmentSize() int64
	GetWorkdir() string
	GetLogger() stdlog.Logger
}
