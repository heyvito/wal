package wal

import "github.com/go-stdlog/stdlog"

type Config struct {
	// DataSegmentSize defines the maximum size of a given Data Segment. This
	// values does not affect segments already present in the disk, if any.
	DataSegmentSize int64

	// IndexSegmentSize indicates the maximum size of the system index before
	// it is split into a next file. Each index record takes 41 bytes
	// choosing a multiple of that value will prevent index segments from
	// consuming more than it may require.
	IndexSegmentSize int64

	// WorkDir represents the absolute path to the directory where the WAL will
	// retain information. It is advised to use a directory in which only the
	// WAL instance will have access, as all files within that directory will
	// be automatically managed by the instance itself.
	WorkDir string

	// Logger allows a given stdlog.Logger instance to be set as the system
	// logger. If unset, no logs will be generated.
	Logger stdlog.Logger
}

func (c Config) GetIndexSegmentSize() int64 {
	return c.IndexSegmentSize
}

func (c Config) GetDataSegmentSize() int64 {
	return c.DataSegmentSize
}

func (c Config) GetWorkdir() string {
	return c.WorkDir
}

func (c Config) GetLogger() stdlog.Logger {
	if c.Logger != nil {
		return c.Logger.Named("wal")
	}
	return stdlog.Discard
}
