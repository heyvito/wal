package internal

import (
	"crypto/rand"
	"encoding/hex"
	"os"
	"strings"
	"testing"

	"github.com/go-stdlog/stdlog"
	"github.com/stretchr/testify/require"
)

func mustByesFromHex(s string) []byte {
	s = strings.ReplaceAll(s, " ", "")
	v, err := hex.DecodeString(s)
	if err != nil {
		panic(err)
	}
	return v
}

type DummyConfig struct {
	IndexSegmentSize int64
	DataSegmentSize  int64
	WorkDir          string
	Logger           stdlog.Logger
}

func (d DummyConfig) GetIndexSegmentSize() int64 {
	return d.IndexSegmentSize
}

func (d DummyConfig) GetDataSegmentSize() int64 {
	return d.DataSegmentSize
}

func (d DummyConfig) GetWorkdir() string {
	return d.WorkDir
}

func (d DummyConfig) GetLogger() stdlog.Logger {
	return d.Logger
}

func WithLogger() DummyOpt {
	return func(d *DummyConfig) { d.Logger = stdlog.NewStd(os.Stdout) }
}

func WithIndexSegmentSize(size int64) DummyOpt {
	return func(d *DummyConfig) { d.IndexSegmentSize = size }
}

func WithDataSegmentSize(size int64) DummyOpt {
	return func(d *DummyConfig) { d.DataSegmentSize = size }
}

type DummyOpt func(*DummyConfig)

func NewDummyConfig(t *testing.T, dummyOpts ...DummyOpt) *DummyConfig {
	t.Helper()
	d := &DummyConfig{
		IndexSegmentSize: IndexRecordSize * 3,
		DataSegmentSize:  64,
		WorkDir:          t.TempDir(),
		Logger:           stdlog.Discard,
	}

	for _, opt := range dummyOpts {
		opt(d)
	}

	return d
}

func randomData(t *testing.T, size int64) []byte {
	t.Helper()

	data := make([]byte, size)
	n, err := rand.Read(data)
	require.NoError(t, err)
	require.Equal(t, int64(n), size)
	return data
}
