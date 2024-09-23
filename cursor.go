package wal

import "io"

type Cursor interface {
	Next() bool
	Read() (io.Reader, error)
	Offset() int64
}
