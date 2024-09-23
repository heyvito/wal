package internal

import "io"

type IndexCursor interface {
	Next() bool
	Read() (io.Reader, error)
	Offset() int64
}

type indexCursor struct {
	index  *Index
	wants  int64
	record IndexRecord
}

func (i *indexCursor) Next() bool {
	err := i.index.LookupMeta(i.wants, &i.record)
	if err != nil {
		return false
	}
	i.wants++
	return true
}

func (i *indexCursor) Read() (io.Reader, error) {
	return i.index.ReadRecord(&i.record)
}

func (i *indexCursor) Offset() int64 {
	// Assumes Next() has been called (as it should)
	return i.wants - 1
}
