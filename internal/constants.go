package internal

import "encoding/binary"

var be = binary.BigEndian

var indexSegmentOffsets = struct {
	SegmentID    uint8
	Size         uint8
	UpperRecord  uint8
	LowerRecord  uint8
	RecordsCount uint8
	Cursor       uint8
	Flags        uint8
}{
	SegmentID:    0,
	Size:         8,
	LowerRecord:  16,
	UpperRecord:  24,
	RecordsCount: 32,
	Cursor:       40,
	Flags:        48,
}

var indexRecordOffsets = struct {
	RecordID           uint8
	DataSegmentStartID uint8
	DataSegmentEndID   uint8
	DataSegmentOffset  uint8
	Size               uint8
	Flags              uint8
}{
	RecordID:           0,
	DataSegmentStartID: 8,
	DataSegmentEndID:   16,
	DataSegmentOffset:  24,
	Size:               32,
	Flags:              40,
}

var dataSegmentOffsets = struct {
	SegmentID uint8
	Size      uint8
	Cursor    uint8
}{
	SegmentID: 0,
	Size:      8,
	Cursor:    16,
}
