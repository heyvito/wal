package internal

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIndexRecordWrite(t *testing.T) {
	rec := IndexRecord{
		RecordID:           10,
		DataSegmentStartID: 20,
		DataSegmentEndID:   30,
		DataSegmentOffset:  40,
		Size:               50,
		Purged:             false,
	}
	data := make([]byte, IndexRecordSize)
	rec.Write(data)
	expected := mustByesFromHex("00000000 0000000A 00000000 00000014 00000000 0000001E 00000000 00000028 00000000 00000032 00")
	assert.Equal(t, expected, data)
}

func TestIndexRecordRead(t *testing.T) {
	data := mustByesFromHex("00000000 0000000A 00000000 00000014 00000000 0000001E 00000000 00000028 00000000 00000032 00")
	expected := IndexRecord{
		RecordID:           10,
		DataSegmentStartID: 20,
		DataSegmentEndID:   30,
		DataSegmentOffset:  40,
		Size:               50,
		Purged:             false,
	}
	current := IndexRecord{}
	current.Read(data)
	assert.Equal(t, expected, current)
}
