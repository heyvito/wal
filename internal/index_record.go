package internal

const IndexRecordSize = 8*5 + 1

type IndexRecord struct {
	RecordID           int64
	DataSegmentOffset  int64
	DataSegmentStartID int64
	DataSegmentEndID   int64
	Size               int64
	Purged             bool
}

func (i *IndexRecord) Read(b []byte) {
	i.RecordID = int64(be.Uint64(b[indexRecordOffsets.RecordID:]))
	i.DataSegmentStartID = int64(be.Uint64(b[indexRecordOffsets.DataSegmentStartID:]))
	i.DataSegmentEndID = int64(be.Uint64(b[indexRecordOffsets.DataSegmentEndID:]))
	i.DataSegmentOffset = int64(be.Uint64(b[indexRecordOffsets.DataSegmentOffset:]))
	i.Size = int64(be.Uint64(b[indexRecordOffsets.Size:]))
	flags := b[indexRecordOffsets.Flags]
	i.Purged = flags&0x01 != 0x00
}

func (i *IndexRecord) Write(b []byte) {
	be.PutUint64(b[indexRecordOffsets.RecordID:], uint64(i.RecordID))
	be.PutUint64(b[indexRecordOffsets.DataSegmentStartID:], uint64(i.DataSegmentStartID))
	be.PutUint64(b[indexRecordOffsets.DataSegmentEndID:], uint64(i.DataSegmentEndID))
	be.PutUint64(b[indexRecordOffsets.DataSegmentOffset:], uint64(i.DataSegmentOffset))
	be.PutUint64(b[indexRecordOffsets.Size:], uint64(i.Size))
	flags := byte(0x00)
	if i.Purged {
		flags |= 0x01
	}
	b[indexRecordOffsets.Flags] = flags
}

func SetIndexRecordPurged(b []byte) {
	flags := b[indexRecordOffsets.Flags]
	flags |= 0x01
	b[indexRecordOffsets.Flags] = flags
}

func IsIndexRecordPurged(b []byte) bool {
	return b[indexRecordOffsets.Flags]&0x01 != 0
}
