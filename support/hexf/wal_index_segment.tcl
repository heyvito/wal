big_endian

section "Metadata" {
    uint64 "Segment ID"
    uint64 "Size"
    uint64 "Lower Record"
    uint64 "Upper Record"
    uint64 "Records Count"
    set cursor [uint64]
    move -8
    entry "Cursor" $cursor
    move 8
    set flags [uint8]
    move -1
    set purged [expr $flags & 1 == 1]
    entry "Purged" $purged 1
    move 1
}

while {[pos] < [expr $cursor + 49]} {
    section "Record" {
        uint64 "Record ID"
        uint64 "Data Segment Start ID"
        uint64 "Data Segment End ID"
        uint64 "Data Segment Offset"
        uint64 "Size"
        byte "Flags"
    }
}
