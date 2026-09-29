package tsstore

import (
	"encoding/binary"
	"hash/crc32"
	"math"
)

// WAL record: [u64 len][payload len][u32 crc]
// payload: u8 kind, i64 version, str series, i64 ts, f64 value

const (
	walKindAppend = 1
	walKindRevise = 2
	walKindExpire = 3
)

func encodeWAL(kind byte, version int64, series string, ts int64, value float64) []byte {
	p := make([]byte, 0, 1+8+4+len(series)+8+8)
	p = append(p, kind)
	p = binary.BigEndian.AppendUint64(p, uint64(version))
	p = binary.BigEndian.AppendUint32(p, uint32(len(series)))
	p = append(p, series...)
	p = binary.BigEndian.AppendUint64(p, uint64(ts))
	var fb [8]byte
	binary.BigEndian.PutUint64(fb[:], math.Float64bits(value))
	p = append(p, fb[:]...)

	rec := make([]byte, 0, 8+len(p)+4)
	rec = binary.BigEndian.AppendUint64(rec, uint64(len(p)))
	rec = append(rec, p...)
	var crc [4]byte
	binary.BigEndian.PutUint32(crc[:], crc32.ChecksumIEEE(p))
	rec = append(rec, crc[:]...)
	return rec
}

// decodeWALRecord decodes one record from b. It returns the number of bytes
// consumed and the decoded fields. ok is false for a torn/invalid tail record.
func decodeWALRecord(b []byte) (consumed int, kind byte, version int64, series string, ts int64, value float64, ok bool) {
	if len(b) < 8 {
		return 0, 0, 0, "", 0, 0, false
	}
	plen := int(binary.BigEndian.Uint64(b[:8]))
	total := 8 + plen + 4
	if plen < 1+8+4+8+8 || len(b) < total {
		return 0, 0, 0, "", 0, 0, false
	}
	p := b[8 : 8+plen]
	crc := binary.BigEndian.Uint32(b[8+plen:])
	if crc32.ChecksumIEEE(p) != crc {
		return 0, 0, 0, "", 0, 0, false
	}
	off := 0
	kind = p[off]
	off++
	version = int64(binary.BigEndian.Uint64(p[off:]))
	off += 8
	slen := int(binary.BigEndian.Uint32(p[off:]))
	off += 4
	if slen > plen-off-8-8 {
		return 0, 0, 0, "", 0, 0, false
	}
	series = string(p[off : off+slen])
	off += slen
	ts = int64(binary.BigEndian.Uint64(p[off:]))
	off += 8
	value = math.Float64frombits(binary.BigEndian.Uint64(p[off:]))
	return total, kind, version, series, ts, value, true
}

// Segment file layout (all big-endian):
//   magic "TSSG" u32=1
//   u32 record count
//   records, each:
//     u8 flags (1 = deleted)
//     i64 version, i64 timestamp, f64 value
//     u32 series length, series bytes
//   crc32 of all record bytes

var segmentMagic = []byte("TSSG")

const segmentFlagDeleted = 1

func encodeSegment(entries []entryWithSeries) []byte {
	bodySize := 0
	for _, e := range entries {
		bodySize += 1 + 8 + 8 + 8 + 4 + len(e.series)
	}
	buf := make([]byte, 0, 4+4+4+bodySize)
	buf = append(buf, segmentMagic...)
	buf = binary.BigEndian.AppendUint32(buf, 1)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(entries)))
	bodyStart := len(buf)
	for _, e := range entries {
		flags := byte(0)
		if e.entry.deleted {
			flags = segmentFlagDeleted
		}
		buf = append(buf, flags)
		buf = binary.BigEndian.AppendUint64(buf, uint64(e.entry.version))
		buf = binary.BigEndian.AppendUint64(buf, uint64(e.entry.ts))
		buf = binary.BigEndian.AppendUint64(buf, math.Float64bits(e.entry.value))
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(e.series)))
		buf = append(buf, e.series...)
	}
	var crc [4]byte
	binary.BigEndian.PutUint32(crc[:], crc32.ChecksumIEEE(buf[bodyStart:]))
	buf = append(buf, crc[:]...)
	return buf
}

type entryWithSeries struct {
	series string
	entry  entry
}

// decodeSegment parses a segment file. A truncated or corrupted file
// returns an error so callers never load partial compaction output.
func decodeSegment(buf []byte) ([]entryWithSeries, error) {
	if len(buf) < 4+4+4+4 {
		return nil, errCorruptSegment
	}
	if string(buf[:4]) != "TSSG" {
		return nil, errCorruptSegment
	}
	if binary.BigEndian.Uint32(buf[4:8]) != 1 {
		return nil, errCorruptSegment
	}
	n := binary.BigEndian.Uint32(buf[8:12])
	bodyEnd := len(buf) - 4
	if crc32.ChecksumIEEE(buf[12:bodyEnd]) != binary.BigEndian.Uint32(buf[bodyEnd:]) {
		return nil, errCorruptSegment
	}
	out := make([]entryWithSeries, 0, n)
	off := 12
	for i := uint32(0); i < n; i++ {
		if off+1+8+8+8+4 > bodyEnd {
			return nil, errCorruptSegment
		}
		flags := buf[off]
		off++
		version := int64(binary.BigEndian.Uint64(buf[off:]))
		off += 8
		ts := int64(binary.BigEndian.Uint64(buf[off:]))
		off += 8
		value := math.Float64frombits(binary.BigEndian.Uint64(buf[off:]))
		off += 8
		slen := int(binary.BigEndian.Uint32(buf[off:]))
		off += 4
		if off+slen > bodyEnd {
			return nil, errCorruptSegment
		}
		series := string(buf[off : off+slen])
		off += slen
		out = append(out, entryWithSeries{series: series, entry: entry{
			version: version,
			ts:      ts,
			value:   value,
			deleted: flags&segmentFlagDeleted != 0,
		}})
	}
	if off != bodyEnd {
		return nil, errCorruptSegment
	}
	return out, nil
}
