package timeseries

import (
	"bufio"
	"encoding/binary"
	"hash/crc32"
	"io"
	"math"
)

// recordKind separates raw observations from fixed-width bucket aggregates and
// aggregate tombstones.
const (
	recordKindObservation byte = 0
	recordKindAggregate   byte = 1
	recordKindTombstone   byte = 2
)

func encodeRecord(record Record) []byte {
	size := 1 + 8 + 8 + 4 + len(record.Key) + 4 + len(record.Series) + 4 + len(record.Value) + 1
	if record.Kind != recordKindObservation {
		size += 8 + 8 + 8 + 4 + len(record.Aggregator) + 8
	}
	buf := make([]byte, size)
	buf[0] = record.Kind
	pos := 1
	binary.LittleEndian.PutUint64(buf[pos:], record.Version)
	pos += 8
	binary.LittleEndian.PutUint64(buf[pos:], uint64(record.Timestamp))
	pos += 8
	putBytes := func(value []byte) {
		binary.LittleEndian.PutUint32(buf[pos:], uint32(len(value)))
		pos += 4
		pos += copy(buf[pos:], value)
	}
	putBytes([]byte(record.Key))
	putBytes([]byte(record.Series))
	putBytes(record.Value)
	if record.Deleted {
		buf[pos] = 1
	}
	pos++
	if record.Kind != recordKindObservation {
		binary.LittleEndian.PutUint64(buf[pos:], uint64(record.BucketStart))
		pos += 8
		binary.LittleEndian.PutUint64(buf[pos:], uint64(record.BucketWidth))
		pos += 8
		binary.LittleEndian.PutUint64(buf[pos:], record.SourceVersion)
		pos += 8
		putBytes([]byte(record.Aggregator))
		binary.LittleEndian.PutUint64(buf[pos:], math.Float64bits(record.AggregateValue))
		pos += 8
	}
	return buf
}

func decodeRecord(buf []byte) (Record, error) {
	if len(buf) < 26 {
		return Record{}, ErrCorruptStorage
	}
	record := Record{Kind: buf[0]}
	if record.Kind > recordKindTombstone {
		return Record{}, ErrCorruptStorage
	}
	record.Version = binary.LittleEndian.Uint64(buf[1:9])
	record.Timestamp = int64(binary.LittleEndian.Uint64(buf[9:17]))
	pos := 17
	getBytes := func() ([]byte, error) {
		if pos+4 > len(buf) {
			return nil, ErrCorruptStorage
		}
		size := int(binary.LittleEndian.Uint32(buf[pos:]))
		pos += 4
		if size < 0 || pos+size > len(buf) {
			return nil, ErrCorruptStorage
		}
		value := append([]byte(nil), buf[pos:pos+size]...)
		pos += size
		return value, nil
	}
	var err error
	var data []byte
	if data, err = getBytes(); err != nil {
		return Record{}, err
	}
	record.Key = string(data)
	if data, err = getBytes(); err != nil {
		return Record{}, err
	}
	record.Series = string(data)
	if record.Value, err = getBytes(); err != nil {
		return Record{}, err
	}
	if pos >= len(buf) {
		return Record{}, ErrCorruptStorage
	}
	record.Deleted = buf[pos] == 1
	pos++
	if record.Kind != recordKindObservation {
		if pos+28 > len(buf) {
			return Record{}, ErrCorruptStorage
		}
		record.BucketStart = int64(binary.LittleEndian.Uint64(buf[pos:]))
		pos += 8
		record.BucketWidth = int64(binary.LittleEndian.Uint64(buf[pos:]))
		pos += 8
		record.SourceVersion = binary.LittleEndian.Uint64(buf[pos:])
		pos += 8
		if data, err = getBytes(); err != nil {
			return Record{}, err
		}
		record.Aggregator = string(data)
		if pos+8 > len(buf) {
			return Record{}, ErrCorruptStorage
		}
		record.AggregateValue = math.Float64frombits(binary.LittleEndian.Uint64(buf[pos:]))
		pos += 8
	}
	if pos != len(buf) {
		return Record{}, ErrCorruptStorage
	}
	return record, nil
}

func writeFramedRecord(w io.Writer, record Record, commit bool) error {
	payload := encodeRecord(record)
	header := make([]byte, 13)
	binary.LittleEndian.PutUint32(header[0:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(header[4:8], crc32.ChecksumIEEE(payload))
	if commit {
		header[12] = 1
	}
	if _, err := w.Write(header); err != nil {
		return err
	}
	_, err := w.Write(payload)
	return err
}

func readFramedRecord(r *bufio.Reader) (Record, bool, bool, error) {
	header := make([]byte, 13)
	if _, err := io.ReadFull(r, header); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return Record{}, false, false, nil
		}
		return Record{}, false, false, err
	}
	size := binary.LittleEndian.Uint32(header[0:4])
	checksum := binary.LittleEndian.Uint32(header[4:8])
	if size > 64<<20 {
		return Record{}, false, false, ErrCorruptStorage
	}
	payload := make([]byte, size)
	if _, err := io.ReadFull(r, payload); err != nil {
		return Record{}, false, false, nil
	}
	if crc32.ChecksumIEEE(payload) != checksum {
		return Record{}, false, false, ErrCorruptStorage
	}
	record, err := decodeRecord(payload)
	return record, header[12] == 1, true, err
}
