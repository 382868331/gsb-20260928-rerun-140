package timeseries

import (
	"bufio"
	"encoding/binary"
	"hash/crc32"
	"io"
)

func encodeRecord(record Record) []byte {
	// Bit 0 marks tombstones and bit 1 aggregate records. Aggregates also set
	// bit 2 because newer records carry a sample count; old aggregate records
	// decode with zero samples.
	size := 1 + 8 + 8 + 4 + len(record.Key) + 4 + len(record.Series) + 4 + len(record.Value)
	if record.IsAggregate {
		size += 4 + len(record.Name) + 8 + 8 + 8
	}
	buf := make([]byte, size)
	pos := 1
	if record.Deleted {
		buf[0] = 1
	}
	if record.IsAggregate {
		buf[0] |= 2
		buf[0] |= 4
	}
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
	if record.IsAggregate {
		putBytes([]byte(record.Name))
		binary.LittleEndian.PutUint64(buf[pos:], uint64(record.Width))
		pos += 8
		binary.LittleEndian.PutUint64(buf[pos:], record.SourceVersion)
		pos += 8
		binary.LittleEndian.PutUint64(buf[pos:], uint64(record.SampleCount))
		pos += 8
	}
	return buf
}

func decodeRecord(buf []byte) (Record, error) {
	if len(buf) < 25 {
		return Record{}, ErrCorruptStorage
	}
	record := Record{Deleted: buf[0] == 1}
	record.IsAggregate = buf[0]&2 != 0
	hasSampleCount := buf[0]&4 != 0
	record.Deleted = buf[0]&1 != 0
	pos := 9
	record.Version = binary.LittleEndian.Uint64(buf[1:9])
	record.Timestamp = int64(binary.LittleEndian.Uint64(buf[9:17]))
	pos = 17
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
	if record.IsAggregate {
		var name []byte
		if name, err = getBytes(); err != nil {
			return Record{}, err
		}
		record.Name = string(name)
		if pos+16 > len(buf) {
			return Record{}, ErrCorruptStorage
		}
		record.Width = int64(binary.LittleEndian.Uint64(buf[pos:]))
		pos += 8
		record.SourceVersion = binary.LittleEndian.Uint64(buf[pos:])
		pos += 8
		if hasSampleCount {
			if pos+8 > len(buf) {
				return Record{}, ErrCorruptStorage
			}
			record.SampleCount = int64(binary.LittleEndian.Uint64(buf[pos:]))
			pos += 8
		}
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
