package timeseries

import (
	"bufio"
	"encoding/binary"
	"hash/crc32"
	"io"
)

func encodeRecord(record Record) []byte {
	size := 1 + 8 + 8 + 4 + len(record.Key) + 4 + len(record.Series) + 4 + len(record.Value)
	buf := make([]byte, size)
	pos := 1
	if record.Deleted {
		buf[0] = 1
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
	return buf
}

func decodeRecord(buf []byte) (Record, error) {
	if len(buf) < 25 {
		return Record{}, ErrCorruptStorage
	}
	record := Record{Deleted: buf[0] == 1}
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
