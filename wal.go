package timeseries

import (
	"bufio"
	"io"
	"os"
	"path/filepath"
)

const walName = "current.wal"

type walFile struct {
	file        *os.File
	writer      *bufio.Writer
	disableSync bool
}

func walPath(dir string) string { return filepath.Join(dir, walName) }

func openWAL(dir string, disableSync bool) (*walFile, error) {
	file, err := os.OpenFile(walPath(dir), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		file.Close()
		return nil, err
	}
	return &walFile{file: file, writer: bufio.NewWriter(file), disableSync: disableSync}, nil
}

func readWAL(dir string) ([]Record, error) {
	file, err := os.Open(walPath(dir))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer file.Close()
	reader := bufio.NewReader(file)
	var records []Record
	var pending []Record
	for {
		record, committed, ok, err := readFramedRecord(reader)
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		pending = append(pending, record)
		if committed {
			records = append(records, pending...)
			pending = nil
		}
	}
	return records, nil
}

func (w *walFile) append(record Record) error {
	return w.appendBatch([]Record{record})
}

// appendBatch writes every record and only fsyncs the final commit. Recovery
// still accepts the complete trailing batch, while a torn tail is ignored.
func (w *walFile) appendBatch(records []Record) error {
	for i, record := range records {
		if err := writeFramedRecord(w.writer, record, i == len(records)-1); err != nil {
			return err
		}
	}
	if err := w.writer.Flush(); err != nil {
		return err
	}
	if !w.disableSync {
		return w.file.Sync()
	}
	return nil
}

func (w *walFile) Close() error { return w.file.Close() }

func rotateWAL(dir string, old *walFile, disableSync bool) (*walFile, error) {
	temporary := walPath(dir) + ".new"
	file, err := os.OpenFile(temporary, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if !disableSync {
		if err := file.Sync(); err != nil {
			file.Close()
			return nil, err
		}
	}
	if err := file.Close(); err != nil {
		return nil, err
	}
	if err := old.file.Close(); err != nil {
		return nil, err
	}
	if err := os.Rename(temporary, walPath(dir)); err != nil {
		return nil, err
	}
	if err := syncDir(dir); err != nil {
		return nil, err
	}
	return openWAL(dir, disableSync)
}
