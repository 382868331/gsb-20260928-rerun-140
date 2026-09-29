package timeseries

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

type segmentMeta struct {
	ID      uint64 `json:"id"`
	Version uint64 `json:"version"`
}

type manifestData struct {
	Segments []segmentMeta `json:"segments"`
}

func segmentPath(dir string, id uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%020d.seg", id))
}

func manifestPath(dir string) string { return filepath.Join(dir, "manifest.json") }

func readManifest(dir string) (manifestData, error) {
	data, err := os.ReadFile(manifestPath(dir))
	if os.IsNotExist(err) {
		return manifestData{}, nil
	}
	if err != nil {
		return manifestData{}, err
	}
	var manifest manifestData
	if len(data) == 0 {
		return manifest, nil
	}
	if err := json.Unmarshal(data, &manifest); err != nil {
		return manifestData{}, err
	}
	return manifest, nil
}

func writeManifest(dir string, manifest manifestData) error {
	data, err := json.MarshalIndent(&manifest, "", "  ")
	if err != nil {
		return err
	}
	temporary := manifestPath(dir) + ".tmp"
	if err := os.WriteFile(temporary, data, 0o600); err != nil {
		return err
	}
	if err := os.Rename(temporary, manifestPath(dir)); err != nil {
		return err
	}
	return syncDir(dir)
}

func readSegmentFile(dir string, id uint64) ([]Record, error) {
	file, err := os.Open(segmentPath(dir, id))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return readAllFramedRecords(bufio.NewReader(file))
}

func writeSegmentFile(dir string, id uint64, records []Record) error {
	temporary := segmentPath(dir, id) + ".tmp"
	file, err := os.OpenFile(temporary, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	writer := bufio.NewWriter(file)
	for _, record := range records {
		if err := writeFramedRecord(writer, record, true); err != nil {
			file.Close()
			return err
		}
	}
	if err := writer.Flush(); err != nil {
		file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporary, segmentPath(dir, id)); err != nil {
		return err
	}
	return syncDir(dir)
}

func readAllFramedRecords(r *bufio.Reader) ([]Record, error) {
	var records []Record
	for {
		record, _, ok, err := readFramedRecord(r)
		if err != nil {
			return nil, err
		}
		if !ok {
			return records, nil
		}
		records = append(records, record)
	}
}

func syncDir(dir string) error {
	handle, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer handle.Close()
	return handle.Sync()
}
