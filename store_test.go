package timeseries

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"
)

type referencePoint struct {
	timestamp int64
	data      []byte
	deleted   bool
}

func referenceRange(reference map[string]map[int64]referencePoint, series string, start, end int64) []Observation {
	var result []Observation
	for timestamp, point := range reference[series] {
		if !point.deleted && timestamp >= start && timestamp < end {
			result = append(result, Observation{Series: series, Timestamp: timestamp, Data: append([]byte(nil), point.data...)})
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Timestamp < result[j].Timestamp })
	return result
}

func assertObservations(t *testing.T, got []Observation, want []Observation) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("range length = %d, want %d (%v vs %v)", len(got), len(want), got, want)
	}
	for i := range got {
		if got[i].Series != want[i].Series || got[i].Timestamp != want[i].Timestamp || !bytes.Equal(got[i].Data, want[i].Data) {
			t.Fatalf("point %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func openTestStore(t *testing.T) *Store {
	t.Helper()
	store, err := Open(t.TempDir(), &Options{DisableSync: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func TestOutOfOrderAppendRangeAndRevisions(t *testing.T) {
	store := openTestStore(t)
	reference := map[string]map[int64]referencePoint{"cpu": {}}
	timestamps := []int64{30, 10, 20, 50, 40}
	for _, timestamp := range timestamps {
		data := []byte{byte(timestamp)}
		if _, err := store.Append(Observation{Series: "cpu", Timestamp: timestamp, Data: data}); err != nil {
			t.Fatal(err)
		}
		reference["cpu"][timestamp] = referencePoint{timestamp: timestamp, data: data}
	}

	assertObservations(t, store.Snapshot().Range("cpu", 20, 50), referenceRange(reference, "cpu", 20, 50))
	if got := store.Snapshot().Range("cpu", 20, 20); len(got) != 0 {
		t.Fatalf("empty half-open range returned %v", got)
	}

	before := store.Snapshot()
	_, err := store.Revise(Observation{Series: "cpu", Timestamp: 30, Data: []byte("updated")})
	if err != nil {
		t.Fatal(err)
	}
	reference["cpu"][30] = referencePoint{timestamp: 30, data: []byte("updated")}
	after := store.Snapshot()
	if got := before.Range("cpu", 20, 50); !bytes.Equal(got[1].Data, []byte{30}) {
		t.Fatalf("old snapshot changed: %q", got[1].Data)
	}
	assertObservations(t, after.Range("cpu", 20, 50), referenceRange(reference, "cpu", 20, 50))
	defer before.Close()
	defer after.Close()

	if _, err := store.Append(Observation{Series: "cpu", Timestamp: 10, Data: []byte("dup")}); !errors.Is(err, ErrObservationExists) {
		t.Fatalf("duplicate append error = %v", err)
	}
}

func TestDeleteBeforeSnapshotAndPhysicalCompaction(t *testing.T) {
	store := openTestStore(t)
	for _, timestamp := range []int64{1, 5, 10, 20} {
		if _, err := store.Append(Observation{Series: "s", Timestamp: timestamp, Data: []byte{byte(timestamp)}}); err != nil {
			t.Fatal(err)
		}
	}
	old := store.Snapshot()
	version, err := store.DeleteBefore(10)
	if err != nil || version != 7 {
		t.Fatalf("delete version = %d, err = %v", version, err)
	}
	current := store.Snapshot()
	if got := current.Range("s", 0, 100); len(got) != 2 {
		t.Fatalf("current view after expiry = %v", got)
	}
	current.Close()
	if got := old.Range("s", 0, 100); len(got) != 4 {
		t.Fatalf("expired snapshot = %v", got)
	}
	old.Close()
	if _, err := store.Compact(); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	files, err := os.ReadDir(store.path)
	if err != nil {
		t.Fatal(err)
	}
	var segmentCount int
	for _, file := range files {
		if filepath.Ext(file.Name()) == ".seg" {
			segmentCount++
		}
	}
	if segmentCount != 1 {
		t.Fatalf("segment count after cleanup = %d", segmentCount)
	}
}

func TestCrossSegmentRecovery(t *testing.T) {
	dir := t.TempDir()
	opts := &Options{DisableSync: true}
	store, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	reference := map[string]map[int64]referencePoint{"x": {}}
	appendPoint := func(timestamp int64, data string) {
		t.Helper()
		if _, err := store.Append(Observation{Series: "x", Timestamp: timestamp, Data: []byte(data)}); err != nil {
			t.Fatal(err)
		}
		reference["x"][timestamp] = referencePoint{timestamp: timestamp, data: []byte(data)}
	}
	appendPoint(30, "seg1-30")
	appendPoint(10, "seg1-10")
	if _, err := store.Compact(); err != nil {
		t.Fatal(err)
	}
	appendPoint(50, "wal-50")
	appendPoint(20, "wal-20")
	if _, err := store.Revise(Observation{Series: "x", Timestamp: 10, Data: []byte("revised")}); err != nil {
		t.Fatal(err)
	}
	reference["x"][10] = referencePoint{timestamp: 10, data: []byte("revised")}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := reopened.Snapshot()
	assertObservations(t, snapshot.Range("x", 0, 100), referenceRange(reference, "x", 0, 100))
	snapshot.Close()
	if _, err := reopened.Compact(); err != nil {
		t.Fatal(err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err = Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	snapshot = reopened.Snapshot()
	assertObservations(t, snapshot.Range("x", 20, 60), referenceRange(reference, "x", 20, 60))
	snapshot.Close()
}

func TestCompactionWaitsForInFlightSnapshot(t *testing.T) {
	store := openTestStore(t)
	if _, err := store.Append(Observation{Series: "s", Timestamp: 1, Data: []byte("v")}); err != nil {
		t.Fatal(err)
	}
	snapshot := store.Snapshot()
	done := make(chan error, 1)
	go func() { _, err := store.Compact(); done <- err }()
	select {
	case err := <-done:
		t.Fatalf("compaction finished before snapshot release: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	if got := snapshot.Range("s", 0, 10); len(got) != 1 {
		t.Fatalf("pinned snapshot lost data: %v", got)
	}
	snapshot.Close()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestInterruptedCompactionRecoversAndCleansUp(t *testing.T) {
	dir := t.TempDir()
	opts := &Options{DisableSync: true}
	store, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(Observation{Series: "s", Timestamp: 7, Data: []byte("v")}); err != nil {
		t.Fatal(err)
	}
	store.compactHook = func(string) {}
	_, err = store.Compact()
	if !errors.Is(err, errHookExit) {
		t.Fatalf("interrupted compaction error = %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := reopened.Snapshot()
	got := snapshot.Range("s", 0, 100)
	if len(got) != 1 || string(got[0].Data) != "v" {
		t.Fatalf("recovered interrupted compaction view = %v", got)
	}
	snapshot.Close()
	if _, err := reopened.Compact(); err != nil {
		t.Fatal(err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	segments := 0
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) == ".seg" {
			segments++
		}
	}
	if segments != 1 {
		t.Fatalf("segments after interrupted cleanup = %d", segments)
	}
}
