package timeseries

import (
	"errors"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func appendFloat(t *testing.T, store *Store, series string, timestamp int64, value float64) {
	t.Helper()
	data := []byte(strconv.FormatFloat(value, 'g', -1, 64))
	if _, err := store.Append(Observation{Series: series, Timestamp: timestamp, Data: data}); err != nil {
		t.Fatal(err)
	}
}

func assertFloat(t *testing.T, got, want float64) {
	t.Helper()
	if math.Abs(got-want) > 1e-9 {
		t.Fatalf("aggregate value = %v, want %v", got, want)
	}
}

func waitUntilBlocked(done <-chan error) error {
	select {
	case err := <-done:
		return err
	case <-time.After(100 * time.Millisecond):
		return nil
	}
}

func mustAggregates(t *testing.T, snapshot *Snapshot, series, aggregator string, width, start, end int64) []BucketAggregate {
	t.Helper()
	result, err := snapshot.Aggregates(series, aggregator, width, start, end)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func TestDownsampleCrossBucketsEmptyAndRevisions(t *testing.T) {
	store := openTestStore(t)
	appendFloat(t, store, "cpu", 0, 1)
	appendFloat(t, store, "cpu", 9, 3)   // bucket [0,10)
	appendFloat(t, store, "cpu", 10, 5)  // bucket [10,20)
	appendFloat(t, store, "cpu", 19, 7)  // bucket [10,20)
	appendFloat(t, store, "cpu", 25, 11) // bucket [20,30)

	if _, err := store.Downsample("cpu", "sum", 10); err != nil {
		t.Fatal(err)
	}
	old := store.Snapshot()
	got := mustAggregates(t, old, "cpu", "sum", 10, 0, 30)
	wantStarts := []int64{0, 10, 20}
	if len(got) != len(wantStarts) {
		t.Fatalf("buckets = %v", got)
	}
	for i, start := range wantStarts {
		if got[i].Bucket.Start != start || got[i].Bucket.Width != 10 {
			t.Fatalf("bucket %d = %+v", i, got[i].Bucket)
		}
	}
	assertFloat(t, got[0].Value, 4)
	assertFloat(t, got[1].Value, 12)
	assertFloat(t, got[2].Value, 11)
	if got[0].Count != 2 || got[1].Count != 2 || got[2].Count != 1 {
		t.Fatalf("counts = %d %d %d", got[0].Count, got[1].Count, got[2].Count)
	}

	// Empty buckets are included by a range that spans them.
	sparse := mustAggregates(t, old, "cpu", "sum", 10, 0, 60)
	if len(sparse) != 6 {
		t.Fatalf("sparse buckets = %v", sparse)
	}
	for index := 3; index < 6; index++ {
		if sparse[index].Value != 0 || sparse[index].Count != 0 {
			t.Fatalf("empty bucket %d = %+v", index, sparse[index])
		}
	}

	// A late revision touches only its own bucket; the pre-revision snapshot
	// keeps both raw values and aggregate values.
	if _, err := store.Revise(Observation{Series: "cpu", Timestamp: 9, Data: []byte("13")}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Downsample("cpu", "sum", 10); err != nil {
		t.Fatal(err)
	}
	newSnap := store.Snapshot()
	defer newSnap.Close()
	if values := old.Range("cpu", 0, 20); string(values[1].Data) != "3" {
		t.Fatalf("old snapshot raw value changed: %q", values[1].Data)
	}
	assertFloat(t, mustAggregates(t, old, "cpu", "sum", 10, 0, 10)[0].Value, 4)
	assertFloat(t, mustAggregates(t, newSnap, "cpu", "sum", 10, 0, 10)[0].Value, 14)
	assertFloat(t, mustAggregates(t, newSnap, "cpu", "sum", 10, 10, 20)[0].Value, 12)
	if values := newSnap.Range("cpu", 0, 20); string(values[1].Data) != "13" {
		t.Fatalf("new snapshot raw value = %q", values[1].Data)
	}
	old.Close()

	// Repeated downsample without data changes creates no new version.
	version := store.Version()
	if _, err := store.Downsample("cpu", "sum", 10); err != nil {
		t.Fatal(err)
	}
	if store.Version() != version {
		t.Fatalf("idempotent downsample advanced version %d -> %d", version, store.Version())
	}
}

func TestDeleteBeforeUpdatesAffectedAggregateBuckets(t *testing.T) {
	store := openTestStore(t)
	appendFloat(t, store, "s", 5, 2)
	appendFloat(t, store, "s", 15, 8)
	appendFloat(t, store, "s", 25, 3)
	if _, err := store.Downsample("s", "sum", 10); err != nil {
		t.Fatal(err)
	}
	old := store.Snapshot()
	if _, err := store.DeleteBefore(15); err != nil {
		t.Fatal(err)
	}
	newSnap := store.Snapshot()
	defer newSnap.Close()

	got := mustAggregates(t, newSnap, "s", "sum", 10, 0, 30)
	if len(got) != 3 {
		t.Fatalf("buckets = %v", got)
	}
	if got[0].Count != 0 || got[0].Value != 0 {
		t.Fatalf("fully deleted bucket = %+v", got[0])
	}
	if got[1].Count != 1 || got[1].Value != 8 {
		t.Fatalf("partially deleted bucket = %+v", got[1])
	}
	if got[2].Count != 1 || got[2].Value != 3 {
		t.Fatalf("unrelated bucket = %+v", got[2])
	}

	oldRows := old.Range("s", 0, 30)
	if len(oldRows) != 3 {
		t.Fatalf("old raw rows = %v", oldRows)
	}
	oldAggregates := mustAggregates(t, old, "s", "sum", 10, 0, 30)
	assertFloat(t, oldAggregates[0].Value, 2)
	assertFloat(t, oldAggregates[1].Value, 8)
	assertFloat(t, oldAggregates[2].Value, 3)
	old.Close()
}

func TestDownsampleCompactionWaitsAndRecovers(t *testing.T) {
	dir := t.TempDir()
	opts := &Options{DisableSync: true}
	store, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	appendFloat(t, store, "x", -3, 1)
	appendFloat(t, store, "x", 7, 2)
	appendFloat(t, store, "x", 21, 4)
	if _, err := store.Downsample("x", "sum", 10); err != nil {
		t.Fatal(err)
	}

	pinned := store.Snapshot()
	done := make(chan error, 1)
	go func() { _, err := store.Compact(); done <- err }()
	if err := waitUntilBlocked(done); err != nil {
		t.Fatalf("compaction did not wait for reader: %v", err)
	}
	before := mustAggregates(t, pinned, "x", "sum", 10, -10, 30)
	if len(before) != 4 || before[0].Value != 1 {
		t.Fatalf("pinned aggregates during cleanup = %v", before)
	}
	pinned.Close()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := reopened.Snapshot()
	got := mustAggregates(t, snapshot, "x", "sum", 10, -10, 30)
	if len(got) != 4 {
		t.Fatalf("recovered buckets = %v", got)
	}
	assertFloat(t, got[0].Value, 1)
	assertFloat(t, got[1].Value, 2)
	if got[2].Count != 0 {
		t.Fatalf("empty bucket restored with data: %+v", got[2])
	}
	assertFloat(t, got[3].Value, 4)
	snapshot.Close()

	version := reopened.Version()
	if _, err := reopened.Downsample("x", "sum", 10); err != nil {
		t.Fatal(err)
	}
	if reopened.Version() != version {
		t.Fatalf("aggregate versions grew after recovery: %d -> %d", version, reopened.Version())
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestInterruptedDownsampleCompactionRecoversHalfOpen(t *testing.T) {
	dir := t.TempDir()
	opts := &Options{DisableSync: true}
	store, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	appendFloat(t, store, "s", 0, 1)
	appendFloat(t, store, "s", 10, 2)
	appendFloat(t, store, "s", 19, 3)
	appendFloat(t, store, "s", 20, 5)
	if _, err := store.Downsample("s", "sum", 10); err != nil {
		t.Fatal(err)
	}
	store.compactHook = func(string) {}
	if _, err := store.Compact(); !errors.Is(err, errHookExit) {
		t.Fatalf("interrupt = %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := reopened.Snapshot()
	got := mustAggregates(t, snapshot, "s", "sum", 10, 10, 30)
	if len(got) != 2 {
		t.Fatalf("half-open buckets = %v", got)
	}
	assertFloat(t, got[0].Value, 5)
	assertFloat(t, got[1].Value, 5)
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
		t.Fatalf("segments after recovery cleanup = %d", segments)
	}
}

func TestUnknownAggregatorAndBadWidth(t *testing.T) {
	store := openTestStore(t)
	if _, err := store.Downsample("s", "missing", 10); !errors.Is(err, ErrUnknownAggregator) {
		t.Fatalf("unknown aggregator error = %v", err)
	}
	if _, err := store.Downsample("s", "sum", 0); !errors.Is(err, ErrInvalidBucketWidth) {
		t.Fatalf("bad width error = %v", err)
	}
	snapshot := store.Snapshot()
	defer snapshot.Close()
	if _, err := snapshot.Aggregates("s", "sum", -1, 0, 10); !errors.Is(err, ErrInvalidBucketWidth) {
		t.Fatalf("snapshot bad width error = %v", err)
	}
}
