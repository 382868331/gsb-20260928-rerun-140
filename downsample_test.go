package timeseries

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
)

func sumBytes(points []Observation) []byte {
	var total int64
	for _, point := range points {
		total += int64(point.Data[0])
	}
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(total))
	return buf
}

func bucketValue(value int64) []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(value))
	return buf
}

func assertAggregate(t *testing.T, got Aggregate, start int64, value int64) {
	t.Helper()
	if got.BucketStart != start || !bytes.Equal(got.Value, bucketValue(value)) {
		t.Fatalf("aggregate at %d = %+v, want start %d value %d", start, got, start, value)
	}
}

func appendAt(t *testing.T, store *Store, timestamp int64, value byte) {
	t.Helper()
	if _, err := store.Append(Observation{Series: "cpu", Timestamp: timestamp, Data: []byte{value}}); err != nil {
		t.Fatal(err)
	}
}

func TestDownsampleBucketsBoundariesAndEmptyBuckets(t *testing.T) {
	store := openTestStore(t)
	// Width 10 buckets; [0,30) crosses boundaries and leaves bucket [20,30) empty.
	appendAt(t, store, 0, 1)
	appendAt(t, store, 9, 2)
	appendAt(t, store, 10, 4)
	appendAt(t, store, 19, 8)
	if err := store.RegisterAggregator("sum", 10, sumBytes); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Downsample("sum", "cpu", 0, 30); err != nil {
		t.Fatal(err)
	}
	got := store.Snapshot().Aggregates("sum", "cpu", 0, 30)
	if len(got) != 3 {
		t.Fatalf("bucket count = %d, want 3: %v", len(got), got)
	}
	assertAggregate(t, got[0], 0, 3)
	assertAggregate(t, got[1], 10, 12)
	assertAggregate(t, got[2], 20, 0)

	// A query starting inside a bucket and ending at a boundary is half-open on
	// bucket starts and on point time.
	partial := store.Snapshot().Aggregates("sum", "cpu", 10, 20)
	if len(partial) != 1 || partial[0].BucketStart != 10 {
		t.Fatalf("half-open bucket query = %v", partial)
	}
}

func TestLateRevisionUpdatesAggregateButOldSnapshotsStable(t *testing.T) {
	store := openTestStore(t)
	appendAt(t, store, 5, 10)
	appendAt(t, store, 15, 1)
	calls := 0
	counting := func(points []Observation) []byte {
		calls++
		return sumBytes(points)
	}
	if err := store.RegisterAggregator("sum", 10, counting); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Downsample("sum", "cpu", 0, 20); err != nil {
		t.Fatal(err)
	}
	firstCalls := calls
	old := store.Snapshot()

	// A late revision lands in the historical bucket [0,10); timestamp and
	// bucket membership never move.
	if _, err := store.Revise(Observation{Series: "cpu", Timestamp: 5, Data: []byte{40}}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Downsample("sum", "cpu", 0, 20); err != nil {
		t.Fatal(err)
	}
	if calls != firstCalls+1 {
		t.Fatalf("aggregator calls = %d, want exactly one recompute (%d+1)", calls, firstCalls)
	}
	// Unchanged bucket [10,20) is not recomputed; running again changes nothing.
	if _, err := store.Downsample("sum", "cpu", 0, 20); err != nil {
		t.Fatal(err)
	}
	if calls != firstCalls+1 {
		t.Fatalf("repeated downsample invoked aggregator: %d", calls)
	}

	oldAggs := old.Aggregates("sum", "cpu", 0, 20)
	if len(oldAggs) != 2 || !bytes.Equal(oldAggs[0].Value, bucketValue(10)) {
		t.Fatalf("old snapshot aggregates changed: %v", oldAggs)
	}
	oldRows := old.Range("cpu", 0, 20)
	if oldRows[0].Data[0] != 10 {
		t.Fatalf("old snapshot raw value changed: %v", oldRows[0])
	}
	current := store.Snapshot()
	newAggs := current.Aggregates("sum", "cpu", 0, 20)
	if !bytes.Equal(newAggs[0].Value, bucketValue(40)) {
		t.Fatalf("revised aggregate = %v", newAggs[0].Value)
	}
	if newRows := current.Range("cpu", 0, 20); newRows[0].Data[0] != 40 {
		t.Fatalf("revised raw value = %v", newRows[0])
	}
	old.Close()
	current.Close()
}

func TestDeleteCascadesToAggregatesAcrossBoundary(t *testing.T) {
	store := openTestStore(t)
	appendAt(t, store, 5, 3)    // bucket [0,10): fully before cutoff 15
	appendAt(t, store, 12, 100) // bucket [10,20): expires (straddles cutoff)
	appendAt(t, store, 15, 5)   // bucket [10,20): survives
	appendAt(t, store, 25, 7)   // bucket [20,30): untouched
	if err := store.RegisterAggregator("sum", 10, sumBytes); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Downsample("sum", "cpu", 0, 30); err != nil {
		t.Fatal(err)
	}
	before := store.Snapshot()
	if _, err := store.DeleteBefore(15); err != nil {
		t.Fatal(err)
	}
	after := store.Snapshot()
	aggs := after.Aggregates("sum", "cpu", 0, 50)
	if len(aggs) != 2 {
		t.Fatalf("fully expired bucket should be tombstoned, got %v", aggs)
	}
	// Bucket [0,10) is fully before the cutoff and is tombstoned. Bucket
	// [10,20) straddles it: point 12 disappears, point 15 survives.
	assertAggregate(t, aggs[0], 10, 5)
	assertAggregate(t, aggs[1], 20, 7)

	// Old snapshot still explains its aggregates from retained evidence.
	oldAggs := before.Aggregates("sum", "cpu", 0, 50)
	if len(oldAggs) != 3 || !bytes.Equal(oldAggs[0].Value, bucketValue(3)) ||
		!bytes.Equal(oldAggs[1].Value, bucketValue(105)) {
		t.Fatalf("pre-expiry aggregates lost: %v", oldAggs)
	}
	if rows := before.Range("cpu", 0, 50); len(rows) != 4 {
		t.Fatalf("pre-expiry raw evidence lost: %v", rows)
	}
	before.Close()
	after.Close()

	if _, err := store.Compact(); err != nil {
		t.Fatal(err)
	}
	reopened := reopenStore(t, store)
	final := reopened.Snapshot()
	if rows := final.Range("cpu", 0, 50); len(rows) != 2 || rows[0].Timestamp != 15 {
		t.Fatalf("surviving evidence after compaction = %v", rows)
	}
	aggs = final.Aggregates("sum", "cpu", 0, 50)
	if len(aggs) != 2 {
		t.Fatalf("aggregates after restart = %v", aggs)
	}
	assertAggregate(t, aggs[0], 10, 5)
	assertAggregate(t, aggs[1], 20, 7)
	final.Close()
}

func reopenStore(t *testing.T, store *Store) *Store {
	t.Helper()
	dir := store.path
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(dir, &Options{DisableSync: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	return reopened
}

func TestRecoveryDoesNotReaggregateOrDuplicate(t *testing.T) {
	dir := t.TempDir()
	opts := &Options{DisableSync: true}
	store, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	appendAt(t, store, 0, 2)
	appendAt(t, store, 11, 4)
	var calls int
	if err := store.RegisterAggregator("sum", 10, func(points []Observation) []byte {
		calls++
		return sumBytes(points)
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Downsample("sum", "cpu", 0, 20); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("initial calls = %d", calls)
	}
	if _, err := store.Revise(Observation{Series: "cpu", Timestamp: 0, Data: []byte{20}}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Downsample("sum", "cpu", 0, 20); err != nil {
		t.Fatal(err)
	}
	if calls != 3 {
		t.Fatalf("calls after revision = %d", calls)
	}

	// Interrupt a compaction after the merged segment is committed, then reopen.
	store.compactHook = func(string) {}
	if _, err := store.Compact(); !errors.Is(err, errHookExit) {
		t.Fatalf("interrupt = %v", err)
	}
	reopened := reopenStore(t, store)
	if err := reopened.RegisterAggregator("sum", 10, func(points []Observation) []byte {
		calls++
		return sumBytes(points)
	}); err != nil {
		t.Fatal(err)
	}
	snapshot := reopened.Snapshot()
	aggs := snapshot.Aggregates("sum", "cpu", 0, 20)
	if len(aggs) != 2 {
		t.Fatalf("recovered aggregates = %v", aggs)
	}
	assertAggregate(t, aggs[0], 0, 20)
	assertAggregate(t, aggs[1], 10, 4)
	if rows := snapshot.Range("cpu", 0, 20); len(rows) != 2 {
		t.Fatalf("recovered raw points = %v", rows)
	}
	snapshot.Close()

	// Completing the interrupted compaction must not invoke the aggregator.
	if _, err := reopened.Downsample("sum", "cpu", 0, 20); err != nil {
		t.Fatal(err)
	}
	if calls != 3 {
		t.Fatalf("recovery/re-downsample re-ran aggregation, calls = %d", calls)
	}
	if _, err := reopened.Compact(); err != nil {
		t.Fatalf("resume compaction: %v", err)
	}
	if _, err := reopened.Downsample("sum", "cpu", 0, 20); err != nil {
		t.Fatal(err)
	}
	if calls != 3 {
		t.Fatalf("compaction re-ran aggregation, calls = %d", calls)
	}

	reopened = reopenStore(t, reopened)
	if err := reopened.RegisterAggregator("sum", 10, func(points []Observation) []byte {
		calls++
		return sumBytes(points)
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.Downsample("sum", "cpu", 0, 20); err != nil {
		t.Fatal(err)
	}
	if calls != 3 {
		t.Fatalf("reopen re-ran aggregation, calls = %d", calls)
	}
	snapshot = reopened.Snapshot()
	aggs = snapshot.Aggregates("sum", "cpu", 0, 20)
	if len(aggs) != 2 {
		t.Fatalf("post-restart duplicates = %v", aggs)
	}
	snapshot.Close()
}
