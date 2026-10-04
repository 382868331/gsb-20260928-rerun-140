package timeseries

import (
	"encoding/binary"
	"testing"
)

func averageBytes(points []Observation) []byte {
	var total int64
	for _, point := range points {
		total += int64(point.Data[0])
	}
	if len(points) > 0 {
		total /= int64(len(points))
	}
	return bucketValue(total)
}

func TestCrossResolutionViewSegmentsSparseAndZeroBuckets(t *testing.T) {
	store := openTestStore(t)
	appendAt(t, store, 0, 3)
	appendAt(t, store, 2, 5)
	appendAt(t, store, 10, 20)
	appendAt(t, store, 12, 40)
	if err := store.RegisterAggregator("avg", 10, averageBytes); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Downsample("avg", "cpu", 0, 30); err != nil {
		t.Fatal(err)
	}

	view, err := store.CreateQueryView("mixed", "cpu", 0, 30, []ViewTier{
		{Start: 0, End: 10, Width: 1},
		{Start: 10, End: 20, Width: 10, Aggregator: "avg"},
		{Start: 20, End: 30, Width: 10, Aggregator: "avg"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()

	result, err := view.Read()
	if err != nil {
		t.Fatal(err)
	}
	if result.Version != store.Version() || len(result.Segments) != 3 {
		t.Fatalf("result = %+v", result)
	}
	raw := result.Segments[0]
	if raw.Aggregator != "" || raw.Start != 0 || raw.End != 10 || len(raw.Buckets) != 10 {
		t.Fatalf("raw segment = %+v", raw)
	}
	if raw.Buckets[0].Status != BucketObserved || raw.Buckets[0].SampleCount != 1 {
		t.Fatalf("observed bucket = %+v", raw.Buckets[0])
	}
	if raw.Buckets[1].Status != BucketMissingObservation || raw.Buckets[1].SampleCount != 0 {
		t.Fatalf("missing bucket = %+v", raw.Buckets[1])
	}
	fine := result.Segments[1]
	if fine.Buckets[0].Status != BucketObserved || fine.Buckets[0].SampleCount != 2 ||
		binary.LittleEndian.Uint64(fine.Buckets[0].Value) != 30 {
		t.Fatalf("average bucket = %+v", fine.Buckets[0])
	}
	empty := result.Segments[2]
	if empty.Coverage != CoverageComplete || empty.Buckets[0].Status != BucketExplicitZero || empty.Buckets[0].SampleCount != 0 {
		t.Fatalf("empty bucket = %+v", empty)
	}
}

func TestQueryViewShowsPartialCoverageForUnmaterializedAggregate(t *testing.T) {
	store := openTestStore(t)
	appendAt(t, store, 0, 1)
	if err := store.RegisterAggregator("avg", 10, averageBytes); err != nil {
		t.Fatal(err)
	}
	view, err := store.CreateQueryView("partial", "cpu", 0, 10, []ViewTier{{Start: 0, End: 10, Width: 10, Aggregator: "avg"}})
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	result, err := view.Read()
	if err != nil {
		t.Fatal(err)
	}
	if result.Segments[0].Coverage != CoveragePartial || result.Segments[0].Buckets[0].Status != BucketUnmaterialized {
		t.Fatalf("unmaterialized segment = %+v", result.Segments[0])
	}
}

func TestSavedQueryViewStableAcrossCorrectionsRestartAndDiff(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, &Options{DisableSync: true})
	if err != nil {
		t.Fatal(err)
	}
	registerTestAverage(t, store)
	appendAt(t, store, 0, 10)
	appendAt(t, store, 10, 10)
	if _, err := store.Downsample("avg", "cpu", 0, 20); err != nil {
		t.Fatal(err)
	}
	old, err := store.CreateQueryView("old", "cpu", 0, 20, []ViewTier{{Start: 0, End: 10, Width: 1}, {Start: 10, End: 20, Width: 10, Aggregator: "avg"}})
	if err != nil {
		t.Fatal(err)
	}
	old.Close()

	if _, err := store.Revise(Observation{Series: "cpu", Timestamp: 0, Data: []byte{40}}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(Observation{Series: "cpu", Timestamp: 15, Data: []byte{30}}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Downsample("avg", "cpu", 0, 20); err != nil {
		t.Fatal(err)
	}
	newView, err := store.CreateQueryView("new", "cpu", 0, 20, []ViewTier{{Start: 0, End: 10, Width: 1}, {Start: 10, End: 20, Width: 10, Aggregator: "avg"}})
	if err != nil {
		t.Fatal(err)
	}
	newView.Close()

	changes, err := store.DiffQueryViews("old", "new")
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 2 || changes[0] != (ChangeRange{Start: 0, End: 1}) || changes[1] != (ChangeRange{Start: 10, End: 20}) {
		t.Fatalf("changes = %+v", changes)
	}

	store = reopenStore(t, store)
	opened, err := store.OpenQueryView("old")
	if err != nil {
		t.Fatal(err)
	}
	result, err := opened.Read()
	if err != nil {
		t.Fatal(err)
	}
	if result.Version != 4 || result.Segments[0].Buckets[0].Value[0] != 10 ||
		result.Segments[1].Buckets[0].SampleCount != 1 || result.Segments[1].Buckets[0].Value[0] != 10 {
		t.Fatalf("old view changed after restart: %+v", result)
	}
	opened.Close()
}

func TestCancelQueryViewReleasesCompactionHistory(t *testing.T) {
	store := openTestStore(t)
	registerTestAverage(t, store)
	appendAt(t, store, 0, 1)
	if _, err := store.Downsample("avg", "cpu", 0, 10); err != nil {
		t.Fatal(err)
	}
	view, err := store.CreateQueryView("old", "cpu", 0, 10, []ViewTier{{Start: 0, End: 10, Width: 10, Aggregator: "avg"}})
	if err != nil {
		t.Fatal(err)
	}
	view.Close()
	if _, err := store.Revise(Observation{Series: "cpu", Timestamp: 0, Data: []byte{9}}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Downsample("avg", "cpu", 0, 10); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Compact(); err != nil {
		t.Fatal(err)
	}
	opened, err := store.OpenQueryView("old")
	if err != nil {
		t.Fatal(err)
	}
	if result, err := opened.Read(); err != nil || result.Segments[0].Buckets[0].Value[0] != 1 {
		t.Fatalf("retained old view = %+v, %v", result, err)
	}
	opened.Close()

	if err := store.CancelQueryView("old"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Compact(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.OpenQueryView("old"); err != ErrQueryViewNotFound {
		t.Fatalf("canceled view error = %v", err)
	}
}

func registerTestAverage(t *testing.T, store *Store) {
	t.Helper()
	if err := store.RegisterAggregator("avg", 10, averageBytes); err != nil {
		t.Fatal(err)
	}
}
