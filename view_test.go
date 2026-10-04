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

func TestReadRangeCollapsesMissingRunsAndExpands(t *testing.T) {
	store := openTestStore(t)
	appendAt(t, store, 3, 7)
	appendAt(t, store, 4000, 9)
	view, err := store.CreateQueryView("sparse", "cpu", 0, 5000, []ViewTier{{Start: 0, End: 5000, Width: 1}})
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()

	collapsed, err := view.ReadRange(0, 5000, true)
	if err != nil {
		t.Fatal(err)
	}
	entries := collapsed.Segments[0].Entries
	if len(entries) != 5 {
		t.Fatalf("collapsed entries = %+v", entries)
	}
	if entries[0].Status != BucketMissingObservation || entries[0].Start != 0 || entries[0].End != 3 || entries[0].Count != 3 {
		t.Fatalf("leading gap = %+v", entries[0])
	}
	if entries[1].Status != BucketObserved || entries[1].Count != 1 || entries[1].Value[0] != 7 {
		t.Fatalf("observed = %+v", entries[1])
	}
	if entries[2].Status != BucketMissingObservation || entries[2].Start != 4 || entries[2].End != 4000 || entries[2].Count != 3996 {
		t.Fatalf("middle gap = %+v", entries[2])
	}

	expanded, err := view.ReadRange(2, 6, false)
	if err != nil {
		t.Fatal(err)
	}
	if expanded.Version != collapsed.Version || len(expanded.Segments[0].Entries) != 4 {
		t.Fatalf("expanded = %+v", expanded)
	}
	for _, entry := range expanded.Segments[0].Entries {
		if entry.Count != 1 || entry.Width != 1 {
			t.Fatalf("expanded entry = %+v", entry)
		}
	}
	if expanded.Segments[0].Entries[1].Status != BucketObserved || expanded.Segments[0].Entries[1].SampleCount != 1 {
		t.Fatalf("expanded observed = %+v", expanded.Segments[0].Entries[1])
	}

	if _, err := view.ReadRange(10, 10, true); err != ErrInvalidRange {
		t.Fatalf("empty window error = %v", err)
	}
	if _, err := view.ReadRange(4000, 6000, true); err != ErrInvalidRange {
		t.Fatalf("out of view error = %v", err)
	}
}

func TestReadRangePinnedVersionAcrossPagesAndCloseReleases(t *testing.T) {
	store := openTestStore(t)
	registerTestAverage(t, store)
	appendAt(t, store, 0, 1)
	appendAt(t, store, 10, 2)
	if _, err := store.Downsample("avg", "cpu", 0, 20); err != nil {
		t.Fatal(err)
	}
	view, err := store.CreateQueryView("paged", "cpu", 0, 20, []ViewTier{
		{Start: 0, End: 10, Width: 10, Aggregator: "avg"},
		{Start: 10, End: 20, Width: 10, Aggregator: "avg"},
	})
	if err != nil {
		t.Fatal(err)
	}

	first, err := view.ReadRange(0, 10, true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Revise(Observation{Series: "cpu", Timestamp: 10, Data: []byte{50}}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Downsample("avg", "cpu", 0, 20); err != nil {
		t.Fatal(err)
	}
	second, err := view.ReadRange(10, 20, true)
	if err != nil {
		t.Fatal(err)
	}
	if first.Version != second.Version || second.Version != view.Version() {
		t.Fatalf("page versions drifted: %d vs %d", first.Version, second.Version)
	}
	if got := second.Segments[0].Entries[0].Value[0]; got != 2 {
		t.Fatalf("late revise changed seen page: %d", got)
	}
	if got := second.Segments[0].Entries[0].SampleCount; got != 1 {
		t.Fatalf("sample count = %d", got)
	}
	view.Close()
	if _, err := view.ReadRange(0, 10, true); err != ErrQueryViewNotFound {
		t.Fatalf("closed view error = %v", err)
	}
}

func TestReadRangeAcrossTiersDiffWindowsAndReopen(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, &Options{DisableSync: true})
	if err != nil {
		t.Fatal(err)
	}
	registerTestAverage(t, store)
	appendAt(t, store, 5, 10)
	appendAt(t, store, 100, 20)
	appendAt(t, store, 205, 30)
	if _, err := store.Downsample("avg", "cpu", 200, 300); err != nil {
		t.Fatal(err)
	}
	tiers := []ViewTier{
		{Start: 0, End: 200, Width: 1},
		{Start: 200, End: 300, Width: 10, Aggregator: "avg"},
	}
	before, err := store.CreateQueryView("before", "cpu", 0, 300, tiers)
	if err != nil {
		t.Fatal(err)
	}
	before.Close()

	if _, err := store.Revise(Observation{Series: "cpu", Timestamp: 100, Data: []byte{60}}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(Observation{Series: "cpu", Timestamp: 215, Data: []byte{40}}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Downsample("avg", "cpu", 200, 300); err != nil {
		t.Fatal(err)
	}
	after, err := store.CreateQueryView("after", "cpu", 0, 300, tiers)
	if err != nil {
		t.Fatal(err)
	}
	after.Close()

	changes, err := store.DiffQueryViews("before", "after")
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 2 || changes[0] != (ChangeRange{Start: 100, End: 101}) || changes[1] != (ChangeRange{Start: 210, End: 220}) {
		t.Fatalf("changes = %+v", changes)
	}

	store = reopenStore(t, store)
	oldView, err := store.OpenQueryView("before")
	if err != nil {
		t.Fatal(err)
	}
	defer oldView.Close()
	newView, err := store.OpenQueryView("after")
	if err != nil {
		t.Fatal(err)
	}
	defer newView.Close()

	for _, window := range changes {
		oldPage, err := oldView.ReadRange(window.Start, window.End, false)
		if err != nil {
			t.Fatal(err)
		}
		newPage, err := newView.ReadRange(window.Start, window.End, false)
		if err != nil {
			t.Fatal(err)
		}
		if oldPage.Version == newPage.Version {
			t.Fatalf("views share version %d", oldPage.Version)
		}
	}
	oldGap, err := oldView.ReadRange(0, 300, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(oldGap.Segments) != 2 || oldGap.Segments[0].Width != 1 || oldGap.Segments[1].Width != 10 {
		t.Fatalf("segments = %+v", oldGap.Segments)
	}
	newGap, err := newView.ReadRange(0, 300, true)
	if err != nil {
		t.Fatal(err)
	}
	if oldGap.Segments[0].Entries[0].Count != newGap.Segments[0].Entries[0].Count {
		t.Fatalf("collapsed gap changed across reopen: %+v vs %+v", oldGap.Segments[0].Entries, newGap.Segments[0].Entries)
	}
	oldBucket := oldGap.Segments[1].Entries[1]
	newBucket := newGap.Segments[1].Entries[1]
	if oldBucket.Status != BucketExplicitZero || oldBucket.SampleCount != 0 ||
		newBucket.Status != BucketObserved || newBucket.SampleCount != 1 || newBucket.Value[0] != 40 {
		t.Fatalf("revised window old=%+v new=%+v", oldBucket, newBucket)
	}
}
