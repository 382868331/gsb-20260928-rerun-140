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

func TestQueryViewPagesCollapseGapsAndPreserveEntryMetadata(t *testing.T) {
	store := openTestStore(t)
	registerTestAverage(t, store)
	appendAt(t, store, 0, 3)
	appendAt(t, store, 12, 40)
	if _, err := store.Downsample("avg", "cpu", 0, 30); err != nil {
		t.Fatal(err)
	}
	view, err := store.CreateQueryView("paged", "cpu", 0, 30, []ViewTier{
		{Start: 0, End: 10, Width: 1},
		{Start: 10, End: 20, Width: 10, Aggregator: "avg"},
		{Start: 20, End: 30, Width: 10, Aggregator: "avg"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()

	first, err := view.ReadPage(0, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Entries) != 2 || !first.HasMore || first.NextStart != 10 {
		t.Fatalf("first page = %+v", first)
	}
	if first.Entries[0].Status != BucketObserved || first.Entries[0].SampleCount != 1 || first.Entries[0].SourceVersion != 1 {
		t.Fatalf("observed entry = %+v", first.Entries[0])
	}
	if gap := first.Entries[1]; !gap.Collapsed || gap.Start != 1 || gap.End != 10 || gap.BucketCount != 9 || gap.Status != BucketMissingObservation {
		t.Fatalf("collapsed gap = %+v", gap)
	}

	second, err := view.ReadPage(first.NextStart, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Entries) != 2 || second.HasMore || second.NextStart != 30 {
		t.Fatalf("second page = %+v", second)
	}
	if observed := second.Entries[0]; observed.Status != BucketObserved || observed.SampleCount != 1 || observed.SourceVersion != 2 || observed.Width != 10 || observed.Aggregator != "avg" {
		t.Fatalf("aggregate entry = %+v", observed)
	}
	if zero := second.Entries[1]; zero.Status != BucketExplicitZero || zero.Collapsed || zero.SampleCount != 0 || zero.SourceVersion != 0 {
		t.Fatalf("zero aggregate entry = %+v", zero)
	}
}

func TestQueryViewPagesAndWindowsRemainFixedAfterCorrections(t *testing.T) {
	store := openTestStore(t)
	registerTestAverage(t, store)
	appendAt(t, store, 0, 10)
	appendAt(t, store, 15, 20)
	if _, err := store.Downsample("avg", "cpu", 0, 30); err != nil {
		t.Fatal(err)
	}
	old, err := store.CreateQueryView("old-paged", "cpu", 0, 30, []ViewTier{
		{Start: 0, End: 10, Width: 1},
		{Start: 10, End: 20, Width: 10, Aggregator: "avg"},
		{Start: 20, End: 30, Width: 10, Aggregator: "avg"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()

	if _, err := store.Revise(Observation{Series: "cpu", Timestamp: 0, Data: []byte{30}}); err != nil {
		t.Fatal(err)
	}
	appendAt(t, store, 25, 40)
	if _, err := store.Downsample("avg", "cpu", 0, 30); err != nil {
		t.Fatal(err)
	}
	newView, err := store.CreateQueryView("new-paged", "cpu", 0, 30, []ViewTier{
		{Start: 0, End: 10, Width: 1},
		{Start: 10, End: 20, Width: 10, Aggregator: "avg"},
		{Start: 20, End: 30, Width: 10, Aggregator: "avg"},
	})
	if err != nil {
		t.Fatal(err)
	}
	newView.Close()

	changes, err := store.DiffQueryViews("old-paged", "new-paged")
	if err != nil {
		t.Fatal(err)
	}
	wantChanges := []ChangeRange{{Start: 0, End: 1}, {Start: 20, End: 30}}
	if len(changes) != len(wantChanges) {
		t.Fatalf("changes = %+v, want %+v", changes, wantChanges)
	}
	for i := range wantChanges {
		if changes[i] != wantChanges[i] {
			t.Fatalf("changes = %+v, want %+v", changes, wantChanges)
		}
	}

	page, err := old.ReadPage(0, 2)
	if err != nil {
		t.Fatal(err)
	}
	if page.Version != old.Version() || page.Entries[0].Value[0] != 10 {
		t.Fatalf("old first page changed: %+v", page)
	}
	rest, err := old.ReadPage(page.NextStart, 10)
	if err != nil {
		t.Fatal(err)
	}
	if rest.Entries[1].Status != BucketExplicitZero || rest.HasMore {
		t.Fatalf("old later page changed: %+v", rest)
	}

	changedWindow, err := old.ReadWindow(changes[1].Start, changes[1].End)
	if err != nil {
		t.Fatal(err)
	}
	if changedWindow.Version != old.Version() || len(changedWindow.Segments) != 1 || changedWindow.Segments[0].Buckets[0].Status != BucketExplicitZero {
		t.Fatalf("changed window = %+v", changedWindow)
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
