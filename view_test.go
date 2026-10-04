package timeseries

import (
	"context"
	"errors"
	"sort"
	"testing"
)

func registerViewAggregators(t *testing.T, store *Store) {
	t.Helper()
	if err := store.RegisterAggregator("sum10", 10, sumBytes); err != nil {
		t.Fatal(err)
	}
	if err := store.RegisterAggregator("sum20", 20, sumBytes); err != nil {
		t.Fatal(err)
	}
}

func assertCoverage(t *testing.T, view QueryView) {
	t.Helper()
	cursor := view.Start
	for _, segment := range view.Segments {
		if segment.Start != cursor {
			t.Fatalf("segment %v does not continue at %d (gap/overlap)", segment, cursor)
		}
		type span struct{ start, end int64 }
		var spans []span
		for _, cell := range segment.Cells {
			if cell.Status == CellPresent {
				spans = append(spans, span{cell.Start, cell.End})
			}
		}
		for _, gap := range segment.Gaps {
			spans = append(spans, span{gap.Start, gap.End})
		}
		sort.Slice(spans, func(i, j int) bool { return spans[i].start < spans[j].start })
		covered := segment.Start
		for _, piece := range spans {
			if piece.start != covered {
				t.Fatalf("segment %v leaves %d..%d uncovered/overlapping", segment, covered, piece.start)
			}
			if piece.end <= piece.start || piece.end > segment.End {
				t.Fatalf("bad span %+v in segment %v", piece, segment)
			}
			covered = piece.end
		}
		if covered != segment.End {
			t.Fatalf("segment %v coverage ends at %d", segment, covered)
		}
		cursor = segment.End
	}
	if cursor != view.End {
		t.Fatalf("coverage ends at %d, want %d", cursor, view.End)
	}
}

func TestCrossResolutionViewPartitionsAndDistinguishesZeros(t *testing.T) {
	store := openTestStore(t)
	appendAt(t, store, 0, 1)
	appendAt(t, store, 12, 0) // real observed zero in raw tier, bucket [10,20)
	// fine bucket [20,30) has no observation at all
	appendAt(t, store, 32, 0) // fine bucket [30,40): one real zero sample
	appendAt(t, store, 41, 3) // bucket [40,50): two samples together with 45
	appendAt(t, store, 45, 4)
	appendAt(t, store, 62, 9)
	registerViewAggregators(t, store)
	if _, err := store.Downsample("sum10", "cpu", 0, 80); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Downsample("sum20", "cpu", 0, 80); err != nil {
		t.Fatal(err)
	}

	snapshot := store.Snapshot()
	defer snapshot.Close()
	tiers := []Tier{
		{From: 0, Width: 0},
		{From: 20, Width: 10, Name: "sum10"},
		{From: 55, Width: 20, Name: "sum20"},
	}
	view, err := snapshot.QueryView(context.Background(), "cpu", 0, 80, tiers)
	if err != nil {
		t.Fatal(err)
	}
	if view.Version != snapshot.Version() {
		t.Fatalf("view version %d", view.Version)
	}
	if len(view.Segments) != 3 {
		t.Fatalf("segments = %v", view.Segments)
	}
	// The switch at 55 lands inside coarse bucket [40,60): that whole coarse
	// bucket must stay out of the fine segment, so the boundary snaps up to 60
	// and the coarse bucket never overlaps a fine one.
	if seg := view.Segments[1]; seg.Start != 20 || seg.End != 60 || seg.Width != 10 {
		t.Fatalf("fine segment = %+v", seg)
	}
	if seg := view.Segments[2]; seg.Start != 60 || seg.End != 80 || seg.Width != 20 {
		t.Fatalf("coarse segment = %+v", seg)
	}
	assertCoverage(t, view)
	// Every emitted time belongs to exactly one cell across all segments.
	seen := make(map[int64]bool)
	for _, segment := range view.Segments {
		for _, cell := range segment.Cells {
			for tick := cell.Start; tick < cell.End; tick++ {
				if seen[tick] {
					t.Fatalf("tick %d counted in two segments", tick)
				}
				seen[tick] = true
			}
		}
	}

	fine := view.Segments[1]
	byStart := map[int64]ViewCell{}
	for _, cell := range fine.Cells {
		byStart[cell.Start] = cell
	}
	empty := byStart[20]
	if empty.Status != CellEmpty || empty.Count != 0 {
		t.Fatalf("missing bucket reported as %+v, want explicit empty", empty)
	}
	realZero := byStart[30]
	if realZero.Status != CellPresent || realZero.Count != 1 || !bytesEqual(realZero.Value, bucketValue(0)) {
		t.Fatalf("observed zero reported as %+v", realZero)
	}
	single := byStart[30]
	double := byStart[40]
	if single.Count != 1 || double.Count != 2 {
		t.Fatalf("sample counts single=%d double=%d", single.Count, double.Count)
	}
	if single.Count == double.Count {
		t.Fatal("means over different sample sizes must not be treated as equal")
	}

	// A bucket never downsampled is uncovered rather than empty.
	uncov, err := snapshot.QueryView(context.Background(), "cpu", 80, 100,
		[]Tier{{From: 0, Width: 10, Name: "sum10"}})
	if err != nil {
		t.Fatal(err)
	}
	if cell := uncov.Segments[0].Cells[0]; cell.Status != CellUncovered || cell.Count != 0 {
		t.Fatalf("uncovered bucket = %+v", cell)
	}

	if _, err := snapshot.QueryView(context.Background(), "cpu", 0, 80, []Tier{
		{From: 0, Width: 20, Name: "sum20"},
		{From: 20, Width: 10, Name: "sum10"},
	}); !errors.Is(err, ErrInvalidTier) {
		t.Fatalf("coarse-to-fine tier error = %v", err)
	}
}

func TestPinnedViewSurvivesBackfillCorrectionsReopenAndCancel(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, &Options{DisableSync: true})
	if err != nil {
		t.Fatal(err)
	}
	appendAt(t, store, 5, 1)
	registerViewAggregators(t, store)
	if _, err := store.Downsample("sum10", "cpu", 0, 20); err != nil {
		t.Fatal(err)
	}
	pinnedVersion := store.Version()
	pinned, err := store.SnapshotAt(pinnedVersion)
	if err != nil {
		t.Fatal(err)
	}
	defer pinned.Close()
	firstView, err := pinned.QueryView(context.Background(), "cpu", 0, 20,
		[]Tier{{From: 0, Width: 10, Name: "sum10"}})
	if err != nil {
		t.Fatal(err)
	}

	// Late backfill and a correction spanning several buckets only affect new
	// views; the delivered pinned view stays explainable.
	appendAt(t, store, 15, 6)
	if _, err := store.Revise(Observation{Series: "cpu", Timestamp: 5, Data: []byte{42}}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Downsample("sum10", "cpu", 0, 20); err != nil {
		t.Fatal(err)
	}
	current := store.Snapshot()
	changes, err := pinned.ChangedRanges(current, "sum10", "cpu", 0, 20)
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 1 || changes[0].Start != 0 || changes[0].End != 20 {
		t.Fatalf("changed ranges = %v", changes)
	}
	againView, err := pinned.QueryView(context.Background(), "cpu", 0, 20,
		[]Tier{{From: 0, Width: 10, Name: "sum10"}})
	if err != nil {
		t.Fatal(err)
	}
	if !viewsEqual(firstView, againView) {
		t.Fatalf("pinned view changed: %+v vs %+v", firstView, againView)
	}
	current.Close()

	// A canceled read returns promptly; closing the released snapshot lets
	// background cleanup proceed instead of waiting on an old reader.
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := pinned.QueryView(canceled, "cpu", 0, 20,
		[]Tier{{From: 0, Width: 10, Name: "sum10"}}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel error = %v", err)
	}

	done := make(chan error, 1)
	go func() { _, err := store.Compact(); done <- err }()
	select {
	case <-done:
		t.Fatal("compaction must wait while the pinned old view is open")
	default:
	}
	pinned.Close()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if _, err := store.SnapshotAt(pinnedVersion + 1000); !errors.Is(err, ErrInvalidVersion) {
		t.Fatalf("future version error = %v", err)
	}

	// Reopen and rebuild the same pinned query: identical meaning.
	reopened := reopenStore(t, store)
	registerViewAggregators(t, reopened)
	repinned, err := reopened.SnapshotAt(pinnedVersion)
	if err != nil {
		t.Fatal(err)
	}
	defer repinned.Close()
	reopenedView, err := repinned.QueryView(context.Background(), "cpu", 0, 20,
		[]Tier{{From: 0, Width: 10, Name: "sum10"}})
	if err != nil {
		t.Fatal(err)
	}
	if !viewsEqual(firstView, reopenedView) {
		t.Fatalf("view after reopen differs: %+v vs %+v", firstView, reopenedView)
	}
	newCurrent := reopened.Snapshot()
	defer newCurrent.Close()
	changes, err = repinned.ChangedRanges(newCurrent, "sum10", "cpu", 0, 20)
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 1 || changes[0].End != 20 {
		t.Fatalf("changed ranges after reopen = %v", changes)
	}
}

func viewsEqual(a, b QueryView) bool {
	if a.Start != b.Start || a.End != b.End || a.Version != b.Version || len(a.Segments) != len(b.Segments) {
		return false
	}
	for i := range a.Segments {
		sa, sb := a.Segments[i], b.Segments[i]
		if sa.Start != sb.Start || sa.End != sb.End || sa.Width != sb.Width || len(sa.Cells) != len(sb.Cells) {
			return false
		}
		for j := range sa.Cells {
			ca, cb := sa.Cells[j], sb.Cells[j]
			if ca.Start != cb.Start || ca.End != cb.End || ca.Status != cb.Status ||
				ca.Count != cb.Count || !bytesEqual(ca.Value, cb.Value) {
				return false
			}
		}
	}
	return true
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
