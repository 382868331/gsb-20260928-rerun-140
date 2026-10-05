package timeseries

import (
	"errors"
	"testing"
)

func setupBookmarkViews(t *testing.T, store *Store) {
	t.Helper()
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
		{Start: 10, End: 30, Width: 10, Aggregator: "avg"},
	})
	if err != nil {
		t.Fatal(err)
	}
	view.Close()
}

func TestBookmarkRestoresWindowsAfterReopen(t *testing.T) {
	dir := t.TempDir()
	opts := &Options{DisableSync: true}
	store, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	setupBookmarkViews(t, store)
	bookmark, err := store.CreateBookmark("watch", []BookmarkWindow{
		{View: "mixed", Start: 0, End: 5, Width: 1},
		{View: "mixed", Start: 10, End: 30, Width: 10, Aggregator: "avg"},
	})
	if err != nil {
		t.Fatal(err)
	}
	version := bookmark.Windows()[0].Version
	bookmark.Close()
	if _, err := store.Revise(Observation{Series: "cpu", Timestamp: 2, Data: []byte{9}}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Compact(); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	restored, err := reopened.OpenBookmark("watch")
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	windows := restored.Windows()
	if len(windows) != 2 {
		t.Fatalf("windows = %+v", windows)
	}
	if windows[0].Start != 0 || windows[0].End != 5 || windows[0].Width != 1 || windows[0].Aggregator != "" {
		t.Fatalf("raw window = %+v", windows[0])
	}
	if windows[1].Width != 10 || windows[1].Aggregator != "avg" || windows[1].Version != version {
		t.Fatalf("aggregate window = %+v", windows[1])
	}
	result, err := restored.ReadWindow(0)
	if err != nil {
		t.Fatal(err)
	}
	if result.Version != version || len(result.Segments) != 1 {
		t.Fatalf("result = %+v", result)
	}
	buckets := result.Segments[0].Buckets
	if len(buckets) != 5 || buckets[2].Status != BucketObserved || buckets[2].Value[0] != 5 {
		t.Fatalf("pinned buckets = %+v", buckets)
	}
	aggregate, err := restored.ReadWindow(1)
	if err != nil {
		t.Fatal(err)
	}
	if aggregate.Segments[0].Buckets[0].SampleCount != 2 {
		t.Fatalf("aggregate buckets = %+v", aggregate.Segments[0].Buckets)
	}
}

func TestDiffBookmarksPrioritizesChangedWindows(t *testing.T) {
	store := openTestStore(t)
	setupBookmarkViews(t, store)
	windows := []BookmarkWindow{
		{View: "mixed", Start: 0, End: 5, Width: 1},
		{View: "mixed", Start: 5, End: 10, Width: 1},
		{View: "mixed", Start: 10, End: 30, Width: 10, Aggregator: "avg"},
	}
	before, err := store.CreateBookmark("before", windows)
	if err != nil {
		t.Fatal(err)
	}
	before.Close()
	if _, err := store.Revise(Observation{Series: "cpu", Timestamp: 12, Data: []byte{60}}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Downsample("avg", "cpu", 0, 30); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateQueryView("mixed-v2", "cpu", 0, 30, []ViewTier{
		{Start: 0, End: 10, Width: 1},
		{Start: 10, End: 30, Width: 10, Aggregator: "avg"},
	}); err != nil {
		t.Fatal(err)
	}
	after, err := store.CreateBookmark("after", []BookmarkWindow{
		{View: "mixed-v2", Start: 0, End: 5, Width: 1},
		{View: "mixed-v2", Start: 5, End: 10, Width: 1},
		{View: "mixed-v2", Start: 10, End: 30, Width: 10, Aggregator: "avg"},
	})
	if err != nil {
		t.Fatal(err)
	}
	after.Close()

	diff, err := store.DiffBookmarks("before", "after")
	if err != nil {
		t.Fatal(err)
	}
	if len(diff.Windows) != 3 {
		t.Fatalf("diff = %+v", diff)
	}
	first := diff.Windows[0]
	if !first.Changed || first.Index != 2 {
		t.Fatalf("changed window not first: %+v", diff.Windows)
	}
	if len(first.Changes) != 1 || first.Changes[0].Start != 10 || first.Changes[0].End != 20 {
		t.Fatalf("changes = %+v", first.Changes)
	}
	for _, other := range diff.Windows[1:] {
		if other.Changed {
			t.Fatalf("unchanged windows must come last: %+v", diff.Windows)
		}
	}
}

func TestCancelBookmarkReleasesOnlyItsOwnReferences(t *testing.T) {
	store := openTestStore(t)
	setupBookmarkViews(t, store)
	bookmark, err := store.CreateBookmark("temp", []BookmarkWindow{{View: "mixed", Start: 0, End: 5, Width: 1}})
	if err != nil {
		t.Fatal(err)
	}
	bookmark.Close()
	if err := store.CancelBookmark("temp"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.OpenBookmark("temp"); !errors.Is(err, ErrBookmarkNotFound) {
		t.Fatalf("open canceled bookmark = %v", err)
	}
	if _, err := store.Revise(Observation{Series: "cpu", Timestamp: 2, Data: []byte{9}}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Compact(); err != nil {
		t.Fatal(err)
	}
	view, err := store.OpenQueryView("mixed")
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	result, err := view.ReadWindow(0, 5)
	if err != nil {
		t.Fatal(err)
	}
	if result.Segments[0].Buckets[2].Value[0] != 5 {
		t.Fatalf("view lost its pinned version: %+v", result.Segments[0].Buckets)
	}
	if err := store.CancelBookmark("temp"); !errors.Is(err, ErrBookmarkNotFound) {
		t.Fatalf("double cancel = %v", err)
	}
}

func TestBookmarkValidationAndMismatch(t *testing.T) {
	store := openTestStore(t)
	setupBookmarkViews(t, store)
	if _, err := store.CreateBookmark("dup", []BookmarkWindow{{View: "mixed", Start: 0, End: 5, Width: 1}}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateBookmark("dup", []BookmarkWindow{{View: "mixed", Start: 0, End: 5, Width: 1}}); !errors.Is(err, ErrBookmarkExists) {
		t.Fatalf("duplicate = %v", err)
	}
	if _, err := store.CreateBookmark("bad-view", []BookmarkWindow{{View: "missing", Start: 0, End: 5, Width: 1}}); !errors.Is(err, ErrQueryViewNotFound) {
		t.Fatalf("missing view = %v", err)
	}
	if _, err := store.CreateBookmark("bad-tier", []BookmarkWindow{{View: "mixed", Start: 0, End: 20, Width: 1}}); !errors.Is(err, ErrInvalidBookmark) {
		t.Fatalf("cross-tier window = %v", err)
	}
	if _, err := store.CreateBookmark("bad-align", []BookmarkWindow{{View: "mixed", Start: 11, End: 30, Width: 10, Aggregator: "avg"}}); !errors.Is(err, ErrInvalidBookmark) {
		t.Fatalf("unaligned window = %v", err)
	}
	if _, err := store.CreateBookmark("second", []BookmarkWindow{{View: "mixed", Start: 0, End: 8, Width: 1}}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DiffBookmarks("dup", "second"); !errors.Is(err, ErrBookmarkMismatch) {
		t.Fatalf("mismatched diff = %v", err)
	}
}
