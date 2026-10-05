package timeseries

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
)

// BookmarkWindow selects one comparable window inside a saved query view. The
// window must lie within a single tier of the view and use that tier's
// granularity.
type BookmarkWindow struct {
	View       string `json:"view"`
	Start      int64  `json:"start"`
	End        int64  `json:"end"`
	Width      int64  `json:"width"`
	Aggregator string `json:"aggregator,omitempty"`
}

// BookmarkWindowInfo describes a saved window with the series and the data
// version pinned when the bookmark was created.
type BookmarkWindowInfo struct {
	BookmarkWindow
	Series  string `json:"series"`
	Version uint64 `json:"version"`
}

type bookmarkData struct {
	Name    string               `json:"name"`
	Windows []BookmarkWindowInfo `json:"windows"`
}

type bookmarksFile struct {
	Bookmarks []bookmarkData `json:"bookmarks"`
}

// Bookmark is a saved, named set of windows. Each window keeps its position,
// granularity, and pinned version across store reopens.
type Bookmark struct {
	store *Store
	snaps map[uint64]*Snapshot
	data  bookmarkData
}

// BookmarkWindowDiff reports how one window differs between two bookmarks.
type BookmarkWindowDiff struct {
	Index   int
	Window  BookmarkWindowInfo
	Changed bool
	Changes []ChangeRange
}

// BookmarkDiff compares two bookmarks window by window. Windows whose bucket
// contents changed come first, in original window order.
type BookmarkDiff struct {
	From    string
	To      string
	Windows []BookmarkWindowDiff
}

func bookmarksFilePath(dir string) string { return filepath.Join(dir, "bookmarks.json") }

func readBookmarksFile(dir string) ([]bookmarkData, error) {
	bytes, err := os.ReadFile(bookmarksFilePath(dir))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if len(bytes) == 0 {
		return nil, nil
	}
	var file bookmarksFile
	if err := json.Unmarshal(bytes, &file); err != nil {
		return nil, err
	}
	return file.Bookmarks, nil
}

func writeBookmarksFile(dir string, bookmarks []bookmarkData) error {
	data, err := json.MarshalIndent(bookmarksFile{Bookmarks: bookmarks}, "", "  ")
	if err != nil {
		return err
	}
	temporary := bookmarksFilePath(dir) + ".tmp"
	if err := os.WriteFile(temporary, data, 0o600); err != nil {
		return err
	}
	if err := os.Rename(temporary, bookmarksFilePath(dir)); err != nil {
		return err
	}
	return syncDir(dir)
}

// CreateBookmark saves a named set of windows drawn from existing query
// views. Each window pins the version of its view at creation time, and
// compaction retains the evidence needed by that version until the bookmark
// is canceled.
func (s *Store) CreateBookmark(name string, windows []BookmarkWindow) (*Bookmark, error) {
	if name == "" || len(windows) == 0 {
		return nil, ErrInvalidBookmark
	}
	s.mu.Lock()
	if _, exists := s.bookmarks[name]; exists {
		s.mu.Unlock()
		return nil, ErrBookmarkExists
	}
	data := bookmarkData{Name: name}
	for _, window := range windows {
		view, ok := s.queryViews[window.View]
		if !ok {
			s.mu.Unlock()
			return nil, ErrQueryViewNotFound
		}
		if err := validateBookmarkWindow(view, window); err != nil {
			s.mu.Unlock()
			return nil, err
		}
		data.Windows = append(data.Windows, BookmarkWindowInfo{
			BookmarkWindow: window,
			Series:         view.Series,
			Version:        view.Version,
		})
	}
	bookmarks := append(s.bookmarkList(), data)
	if err := writeBookmarksFile(s.path, bookmarks); err != nil {
		s.mu.Unlock()
		return nil, err
	}
	s.bookmarks[name] = data
	bookmark := &Bookmark{store: s, snaps: s.bookmarkSnapshotsLocked(data), data: data}
	s.mu.Unlock()
	return bookmark, nil
}

func validateBookmarkWindow(view queryViewData, window BookmarkWindow) error {
	if window.Start < view.Start || window.End > view.End || window.Start >= window.End {
		return ErrInvalidBookmark
	}
	for _, tier := range view.Tiers {
		if window.Start < tier.Start || window.End > tier.End {
			continue
		}
		if tier.Aggregator != window.Aggregator || tier.Width != window.Width {
			return ErrInvalidBookmark
		}
		if tier.Aggregator != "" {
			if bucketFloor(window.Start, tier.Width) != window.Start || bucketFloor(window.End, tier.Width) != window.End {
				return ErrInvalidBookmark
			}
		}
		return nil
	}
	return ErrInvalidBookmark
}

func (s *Store) bookmarkList() []bookmarkData {
	bookmarks := make([]bookmarkData, 0, len(s.bookmarks))
	for _, bookmark := range s.bookmarks {
		bookmarks = append(bookmarks, bookmark)
	}
	sort.Slice(bookmarks, func(i, j int) bool { return bookmarks[i].Name < bookmarks[j].Name })
	return bookmarks
}

func (s *Store) bookmarkSnapshotsLocked(data bookmarkData) map[uint64]*Snapshot {
	snaps := make(map[uint64]*Snapshot)
	for _, window := range data.Windows {
		if _, ok := snaps[window.Version]; !ok {
			snaps[window.Version] = s.snapshotAtLocked(window.Version)
		}
	}
	return snaps
}

// OpenBookmark restores a saved bookmark with every window's original
// position, granularity, and pinned version.
func (s *Store) OpenBookmark(name string) (*Bookmark, error) {
	s.mu.Lock()
	data, ok := s.bookmarks[name]
	if !ok {
		s.mu.Unlock()
		return nil, ErrBookmarkNotFound
	}
	bookmark := &Bookmark{store: s, snaps: s.bookmarkSnapshotsLocked(data), data: data}
	s.mu.Unlock()
	return bookmark, nil
}

// CancelBookmark removes the durable retention held by one bookmark. Only
// that bookmark's references are released; saved query views and other
// bookmarks keep their own pinned versions.
func (s *Store) CancelBookmark(name string) error {
	s.mu.Lock()
	if _, ok := s.bookmarks[name]; !ok {
		s.mu.Unlock()
		return ErrBookmarkNotFound
	}
	bookmarks := s.bookmarkList()
	filtered := bookmarks[:0]
	for _, bookmark := range bookmarks {
		if bookmark.Name != name {
			filtered = append(filtered, bookmark)
		}
	}
	if err := writeBookmarksFile(s.path, filtered); err != nil {
		s.mu.Unlock()
		return err
	}
	delete(s.bookmarks, name)
	s.mu.Unlock()
	return nil
}

// Close releases the bookmark's in-memory snapshots. The saved bookmark
// remains durable and can be reopened.
func (b *Bookmark) Close() {
	if b == nil || b.snaps == nil {
		return
	}
	for _, snap := range b.snaps {
		snap.Close()
	}
	b.snaps = nil
}

// Windows returns the saved windows with their pinned versions.
func (b *Bookmark) Windows() []BookmarkWindowInfo {
	return append([]BookmarkWindowInfo(nil), b.data.Windows...)
}

// ReadWindow expands one saved window at its pinned version.
func (b *Bookmark) ReadWindow(index int) (*QueryResult, error) {
	if b == nil || b.snaps == nil || index < 0 || index >= len(b.data.Windows) {
		return nil, ErrInvalidBookmark
	}
	window := b.data.Windows[index]
	snap := b.snaps[window.Version]
	if snap == nil {
		return nil, ErrInvalidBookmark
	}
	view := &QueryView{
		store: b.store,
		snap:  snap,
		data: queryViewData{
			Name:    b.data.Name,
			Series:  window.Series,
			Start:   window.Start,
			End:     window.End,
			Version: window.Version,
			Tiers: []ViewTier{{
				Start:      window.Start,
				End:        window.End,
				Width:      window.Width,
				Aggregator: window.Aggregator,
			}},
		},
	}
	return view.Read()
}

// DiffBookmarks compares two bookmarks window by window. Windows must pair up
// by position, granularity, and series. Windows with changed buckets are
// returned first, each with the concrete changed time ranges.
func (s *Store) DiffBookmarks(fromName, toName string) (*BookmarkDiff, error) {
	from, err := s.OpenBookmark(fromName)
	if err != nil {
		return nil, err
	}
	defer from.Close()
	to, err := s.OpenBookmark(toName)
	if err != nil {
		return nil, err
	}
	defer to.Close()
	if len(from.data.Windows) != len(to.data.Windows) {
		return nil, ErrBookmarkMismatch
	}
	var changed, unchanged []BookmarkWindowDiff
	for i := range from.data.Windows {
		left := from.data.Windows[i]
		right := to.data.Windows[i]
		if left.Series != right.Series || left.Start != right.Start || left.End != right.End ||
			left.Width != right.Width || left.Aggregator != right.Aggregator {
			return nil, ErrBookmarkMismatch
		}
		leftBuckets, err := from.windowBuckets(i)
		if err != nil {
			return nil, err
		}
		rightBuckets, err := to.windowBuckets(i)
		if err != nil {
			return nil, err
		}
		if len(leftBuckets) != len(rightBuckets) {
			return nil, ErrBookmarkMismatch
		}
		var changes []ChangeRange
		for j := range leftBuckets {
			a, other := leftBuckets[j], rightBuckets[j]
			if a.Start != other.Start || a.Status != other.Status || a.SampleCount != other.SampleCount || !bytesEqual(a.Value, other.Value) {
				changes = appendInterval(changes, a.Start, a.End)
			}
		}
		diff := BookmarkWindowDiff{Index: i, Window: left, Changed: len(changes) > 0, Changes: changes}
		if diff.Changed {
			changed = append(changed, diff)
		} else {
			unchanged = append(unchanged, diff)
		}
	}
	return &BookmarkDiff{From: fromName, To: toName, Windows: append(changed, unchanged...)}, nil
}

func (b *Bookmark) windowBuckets(index int) ([]QueryBucket, error) {
	result, err := b.ReadWindow(index)
	if err != nil {
		return nil, err
	}
	var buckets []QueryBucket
	for _, segment := range result.Segments {
		buckets = append(buckets, segment.Buckets...)
	}
	return buckets, nil
}
