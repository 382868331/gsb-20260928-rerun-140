package timeseries

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
)

type BucketStatus string

const (
	BucketObserved           BucketStatus = "observed"
	BucketMissingObservation BucketStatus = "missing-observation"
	BucketExplicitZero       BucketStatus = "explicit-zero"
	BucketUnmaterialized     BucketStatus = "unmaterialized"
)

type Coverage string

const (
	CoverageComplete Coverage = "complete"
	CoveragePartial  Coverage = "partial"
)

// ViewTier describes one contiguous resolution section. An empty Aggregator
// means raw observations, whose granularity is one time unit.
type ViewTier struct {
	Start      int64  `json:"start"`
	End        int64  `json:"end"`
	Width      int64  `json:"width"`
	Aggregator string `json:"aggregator,omitempty"`
}

type queryViewData struct {
	Name    string     `json:"name"`
	Series  string     `json:"series"`
	Start   int64      `json:"start"`
	End     int64      `json:"end"`
	Version uint64     `json:"version"`
	Tiers   []ViewTier `json:"tiers"`
}

type queryViewsFile struct {
	Views []queryViewData `json:"views"`
}

// QueryView is a fixed-version, reusable cross-resolution read plan. Saved
// views remain available after reopening the store.
type QueryView struct {
	store *Store
	snap  *Snapshot
	data  queryViewData
}

type QueryBucket struct {
	Start       int64
	End         int64
	Width       int64
	Aggregator  string
	Value       []byte
	SampleCount int64
	Status      BucketStatus
}

type QuerySegment struct {
	Start      int64
	End        int64
	Width      int64
	Aggregator string
	Coverage   Coverage
	Buckets    []QueryBucket
}

type QueryResult struct {
	View     string
	Series   string
	Start    int64
	End      int64
	Version  uint64
	Segments []QuerySegment
}

type ChangeRange struct {
	Start int64
	End   int64
}

func viewsFilePath(dir string) string { return filepath.Join(dir, "views.json") }

func readQueryViewsFile(dir string) ([]queryViewData, error) {
	bytes, err := os.ReadFile(viewsFilePath(dir))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if len(bytes) == 0 {
		return nil, nil
	}
	var file queryViewsFile
	if err := json.Unmarshal(bytes, &file); err != nil {
		return nil, err
	}
	return file.Views, nil
}

func writeQueryViewsFile(dir string, views []queryViewData) error {
	data, err := json.MarshalIndent(queryViewsFile{Views: views}, "", "  ")
	if err != nil {
		return err
	}
	temporary := viewsFilePath(dir) + ".tmp"
	if err := os.WriteFile(temporary, data, 0o600); err != nil {
		return err
	}
	if err := os.Rename(temporary, viewsFilePath(dir)); err != nil {
		return err
	}
	return syncDir(dir)
}

func (s *Store) validateTiersLocked(series string, start, end int64, tiers []ViewTier) error {
	if start >= end || len(tiers) == 0 {
		return ErrInvalidQueryView
	}
	if tiers[0].Start != start {
		return ErrInvalidQueryView
	}
	for i, tier := range tiers {
		if tier.Start < start || tier.End > end || tier.Start >= tier.End {
			return ErrInvalidQueryView
		}
		if i > 0 && tier.Start != tiers[i-1].End {
			return ErrInvalidQueryView
		}
		if tier.Aggregator == "" {
			if tier.Width != 1 {
				return ErrInvalidQueryView
			}
			continue
		}
		if tier.Width <= 0 {
			return ErrInvalidQueryView
		}
		if bucketFloor(tier.Start, tier.Width) != tier.Start {
			return ErrInvalidQueryView
		}
		if bucketFloor(tier.End, tier.Width) != tier.End {
			return ErrInvalidQueryView
		}
	}
	lastTier := tiers[len(tiers)-1]
	if lastTier.End != end {
		return ErrInvalidQueryView
	}
	return nil
}

// CreateQueryView saves a view at the current correction version. Later
// revisions only affect newly created views; compaction retains evidence
// needed by this view until it is canceled.
func (s *Store) CreateQueryView(name, series string, start, end int64, tiers []ViewTier) (*QueryView, error) {
	if name == "" || series == "" {
		return nil, ErrInvalidQueryView
	}
	copied := append([]ViewTier(nil), tiers...)
	s.mu.Lock()
	if _, exists := s.queryViews[name]; exists {
		s.mu.Unlock()
		return nil, ErrQueryViewExists
	}
	if err := s.validateTiersLocked(series, start, end, copied); err != nil {
		s.mu.Unlock()
		return nil, err
	}
	data := queryViewData{Name: name, Series: series, Start: start, End: end, Version: s.state.version, Tiers: copied}
	views := append(s.queryViewList(), data)
	if err := writeQueryViewsFile(s.path, views); err != nil {
		s.mu.Unlock()
		return nil, err
	}
	s.queryViews[name] = data
	snap := s.snapshotLocked()
	s.mu.Unlock()
	return &QueryView{store: s, snap: snap, data: data}, nil
}

func (s *Store) queryViewList() []queryViewData {
	views := make([]queryViewData, 0, len(s.queryViews))
	for _, view := range s.queryViews {
		views = append(views, view)
	}
	sort.Slice(views, func(i, j int) bool { return views[i].Name < views[j].Name })
	return views
}

// OpenQueryView opens a saved view at its original fixed version. Stored
// aggregate evidence is readable without registering the callback again; the
// callback is only needed when future Downsample calls compute new buckets.
func (s *Store) OpenQueryView(name string) (*QueryView, error) {
	s.mu.Lock()
	data, ok := s.queryViews[name]
	if !ok {
		s.mu.Unlock()
		return nil, ErrQueryViewNotFound
	}
	if err := s.validateSavedTiersLocked(data.Start, data.End, data.Tiers); err != nil {
		s.mu.Unlock()
		return nil, err
	}
	snap := s.snapshotAtLocked(data.Version)
	s.mu.Unlock()
	return &QueryView{store: s, snap: snap, data: data}, nil
}

func (s *Store) validateSavedTiersLocked(start, end int64, tiers []ViewTier) error {
	if start >= end || len(tiers) == 0 || tiers[0].Start != start {
		return ErrInvalidQueryView
	}
	for i, tier := range tiers {
		if tier.Start < start || tier.End > end || tier.Start >= tier.End {
			return ErrInvalidQueryView
		}
		if i > 0 && tier.Start != tiers[i-1].End {
			return ErrInvalidQueryView
		}
		if tier.Aggregator == "" {
			if tier.Width != 1 {
				return ErrInvalidQueryView
			}
			continue
		}
		if tier.Width <= 0 || bucketFloor(tier.Start, tier.Width) != tier.Start || bucketFloor(tier.End, tier.Width) != tier.End {
			return ErrInvalidQueryView
		}
	}
	if tiers[len(tiers)-1].End != end {
		return ErrInvalidQueryView
	}
	return nil
}

// CancelQueryView removes the durable retention associated with a saved view.
func (s *Store) CancelQueryView(name string) error {
	s.mu.Lock()
	if _, ok := s.queryViews[name]; !ok {
		s.mu.Unlock()
		return ErrQueryViewNotFound
	}
	views := s.queryViewList()
	filtered := views[:0]
	for _, view := range views {
		if view.Name != name {
			filtered = append(filtered, view)
		}
	}
	if err := writeQueryViewsFile(s.path, filtered); err != nil {
		s.mu.Unlock()
		return err
	}
	delete(s.queryViews, name)
	s.mu.Unlock()
	return nil
}

func (v *QueryView) Close() {
	if v != nil && v.snap != nil {
		v.snap.Close()
		v.snap = nil
	}
}

func (v *QueryView) Version() uint64 { return v.data.Version }

// Read returns disjoint, half-open segments. Adjacent segments meet at exact
// tier boundaries, so no observation or bucket is counted twice.
func (v *QueryView) Read() (*QueryResult, error) {
	if v == nil || v.snap == nil {
		return nil, ErrQueryViewNotFound
	}
	result := &QueryResult{View: v.data.Name, Series: v.data.Series, Start: v.data.Start, End: v.data.End, Version: v.data.Version}
	for _, tier := range v.data.Tiers {
		segment := QuerySegment{Start: tier.Start, End: tier.End, Width: tier.Width, Aggregator: tier.Aggregator, Coverage: CoverageComplete}
		if tier.Aggregator == "" {
			segment.Buckets = v.rawBuckets(tier.Start, tier.End)
		} else {
			buckets, err := v.aggregateBuckets(tier.Aggregator, tier.Start, tier.End, tier.Width)
			if err != nil {
				return nil, err
			}
			segment.Buckets = buckets
			for _, bucket := range buckets {
				if bucket.Status == BucketUnmaterialized {
					segment.Coverage = CoveragePartial
					break
				}
			}
		}
		result.Segments = append(result.Segments, segment)
	}
	return result, nil
}

func (v *QueryView) rawBuckets(start, end int64) []QueryBucket {
	points := v.snap.Range(v.data.Series, start, end)
	byTime := make(map[int64]Observation, len(points))
	for _, point := range points {
		byTime[point.Timestamp] = point
	}
	buckets := make([]QueryBucket, 0)
	for timestamp := start; timestamp < end; timestamp++ {
		bucket := QueryBucket{Start: timestamp, End: timestamp + 1, Width: 1, Status: BucketMissingObservation}
		if point, ok := byTime[timestamp]; ok {
			bucket.Value = append([]byte(nil), point.Data...)
			bucket.SampleCount = 1
			bucket.Status = BucketObserved
		}
		buckets = append(buckets, bucket)
	}
	return buckets
}

func (v *QueryView) aggregateBuckets(name string, start, end, width int64) ([]QueryBucket, error) {
	aggregates := v.snap.Aggregates(name, v.data.Series, start, end)
	byStart := make(map[int64]Aggregate, len(aggregates))
	for _, aggregate := range aggregates {
		byStart[aggregate.BucketStart] = aggregate
	}
	buckets := make([]QueryBucket, 0)
	for bucketStart := start; bucketStart < end; bucketStart += width {
		bucket := QueryBucket{Start: bucketStart, End: bucketStart + width, Width: width, Aggregator: name, Status: BucketUnmaterialized}
		if aggregate, ok := byStart[bucketStart]; ok {
			bucket.Value = append([]byte(nil), aggregate.Value...)
			bucket.SampleCount = aggregate.SampleCount
			legacyCount := aggregate.SampleCount == 0
			if legacyCount {
				bucket.SampleCount = v.snap.rawCountAt(aggregate.SourceVersion, v.data.Series, bucketStart, bucketStart+width)
			}
			if bucket.SampleCount == 0 {
				bucket.Status = BucketExplicitZero
			} else {
				bucket.Status = BucketObserved
			}
		}
		buckets = append(buckets, bucket)
	}
	return buckets, nil
}

func (s *Snapshot) rawCountAt(version uint64, series string, start, end int64) int64 {
	var count int64
	for _, records := range s.state.versions {
		record, ok := visibleAt(records, version)
		if !ok || record.IsAggregate || record.Series != series || record.Timestamp < start || record.Timestamp >= end {
			continue
		}
		count++
	}
	return count
}

// DiffQueryViews reports minimal bucket ranges whose values or missing/zero
// status differ between two comparable saved views. The views must address the
// same series, time range, and tiers.
func (s *Store) DiffQueryViews(fromName, toName string) ([]ChangeRange, error) {
	from, err := s.OpenQueryView(fromName)
	if err != nil {
		return nil, err
	}
	defer from.Close()
	to, err := s.OpenQueryView(toName)
	if err != nil {
		return nil, err
	}
	defer to.Close()
	if from.data.Series != to.data.Series || from.data.Start != to.data.Start || from.data.End != to.data.End || len(from.data.Tiers) != len(to.data.Tiers) {
		return nil, ErrQueryViewMismatch
	}
	for i := range from.data.Tiers {
		if from.data.Tiers[i] != to.data.Tiers[i] {
			return nil, ErrQueryViewMismatch
		}
	}
	left, err := from.Read()
	if err != nil {
		return nil, err
	}
	right, err := to.Read()
	if err != nil {
		return nil, err
	}
	var intervals []ChangeRange
	for i := range left.Segments {
		a := left.Segments[i].Buckets
		b := right.Segments[i].Buckets
		if len(a) != len(b) {
			return nil, ErrQueryViewMismatch
		}
		for j := range a {
			if a[j].Start != b[j].Start || a[j].Status != b[j].Status || a[j].SampleCount != b[j].SampleCount || !bytesEqual(a[j].Value, b[j].Value) {
				intervals = appendInterval(intervals, a[j].Start, a[j].End)
			}
		}
	}
	return intervals, nil
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

func appendInterval(intervals []ChangeRange, start, end int64) []ChangeRange {
	if len(intervals) > 0 && intervals[len(intervals)-1].End == start {
		intervals[len(intervals)-1].End = end
		return intervals
	}
	return append(intervals, ChangeRange{Start: start, End: end})
}
