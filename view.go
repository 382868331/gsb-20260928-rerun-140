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
	Start         int64
	End           int64
	Width         int64
	Aggregator    string
	Value         []byte
	SampleCount   int64
	SourceVersion uint64
	Status        BucketStatus
}

// QueryEntry is one browsable item. A collapsed item represents a run of
// consecutive raw missing observations; aggregate zero buckets and
// unmaterialized buckets remain separate entries.
type QueryEntry struct {
	Start         int64
	End           int64
	Width         int64
	Aggregator    string
	Value         []byte
	SampleCount   int64
	SourceVersion uint64
	Status        BucketStatus
	Collapsed     bool
	BucketCount   int64
}

// QueryPage is a fixed-version, cursor-based slice of a query view. Use
// NextStart as the next read cursor when HasMore is true.
type QueryPage struct {
	View      string
	Series    string
	Start     int64
	End       int64
	Version   uint64
	Entries   []QueryEntry
	NextStart int64
	HasMore   bool
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
		segment, err := v.buildSegment(tier.Start, tier.End, tier)
		if err != nil {
			return nil, err
		}
		result.Segments = append(result.Segments, segment)
	}
	return result, nil
}

// ReadWindow expands an arbitrary saved-view window. Aggregate window edges
// must lie on bucket boundaries; raw windows may use any one-unit boundary.
func (v *QueryView) ReadWindow(start, end int64) (*QueryResult, error) {
	if v == nil || v.snap == nil {
		return nil, ErrQueryViewNotFound
	}
	if start < v.data.Start || end > v.data.End || start >= end {
		return nil, ErrInvalidQueryView
	}
	result := &QueryResult{View: v.data.Name, Series: v.data.Series, Start: start, End: end, Version: v.data.Version}
	for _, tier := range v.data.Tiers {
		if tier.End <= start || tier.Start >= end {
			continue
		}
		segmentStart := maxInt64(tier.Start, start)
		segmentEnd := minInt64(tier.End, end)
		if tier.Aggregator != "" {
			if bucketFloor(segmentStart, tier.Width) != segmentStart || bucketFloor(segmentEnd, tier.Width) != segmentEnd {
				return nil, ErrInvalidQueryView
			}
		}
		segment, err := v.buildSegment(segmentStart, segmentEnd, tier)
		if err != nil {
			return nil, err
		}
		result.Segments = append(result.Segments, segment)
	}
	return result, nil
}

// ReadPage returns at most limit browsable entries beginning at cursor. A
// limit smaller than one returns an invalid-argument error.
func (v *QueryView) ReadPage(cursor int64, limit int) (*QueryPage, error) {
	if v == nil || v.snap == nil {
		return nil, ErrQueryViewNotFound
	}
	if cursor < v.data.Start || cursor >= v.data.End || limit <= 0 {
		return nil, ErrInvalidQueryView
	}
	page := &QueryPage{View: v.data.Name, Series: v.data.Series, Start: cursor, End: v.data.End, Version: v.data.Version}
	position := cursor
	for _, tier := range v.data.Tiers {
		if tier.End <= position {
			continue
		}
		if tier.Start > position || len(page.Entries) >= limit {
			break
		}
		if tier.Aggregator != "" && bucketFloor(position, tier.Width) != position {
			return nil, ErrInvalidQueryView
		}
		var entries []QueryEntry
		var nextPosition int64
		var err error
		if tier.Aggregator == "" {
			entries, nextPosition, err = v.rawEntries(position, tier.End, limit-len(page.Entries))
		} else {
			entries, nextPosition, err = v.aggregateEntries(tier.Aggregator, position, tier.End, tier.Width, limit-len(page.Entries))
		}
		if err != nil {
			return nil, err
		}
		page.Entries = append(page.Entries, entries...)
		position = nextPosition
		if len(page.Entries) >= limit {
			break
		}
	}
	page.NextStart = position
	page.HasMore = position < v.data.End
	return page, nil
}

func (v *QueryView) buildSegment(start, end int64, tier ViewTier) (QuerySegment, error) {
	segment := QuerySegment{Start: start, End: end, Width: tier.Width, Aggregator: tier.Aggregator, Coverage: CoverageComplete}
	if tier.Aggregator == "" {
		segment.Buckets = v.rawBuckets(start, end)
	} else {
		buckets, err := v.aggregateBuckets(tier.Aggregator, start, end, tier.Width)
		if err != nil {
			return QuerySegment{}, err
		}
		segment.Buckets = buckets
		for _, bucket := range buckets {
			if bucket.Status == BucketUnmaterialized {
				segment.Coverage = CoveragePartial
				break
			}
		}
	}
	return segment, nil
}

func (v *QueryView) rawBuckets(start, end int64) []QueryBucket {
	byTime := v.rawRecords(start, end)
	buckets := make([]QueryBucket, 0)
	for timestamp := start; timestamp < end; timestamp++ {
		bucket := QueryBucket{Start: timestamp, End: timestamp + 1, Width: 1, Status: BucketMissingObservation}
		if record, ok := byTime[timestamp]; ok {
			bucket.Value = append([]byte(nil), record.Value...)
			bucket.SampleCount = 1
			bucket.SourceVersion = record.Version
			bucket.Status = BucketObserved
		}
		buckets = append(buckets, bucket)
	}
	return buckets
}

type rawRecord struct {
	Value   []byte
	Version uint64
}

func (v *QueryView) rawRecords(start, end int64) map[int64]rawRecord {
	records := make(map[int64]rawRecord)
	for _, versions := range v.snap.state.versions {
		record, ok := visibleAt(versions, v.snap.version)
		if !ok || record.IsAggregate || record.Series != v.data.Series || record.Timestamp < start || record.Timestamp >= end {
			continue
		}
		records[record.Timestamp] = rawRecord{Value: append([]byte(nil), record.Value...), Version: record.Version}
	}
	return records
}

func (v *QueryView) rawEntries(start, end int64, limit int) ([]QueryEntry, int64, error) {
	records := v.rawRecords(start, end)
	timestamps := make([]int64, 0, len(records))
	for timestamp := range records {
		timestamps = append(timestamps, timestamp)
	}
	sort.Slice(timestamps, func(i, j int) bool { return timestamps[i] < timestamps[j] })
	entries := make([]QueryEntry, 0, limit)
	position := start
	for _, timestamp := range timestamps {
		if timestamp > position {
			entries = append(entries, QueryEntry{Start: position, End: timestamp, Width: 1, Status: BucketMissingObservation, Collapsed: true, BucketCount: timestamp - position})
			position = timestamp
			if len(entries) >= limit {
				return entries, position, nil
			}
		}
		record := records[timestamp]
		entries = append(entries, QueryEntry{Start: timestamp, End: timestamp + 1, Width: 1, Value: record.Value, SampleCount: 1, SourceVersion: record.Version, Status: BucketObserved, BucketCount: 1})
		position = timestamp + 1
		if len(entries) >= limit {
			return entries, position, nil
		}
	}
	if position < end {
		entries = append(entries, QueryEntry{Start: position, End: end, Width: 1, Status: BucketMissingObservation, Collapsed: true, BucketCount: end - position})
		position = end
	}
	return entries, position, nil
}

func (v *QueryView) aggregateEntries(name string, start, end, width int64, limit int) ([]QueryEntry, int64, error) {
	buckets, err := v.aggregateBuckets(name, start, end, width)
	if err != nil {
		return nil, start, err
	}
	if len(buckets) > limit {
		buckets = buckets[:limit]
	}
	entries := make([]QueryEntry, 0, len(buckets))
	for _, bucket := range buckets {
		entries = append(entries, QueryEntry{Start: bucket.Start, End: bucket.End, Width: bucket.Width, Aggregator: bucket.Aggregator, Value: bucket.Value, SampleCount: bucket.SampleCount, SourceVersion: bucket.SourceVersion, Status: bucket.Status, BucketCount: 1})
	}
	nextPosition := end
	if len(buckets) > 0 {
		nextPosition = buckets[len(buckets)-1].End
	}
	return entries, nextPosition, nil
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
			bucket.SourceVersion = aggregate.SourceVersion
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
		segmentIntervals := intervals[:0:0]
		if len(a) != len(b) {
			return nil, ErrQueryViewMismatch
		}
		for j := range a {
			if a[j].Start != b[j].Start || a[j].Status != b[j].Status || a[j].SampleCount != b[j].SampleCount || !bytesEqual(a[j].Value, b[j].Value) {
				segmentIntervals = appendInterval(segmentIntervals, a[j].Start, a[j].End)
			}
		}
		intervals = append(intervals, segmentIntervals...)
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

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
