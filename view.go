package timeseries

import (
	"context"
	"sort"
)

// Tier selects one resolution for part of a cross-resolution query. Width 0
// means raw observations; otherwise aggregate Name must be registered and its
// bucket width must equal Width. A later tier may only use an equal or wider
// width, so segments never split a bucket and cannot double count.
type Tier struct {
	From  int64
	Width int64
	Name  string
}

// CellStatus distinguishes genuinely observed values from absent evidence.
type CellStatus int

const (
	// CellPresent means observations fed the cell (their aggregate may still
	// be a real zero such as a sum of cancelling values).
	CellPresent CellStatus = iota
	// CellEmpty means the cell was explicitly evaluated with zero samples:
	// there is no raw observation behind it.
	CellEmpty
	// CellUncovered means no aggregate bucket covers the time range, so the
	// cell cannot be explained from precomputed resolutions.
	CellUncovered
)

// ViewCell is one aligned, non-overlapping piece of a cross-resolution view.
type ViewCell struct {
	Start  int64
	End    int64
	Width  int64
	Name   string
	Status CellStatus
	Value  []byte
	// Count is the number of raw observations behind the cell. It is 1 for raw
	// points, the bucket sample count for aggregates, and 0 for empty or
	// uncovered cells. Averages over cells must weight by Count; cells with
	// different sample sizes are never equivalent.
	Count int
}

// ViewSegment explains one contiguous [Start, End) stretch and which
// resolution produced it. Segments partition the requested half-open range,
// so junctions never count the same instant twice.
type ViewSegment struct {
	Start int64
	End   int64
	Width int64
	Name  string
	Cells []ViewCell
	// Gaps reports half-open intervals inside the segment without evidence.
	// For aggregate segments a gap means an explicitly empty or uncovered
	// bucket; for raw segments it means no observation timestamp is present.
	Gaps []Gap
}

// Gap describes a span with no observations, separately from observed zeros.
type Gap struct {
	Start int64
	End   int64
}

// QueryView is the whole cross-resolution answer for one pinned data version.
type QueryView struct {
	Series   string
	Start    int64
	End      int64
	Version  uint64
	Segments []ViewSegment
}

// QueryView resolves [start, end) across several resolutions at the snapshot's
// fixed data version. Each emitted cell keeps its own granularity, sample
// count, and coverage, so junction points are never counted twice, missing
// observations stay separate from real zeros, and means of differently sized
// samples are never presented as equal.
func (s *Snapshot) QueryView(ctx context.Context, series string, start, end int64, tiers []Tier) (QueryView, error) {
	if start > end {
		return QueryView{}, ErrInvalidRange
	}
	view := QueryView{Series: series, Start: start, End: end, Version: s.version}
	if start == end {
		return view, nil
	}
	segments, err := s.planTiers(start, end, tiers)
	if err != nil {
		return QueryView{}, err
	}
	for index := range segments {
		if err := ctx.Err(); err != nil {
			return QueryView{}, err
		}
		segment := &segments[index]
		if segment.Width == 0 {
			segment.Cells, segment.Gaps = s.rawCells(ctx, series, segment.Start, segment.End)
		} else {
			segment.Cells, segment.Gaps = s.aggregateCells(ctx, segment.Name, segment.Width, series, segment.Start, segment.End)
		}
	}
	view.Segments = segments
	return view, nil
}

// planTiers partitions [start, end) into disjoint, bucket-aligned segments.
// When a tier switch lands inside a bucket the boundary is snapped UP to the
// next (coarser, multiple) tier's bucket width. The finer resolution keeps
// every whole bucket it started, so no bucket's value is ever split or shared
// between segments and junctions cannot double count.
func (s *Snapshot) planTiers(start, end int64, tiers []Tier) ([]ViewSegment, error) {
	if len(tiers) == 0 {
		return nil, ErrInvalidTier
	}
	ordered := append([]Tier(nil), tiers...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].From < ordered[j].From })
	if ordered[0].From > start {
		return nil, ErrInvalidTier
	}
	for index := 1; index < len(ordered); index++ {
		if ordered[index].From == ordered[index-1].From {
			return nil, ErrInvalidTier
		}
	}
	for index, tier := range ordered {
		if tier.Width < 0 {
			return nil, ErrInvalidTier
		}
		if tier.Width > 0 && tier.Name == "" {
			return nil, ErrInvalidTier
		}
		if index > 0 {
			previous := ordered[index-1].Width
			if previous != 0 {
				if tier.Width != 0 && (tier.Width < previous || tier.Width%previous != 0) {
					return nil, ErrInvalidTier
				}
			} else if tier.Width == 0 {
				return nil, ErrInvalidTier
			}
		}
	}
	var segments []ViewSegment
	for index, tier := range ordered {
		segmentStart := tier.From
		if index == 0 {
			segmentStart = start
		} else if index > 0 {
			segmentStart = bucketCeil(segmentStart, effectiveWidth(tier.Width))
		}
		if segmentStart >= end {
			if len(segments) > 0 {
				segments[len(segments)-1].End = end
			}
			break
		}
		if len(segments) > 0 {
			segments[len(segments)-1].End = segmentStart
		}
		segments = append(segments, ViewSegment{
			Start: segmentStart,
			End:   end,
			Width: tier.Width,
			Name:  tier.Name,
		})
	}
	return segments, nil
}

func effectiveWidth(width int64) int64 {
	if width <= 0 {
		return 1
	}
	return width
}

func bucketCeil(timestamp, width int64) int64 {
	floor := bucketFloor(timestamp, width)
	if floor == timestamp {
		return timestamp
	}
	return floor + width
}

func (s *Snapshot) rawCells(ctx context.Context, series string, start, end int64) ([]ViewCell, []Gap) {
	points := s.Range(series, start, end)
	cells := make([]ViewCell, 0, len(points))
	var gaps []Gap
	cursor := start
	for _, point := range points {
		if err := ctx.Err(); err != nil {
			return cells, gaps
		}
		if cursor < point.Timestamp {
			gaps = append(gaps, Gap{Start: cursor, End: point.Timestamp})
		}
		cells = append(cells, ViewCell{
			Start:  point.Timestamp,
			End:    point.Timestamp + 1,
			Status: CellPresent,
			Value:  append([]byte(nil), point.Data...),
			Count:  1,
		})
		cursor = point.Timestamp + 1
	}
	if cursor < end {
		gaps = append(gaps, Gap{Start: cursor, End: end})
	}
	return cells, gaps
}

func (s *Snapshot) aggregateCells(ctx context.Context, name string, width int64, series string, start, end int64) ([]ViewCell, []Gap) {
	firstBucket := bucketFloor(start, width)
	lastBucket := bucketFloor(end-1, width)
	known := make(map[int64]Aggregate)
	for _, aggregate := range s.Aggregates(name, series, firstBucket, lastBucket+width) {
		known[aggregate.BucketStart] = aggregate
	}
	var cells []ViewCell
	var gaps []Gap
	for bucketStart := firstBucket; bucketStart <= lastBucket; bucketStart += width {
		if err := ctx.Err(); err != nil {
			return cells, gaps
		}
		cellStart := bucketStart
		if cellStart < start {
			cellStart = start
		}
		cellEnd := bucketStart + width
		if cellEnd > end {
			cellEnd = end
		}
		aggregate, ok := known[bucketStart]
		if !ok {
			cells = append(cells, ViewCell{
				Start:  cellStart,
				End:    cellEnd,
				Width:  width,
				Name:   name,
				Status: CellUncovered,
			})
			gaps = append(gaps, Gap{Start: cellStart, End: cellEnd})
			continue
		}
		status := CellPresent
		if aggregate.Count == 0 {
			status = CellEmpty
			gaps = append(gaps, Gap{Start: cellStart, End: cellEnd})
		}
		cells = append(cells, ViewCell{
			Start:  cellStart,
			End:    cellEnd,
			Width:  width,
			Name:   name,
			Status: status,
			Value:  append([]byte(nil), aggregate.Value...),
			Count:  aggregate.Count,
		})
	}
	return cells, gaps
}

// ChangedRanges reports disjoint half-open spans whose aggregate buckets
// differ between the receiver's pinned version and other (typically newer).
// A correction spanning several buckets appears as the exact union of the
// affected bucket spans, including buckets that became deleted or uncovered.
func (s *Snapshot) ChangedRanges(other *Snapshot, name, series string, start, end int64) ([]Gap, error) {
	if start > end {
		return nil, ErrInvalidRange
	}
	changed := make(map[int64]bool)
	seen := func(bucketStart int64) {
		oldRecord, oldOK := s.aggregateRecord(name, series, bucketStart)
		newRecord, newOK := other.aggregateRecord(name, series, bucketStart)
		if oldOK != newOK {
			changed[bucketStart] = true
			return
		}
		if !oldOK {
			return
		}
		if oldRecord.Deleted != newRecord.Deleted ||
			oldRecord.SourceVersion != newRecord.SourceVersion ||
			!equalBytes(oldRecord.Value, newRecord.Value) {
			changed[bucketStart] = true
		}
	}
	for _, aggregate := range s.Aggregates(name, series, start, end) {
		seen(aggregate.BucketStart)
	}
	for _, aggregate := range other.Aggregates(name, series, start, end) {
		seen(aggregate.BucketStart)
	}
	starts := make([]int64, 0, len(changed))
	for bucketStart := range changed {
		starts = append(starts, bucketStart)
	}
	sort.Slice(starts, func(i, j int) bool { return starts[i] < starts[j] })
	var ranges []Gap
	for _, bucketStart := range starts {
		width := int64(0)
		if record, ok := s.aggregateRecord(name, series, bucketStart); ok && record.Width > 0 {
			width = record.Width
		} else if record, ok := other.aggregateRecord(name, series, bucketStart); ok && record.Width > 0 {
			width = record.Width
		}
		spanStart := bucketStart
		if spanStart < start {
			spanStart = start
		}
		spanEnd := bucketStart + width
		if spanEnd > end {
			spanEnd = end
		}
		if len(ranges) > 0 && ranges[len(ranges)-1].End == spanStart {
			ranges[len(ranges)-1].End = spanEnd
		} else {
			ranges = append(ranges, Gap{Start: spanStart, End: spanEnd})
		}
	}
	return ranges, nil
}

func (s *Snapshot) aggregateRecord(name, series string, bucketStart int64) (Record, bool) {
	records := s.state.versions[makeAggregateKey(name, series, bucketStart)]
	idx := sort.Search(len(records), func(i int) bool {
		return records[i].Version > s.version
	})
	if idx == 0 {
		return Record{}, false
	}
	return records[idx-1], true
}

func equalBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for index := range a {
		if a[index] != b[index] {
			return false
		}
	}
	return true
}
