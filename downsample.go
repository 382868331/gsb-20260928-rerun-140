package timeseries

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"sync"
)

// Aggregation computes one scalar value for all observations in one bucket.
// Implementations must be comparable only by the registered aggregator name.
type Aggregation interface {
	// Add folds one visible observation value into the running aggregation.
	Add(value []byte) error
	// Result returns the scalar value for all Add calls in one bucket.
	Result() (float64, error)
}

// AggregationFactory builds a fresh aggregation for one bucket.
type AggregationFactory func() Aggregation

var (
	aggregatorMu    sync.RWMutex
	aggregatorNames = map[string]AggregationFactory{}
)

// RegisterAggregator makes a named aggregation available to Downsample and
// Snapshot.Aggregates. Registering the same name replaces the factory.
func RegisterAggregator(name string, factory AggregationFactory) {
	if name == "" {
		panic("timeseries: aggregator name must not be empty")
	}
	aggregatorMu.Lock()
	defer aggregatorMu.Unlock()
	aggregatorNames[name] = factory
}

func aggregatorFactory(name string) (AggregationFactory, bool) {
	aggregatorMu.RLock()
	defer aggregatorMu.RUnlock()
	factory, ok := aggregatorNames[name]
	return factory, ok
}

func parseFloat64(value []byte) (float64, error) {
	number, err := strconv.ParseFloat(string(value), 64)
	if err != nil {
		return 0, err
	}
	return number, nil
}

type sumAggregation struct {
	sum   float64
	count int
}

func (a *sumAggregation) Add(value []byte) error {
	number, err := parseFloat64(value)
	if err != nil {
		return err
	}
	a.sum += number
	a.count++
	return nil
}

func (a *sumAggregation) Result() (float64, error) { return a.sum, nil }

type countAggregation struct{ count int }

func (a *countAggregation) Add([]byte) error         { a.count++; return nil }
func (a *countAggregation) Result() (float64, error) { return float64(a.count), nil }

type averageAggregation struct{ sumAggregation }

func (a *averageAggregation) Result() (float64, error) {
	if a.count == 0 {
		return 0, nil
	}
	return a.sum / float64(a.count), nil
}

func init() {
	RegisterAggregator("sum", func() Aggregation { return &sumAggregation{} })
	RegisterAggregator("count", func() Aggregation { return &countAggregation{} })
	RegisterAggregator("avg", func() Aggregation { return &averageAggregation{} })
}

// Bucket is one fixed-width, half-open interval [Start, Start+Width).
type Bucket struct {
	Start int64
	Width int64
}

// BucketAggregate is the aggregate of one series within one half-open bucket.
// Count is zero for buckets that contain no visible observations.
type BucketAggregate struct {
	Series        string
	Bucket        Bucket
	Value         float64
	Count         int
	SourceVersion uint64
}

func bucketStart(timestamp, width int64) int64 {
	if timestamp >= 0 {
		return timestamp - timestamp%width
	}
	adjusted := timestamp + 1 - ((timestamp + 1) % width) - width
	return adjusted
}

func aggregateKey(series, name string, width, start int64) string {
	return fmt.Sprintf("%s\x00agg\x00%d\x00%d\x00%s", series, width, start, name)
}

// Downsample (re)computes fixed-width bucket aggregates for one series.
// Aggregates are ordinary MVCC records: a revision creates new aggregate
// versions only for the bucket containing its timestamp, while snapshots
// taken before Downsample continue reading the prior aggregate versions.
// Empty buckets are represented on read by Snapshot.Aggregates and are not
// stored until they contain observations.
func (s *Store) Downsample(series, aggregator string, width int64) (uint64, error) {
	if series == "" {
		return 0, ErrEmptySeries
	}
	if width <= 0 {
		return 0, ErrInvalidBucketWidth
	}
	factory, ok := aggregatorFactory(aggregator)
	if !ok {
		return 0, ErrUnknownAggregator
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	type groupedBucket struct {
		start         int64
		records       []Record
		sourceVersion uint64
	}
	groups := map[int64]*groupedBucket{}
	for _, records := range s.state.versions {
		record, visible := visibleAt(records, s.state.version)
		if !visible || record.Series != series || record.Kind != recordKindObservation {
			continue
		}
		start := bucketStart(record.Timestamp, width)
		group := groups[start]
		if group == nil {
			group = &groupedBucket{start: start}
			groups[start] = group
		}
		group.records = append(group.records, record)
		if record.Version > group.sourceVersion {
			group.sourceVersion = record.Version
		}
	}

	var writes []Record
	starts := make([]int64, 0, len(groups))
	for start := range groups {
		starts = append(starts, start)
	}
	sort.Slice(starts, func(i, j int) bool { return starts[i] < starts[j] })
	baseVersion := s.state.version
	for _, start := range starts {
		group := groups[start]
		key := aggregateKey(series, aggregator, width, start)
		if existing := s.state.versions[key]; len(existing) > 0 {
			latest := existing[len(existing)-1]
			if !latest.Deleted && latest.SourceVersion == group.sourceVersion {
				continue
			}
		}
		agg := factory()
		for _, record := range group.records {
			if err := agg.Add(record.Value); err != nil {
				return 0, err
			}
		}
		value, err := agg.Result()
		if err != nil {
			return 0, err
		}
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return 0, ErrCorruptStorage
		}
		record := Record{
			Key:            key,
			Series:         series,
			Timestamp:      start,
			Value:          []byte(strconv.FormatFloat(value, 'g', -1, 64)),
			Version:        baseVersion + uint64(len(writes)) + 1,
			Kind:           recordKindAggregate,
			BucketStart:    start,
			BucketWidth:    width,
			Aggregator:     aggregator,
			AggregateValue: value,
			SourceVersion:  group.sourceVersion,
		}
		writes = append(writes, record)
	}
	if len(writes) == 0 {
		return s.state.version, nil
	}
	if err := s.commitBatch(writes); err != nil {
		return 0, err
	}
	return s.state.version, nil
}

// Aggregates reads fixed-width aggregates for [start, end). Buckets that
// overlap the half-open range are returned in bucket-start order; buckets
// with no observations have Count and Value set to zero.
func (s *Snapshot) Aggregates(series, aggregator string, width, start, end int64) ([]BucketAggregate, error) {
	if width <= 0 {
		return nil, ErrInvalidBucketWidth
	}
	if start > end {
		return nil, ErrInvalidRange
	}
	if _, ok := aggregatorFactory(aggregator); !ok {
		return nil, ErrUnknownAggregator
	}
	if start == end {
		return nil, nil
	}
	first := bucketStart(start, width)
	last := bucketStart(end-1, width)
	byStart := make(map[int64]BucketAggregate)
	for _, records := range s.state.versions {
		record, visible := visibleAt(records, s.version)
		if !visible || record.Kind != recordKindAggregate ||
			record.Series != series || record.Aggregator != aggregator || record.BucketWidth != width {
			continue
		}
		if record.BucketStart < first || record.BucketStart > last {
			continue
		}
		byStart[record.BucketStart] = BucketAggregate{
			Series:        series,
			Bucket:        Bucket{Start: record.BucketStart, Width: width},
			Value:         record.AggregateValue,
			Count:         countVisibleInBucket(s.state, s.version, series, record.BucketStart, width),
			SourceVersion: record.SourceVersion,
		}
	}
	result := make([]BucketAggregate, 0, (last-first)/width+1)
	for bucket := first; ; bucket += width {
		aggregate, computed := byStart[bucket]
		if !computed {
			aggregate = BucketAggregate{
				Series: series,
				Bucket: Bucket{Start: bucket, Width: width},
			}
		}
		result = append(result, aggregate)
		if bucket == last {
			return result, nil
		}
		if bucket > math.MaxInt64-width {
			return result, nil
		}
	}
}

func countVisibleInBucket(state *immutableState, version uint64, series string, start, width int64) int {
	count := 0
	for _, records := range state.versions {
		record, visible := visibleAt(records, version)
		if visible && record.Series == series && record.Kind == recordKindObservation &&
			record.Timestamp >= start && record.Timestamp < start+width {
			count++
		}
	}
	return count
}
