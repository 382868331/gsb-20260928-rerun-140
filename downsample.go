package timeseries

import "sort"

// Aggregator turns the observations of one fixed-width bucket into one
// aggregate value. The argument is nil for an empty bucket, so callers can
// distinguish a bucket with no points if they want to.
type Aggregator func(points []Observation) []byte

// Aggregate is one versioned value of a downsampling bucket.
type Aggregate struct {
	Series      string
	BucketStart int64
	Value       []byte
}

// RegisterAggregator binds a name to a bucket width and callback. The callback
// is used by Downsample and by DeleteBefore when a bucket straddles the expiry
// cutoff. Registration is local to this open store and must be repeated after
// reopening; aggregate data itself is durable.
func (s *Store) RegisterAggregator(name string, width int64, fn Aggregator) error {
	if name == "" || width <= 0 || fn == nil {
		return ErrInvalidAggregator
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.aggregators[name] = registeredAggregator{width: width, fn: fn}
	return nil
}

// Downsample recomputes aggregate versions for every width-aligned bucket that
// intersects [start, end) and has changed since its stored aggregate version.
// Empty buckets are evaluated once too, so gaps carry explicit values. The
// aggregator is invoked at most once per changed bucket; reruns after a crash
// or reopen skip buckets whose inputs are unchanged and never re-invoke it.
func (s *Store) Downsample(name, series string, start, end int64) (uint64, error) {
	if start > end {
		return 0, ErrInvalidRange
	}
	s.mu.Lock()
	aggregator, ok := s.aggregators[name]
	if !ok {
		s.mu.Unlock()
		return 0, ErrAggregatorNotRegistered
	}
	state := s.state
	version := state.version
	width := aggregator.width
	firstBucket := bucketFloor(start, width)
	lastStart := bucketFloor(end-1, width)
	var batch []Record
	for bucketStart := firstBucket; bucketStart <= lastStart; bucketStart += width {
		points, sourceVersion := bucketInputs(state, series, bucketStart, bucketStart+width, version)
		key := makeAggregateKey(name, series, bucketStart)
		if records := state.versions[key]; len(records) > 0 {
			latest := records[len(records)-1]
			if latest.SourceVersion == sourceVersion {
				continue
			}
		}
		value := aggregator.fn(points)
		batch = append(batch, Record{
			Key:           key,
			Series:        series,
			Timestamp:     bucketStart,
			Value:         append([]byte(nil), value...),
			Version:       version + uint64(len(batch)) + 1,
			IsAggregate:   true,
			Name:          name,
			Width:         width,
			SourceVersion: sourceVersion,
		})
	}
	if len(batch) == 0 {
		s.mu.Unlock()
		return version, nil
	}
	if err := s.commitBatch(batch); err != nil {
		s.mu.Unlock()
		return 0, err
	}
	resultVersion := s.state.version
	s.mu.Unlock()
	return resultVersion, nil
}

// bucketInputs returns the observations visible at version inside
// [bucketStart, bucketEnd) plus the maximum MVCC version that affected them.
// Deleted keys still contribute their tombstone version so expiry is visible
// as a source change and forces a recomputation.
func bucketInputs(state *immutableState, series string, bucketStart, bucketEnd int64, version uint64) ([]Observation, uint64) {
	var points []Observation
	var sourceVersion uint64
	for _, records := range state.versions {
		for _, record := range records {
			if record.Version > version || record.IsAggregate || record.Series != series {
				continue
			}
			if record.Timestamp < bucketStart || record.Timestamp >= bucketEnd {
				continue
			}
			if record.Version > sourceVersion {
				sourceVersion = record.Version
			}
		}
		idx := sort.Search(len(records), func(i int) bool {
			return records[i].Version > version
		})
		if idx == 0 {
			continue
		}
		record := records[idx-1]
		if record.IsAggregate || record.Series != series || record.Deleted {
			continue
		}
		if record.Timestamp < bucketStart || record.Timestamp >= bucketEnd {
			continue
		}
		points = append(points, Observation{
			Series:    record.Series,
			Timestamp: record.Timestamp,
			Data:      append([]byte(nil), record.Value...),
		})
	}
	sort.Slice(points, func(i, j int) bool { return points[i].Timestamp < points[j].Timestamp })
	return points, sourceVersion
}

func bucketFloor(timestamp, width int64) int64 {
	if timestamp >= 0 {
		return timestamp - timestamp%width
	}
	return timestamp - (timestamp%width+width)%width
}
