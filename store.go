package timeseries

import (
	"fmt"
	"os"
	"sort"
	"sync"
)

// Observation is one timestamped value in a named series. Series and timestamp
// together identify the observation; revisions replace its value, not its key.
type Observation struct {
	Series    string
	Timestamp int64
	Data      []byte
}

// Record is one immutable version of an observation.
type Record struct {
	Key       string
	Series    string
	Timestamp int64
	Value     []byte
	Version   uint64
	Deleted   bool
	// Aggregate records version a fixed-width downsampling bucket. They share
	// the MVCC key space with raw observations but use a distinct key prefix.
	IsAggregate   bool
	Name          string
	Width         int64
	SourceVersion uint64
}

type observationKey struct {
	series    string
	timestamp int64
}

func makeKey(series string, timestamp int64) string {
	return fmt.Sprintf("%s\x00%d", series, timestamp)
}

func makeAggregateKey(name, series string, bucketStart int64) string {
	return fmt.Sprintf("\x01agg\x00%s\x00%s\x00%d", name, series, bucketStart)
}

// registeredAggregator remembers the user callback so expiry can recompute
// buckets straddling the cutoff without asking the caller again.
type registeredAggregator struct {
	width int64
	fn    func([]Observation) []byte
}

type immutableState struct {
	version uint64
	// versions stores MVCC versions in ascending Version order for each key.
	versions map[string][]Record
}

func (s *immutableState) clone() *immutableState {
	next := &immutableState{version: s.version, versions: make(map[string][]Record, len(s.versions)+1)}
	for key, records := range s.versions {
		next.versions[key] = records
	}
	return next
}

func visibleAt(records []Record, version uint64) (Record, bool) {
	idx := sort.Search(len(records), func(i int) bool {
		return records[i].Version > version
	})
	if idx == 0 {
		return Record{}, false
	}
	record := records[idx-1]
	return record, !record.Deleted
}

// Store is an embedded, durable time-series store. Its zero value is not
// usable; open it with Open.
type Store struct {
	mu          sync.Mutex
	path        string
	state       *immutableState
	wal         *walFile
	segments    []segmentMeta
	nextSeg     uint64
	pending     []Record
	refs        *snapshotRefs
	deleting    bool
	waiters     []chan struct{}
	compactHook func(stage string)
	aggregators map[string]registeredAggregator
}

type Options struct {
	// DisableSync makes tests faster while still exercising recovery code.
	DisableSync bool
}

// Open loads committed segments and the write-ahead log from dir.
func Open(dir string, opts *Options) (*Store, error) {
	if opts == nil {
		opts = &Options{}
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	manifest, err := readManifest(dir)
	if err != nil {
		return nil, err
	}
	state := &immutableState{versions: make(map[string][]Record)}
	var ordered []Record
	for _, segment := range manifest.Segments {
		records, err := readSegmentFile(dir, segment.ID)
		if err != nil {
			return nil, err
		}
		ordered = append(ordered, records...)
	}
	walRecords, err := readWAL(dir)
	if err != nil {
		return nil, err
	}
	committedNewSegment := false
	if len(manifest.Segments) > 0 && len(walRecords) > 0 {
		if manifest.Segments[0].Version >= walRecords[len(walRecords)-1].Version {
			committedNewSegment = true
		}
	}
	ordered = append(ordered, walRecords...)
	if err := applyRecords(state, ordered); err != nil {
		return nil, err
	}
	wal, err := openWAL(dir, opts.DisableSync)
	if err != nil {
		return nil, err
	}
	if committedNewSegment {
		temporary := walPath(dir) + ".new"
		file, err := os.OpenFile(temporary, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0o600)
		if err != nil {
			return nil, err
		}
		if err := file.Close(); err != nil {
			return nil, err
		}
		if err := os.Rename(temporary, walPath(dir)); err != nil {
			return nil, err
		}
		if err := wal.Close(); err != nil {
			return nil, err
		}
		wal, err = openWAL(dir, opts.DisableSync)
		if err != nil {
			return nil, err
		}
		walRecords = nil
	}
	nextSegment := uint64(1)
	for _, segment := range manifest.Segments {
		if segment.ID >= nextSegment {
			nextSegment = segment.ID + 1
		}
	}
	return &Store{
		path:        dir,
		state:       state,
		wal:         wal,
		segments:    manifest.Segments,
		nextSeg:     nextSegment,
		pending:     append([]Record(nil), walRecords...),
		refs:        newSnapshotRefs(),
		aggregators: make(map[string]registeredAggregator),
	}, nil
}

// Close releases the WAL. Data already acknowledged by Append, Revise, or
// DeleteBefore remains durable.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.wal.Close()
}

// Version returns the current monotonically increasing data version.
func (s *Store) Version() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state.version
}

// Append adds a new observation. An observation is identified by series plus
// timestamp; appending that key twice returns ErrObservationExists.
func (s *Store) Append(observation Observation) (uint64, error) {
	if observation.Series == "" {
		return 0, ErrEmptySeries
	}
	key := makeKey(observation.Series, observation.Timestamp)
	s.mu.Lock()
	defer s.mu.Unlock()
	if records := s.state.versions[key]; len(records) > 0 && !records[len(records)-1].Deleted {
		return 0, ErrObservationExists
	}
	record := Record{
		Key:       key,
		Series:    observation.Series,
		Timestamp: observation.Timestamp,
		Value:     append([]byte(nil), observation.Data...),
		Version:   s.state.version + 1,
	}
	if err := s.commit(record); err != nil {
		return 0, err
	}
	return record.Version, nil
}

// Revise replaces the value of an existing observation. A late revision never
// changes its timestamp or range membership.
func (s *Store) Revise(observation Observation) (uint64, error) {
	if observation.Series == "" {
		return 0, ErrEmptySeries
	}
	key := makeKey(observation.Series, observation.Timestamp)
	s.mu.Lock()
	defer s.mu.Unlock()
	records := s.state.versions[key]
	if len(records) == 0 || records[len(records)-1].Deleted {
		return 0, ErrObservationNotFound
	}
	record := Record{
		Key:       key,
		Series:    observation.Series,
		Timestamp: observation.Timestamp,
		Value:     append([]byte(nil), observation.Data...),
		Version:   s.state.version + 1,
	}
	if err := s.commit(record); err != nil {
		return 0, err
	}
	return record.Version, nil
}

// DeleteBefore removes observations with timestamps earlier than before. The
// deletion is versioned, so earlier explicit snapshots remain readable.
func (s *Store) DeleteBefore(before int64) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	next := s.state.clone()
	var toDelete []Record
	for key, records := range s.state.versions {
		if len(records) == 0 {
			continue
		}
		latest := records[len(records)-1]
		if latest.IsAggregate || latest.Deleted || latest.Timestamp >= before {
			continue
		}
		record := Record{
			Key:       key,
			Series:    latest.Series,
			Timestamp: latest.Timestamp,
			Version:   next.version + uint64(len(toDelete)) + 1,
			Deleted:   true,
		}
		next.versions[key] = append(append([]Record(nil), records...), record)
		next.version = record.Version
		toDelete = append(toDelete, record)
	}
	if len(toDelete) == 0 {
		return s.state.version, nil
	}
	sort.Slice(toDelete, func(i, j int) bool { return toDelete[i].Version < toDelete[j].Version })
	cascade, err := s.cascadeAggregationsLocked(next, toDelete, before)
	if err != nil {
		return 0, err
	}
	batch := append(toDelete, cascade...)
	sort.Slice(batch, func(i, j int) bool { return batch[i].Version < batch[j].Version })
	if err := s.commitBatch(batch); err != nil {
		return 0, err
	}
	return s.state.version, nil
}

// cascadeAggregationsLocked keeps downsampled buckets explainable after an
// expiry: a bucket fully before the cutoff receives a versioned tombstone, and
// a bucket straddling the cutoff is recomputed from the surviving points. Raw
// evidence is only made invisible inside the same MVCC version, never removed
// physically here; compaction waits for in-flight readers.
func (s *Store) cascadeAggregationsLocked(next *immutableState, rawTombstones []Record, before int64) ([]Record, error) {
	baseVersion := rawTombstones[len(rawTombstones)-1].Version
	var cascade []Record
	for key, records := range next.versions {
		if len(records) == 0 {
			continue
		}
		latest := records[len(records)-1]
		if !latest.IsAggregate {
			continue
		}
		bucketEnd := latest.Timestamp + latest.Width
		if bucketEnd <= before {
			if latest.Deleted {
				continue
			}
			cascade = append(cascade, Record{
				Key:           key,
				Series:        latest.Series,
				Timestamp:     latest.Timestamp,
				Version:       baseVersion + uint64(len(cascade)) + 1,
				Deleted:       true,
				IsAggregate:   true,
				Name:          latest.Name,
				Width:         latest.Width,
				SourceVersion: next.version,
			})
			continue
		}
		if latest.Timestamp >= before {
			continue
		}
		aggregator, registered := s.aggregators[latest.Name]
		if !registered || aggregator.width != latest.Width {
			// Without the callback we cannot recompute the partial bucket. The
			// raw evidence stays versioned, so reregistering and running
			// Downsample later still reconstructs the answer.
			continue
		}
		points, sourceVersion := bucketInputs(next, latest.Series, latest.Timestamp, bucketEnd, next.version)
		if sourceVersion == latest.SourceVersion {
			continue
		}
		value := aggregator.fn(points)
		cascade = append(cascade, Record{
			Key:           key,
			Series:        latest.Series,
			Timestamp:     latest.Timestamp,
			Value:         append([]byte(nil), value...),
			Version:       baseVersion + uint64(len(cascade)) + 1,
			IsAggregate:   true,
			Name:          latest.Name,
			Width:         latest.Width,
			SourceVersion: sourceVersion,
		})
	}
	for i := range cascade {
		next.version = cascade[i].Version
		next.versions[cascade[i].Key] = append(
			append([]Record(nil), next.versions[cascade[i].Key]...), cascade[i])
	}
	return cascade, nil
}

func (s *Store) commit(record Record) error {
	return s.commitBatch([]Record{record})
}

// commitBatch applies a contiguous set of records already assigned consecutive
// versions under the store lock and makes them durable as one WAL batch.
func (s *Store) commitBatch(records []Record) error {
	next := s.state.clone()
	for _, record := range records {
		next.version = record.Version
		next.versions[record.Key] = append(append([]Record(nil), s.state.versions[record.Key]...), record)
	}
	if err := s.wal.appendBatch(records); err != nil {
		return err
	}
	s.state = next
	s.pending = append(s.pending, records...)
	return nil
}

func applyRecords(state *immutableState, records []Record) error {
	seen := make(map[uint64]string)
	sorted := append([]Record(nil), records...)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].Version < sorted[j].Version })
	for _, record := range sorted {
		if record.Version == 0 {
			return ErrCorruptStorage
		}
		previous, duplicate := seen[record.Version]
		if duplicate && previous != record.Key {
			return ErrCorruptStorage
		}
		if duplicate {
			continue
		}
		seen[record.Version] = record.Key
		state.version = record.Version
		state.versions[record.Key] = append(state.versions[record.Key], record)
	}
	return nil
}

// Range returns visible values with start <= timestamp < end for one series.
// End is exclusive, which keeps adjacent ranges disjoint for out-of-order data.
func (s *Store) Range(series string, start, end int64) ([]Observation, uint64, error) {
	if start > end {
		return nil, 0, ErrInvalidRange
	}
	snapshot := s.Snapshot()
	defer snapshot.Close()
	return snapshot.Range(series, start, end), snapshot.Version(), nil
}

// Aggregates returns currently visible downsampled buckets for the named view.
func (s *Store) Aggregates(name, series string, start, end int64) ([]Aggregate, uint64, error) {
	if start > end {
		return nil, 0, ErrInvalidRange
	}
	snapshot := s.Snapshot()
	defer snapshot.Close()
	return snapshot.Aggregates(name, series, start, end), snapshot.Version(), nil
}
