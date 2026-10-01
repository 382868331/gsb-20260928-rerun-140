package timeseries

import (
	"fmt"
	"os"
	"sort"
	"strconv"
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
	Key            string
	Series         string
	Timestamp      int64
	Value          []byte
	Version        uint64
	Deleted        bool
	Kind           byte
	BucketStart    int64
	BucketWidth    int64
	Aggregator     string
	AggregateValue float64
	SourceVersion  uint64
}

type observationKey struct {
	series    string
	timestamp int64
}

func makeKey(series string, timestamp int64) string {
	return fmt.Sprintf("%s\x00%d", series, timestamp)
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
		path:     dir,
		state:    state,
		wal:      wal,
		segments: manifest.Segments,
		nextSeg:  nextSegment,
		pending:  append([]Record(nil), walRecords...),
		refs:     newSnapshotRefs(),
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
	var rawTombstones []Record
	for key, records := range s.state.versions {
		if len(records) == 0 {
			continue
		}
		latest := records[len(records)-1]
		if latest.Kind != recordKindObservation || latest.Deleted || latest.Timestamp >= before {
			continue
		}
		record := Record{
			Key:       key,
			Series:    latest.Series,
			Timestamp: latest.Timestamp,
			Kind:      recordKindObservation,
			Deleted:   true,
		}
		rawTombstones = append(rawTombstones, record)
	}
	if len(rawTombstones) == 0 {
		return s.state.version, nil
	}
	sort.Slice(rawTombstones, func(i, j int) bool {
		if rawTombstones[i].Series != rawTombstones[j].Series {
			return rawTombstones[i].Series < rawTombstones[j].Series
		}
		return rawTombstones[i].Timestamp < rawTombstones[j].Timestamp
	})

	// Build the post-deletion view in a cloned state so affected aggregates can
	// be recomputed from the exact records that remain visible. Raw tombstones
	// and aggregate revisions share one WAL batch: an interrupted cleanup can
	// never leave aggregates without the evidence that explains them.
	next := s.state.clone()
	var batch []Record
	version := s.state.version
	for index, record := range rawTombstones {
		version = version + uint64(index) + 1
		record.Version = version
		next.versions[record.Key] = append(append([]Record(nil), next.versions[record.Key]...), record)
		batch = append(batch, record)
	}
	next.version = version

	aggregateChanges, err := s.affectedAggregateDeletes(next, before, version)
	if err != nil {
		return 0, err
	}
	for _, record := range aggregateChanges {
		next.version = record.Version
		next.versions[record.Key] = append(append([]Record(nil), next.versions[record.Key]...), record)
		batch = append(batch, record)
	}
	if err := s.commitBatch(batch); err != nil {
		return 0, err
	}
	return s.state.version, nil
}

func (s *Store) affectedAggregateDeletes(state *immutableState, before int64, version uint64) ([]Record, error) {
	type aggregateIdentity struct {
		series     string
		aggregator string
		width      int64
		start      int64
	}
	affected := map[aggregateIdentity]struct{}{}
	for _, records := range state.versions {
		latest := records[len(records)-1]
		if latest.Kind != recordKindAggregate || latest.Timestamp >= before {
			continue
		}
		affected[aggregateIdentity{latest.Series, latest.Aggregator, latest.BucketWidth, latest.Timestamp}] = struct{}{}
	}

	identities := make([]aggregateIdentity, 0, len(affected))
	for identity := range affected {
		identities = append(identities, identity)
	}
	sort.Slice(identities, func(i, j int) bool {
		if identities[i].series != identities[j].series {
			return identities[i].series < identities[j].series
		}
		if identities[i].aggregator != identities[j].aggregator {
			return identities[i].aggregator < identities[j].aggregator
		}
		if identities[i].width != identities[j].width {
			return identities[i].width < identities[j].width
		}
		return identities[i].start < identities[j].start
	})

	var changes []Record
	for _, identity := range identities {
		key := aggregateKey(identity.series, identity.aggregator, identity.width, identity.start)
		latest := state.versions[key][len(state.versions[key])-1]
		if latest.Deleted {
			continue
		}
		var remaining []Record
		var sourceVersion uint64
		for _, records := range state.versions {
			record, visible := visibleAt(records, version)
			if !visible || record.Kind != recordKindObservation || record.Series != identity.series ||
				record.Timestamp < identity.start || record.Timestamp >= identity.start+identity.width {
				continue
			}
			remaining = append(remaining, record)
			if record.Version > sourceVersion {
				sourceVersion = record.Version
			}
		}
		version = version + 1
		record := Record{
			Key:         key,
			Series:      identity.series,
			Timestamp:   identity.start,
			Kind:        recordKindAggregate,
			BucketStart: identity.start,
			BucketWidth: identity.width,
			Aggregator:  identity.aggregator,
			Version:     version,
		}
		if len(remaining) == 0 {
			record.Kind = recordKindTombstone
			record.Deleted = true
		} else {
			factory, ok := aggregatorFactory(identity.aggregator)
			if !ok {
				return nil, ErrUnknownAggregator
			}
			aggregation := factory()
			sort.Slice(remaining, func(i, j int) bool { return remaining[i].Timestamp < remaining[j].Timestamp })
			for _, remainingRecord := range remaining {
				if err := aggregation.Add(remainingRecord.Value); err != nil {
					return nil, err
				}
			}
			value, err := aggregation.Result()
			if err != nil {
				return nil, err
			}
			record.Value = []byte(strconv.FormatFloat(value, 'g', -1, 64))
			record.AggregateValue = value
			record.SourceVersion = sourceVersion
		}
		changes = append(changes, record)
	}
	return changes, nil
}

func (s *Store) commit(record Record) error {
	return s.commitBatch([]Record{record})
}

func (s *Store) commitBatch(records []Record) error {
	if len(records) == 0 {
		return nil
	}
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
		if duplicate {
			if previous != record.Key {
				return ErrCorruptStorage
			}
			continue
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
