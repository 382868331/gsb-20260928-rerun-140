package timeseries

import (
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
)

type snapshotRefs struct {
	mu    sync.Mutex
	count int
	zero  chan struct{}
}

func newSnapshotRefs() *snapshotRefs {
	return &snapshotRefs{}
}

func (r *snapshotRefs) zeroChan() chan struct{} {
	if r.zero == nil {
		ch := make(chan struct{})
		close(ch)
		return ch
	}
	return r.zero
}

func (r *snapshotRefs) add() {
	r.mu.Lock()
	r.count++
	if r.count == 1 {
		r.zero = make(chan struct{})
	}
	r.mu.Unlock()
}

func (r *snapshotRefs) remove() {
	r.mu.Lock()
	r.count--
	if r.count == 0 {
		close(r.zero)
	}
	r.mu.Unlock()
}

// Snapshot is an explicit, immutable read view. Close releases its pin.
type Snapshot struct {
	state    *immutableState
	refs     *snapshotRefs
	version  uint64
	released atomic.Bool
}

// Snapshot captures the latest committed data version.
func (s *Store) Snapshot() *Snapshot {
	s.mu.Lock()
	for s.deleting {
		waiter := make(chan struct{})
		s.waiters = append(s.waiters, waiter)
		s.mu.Unlock()
		<-waiter
		s.mu.Lock()
	}
	s.refs.add()
	state := s.state
	refs := s.refs
	s.mu.Unlock()
	return &Snapshot{state: state, refs: refs, version: state.version}
}

// SnapshotAt pins an explicit, already committed data version. Later backfill
// and corrections only create newer versions, so a delivered query view built
// on the returned snapshot keeps its original meaning even after new data is
// appended, downsampled again, or compacted away physically.
func (s *Store) SnapshotAt(version uint64) (*Snapshot, error) {
	s.mu.Lock()
	for s.deleting {
		waiter := make(chan struct{})
		s.waiters = append(s.waiters, waiter)
		s.mu.Unlock()
		<-waiter
		s.mu.Lock()
	}
	if version == 0 || version > s.state.version {
		s.mu.Unlock()
		return nil, ErrInvalidVersion
	}
	s.refs.add()
	state := s.state
	refs := s.refs
	s.mu.Unlock()
	return &Snapshot{state: state, refs: refs, version: version}, nil
}

// Version returns the snapshot's data version.
func (s *Snapshot) Version() uint64 { return s.version }

// Range reads from the immutable snapshot. Half-open boundaries are unaffected
// by insertion or revision order.
func (s *Snapshot) Range(series string, start, end int64) []Observation {
	if start > end {
		return nil
	}
	var result []Observation
	for _, records := range s.state.versions {
		record, ok := visibleAt(records, s.version)
		if !ok || record.IsAggregate || record.Series != series || record.Timestamp < start || record.Timestamp >= end {
			continue
		}
		result = append(result, Observation{
			Series:    record.Series,
			Timestamp: record.Timestamp,
			Data:      append([]byte(nil), record.Value...),
		})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Timestamp < result[j].Timestamp })
	return result
}

// Aggregates reads downsampled bucket versions visible to the snapshot for the
// named view. A bucket is returned when its start lies in [start, end); bucket
// ends stay half-open across boundaries, matching Range.
func (s *Snapshot) Aggregates(name, series string, start, end int64) []Aggregate {
	if start > end {
		return nil
	}
	var result []Aggregate
	for _, records := range s.state.versions {
		record, ok := visibleAt(records, s.version)
		if !ok || !record.IsAggregate || record.Name != name || record.Series != series {
			continue
		}
		if record.Timestamp < start || record.Timestamp >= end {
			continue
		}
		result = append(result, Aggregate{
			Series:      record.Series,
			BucketStart: record.Timestamp,
			Value:       append([]byte(nil), record.Value...),
			Count:       record.Count,
		})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].BucketStart < result[j].BucketStart })
	return result
}

// Close unpins the snapshot. It is safe to call more than once.
func (s *Snapshot) Close() {
	if s.released.CompareAndSwap(false, true) {
		s.refs.remove()
	}
}

func (s *Store) waitForSnapshots() {
	s.mu.Lock()
	refs := s.refs
	s.mu.Unlock()
	runtime.KeepAlive(refs)
	refs.mu.Lock()
	count := refs.count
	refs.mu.Unlock()
	if count == 0 {
		return
	}
	<-refs.zero
}

func (s *Store) wakeWaitersLocked() {
	waiters := s.waiters
	s.waiters = nil
	s.deleting = false
	for _, waiter := range waiters {
		close(waiter)
	}
}
