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
		if !ok || record.Series != series || record.Timestamp < start || record.Timestamp >= end {
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
