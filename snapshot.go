package tsstore

import "sync"

// Snapshot is a consistent read-only view at one version.
type Snapshot struct {
	store  *Store
	asOf   int64
	mem    memtable
	segs   []*segment
	ref    *snapshotRef
	closed bool
	mu     sync.Mutex
}

// Snapshot returns a consistent view of all confirmed writes so far.
// Reads from the snapshot remain valid while the live store is mutated,
// expired, flushed, compacted, or closed.
func (s *Store) Snapshot() (*Snapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, ErrClosed
	}
	snap := &Snapshot{
		store: s,
		asOf:  s.maxVer,
		mem:   s.active.clone(),
		segs:  make([]*segment, len(s.segments)),
		ref:   &snapshotRef{asOf: s.maxVer},
	}
	copy(snap.segs, s.segments)
	s.snapMu.Lock()
	s.snapshots[snap.ref] = struct{}{}
	s.snapMu.Unlock()
	return snap, nil
}

// Version returns the version this snapshot reads at.
func (snap *Snapshot) Version() int64 { return snap.asOf }

// Close releases the snapshot. Segments needed only by this snapshot may be
// physically removed once it is closed.
func (snap *Snapshot) Close() error {
	snap.mu.Lock()
	if snap.closed {
		snap.mu.Unlock()
		return ErrClosed
	}
	snap.closed = true
	snap.mu.Unlock()

	st := snap.store
	st.snapMu.Lock()
	delete(st.snapshots, snap.ref)
	st.snapMu.Unlock()
	snap.mem = nil
	snap.segs = nil
	return nil
}

// minSnapshotVersion returns the smallest asOf of live snapshots, or -1.
func (s *Store) minSnapshotVersion() (int64, bool) {
	s.snapMu.Lock()
	defer s.snapMu.Unlock()
	if len(s.snapshots) == 0 {
		return 0, false
	}
	min := int64(0)
	for ref := range s.snapshots {
		if min == 0 || ref.asOf < min {
			min = ref.asOf
		}
	}
	return min, true
}
