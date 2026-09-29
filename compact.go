package tsstore

import (
	"os"
	"sort"
)

// Compact merges all segments into one, dropping versions that no existing
// snapshot can read and physically removing expired observations. It is
// crash-safe: an interrupted compaction leaves a valid, openable store.
func (s *Store) Compact() error {
	s.compactMu.Lock()
	defer s.compactMu.Unlock()

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrClosed
	}

	// Persist the active memtable as an extra, still uncommitted segment.
	// Its WAL is NOT rotated: on a crash before manifest commit, recovery
	// ignores the orphaned file and replays the WAL instead.
	activeSeg, _, err := s.flushActiveLocked(false)
	if err != nil {
		s.mu.Unlock()
		return err
	}
	inputs := make([]*segment, 0, len(s.segments)+1)
	inputs = append(inputs, s.segments...)
	if activeSeg != nil {
		inputs = append(inputs, activeSeg)
	}
	if len(inputs) == 0 {
		s.mu.Unlock()
		return nil
	}
	safe, hasSnap := s.minSnapshotVersion()
	id := s.nextSegID // one past any input, including the flushed active seg
	s.nextSegID++
	outPath := s.segPath(id)
	hook := s.hookBeforeManifestCommit

	merged := mergeEntries(inputs, safe, hasSnap)
	tmp := outPath + ".tmp"
	if err := s.writeFileLocked(tmp, encodeSegment(merged)); err != nil {
		s.mu.Unlock()
		return err
	}
	if err := s.renameFileLocked(tmp, outPath); err != nil {
		s.mu.Unlock()
		return err
	}

	if hook != nil {
		s.mu.Unlock()
		hook()
		// Test hook simulates a process crash; if it returns we re-lock.
		s.mu.Lock()
	}

	newSeg, err := loadSegment(outPath, id)
	if err != nil {
		s.mu.Unlock()
		return err
	}
	inputIDs := make(map[uint64]bool, len(inputs))
	for _, in := range inputs {
		inputIDs[in.id] = true
	}
	kept := make([]*segment, 0, len(s.segments)-len(inputs)+1)
	for _, seg := range s.segments {
		if !inputIDs[seg.id] {
			kept = append(kept, seg)
		}
	}
	kept = append(kept, newSeg)
	sort.Slice(kept, func(i, j int) bool { return kept[i].id < kept[j].id })
	s.segments = kept
	s.segStates[id] = &segmentState{ID: id}
	for _, in := range inputs {
		delete(s.segStates, in.id)
	}

	if err := s.persistManifest(nil); err != nil {
		s.mu.Unlock()
		return err
	}

	// All inputs (including the just-flushed active memtable) are now in the
	// merged segment: rotate the WAL and start a fresh memtable.
	if err := s.rotateWALLocked(); err != nil {
		s.mu.Unlock()
		return err
	}
	s.active = newMemtable()

	// Old snapshots keep in-memory pointers to the input segments, so their
	// files can be removed now without disturbing in-flight reads.
	for _, in := range inputs {
		_ = os.Remove(in.path)
	}
	if err := s.persistManifest(nil); err != nil {
		s.mu.Unlock()
		return err
	}
	s.mu.Unlock()
	return nil
}

// mergeEntries combines all cells across segments.
// Versions > safeVersion are always retained (needed by a live snapshot).
// At or below the safe version only the newest entry survives, and it is
// dropped entirely when it is a deletion.
func mergeEntries(segs []*segment, safe int64, hasSnap bool) []entryWithSeries {
	type key struct {
		series string
		ts     int64
	}
	all := map[key][]entry{}
	order := []key{}
	for _, seg := range segs {
		for series, sm := range seg.entries {
			for ts, vers := range sm {
				k := key{series, ts}
				if _, ok := all[k]; !ok {
					order = append(order, k)
				}
				cp := make([]entry, len(vers))
				copy(cp, vers)
				all[k] = append(all[k], cp...)
			}
		}
	}
	sort.Slice(order, func(i, j int) bool {
		if order[i].series != order[j].series {
			return order[i].series < order[j].series
		}
		return order[i].ts < order[j].ts
	})
	var out []entryWithSeries
	for _, k := range order {
		vers := all[k]
		sort.Slice(vers, func(i, j int) bool { return vers[i].version < vers[j].version })
		// Versions strictly below the oldest live snapshot can never be read
		// again: keep only the newest one (dropping it when deleted).
		cut := len(vers)
		if hasSnap {
			cut = sort.Search(len(vers), func(i int) bool { return vers[i].version >= safe })
		}
		if cut > 0 {
			e := vers[cut-1]
			if !e.deleted {
				out = append(out, entryWithSeries{series: k.series, entry: e})
			}
		}
		for _, e := range vers[cut:] {
			out = append(out, entryWithSeries{series: k.series, entry: e})
		}
	}
	return out
}
