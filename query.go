package tsstore

import "sort"

// Range returns observations with start <= timestamp < end from the latest
// committed state, ordered by timestamp. A nil/empty series or an empty
// interval returns nil. Range scans each segment and the active memtable.
func (s *Store) Range(series string, start, end int64) ([]Observation, error) {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return nil, ErrClosed
	}
	asOf := s.maxVer
	mem := s.active.clone()
	segs := make([]*segment, len(s.segments))
	copy(segs, s.segments)
	s.mu.RUnlock()
	return rangeRead(series, start, end, asOf, mem, segs), nil
}

// Range is the snapshot-scoped variant.
func (snap *Snapshot) Range(series string, start, end int64) ([]Observation, error) {
	snap.mu.Lock()
	if snap.closed {
		snap.mu.Unlock()
		return nil, ErrClosed
	}
	mem := snap.mem
	segs := snap.segs
	asOf := snap.asOf
	snap.mu.Unlock()
	return rangeRead(series, start, end, asOf, mem, segs), nil
}

func rangeRead(series string, start, end, asOf int64, mem memtable, segs []*segment) []Observation {
	if start >= end {
		return nil
	}
	// ts -> merged versions
	byTS := map[int64][]entry{}
	add := func(vers []entry) {
		if len(vers) == 0 {
			return
		}
		ts := vers[0].ts
		cp := make([]entry, len(vers))
		copy(cp, vers)
		byTS[ts] = append(byTS[ts], cp...)
	}
	if sm, ok := mem[series]; ok {
		for ts, vers := range sm {
			if ts >= start && ts < end {
				add(vers)
			}
		}
	}
	for _, seg := range segs {
		if lo, ok := seg.mins[series]; ok {
			if lo >= end || seg.maxs[series] < start {
				continue
			}
		}
		sm := seg.entries[series]
		for ts, vers := range sm {
			if ts >= start && ts < end {
				add(vers)
			}
		}
	}
	var out []Observation
	for ts, vers := range byTS {
		sort.Slice(vers, func(i, j int) bool { return vers[i].version < vers[j].version })
		e := versionAt(vers, asOf)
		if e == nil || e.deleted {
			continue
		}
		out = append(out, Observation{Timestamp: ts, Value: e.value})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Timestamp < out[j].Timestamp })
	return out
}

// Bounds describes a series time range and its latest version.
type Bounds struct {
	Min, Max int64
	HasData  bool
	Version  int64
}

// Bounds returns the current time boundaries of live data in a series.
func (s *Store) Bounds(series string) (Bounds, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return Bounds{}, ErrClosed
	}
	b := boundsRead(series, s.maxVer, s.active, s.segments)
	b.Version = s.maxVer
	return b, nil
}

// Bounds returns the boundaries visible within a snapshot.
func (snap *Snapshot) Bounds(series string) (Bounds, error) {
	snap.mu.Lock()
	if snap.closed {
		snap.mu.Unlock()
		return Bounds{}, ErrClosed
	}
	mem, segs, asOf := snap.mem, snap.segs, snap.asOf
	snap.mu.Unlock()
	b := boundsRead(series, asOf, mem, segs)
	b.Version = asOf
	return b, nil
}

func boundsRead(series string, asOf int64, mem memtable, segs []*segment) Bounds {
	byTS := map[int64][]entry{}
	if sm, ok := mem[series]; ok {
		for ts, vers := range sm {
			cp := make([]entry, len(vers))
			copy(cp, vers)
			byTS[ts] = append(byTS[ts], cp...)
		}
	}
	for _, seg := range segs {
		for ts, vers := range seg.entries[series] {
			cp := make([]entry, len(vers))
			copy(cp, vers)
			byTS[ts] = append(byTS[ts], cp...)
		}
	}
	b := Bounds{}
	for ts, vers := range byTS {
		sort.Slice(vers, func(i, j int) bool { return vers[i].version < vers[j].version })
		e := versionAt(vers, asOf)
		if e == nil || e.deleted {
			continue
		}
		if !b.HasData {
			b.HasData = true
			b.Min, b.Max = ts, ts
		} else {
			if ts < b.Min {
				b.Min = ts
			}
			if ts > b.Max {
				b.Max = ts
			}
		}
	}
	return b
}
