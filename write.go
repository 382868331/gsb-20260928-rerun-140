package tsstore

import (
	"sort"
)

func (s *Store) flushLocked() error {
	_, _, err := s.flushActiveLocked(true)
	return err
}

// Append writes a new observation. It fails with ErrAlreadyExists if the
// timestamp already has a live value; use Revise to replace it.
func (s *Store) Append(series string, ts int64, value float64) error {
	return s.mutate(series, ts, value, true)
}

// Revise replaces a previously written observation. It fails with
// ErrNotFound if the timestamp has no live value.
func (s *Store) Revise(series string, ts int64, value float64) error {
	return s.mutate(series, ts, value, false)
}

func (s *Store) mutate(series string, ts int64, value float64, appendOnly bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	ver := s.maxVer + 1
	vers := s.cellVersionsLocked(series, ts)
	live := s.liveLocked(series, ts)
	kind := byte(walKindRevise)
	if appendOnly {
		if live != nil {
			return ErrAlreadyExists
		}
		kind = byte(walKindAppend)
	} else {
		if live == nil {
			return ErrNotFound
		}
	}
	_ = vers
	rec := encodeWAL(kind, ver, series, ts, value)
	if err := s.writeWALLocked(rec); err != nil {
		return err
	}
	s.active.apply(series, entry{version: ver, ts: ts, value: value})
	s.maxVer = ver
	if s.active.count() >= s.opt.FlushRecords {
		if err := s.flushLocked(); err != nil {
			return err
		}
	}
	return nil
}

// cellVersionsLocked returns merged version-ordered entries for a cell.
func (s *Store) cellVersionsLocked(series string, ts int64) []entry {
	var all []entry
	if v := s.active.cells(series, ts); len(v) > 0 {
		cp := make([]entry, len(v))
		copy(cp, v)
		all = append(all, cp...)
	}
	for _, seg := range s.segments {
		if v := seg.cells(series, ts); len(v) > 0 {
			cp := make([]entry, len(v))
			copy(cp, v)
			all = append(all, cp...)
		}
	}
	if len(all) <= 1 {
		return all
	}
	sort.Slice(all, func(i, j int) bool { return all[i].version < all[j].version })
	return all
}

// liveLocked returns the current live entry for a cell, or nil.
func (s *Store) liveLocked(series string, ts int64) *entry {
	vers := s.cellVersionsLocked(series, ts)
	e := versionAt(vers, s.maxVer)
	if e == nil || e.deleted {
		return nil
	}
	return e
}

func (s *Store) writeWALLocked(rec []byte) error {
	if _, err := s.wal.Write(rec); err != nil {
		return err
	}
	if err := s.wal.Sync(); err != nil {
		return err
	}
	return nil
}

// ExpireBefore tombstones every live observation in series with a timestamp
// strictly before before. Tombstones are WAL-durable; physical space is
// reclaimed by Compact after a Flush.
func (s *Store) ExpireBefore(series string, before int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, ErrClosed
	}
	type cell struct{ ts int64 }
	var targets []int64
	seen := map[int64]bool{}
	for ts := range s.active[series] {
		if ts < before {
			seen[ts] = true
			targets = append(targets, ts)
		}
	}
	for _, seg := range s.segments {
		for ts := range seg.entries[series] {
			if ts < before && !seen[ts] {
				seen[ts] = true
				targets = append(targets, ts)
			}
		}
	}
	sort.Slice(targets, func(i, j int) bool { return targets[i] < targets[j] })
	n := 0
	for _, ts := range targets {
		live := s.liveLocked(series, ts)
		if live == nil {
			continue
		}
		s.maxVer++
		rec := encodeWAL(walKindExpire, s.maxVer, series, ts, 0)
		if err := s.writeWALLocked(rec); err != nil {
			return n, err
		}
		s.active.apply(series, entry{version: s.maxVer, ts: ts, deleted: true})
		n++
	}
	if s.active.count() >= s.opt.FlushRecords {
		if err := s.flushLocked(); err != nil {
			return n, err
		}
	}
	return n, nil
}

// Close flushes metadata and releases files. Already-confirmed writes are
// durable regardless of Close, but Close also persists the manifest.
func (s *Store) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrClosed
	}
	s.closed = true
	err := s.persistManifest(nil)
	if werr := s.wal.Close(); err == nil {
		err = werr
	}
	s.mu.Unlock()
	return err
}
