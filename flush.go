package tsstore

import (
	"os"
)

// Flush forces the current memtable into a segment file.
func (s *Store) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	_, _, err := s.flushActiveLocked(true)
	return err
}

// flushActiveLocked writes the active memtable into a segment. When rotate
// is true it also rotates the WAL (normal flush); when false the segment is
// created as an uncommitted compaction input and the WAL/memtable are left
// untouched for concurrent writers.
func (s *Store) flushActiveLocked(rotate bool) (*segment, []entryWithSeries, error) {
	if s.active.count() == 0 {
		return nil, nil, nil
	}
	id := s.nextSegID
	s.nextSegID++
	path := s.segPath(id)
	seg, raw := segmentFromMemtable(id, path, s.active.clone())
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, encodeSegment(raw), 0o644); err != nil {
		return nil, nil, err
	}
	if err := syncFile(tmp); err != nil {
		return nil, nil, err
	}
	if err := os.Rename(tmp, path); err != nil {
		return nil, nil, err
	}
	if err := syncDir(s.dir); err != nil {
		return nil, nil, err
	}
	if !rotate {
		return seg, raw, nil
	}
	s.segments = append(s.segments, seg)
	s.segStates[id] = &segmentState{ID: id}

	// The segment is durable: rotate the WAL so replayed records are never
	// applied twice. First persist the manifest referencing the new segment.
	if err := s.persistManifest(nil); err != nil {
		return nil, nil, err
	}
	if err := s.rotateWALLocked(); err != nil {
		return nil, nil, err
	}
	s.active = newMemtable()
	return nil, nil, nil
}

func (s *Store) writeFileLocked(path string, buf []byte) error {
	if err := os.WriteFile(path, buf, 0o644); err != nil {
		return err
	}
	return syncFile(path)
}

func (s *Store) renameFileLocked(from, to string) error {
	if err := os.Rename(from, to); err != nil {
		return err
	}
	return syncDir(s.dir)
}

// rotateWALLocked atomically replaces the WAL with an empty one.
func (s *Store) rotateWALLocked() error {
	walNew := s.walPath + ".new"
	nf, err := os.OpenFile(walNew, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if err := nf.Sync(); err != nil {
		nf.Close()
		return err
	}
	if err := nf.Close(); err != nil {
		return err
	}
	if err := s.wal.Close(); err != nil {
		return err
	}
	if err := os.Rename(walNew, s.walPath); err != nil {
		return err
	}
	if err := syncDir(s.dir); err != nil {
		return err
	}
	f, err := os.OpenFile(s.walPath, os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	s.wal = f
	return nil
}
