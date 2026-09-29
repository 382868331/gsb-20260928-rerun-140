package timeseries

import (
	"os"
	"sort"
)

// Compact moves WAL revisions into an immutable segment and merges existing
// segments. Old files are deleted only after pre-compaction snapshots close.
func (s *Store) Compact() (uint64, error) {
	s.mu.Lock()
	if len(s.pending) == 0 && len(s.segments) <= 1 {
		version := s.state.version
		s.mu.Unlock()
		return version, nil
	}
	segmentID := s.nextSeg
	pending := append([]Record(nil), s.pending...)
	oldSegments := append([]segmentMeta(nil), s.segments...)
	version := s.state.version

	var all []Record
	for _, old := range oldSegments {
		records, err := readSegmentFile(s.path, old.ID)
		if err != nil {
			s.mu.Unlock()
			return 0, err
		}
		all = append(all, records...)
	}
	all = append(all, pending...)
	all = mergeRecords(all)
	if err := writeSegmentFile(s.path, segmentID, all); err != nil {
		s.mu.Unlock()
		return 0, err
	}
	manifest := manifestData{Segments: append([]segmentMeta{{ID: segmentID, Version: version}}, oldSegments...)}
	if err := writeManifest(s.path, manifest); err != nil {
		s.mu.Unlock()
		return 0, err
	}
	if s.compactHook != nil {
		hook := s.compactHook
		s.mu.Unlock()
		hook("manifest-written")
		return 0, errHookExit
	}
	newWAL, err := rotateWAL(s.path, s.wal, s.wal.disableSync)
	if err != nil {
		s.mu.Unlock()
		return 0, err
	}

	s.wal = newWAL
	s.pending = nil
	s.segments = manifest.Segments
	s.nextSeg = segmentID + 1
	s.deleting = true
	s.mu.Unlock()

	s.waitForSnapshots()

	s.mu.Lock()
	defer s.mu.Unlock()
	for _, old := range oldSegments {
		err := os.Remove(segmentPath(s.path, old.ID))
		if err != nil && !os.IsNotExist(err) {
			return 0, err
		}
	}
	s.segments = manifest.Segments[:1]
	if err := writeManifest(s.path, manifestData{Segments: s.segments}); err != nil {
		s.wakeWaitersLocked()
		return 0, err
	}
	s.wakeWaitersLocked()
	return version, nil
}

func mergeRecords(records []Record) []Record {
	byVersion := make(map[uint64]Record)
	for _, record := range records {
		if _, exists := byVersion[record.Version]; !exists {
			byVersion[record.Version] = record
		}
	}
	versions := make([]uint64, 0, len(byVersion))
	for version := range byVersion {
		versions = append(versions, version)
	}
	sort.Slice(versions, func(i, j int) bool { return versions[i] < versions[j] })
	byKey := make(map[string][]Record)
	for _, version := range versions {
		record := byVersion[version]
		byKey[record.Key] = append(byKey[record.Key], record)
	}
	merged := make([]Record, 0, len(records))
	for _, history := range byKey {
		if history[len(history)-1].Deleted {
			continue
		}
		merged = append(merged, history...)
	}
	sort.Slice(merged, func(i, j int) bool { return merged[i].Version < merged[j].Version })
	return merged
}
