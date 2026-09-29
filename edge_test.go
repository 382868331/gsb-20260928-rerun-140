package tsstore

import "testing"

func TestCompactEmptyAndRecreate(t *testing.T) {
	s, _ := Open(t.TempDir(), Options{FlushRecords: 4})
	if err := s.Compact(); err != nil {
		t.Fatalf("compact empty: %v", err)
	}
	s.Append("s", 1, 1)
	s.ExpireBefore("s", 100)
	// Cell is expired; Append must be allowed to create it again.
	if err := s.Append("s", 1, 2); err != nil {
		t.Fatalf("append after expire: %v", err)
	}
	if err := s.Compact(); err != nil {
		t.Fatalf("compact: %v", err)
	}
	got, _ := s.Range("s", 0, 10)
	assertObs(t, got, []Observation{{1, 2}})
}

func TestRangeEdgeIntervals(t *testing.T) {
	s, _ := Open(t.TempDir(), Options{FlushRecords: 100})
	s.Append("s", 10, 1)
	s.Append("s", 20, 2)
	got, _ := s.Range("s", 10, 20) // half-open: 10 only
	assertObs(t, got, []Observation{{10, 1}})
	if v, _ := s.Range("s", 20, 20); len(v) != 0 {
		t.Fatalf("empty interval returned %v", v)
	}
	if v, _ := s.Range("other", 0, 100); len(v) != 0 {
		t.Fatalf("unknown series returned %v", v)
	}
	b, _ := s.Bounds("missing")
	if b.HasData {
		t.Fatalf("missing series reported data")
	}
}

func TestSnapshotVersionPruningAcrossCompaction(t *testing.T) {
	s, _ := Open(t.TempDir(), Options{FlushRecords: 2})
	s.Append("s", 1, 1)
	s.Append("s", 2, 2)
	snap1, _ := s.Snapshot() // version 2
	s.Revise("s", 1, 10)     // version 3
	s.Flush()
	s.Compact()

	// snap1 reads version 2 even though compaction pruned old revisions.
	got, _ := snap1.Range("s", 0, 100)
	assertObs(t, got, []Observation{{1, 1}, {2, 2}})
	snap1.Close()

	live, _ := s.Range("s", 0, 100)
	assertObs(t, live, []Observation{{1, 10}, {2, 2}})
}
