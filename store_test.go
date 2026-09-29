package tsstore

import (
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"sync"
	"testing"
)

func TestConcurrentAppendSnapshotCompact(t *testing.T) {
	s, _ := Open(tempDir(t), Options{FlushRecords: 16})
	defer s.Close()

	// Seed 500 points first so readers always see a stable boundary set.
	for i := int64(0); i < 500; i++ {
		if err := s.Append("p", i, float64(i)); err != nil {
			t.Fatal(err)
		}
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := 0; j < 30; j++ {
			select {
			case <-stop:
				return
			default:
			}
			snap, err := s.Snapshot()
			if err != nil {
				return
			}
			got, _ := snap.Range("p", 0, 250)
			for k := 1; k < len(got); k++ {
				if got[k].Timestamp <= got[k-1].Timestamp {
					t.Errorf("snapshot range not ordered: %+v", got)
				}
			}
			if len(got) != 250 {
				t.Errorf("snapshot boundary drift: got %d points", len(got))
			}
			snap.Close()
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := 0; j < 5; j++ {
			select {
			case <-stop:
				return
			default:
			}
			if err := s.Compact(); err != nil {
				t.Errorf("compact: %v", err)
				return
			}
		}
	}()

	// Revisions overlap with readers and compactions.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			if err := s.Revise("p", int64(i%500), float64(1000+i)); err != nil {
				t.Errorf("revise: %v", err)
				close(stop)
				return
			}
		}
	}()
	wg.Wait()
	close(stop)

	got, err := s.Range("p", 0, 500)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 500 {
		t.Fatalf("expected 500 live points, got %d", len(got))
	}
}

func tempDir(t *testing.T) string {
	t.Helper()
	d := t.TempDir()
	return d
}

func TestAppendReviseConsistent(t *testing.T) {
	s, err := Open(tempDir(t), Options{FlushRecords: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	for i := int64(1); i <= 5; i++ {
		if err := s.Append("cpu", i, float64(i)); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	if err := s.Append("cpu", 3, 99); err != ErrAlreadyExists {
		t.Fatalf("duplicate append: %v", err)
	}
	if err := s.Revise("cpu", 3, 30); err != nil {
		t.Fatalf("revise: %v", err)
	}
	if err := s.Revise("cpu", 100, 1); err != ErrNotFound {
		t.Fatalf("revise missing: %v", err)
	}
	got, err := s.Range("cpu", 1, 6)
	if err != nil {
		t.Fatal(err)
	}
	want := []Observation{{1, 1}, {2, 2}, {3, 30}, {4, 4}, {5, 5}}
	assertObs(t, got, want)

	b, _ := s.Bounds("cpu")
	if !b.HasData || b.Min != 1 || b.Max != 5 {
		t.Fatalf("bounds = %+v", b)
	}
}

func TestOutOfOrderRangeBoundaries(t *testing.T) {
	s, _ := Open(tempDir(t), Options{FlushRecords: 3})
	defer s.Close()
	// Insert timestamps in shuffled order.
	ts := []int64{50, 10, 40, 20, 30}
	for _, ts := range ts {
		if err := s.Append("x", ts, float64(ts)); err != nil {
			t.Fatal(err)
		}
	}
	got, _ := s.Range("x", 20, 50) // [20,50)
	want := []Observation{{20, 20}, {30, 30}, {40, 40}}
	assertObs(t, got, want)

	got, _ = s.Range("x", 0, 100)
	if len(got) != 5 || got[0].Timestamp != 10 || got[4].Timestamp != 50 {
		t.Fatalf("full range unsorted: %+v", got)
	}
}

func TestSnapshotStableUnderMutation(t *testing.T) {
	s, _ := Open(tempDir(t), Options{FlushRecords: 2})
	defer s.Close()
	for i := int64(1); i <= 5; i++ {
		s.Append("s", i, float64(i))
	}
	snap, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if snap.Version() != 5 {
		t.Fatalf("snap version = %d", snap.Version())
	}

	// Mutate, expire, flush and compact while the snapshot stays open.
	s.Revise("s", 2, 200)
	s.Append("s", 6, 6)
	if _, err := s.ExpireBefore("s", 4); err != nil {
		t.Fatal(err)
	}
	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}

	got, _ := snap.Range("s", 0, 100)
	want := []Observation{{1, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 5}}
	assertObs(t, got, want)

	live, _ := s.Range("s", 0, 100)
	liveWant := []Observation{{4, 4}, {5, 5}, {6, 6}}
	assertObs(t, live, liveWant)
	if err := snap.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestExpireAndCompactPhysicalRemoval(t *testing.T) {
	dir := tempDir(t)
	s, _ := Open(dir, Options{FlushRecords: 3})
	for i := int64(1); i <= 6; i++ {
		s.Append("a", i, float64(i))
	}
	s.Flush()
	n, err := s.ExpireBefore("a", 4)
	if err != nil || n != 3 {
		t.Fatalf("expire n=%d err=%v", n, err)
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	got, _ := s.Range("a", 0, 100)
	assertObs(t, got, []Observation{{4, 4}, {5, 5}, {6, 6}})
	b, _ := s.Bounds("a")
	if b.Min != 4 || b.Max != 6 {
		t.Fatalf("bounds after expire: %+v", b)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Only one merged segment should remain.
	segs, _ := filepath.Glob(filepath.Join(dir, "*.seg"))
	if len(segs) != 1 {
		t.Fatalf("segments left: %v", segs)
	}

	s2, err := Open(dir, Options{FlushRecords: 3})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	got, _ = s2.Range("a", 0, 100)
	assertObs(t, got, []Observation{{4, 4}, {5, 5}, {6, 6}})
}

func TestRestartDurability(t *testing.T) {
	dir := tempDir(t)
	s, _ := Open(dir, Options{FlushRecords: 5})
	for i := int64(1); i <= 3; i++ {
		if err := s.Append("m", i*10, float64(i)); err != nil {
			t.Fatal(err)
		}
	}
	s.Revise("m", 20, 22)
	// Confirmed writes must survive even a hard "crash" without Close.

	s2, err := Open(dir, Options{FlushRecords: 5})
	if err != nil {
		t.Fatal(err)
	}
	got, _ := s2.Range("m", 0, 1000)
	assertObs(t, got, []Observation{{10, 1}, {20, 22}, {30, 3}})
	s2.Append("m", 40, 4)
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}

	s3, err := Open(dir, Options{FlushRecords: 5})
	if err != nil {
		t.Fatal(err)
	}
	defer s3.Close()
	got, _ = s3.Range("m", 0, 1000)
	assertObs(t, got, []Observation{{10, 1}, {20, 22}, {30, 3}, {40, 4}})
}

func TestCrossSegmentReferenceOrder(t *testing.T) {
	dir := tempDir(t)
	s, _ := Open(dir, Options{FlushRecords: 8})
	rng := rand.New(rand.NewPCG(1, 2))
	// Directly sorted reference model.
	ref := map[int64]float64{}
	const N = 500
	for i := 0; i < N; i++ {
		ts := int64(rng.IntN(1000))
		v := rng.Float64() * 100
		if _, exists := ref[ts]; exists {
			if err := s.Revise("r", ts, v); err != nil {
				t.Fatal(err)
			}
		} else {
			if err := s.Append("r", ts, v); err != nil {
				t.Fatal(err)
			}
		}
		ref[ts] = v
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	// New writes land in memtable/WAL after compaction.
	for i := 0; i < 50; i++ {
		ts := int64(2000 + rng.IntN(100))
		s.Append("r", ts, float64(ts))
		ref[ts] = float64(ts)
	}

	got, err := s.Range("r", 300, 1200)
	if err != nil {
		t.Fatal(err)
	}
	var want []Observation
	for ts, v := range ref {
		if ts >= 300 && ts < 1200 {
			want = append(want, Observation{ts, v})
		}
	}
	sort.Slice(want, func(i, j int) bool { return want[i].Timestamp < want[j].Timestamp })
	assertObs(t, got, want)

	// Restart and re-verify the cross-segment result.
	s.Close()
	s2, err := Open(dir, Options{FlushRecords: 8})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	got, _ = s2.Range("r", 300, 1200)
	assertObs(t, got, want)
}

// TestCompactionCrashChild is the subprocess target that "crashes" a
// compaction after the output segment is renamed but before commit.
func TestCompactionCrashChild(t *testing.T) {
	if os.Getenv("TS_COMPACT_CRASH_CHILD") != "1" {
		return
	}
	dir := os.Getenv("TS_DIR")
	s, err := Open(dir, Options{FlushRecords: 4})
	if err != nil {
		t.Fatal(err)
	}
	for i := int64(1); i <= 9; i++ {
		if err := s.Append("c", i, float64(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := s.Revise("c", 5, 50); err != nil {
		t.Fatal(err)
	}
	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}
	s.hookBeforeManifestCommit = func() { os.Exit(0) }
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
}

func TestCompactInterruptionRecovery(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "db")
	bin, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(bin, "-test.run=TestCompactionCrashChild")
	cmd.Env = append(os.Environ(), "TS_COMPACT_CRASH_CHILD=1", "TS_DIR="+dir)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("crash helper: %v\n%s", err, out)
	}
	// Both old segments and the renamed-but-uncommitted output are on disk.
	if segs, _ := filepath.Glob(filepath.Join(dir, "*.seg")); len(segs) < 3 {
		t.Fatalf("expected orphan + inputs after crash, got %v", segs)
	}
	s, err := Open(dir, Options{FlushRecords: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	got, _ := s.Range("c", 0, 100)
	want := []Observation{
		{1, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 50},
		{6, 6}, {7, 7}, {8, 8}, {9, 9},
	}
	assertObs(t, got, want)

	// A follow-up compaction must succeed and yield the same answer.
	if err := s.Compact(); err != nil {
		t.Fatalf("compact after recovery: %v", err)
	}
	got, _ = s.Range("c", 0, 100)
	assertObs(t, got, want)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if segs, _ := filepath.Glob(filepath.Join(dir, "*.seg")); len(segs) != 1 {
		t.Fatalf("expected single merged segment, got %v", segs)
	}
}

func assertObs(t *testing.T, got, want []Observation) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d obs %+v, want %d %+v", len(got), got, len(want), want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("idx %d: got %+v want %+v (full got %+v)", i, got[i], want[i], got)
		}
	}
}

func ExampleStore() {
	dir, _ := os.MkdirTemp("", "tsstore-")
	deleg := func() { _ = os.RemoveAll(dir) }
	_ = deleg
	s, err := Open(dir, Options{FlushRecords: 100})
	if err != nil {
		return
	}
	defer s.Close()
	_ = s.Append("cpu", 1000, 0.9)
	v, _ := s.Range("cpu", 0, 2000)
	fmt.Println(v[0].Value)
	// Output: 0.9
}
