package tsstore

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

// Options configures a Store.
type Options struct {
	// FlushRecords flushes the memtable once it holds this many distinct cells.
	// Zero defaults to 1000.
	FlushRecords int
	// AutoCompact triggers compaction after each flush when true.
	AutoCompact bool
}

// debugFailCompactAfterRename, when non-empty, makes a compaction "crash"
// after the new segment is renamed but before the manifest commits. Test only.

type segmentState struct {
	ID      uint64 `json:"id"`
	Retired bool   `json:"retired"`
}

type manifestData struct {
	NextSegID  uint64         `json:"next_seg_id"`
	Segments   []segmentState `json:"segments"`
	MaxVersion int64          `json:"max_version"`
}

type snapshotRef struct {
	asOf int64
}

// Store is an embedded time-series store.
type Store struct {
	dir string
	opt Options

	mu        sync.RWMutex
	closed    bool
	maxVer    int64
	active    memtable
	segments  []*segment // ordered oldest -> newest
	segStates map[uint64]*segmentState
	nextSegID uint64
	wal       *os.File
	walPath   string

	snapMu    sync.Mutex
	snapshots map[*snapshotRef]struct{}

	compactMu sync.Mutex

	// test hooks
	hookBeforeManifestCommit func()
}

// Open opens (or creates) a store in dir.
func Open(dir string, opt Options) (*Store, error) {
	if opt.FlushRecords <= 0 {
		opt.FlushRecords = 1000
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &Store{
		dir:       dir,
		opt:       opt,
		active:    newMemtable(),
		segStates: make(map[uint64]*segmentState),
		snapshots: make(map[*snapshotRef]struct{}),
	}
	if err := s.recover(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Store) manifestPath() string { return filepath.Join(s.dir, "manifest.json") }

func (s *Store) loadManifest() (*manifestData, error) {
	buf, err := os.ReadFile(s.manifestPath())
	if os.IsNotExist(err) {
		return &manifestData{NextSegID: 1}, nil
	}
	if err != nil {
		return nil, err
	}
	var m manifestData
	if err := json.Unmarshal(buf, &m); err != nil {
		// A torn manifest write: fall back to scanning the directory.
		return nil, err
	}
	if m.NextSegID == 0 {
		m.NextSegID = 1
	}
	return &m, nil
}

func (s *Store) recover() error {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return err
	}
	var segFiles []string
	var walFile string
	hasManifest := false
	for _, e := range entries {
		n := e.Name()
		switch {
		case strings.HasSuffix(n, ".seg"):
			segFiles = append(segFiles, n)
		case n == "wal.log":
			walFile = n
		case n == "manifest.json":
			hasManifest = true
		}
	}
	sort.Strings(segFiles)

	// Load segments. If the manifest is corrupt we accept every well-formed
	// segment found on disk; temporary compaction files are never named *.seg.tmp.
	var man *manifestData
	if hasManifest {
		m, merr := s.loadManifest()
		if merr == nil {
			man = m
		}
	}
	manifestIDs := map[uint64]bool{}
	if man != nil {
		for _, ms := range man.Segments {
			manifestIDs[ms.ID] = true
		}
	}
	for _, n := range segFiles {
		id, ok := parseSegName(n)
		if !ok {
			continue
		}
		if man != nil && !manifestIDs[id] {
			// Renamed output of an interrupted compaction, never committed.
			_ = os.Remove(filepath.Join(s.dir, n))
			continue
		}
		seg, err := loadSegment(filepath.Join(s.dir, n), id)
		if err != nil {
			// Partial/corrupt file that never made it into the manifest.
			if man != nil {
				return fmt.Errorf("tsstore: load %s: %w", n, err)
			}
			continue
		}
		s.segments = append(s.segments, seg)
		st := &segmentState{ID: id}
		if man != nil {
			for _, ms := range man.Segments {
				if ms.ID == id {
					st.Retired = ms.Retired
				}
			}
		}
		s.segStates[id] = st
		if id >= s.nextSegID {
			s.nextSegID = id + 1
		}
	}
	sort.Slice(s.segments, func(i, j int) bool { return s.segments[i].id < s.segments[j].id })
	if man != nil && man.NextSegID > s.nextSegID {
		s.nextSegID = man.NextSegID
	}

	// Replay WAL into a memtable, deduping identical version records.
	m := newMemtable()
	if walFile != "" {
		buf, err := os.ReadFile(filepath.Join(s.dir, walFile))
		if err != nil {
			return err
		}
		for off := 0; off < len(buf); {
			consumed, kind, ver, series, ts, val, ok := decodeWALRecord(buf[off:])
			if !ok {
				break // torn tail after a crash
			}
			off += consumed
			existing := m.cells(series, ts)
			if e := versionAt(existing, ver); e != nil && e.version == ver {
				continue
			}
			de := entry{version: ver, ts: ts, value: val}
			if kind == walKindExpire {
				de.deleted = true
			}
			m.apply(series, de)
			if ver > s.maxVer {
				s.maxVer = ver
			}
		}
		f, err := os.OpenFile(filepath.Join(s.dir, walFile), os.O_RDWR|os.O_APPEND, 0o644)
		if err != nil {
			return err
		}
		s.wal = f
		s.walPath = filepath.Join(s.dir, walFile)
	} else {
		f, err := os.OpenFile(filepath.Join(s.dir, "wal.log"), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
		if err != nil {
			return err
		}
		s.wal = f
		s.walPath = filepath.Join(s.dir, "wal.log")
	}
	if man != nil && man.MaxVersion > s.maxVer {
		s.maxVer = man.MaxVersion
	}

	s.active = m

	// Remove stray temp files from interrupted compactions.
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".tmp") {
			_ = os.Remove(filepath.Join(s.dir, e.Name()))
		}
	}

	if !hasManifest || man == nil {
		return s.persistManifest(nil)
	}
	return nil
}

func parseSegName(n string) (uint64, bool) {
	n = strings.TrimSuffix(n, ".seg")
	var id uint64
	if _, err := fmt.Sscanf(n, "seg-%010d", &id); err != nil {
		return 0, false
	}
	return id, true
}

func (s *Store) segPath(id uint64) string {
	return filepath.Join(s.dir, fmt.Sprintf("seg-%010d.seg", id))
}

// persistManifest writes the manifest atomically. segLock must not be held in a
// way that prevents reading segStates; callers hold s.mu.
func (s *Store) persistManifest(extra func(*manifestData)) error {
	m := &manifestData{NextSegID: s.nextSegID, MaxVersion: s.maxVer}
	for _, st := range s.segStates {
		m.Segments = append(m.Segments, *st)
	}
	sort.Slice(m.Segments, func(i, j int) bool { return m.Segments[i].ID < m.Segments[j].ID })
	if extra != nil {
		extra(m)
	}
	buf, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.manifestPath() + ".tmp"
	if err := os.WriteFile(tmp, buf, 0o644); err != nil {
		return err
	}
	if err := syncFile(tmp); err != nil {
		return err
	}
	if err := os.Rename(tmp, s.manifestPath()); err != nil {
		return err
	}
	return syncDir(s.dir)
}

func syncFile(path string) error {
	f, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
