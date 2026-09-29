package tsstore

import (
	"errors"
	"os"
	"sort"
)

var errCorruptSegment = errors.New("tsstore: corrupt segment file")

type segment struct {
	id      uint64
	path    string
	entries map[string]map[int64][]entry
	mins    map[string]int64
	maxs    map[string]int64
	mv      int64 // max version contained
}

func loadSegment(path string, id uint64) (*segment, error) {
	buf, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	raw, err := decodeSegment(buf)
	if err != nil {
		return nil, err
	}
	s := &segment{
		id:      id,
		path:    path,
		entries: make(map[string]map[int64][]entry),
		mins:    make(map[string]int64),
		maxs:    make(map[string]int64),
	}
	for _, e := range raw {
		sm := s.entries[e.series]
		if sm == nil {
			sm = make(map[int64][]entry)
			s.entries[e.series] = sm
			s.mins[e.series] = e.entry.ts
			s.maxs[e.series] = e.entry.ts
		}
		if e.entry.ts < s.mins[e.series] {
			s.mins[e.series] = e.entry.ts
		}
		if e.entry.ts > s.maxs[e.series] {
			s.maxs[e.series] = e.entry.ts
		}
		if e.entry.version > s.mv {
			s.mv = e.entry.version
		}
		sm[e.entry.ts] = append(sm[e.entry.ts], e.entry)
	}
	return s, nil
}

func segmentFromMemtable(id uint64, path string, m memtable) (*segment, []entryWithSeries) {
	s := &segment{
		id:      id,
		path:    path,
		entries: make(map[string]map[int64][]entry),
		mins:    make(map[string]int64),
		maxs:    make(map[string]int64),
	}
	var all []entryWithSeries
	for series, sm := range m {
		nsm := make(map[int64][]entry, len(sm))
		for ts, vers := range sm {
			cp := make([]entry, len(vers))
			copy(cp, vers)
			sort.Slice(cp, func(i, j int) bool { return cp[i].version < cp[j].version })
			nsm[ts] = cp
			for _, e := range cp {
				all = append(all, entryWithSeries{series: series, entry: e})
			}
			if _, ok := s.mins[series]; !ok || ts < s.mins[series] {
				s.mins[series] = ts
			}
			if ts > s.maxs[series] {
				s.maxs[series] = ts
			}
		}
		s.entries[series] = nsm
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].series != all[j].series {
			return all[i].series < all[j].series
		}
		if all[i].entry.ts != all[j].entry.ts {
			return all[i].entry.ts < all[j].entry.ts
		}
		return all[i].entry.version < all[j].entry.version
	})
	for _, e := range all {
		if e.entry.version > s.mv {
			s.mv = e.entry.version
		}
	}
	return s, all
}

// versionAt returns the latest entry with version <= asOf (nil if none).
func versionAt(vers []entry, asOf int64) *entry {
	i := sort.Search(len(vers), func(i int) bool { return vers[i].version > asOf })
	if i == 0 {
		return nil
	}
	return &vers[i-1]
}

type cellReader interface {
	// cells returns version-ordered entries for (series, ts).
	cells(series string, ts int64) []entry
}

func (s *segment) cells(series string, ts int64) []entry {
	if sm, ok := s.entries[series]; ok {
		return sm[ts]
	}
	return nil
}

func (m memtable) cells(series string, ts int64) []entry {
	if sm, ok := m[series]; ok {
		return sm[ts]
	}
	return nil
}
