package tsstore

import "errors"

// Observation is one sampled point in a series.
type Observation struct {
	Timestamp int64
	Value     float64
}

var (
	// ErrAlreadyExists is returned by Append when the timestamp was already written.
	ErrAlreadyExists = errors.New("tsstore: observation already exists")
	// ErrNotFound is returned by Revise when the timestamp does not exist.
	ErrNotFound = errors.New("tsstore: observation not found")
	// ErrClosed is returned when the store (or snapshot) has been closed.
	ErrClosed = errors.New("tsstore: closed")
)

// entry is one versioned cell value for a (series, timestamp) key.
type entry struct {
	version int64
	ts      int64
	value   float64
	deleted bool
}

type memtable map[string]map[int64][]entry

func newMemtable() memtable {
	return make(memtable)
}

func (m memtable) apply(series string, e entry) {
	sm := m[series]
	if sm == nil {
		sm = make(map[int64][]entry)
		m[series] = sm
	}
	// Entries are applied in monotonically increasing version order.
	sm[e.ts] = append(sm[e.ts], e)
}

func (m memtable) clone() memtable {
	c := make(memtable, len(m))
	for series, sm := range m {
		cm := make(map[int64][]entry, len(sm))
		for ts, vers := range sm {
			cp := make([]entry, len(vers))
			copy(cp, vers)
			cm[ts] = cp
		}
		c[series] = cm
	}
	return c
}

func (m memtable) count() int {
	n := 0
	for _, sm := range m {
		n += len(sm)
	}
	return n
}
