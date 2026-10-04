package timeseries

import "errors"

var (
	ErrEmptySeries             = errors.New("timeseries: series must not be empty")
	ErrObservationExists       = errors.New("timeseries: observation already exists")
	ErrObservationNotFound     = errors.New("timeseries: observation not found")
	ErrInvalidRange            = errors.New("timeseries: invalid time range")
	ErrCorruptStorage          = errors.New("timeseries: corrupt storage")
	ErrInvalidAggregator       = errors.New("timeseries: invalid aggregator definition")
	ErrAggregatorNotRegistered = errors.New("timeseries: aggregator not registered")
	ErrInvalidQueryView        = errors.New("timeseries: invalid query view")
	ErrQueryViewExists         = errors.New("timeseries: query view already exists")
	ErrQueryViewNotFound       = errors.New("timeseries: query view not found")
	ErrQueryViewMismatch       = errors.New("timeseries: query views are not comparable")
	errHookExit                = errors.New("timeseries: simulated compaction interruption")
)
