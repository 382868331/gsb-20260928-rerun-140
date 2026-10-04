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
	ErrInvalidVersion          = errors.New("timeseries: pinned data version is not available")
	ErrInvalidTier             = errors.New("timeseries: invalid resolution tier")
	errHookExit                = errors.New("timeseries: simulated compaction interruption")
)
