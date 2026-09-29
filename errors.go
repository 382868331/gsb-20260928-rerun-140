package timeseries

import "errors"

var (
	ErrEmptySeries         = errors.New("timeseries: series must not be empty")
	ErrObservationExists   = errors.New("timeseries: observation already exists")
	ErrObservationNotFound = errors.New("timeseries: observation not found")
	ErrInvalidRange        = errors.New("timeseries: invalid time range")
	ErrCorruptStorage      = errors.New("timeseries: corrupt storage")
	errHookExit            = errors.New("timeseries: simulated compaction interruption")
)
