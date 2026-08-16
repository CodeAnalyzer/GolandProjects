package errs

import "errors"

var (
	ErrConfigNotLoaded   = errors.New("config not loaded")
	ErrDBConnect         = errors.New("failed to connect to database")
	ErrSchemaInit        = errors.New("failed to init schema")
	ErrNoRelationFilters = errors.New("at least one relation filter must be provided")
	ErrQueryFailed       = errors.New("query failed")
	ErrReviewFailed      = errors.New("review failed")
	ErrStatsFailed       = errors.New("failed to get stats")
	ErrHealthCheckFailed = errors.New("failed to inspect index readiness")
)
