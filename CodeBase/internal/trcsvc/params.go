package trcsvc

import (
	"fmt"
	"time"
)

// normalizeSPIDs проверяет положительность каждого SPID и дедуплицирует
// список с сохранением порядка первого появления.
func normalizeSPIDs(spids []int) ([]int, error) {
	if len(spids) == 0 {
		return nil, nil
	}
	seen := make(map[int]bool, len(spids))
	out := make([]int, 0, len(spids))
	for i, v := range spids {
		if v <= 0 {
			return nil, fmt.Errorf("spids[%d] must be positive, got %d", i, v)
		}
		if !seen[v] {
			seen[v] = true
			out = append(out, v)
		}
	}
	return out, nil
}

// normalizeEventNames запрещает пустые строки и дедуплицирует список имён
// событий с сохранением порядка первого появления.
func normalizeEventNames(names []string) ([]string, error) {
	if len(names) == 0 {
		return nil, nil
	}
	seen := make(map[string]bool, len(names))
	out := make([]string, 0, len(names))
	for i, v := range names {
		if v == "" {
			return nil, fmt.Errorf("event_names[%d] must be non-empty string", i)
		}
		if !seen[v] {
			seen[v] = true
			out = append(out, v)
		}
	}
	return out, nil
}

// validateTimeRange требует time_from < time_to при обеих границах.
func validateTimeRange(from, to *time.Time) error {
	if from != nil && to != nil && !from.Before(*to) {
		return fmt.Errorf("time_from must be before time_to")
	}
	return nil
}

// validateMinDuration требует min_duration_ms >= 0.
func validateMinDuration(ms *int64) error {
	if ms != nil && *ms < 0 {
		return fmt.Errorf("min_duration_ms must be >= 0, got %d", *ms)
	}
	return nil
}

// validateAfterID требует after_id >= 0 (явный 0 допустим — первая страница).
func validateAfterID(id *int64) error {
	if id != nil && *id < 0 {
		return fmt.Errorf("after_id must be >= 0, got %d", *id)
	}
	return nil
}

const (
	eventsFormatFull  = "full"
	eventsFormatShort = "short"
)

// normalizeEventsFormat возвращает признак short-формата; "" эквивалентен full.
func normalizeEventsFormat(format string) (bool, error) {
	switch format {
	case "", eventsFormatFull:
		return false, nil
	case eventsFormatShort:
		return true, nil
	default:
		return false, fmt.Errorf("format must be %q or %q, got %q", eventsFormatFull, eventsFormatShort, format)
	}
}

const (
	maxProceduresTop = 1000
)

// normalizeTop допускает top 0 (все) до 1000.
func normalizeTop(top int) (int, error) {
	if top < 0 || top > maxProceduresTop {
		return 0, fmt.Errorf("top must be between 0 and %d, got %d", maxProceduresTop, top)
	}
	return top, nil
}

// normalizeSortBy проверяет enum сортировки; "" → total_ms.
func normalizeSortBy(sortBy string) (string, error) {
	switch sortBy {
	case "":
		return "total_ms", nil
	case "total_ms", "avg_ms", "max_ms", "count":
		return sortBy, nil
	default:
		return "", fmt.Errorf("sort_by must be one of total_ms, avg_ms, max_ms, count, got %q", sortBy)
	}
}
