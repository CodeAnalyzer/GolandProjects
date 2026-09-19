package trc

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"time"

	"github.com/codebase/internal/store"
	"github.com/lib/pq"
)

// SpidsOptions — параметры сводки активности SPID: опциональный фильтр
// подмножества, временной полуинтервал [TimeFrom; TimeTo) по start_time,
// SortBy: first_time | last_time | event_count | max_duration_ms | error_count,
// Limit 0 = default.
type SpidsOptions struct {
	SPIDs    []int
	TimeFrom *time.Time
	TimeTo   *time.Time
	SortBy   string
	Limit    int
}

// SPIDSummary — наблюдаемые факты активности одного SPID без выгрузки сырых
// событий. FirstTime/LastTime null, если ни у одного события нет start_time.
// Identity-поля — самое частое непустое значение (tie-break: самое раннее
// событие); отсутствуют в JSON, если непустых значений нет.
type SPIDSummary struct {
	SPID                int        `json:"spid"`
	EventCount          int        `json:"event_count"`
	FirstTime           *time.Time `json:"first_time"`
	LastTime            *time.Time `json:"last_time"`
	SPCompletedCount    int        `json:"sp_completed_count"`
	RPCCompletedCount   int        `json:"rpc_completed_count"`
	BatchCompletedCount int        `json:"batch_completed_count"`
	ErrorCount          int        `json:"error_count"`
	MaxDurationMs       int64      `json:"max_duration_ms"`
	ApplicationName     string     `json:"application_name,omitempty"`
	LoginName           string     `json:"login_name,omitempty"`
	HostName            string     `json:"host_name,omitempty"`
}

// eventStartTimeValue возвращает start_time события (колонка 14) или false.
func eventStartTimeValue(ev TRCEvent) (time.Time, bool) {
	st, ok := ev.Columns[14].(SystemTime)
	if !ok {
		return time.Time{}, false
	}
	return st.ToTime()
}

// identityEntry — счётчик значений identity-поля: число вхождений и индекс
// первого встреченного события (tie-break моды).
type identityEntry struct {
	count    int
	firstIdx int
}

// SummarizeSPIDs строит сводку активности SPID в памяти (file-mode):
// счётчики completed-классов, ошибки (колонка 31), границы времени по
// ненулевым start_time, max duration включая нули, мода identity-полей
// (колонки 10/11/8) с tie-break по первому встреченному событию.
func SummarizeSPIDs(events []TRCEvent, opts SpidsOptions) []SPIDSummary {
	spids := make(map[int]bool, len(opts.SPIDs))
	for _, s := range opts.SPIDs {
		spids[s] = true
	}

	type acc struct {
		summary SPIDSummary
	}
	bySPID := make(map[int]*acc)
	var order []int

	// identity считаем через map значений: значение → (count, firstIdx)
	type identMaps struct {
		app   map[string]*identityEntry
		login map[string]*identityEntry
		host  map[string]*identityEntry
	}
	identities := make(map[int]*identMaps)

	for i := range events {
		ev := events[i]
		spid, ok := EventSPID(ev)
		if !ok {
			continue
		}
		if len(opts.SPIDs) > 0 && !spids[spid] {
			continue
		}
		var startTime *time.Time
		if t, ok := eventStartTimeValue(ev); ok {
			startTime = &t
		}
		if (opts.TimeFrom != nil || opts.TimeTo != nil) && startTime == nil {
			continue // события без start_time не попадают в отфильтрованную сводку
		}
		if opts.TimeFrom != nil && startTime.Before(*opts.TimeFrom) {
			continue
		}
		if opts.TimeTo != nil && !startTime.Before(*opts.TimeTo) {
			continue
		}

		a, exists := bySPID[spid]
		if !exists {
			a = &acc{summary: SPIDSummary{SPID: spid}}
			bySPID[spid] = a
			order = append(order, spid)
			identities[spid] = &identMaps{
				app:   make(map[string]*identityEntry),
				login: make(map[string]*identityEntry),
				host:  make(map[string]*identityEntry),
			}
		}
		s := &a.summary
		s.EventCount++
		switch ev.EventName {
		case "SP:Completed":
			s.SPCompletedCount++
		case "RPC:Completed":
			s.RPCCompletedCount++
		case "SQL:BatchCompleted":
			s.BatchCompletedCount++
		}
		if code, ok := ev.Columns[31].(int32); ok && code != 0 {
			s.ErrorCount++
		}
		if ev.DurationMs > s.MaxDurationMs {
			s.MaxDurationMs = ev.DurationMs
		}
		if startTime != nil {
			if s.FirstTime == nil || startTime.Before(*s.FirstTime) {
				t := *startTime
				s.FirstTime = &t
			}
			if s.LastTime == nil || startTime.After(*s.LastTime) {
				t := *startTime
				s.LastTime = &t
			}
		}

		ids := identities[spid]
		countIdent := func(m map[string]*identityEntry, value string) {
			if value == "" {
				return
			}
			if e, ok := m[value]; ok {
				e.count++
			} else {
				m[value] = &identityEntry{count: 1, firstIdx: i}
			}
		}
		countIdent(ids.app, strVal(ev.Columns[10]))
		countIdent(ids.login, strVal(ev.Columns[11]))
		countIdent(ids.host, strVal(ev.Columns[8]))
	}

	result := make([]SPIDSummary, 0, len(order))
	for _, spid := range order {
		a := bySPID[spid]
		s := a.summary
		ids := identities[spid]
		s.ApplicationName = modeValue(ids.app)
		s.LoginName = modeValue(ids.login)
		s.HostName = modeValue(ids.host)
		result = append(result, s)
	}
	SortSPIDSummaries(result, opts.SortBy)
	if opts.Limit > 0 && len(result) > opts.Limit {
		result = result[:opts.Limit]
	}
	return result
}

// modeValue возвращает самое частое значение; при равной частоте — то, что
// встретилось раньше (меньший firstIdx); пусто, если значений нет.
func modeValue(m map[string]*identityEntry) string {
	best := ""
	var bestCount int
	var bestIdx int
	have := false
	for value, e := range m {
		if !have || e.count > bestCount || (e.count == bestCount && e.firstIdx < bestIdx) {
			best, bestCount, bestIdx, have = value, e.count, e.firstIdx, true
		}
	}
	if !have {
		return ""
	}
	return best
}

// SortSPIDSummaries сортирует сводку по SortBy (default event_count);
// NULL-время уходит в хвост, secondary — spid по возрастанию.
func SortSPIDSummaries(list []SPIDSummary, sortBy string) {
	sort.Slice(list, func(i, j int) bool {
		a, b := list[i], list[j]
		less := false
		switch sortBy {
		case "first_time":
			less = timeLess(a.FirstTime, b.FirstTime)
		case "last_time":
			less = timeLess(a.LastTime, b.LastTime)
		case "max_duration_ms":
			less = a.MaxDurationMs > b.MaxDurationMs
		case "error_count":
			less = a.ErrorCount > b.ErrorCount
		default: // event_count
			less = a.EventCount > b.EventCount
		}
		equal := spidSummaryEqual(sortBy, a, b)
		if equal {
			return a.SPID < b.SPID
		}
		return less
	})
}

func timeLess(a, b *time.Time) bool {
	if a == nil {
		return false // NULL — в хвосте
	}
	if b == nil {
		return true
	}
	return a.Before(*b)
}

func spidSummaryEqual(sortBy string, a, b SPIDSummary) bool {
	switch sortBy {
	case "first_time":
		return timeEqual(a.FirstTime, b.FirstTime)
	case "last_time":
		return timeEqual(a.LastTime, b.LastTime)
	case "max_duration_ms":
		return a.MaxDurationMs == b.MaxDurationMs
	case "error_count":
		return a.ErrorCount == b.ErrorCount
	default:
		return a.EventCount == b.EventCount
	}
}

func timeEqual(a, b *time.Time) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Equal(*b)
}

// MaxDurationMsOfEvents возвращает максимальную длительность набора событий —
// эмпирическая проверка доступности метрик длительности (0 = отсутствуют).
func MaxDurationMsOfEvents(events []TRCEvent) int64 {
	var max int64
	for _, ev := range events {
		if ev.DurationMs > max {
			max = ev.DurationMs
		}
	}
	return max
}

// spidsFilterWhere строит WHERE сводки: сессия + опциональные spid = ANY и
// временной полуинтервал по start_time (события без start_time выпадают).
func spidsFilterWhere(sessionID int64, opts SpidsOptions) (string, []interface{}) {
	where := "session_id = $1"
	args := []interface{}{sessionID}
	if len(opts.SPIDs) > 0 {
		where += fmt.Sprintf(" AND spid = ANY($%d)", len(args)+1)
		args = append(args, pq.Array(opts.SPIDs))
	}
	if opts.TimeFrom != nil {
		where += fmt.Sprintf(" AND start_time >= $%d", len(args)+1)
		args = append(args, *opts.TimeFrom)
	}
	if opts.TimeTo != nil {
		where += fmt.Sprintf(" AND start_time < $%d", len(args)+1)
		args = append(args, *opts.TimeTo)
	}
	return where, args
}

// LoadSPIDSummaries строит сводку активности SPID серверно: агрегаты одним
// GROUP BY с COUNT FILTER, identity-мода — топ-1 (count DESC, min(id) ASC)
// по сгруппированным значениям. Сортировка/лимит — общая с file-mode.
func LoadSPIDSummaries(ctx context.Context, db *store.DB, sessionID int64, opts SpidsOptions) ([]SPIDSummary, error) {
	where, args := spidsFilterWhere(sessionID, opts)

	query := fmt.Sprintf(`
		SELECT spid,
		       count(*) AS event_count,
		       min(start_time) AS first_time,
		       max(start_time) AS last_time,
		       count(*) FILTER (WHERE event_name = 'SP:Completed') AS sp_completed_count,
		       count(*) FILTER (WHERE event_name = 'RPC:Completed') AS rpc_completed_count,
		       count(*) FILTER (WHERE event_name = 'SQL:BatchCompleted') AS batch_completed_count,
		       count(*) FILTER (WHERE error IS NOT NULL AND error <> 0) AS error_count,
		       max(duration_ms) AS max_duration_ms
		FROM trc_events
		WHERE %s AND spid IS NOT NULL
		GROUP BY spid`, where)

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to load spid summaries: %w", err)
	}
	defer rows.Close()

	bySPID := make(map[int]*SPIDSummary)
	var order []int
	for rows.Next() {
		var s SPIDSummary
		var firstTS, lastTS sql.NullTime
		if err := rows.Scan(&s.SPID, &s.EventCount, &firstTS, &lastTS,
			&s.SPCompletedCount, &s.RPCCompletedCount, &s.BatchCompletedCount,
			&s.ErrorCount, &s.MaxDurationMs); err != nil {
			return nil, err
		}
		if firstTS.Valid {
			t := firstTS.Time
			s.FirstTime = &t
		}
		if lastTS.Valid {
			t := lastTS.Time
			s.LastTime = &t
		}
		bySPID[s.SPID] = &s
		order = append(order, s.SPID)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// identity-мода: (spid, значение) → count, min(id); затем топ-1 на SPID.
	identQuery := fmt.Sprintf(`
		SELECT spid, field, value FROM (
			SELECT spid, 'app' AS field, application_name AS value,
			       count(*) AS cnt, min(id) AS first_id
			FROM trc_events WHERE %s AND application_name IS NOT NULL AND application_name <> ''
			GROUP BY spid, application_name
			UNION ALL
			SELECT spid, 'login', login_name, count(*), min(id)
			FROM trc_events WHERE %s AND login_name IS NOT NULL AND login_name <> ''
			GROUP BY spid, login_name
			UNION ALL
			SELECT spid, 'host', host_name, count(*), min(id)
			FROM trc_events WHERE %s AND host_name IS NOT NULL AND host_name <> ''
			GROUP BY spid, host_name
		) ranked
		ORDER BY spid, cnt DESC, first_id ASC`, where, where, where)

	identRows, err := db.QueryContext(ctx, identQuery, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to load spid identities: %w", err)
	}
	defer identRows.Close()

	type identKey struct {
		spid  int
		field string
	}
	seen := make(map[identKey]bool)
	for identRows.Next() {
		var spid int
		var field, value string
		if err := identRows.Scan(&spid, &field, &value); err != nil {
			return nil, err
		}
		key := identKey{spid, field}
		if seen[key] {
			continue // топ-1 уже взят (ORDER BY cnt DESC, first_id ASC)
		}
		seen[key] = true
		s, ok := bySPID[spid]
		if !ok {
			continue
		}
		switch field {
		case "app":
			s.ApplicationName = value
		case "login":
			s.LoginName = value
		case "host":
			s.HostName = value
		}
	}
	if err := identRows.Err(); err != nil {
		return nil, err
	}

	result := make([]SPIDSummary, 0, len(order))
	for _, spid := range order {
		result = append(result, *bySPID[spid])
	}
	SortSPIDSummaries(result, opts.SortBy)
	if opts.Limit > 0 && len(result) > opts.Limit {
		result = result[:opts.Limit]
	}
	return result, nil
}

// LoadMaxDurationMs возвращает максимальную длительность сессии — 0 означает
// недоступность метрик длительности (трейс снят без колонки Duration).
func LoadMaxDurationMs(ctx context.Context, db *store.DB, sessionID int64) (int64, error) {
	var max int64
	err := db.QueryRowContext(ctx,
		`SELECT COALESCE(max(duration_ms), 0) FROM trc_events WHERE session_id = $1`,
		sessionID,
	).Scan(&max)
	if err != nil {
		return 0, fmt.Errorf("failed to load max duration: %w", err)
	}
	return max, nil
}
