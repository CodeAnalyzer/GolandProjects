package trcsvc

import (
	"context"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/codebase/internal/query"
	"github.com/codebase/internal/store"
	"github.com/codebase/internal/trc"
)

// resolveSession загружает данные из БД (session_id > 0) или парсит файл.
// Возвращает events, parseResult (для file-mode).
func resolveSession(ctx context.Context, db *store.DB, src SessionSource) (
	events []trc.TRCEvent,
	parseResult *trc.TRCParseResult,
	err error,
) {
	if src.SessionID > 0 && db != nil {
		events, err = trc.LoadEvents(ctx, db, src.SessionID)
		if err != nil {
			return nil, nil, err
		}
		return events, nil, nil
	}
	if src.FilePath == "" {
		return nil, nil, fmt.Errorf("either session_id or file_path is required")
	}
	parseResult, err = trc.ParseFile(src.FilePath)
	if err != nil {
		return nil, nil, err
	}
	return parseResult.Events, parseResult, nil
}

// normalizeLimit нормализует лимит: default=100, max=1000.
func normalizeLimit(limit int) int {
	if limit <= 0 {
		return 100
	}
	if limit > 1000 {
		return 1000
	}
	return limit
}

// ExecuteParse парсит TRC-файл и сохраняет в БД (если db доступен).
func ExecuteParse(ctx context.Context, db *store.DB, filePath string) (*ParseResult, error) {
	if filePath == "" {
		return nil, fmt.Errorf("file_path is required")
	}
	if db != nil {
		sessionID, totalEvents, err := trc.ParseFileToDB(ctx, filePath, db)
		if err != nil {
			return nil, fmt.Errorf("failed to parse trc file: %w", err)
		}
		return &ParseResult{
			SessionID:   sessionID,
			TotalEvents: totalEvents,
		}, nil
	}
	result, err := trc.ParseFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to parse trc file: %w", err)
	}
	return &ParseResult{
		SessionID:   0,
		TotalEvents: len(result.Events),
		Warning:     "database unavailable, session not saved",
	}, nil
}

// ExecuteList возвращает список сессий из БД.
func ExecuteList(ctx context.Context, db *store.DB, limit int) (*ListResult, error) {
	if db == nil {
		return nil, fmt.Errorf("database not available")
	}
	sessions, err := trc.ListSessions(ctx, db, limit)
	if err != nil {
		return nil, err
	}
	return &ListResult{Sessions: sessions}, nil
}

// ExecuteSummary возвращает статистику по сессии.
func ExecuteSummary(ctx context.Context, db *store.DB, src SessionSource) (*SummaryResult, error) {
	if src.SessionID > 0 && db != nil {
		session, err := trc.GetSession(ctx, db, src.SessionID)
		if err != nil {
			return nil, fmt.Errorf("session %d not found: %w", src.SessionID, err)
		}
		totalEvents, _ := trc.LoadEventCount(ctx, db, src.SessionID)
		return &SummaryResult{
			TotalEvents: totalEvents,
			Session:     session,
		}, nil
	}
	_, parseResult, err := resolveSession(ctx, db, src)
	if err != nil {
		return nil, err
	}
	return &SummaryResult{
		TotalEvents: len(parseResult.Events),
		Header:      parseResult.Header,
	}, nil
}

// eventStartTime возвращает start_time события из декодированной колонки 14.
func eventStartTime(ev trc.TRCEvent) (time.Time, bool) {
	st, ok := ev.Columns[14].(trc.SystemTime)
	if !ok {
		return time.Time{}, false
	}
	return st.ToTime()
}

// eventEndTime возвращает end_time события из декодированной колонки 15.
func eventEndTime(ev trc.TRCEvent) (time.Time, bool) {
	st, ok := ev.Columns[15].(trc.SystemTime)
	if !ok {
		return time.Time{}, false
	}
	return st.ToTime()
}

func containsInt(values []int, v int) bool {
	for _, x := range values {
		if x == v {
			return true
		}
	}
	return false
}

func containsString(values []string, v string) bool {
	for _, x := range values {
		if x == v {
			return true
		}
	}
	return false
}

// eventMatchesFilter применяет правила единой модели фильтров в памяти
// (file-mode): множественные SPID (колонка 12) и имена событий, точная
// процедура, полуинтервал [TimeFrom; TimeTo) по start_time (события без
// start_time не попадают в диапазон) и минимальная длительность.
func eventMatchesFilter(ev trc.TRCEvent, f trc.TRCEventFilter) bool {
	if len(f.SPIDs) > 0 {
		spid, ok := trc.EventSPID(ev)
		if !ok || !containsInt(f.SPIDs, spid) {
			return false
		}
	}
	if f.Procedure != "" && ev.Procedure != f.Procedure {
		return false
	}
	if len(f.EventNames) > 0 && !containsString(f.EventNames, ev.EventName) {
		return false
	}
	if f.TimeFrom != nil || f.TimeTo != nil {
		st, ok := eventStartTime(ev)
		if !ok {
			return false
		}
		if f.TimeFrom != nil && st.Before(*f.TimeFrom) {
			return false
		}
		if f.TimeTo != nil && !st.Before(*f.TimeTo) {
			return false
		}
	}
	if f.MinDurationMs != nil && ev.DurationMs < *f.MinDurationMs {
		return false
	}
	return true
}

// eventView строит короткое представление события file-mode; id — логический
// идентификатор (позиция в потоке + 1).
func eventView(ev trc.TRCEvent, id int64) TRCEventView {
	v := TRCEventView{
		ID:         id,
		EventClass: ev.EventClass,
		EventName:  ev.EventName,
		Procedure:  ev.Procedure,
		DurationMs: ev.DurationMs,
	}
	if spid, ok := trc.EventSPID(ev); ok {
		v.SPID = spid
	}
	if t, ok := eventStartTime(ev); ok {
		v.StartTime = &t
	}
	if t, ok := eventEndTime(ev); ok {
		v.EndTime = &t
	}
	return v
}

// trcEventViewsFromRows преобразует short-строки saved-session в представления.
func trcEventViewsFromRows(rows []trc.TRCEventRow) []TRCEventView {
	views := make([]TRCEventView, 0, len(rows))
	for _, r := range rows {
		views = append(views, TRCEventView{
			ID:         r.ID,
			EventClass: r.EventClass,
			EventName:  r.EventName,
			SPID:       r.SPID,
			Procedure:  r.Procedure,
			StartTime:  r.StartTime,
			EndTime:    r.EndTime,
			DurationMs: r.DurationMs,
		})
	}
	return views
}

// ExecuteEvents возвращает страницу событий с фильтрацией по правилам единой
// модели фильтров: saved-session — серверно (включая keyset-пагинацию и
// short-проекцию без JSONB), file-mode — эквивалентно в памяти с курсором
// EventIndex+1. filtered_count возвращается на каждой странице; total_count —
// только на первой (AfterID nil).
func ExecuteEvents(ctx context.Context, db *store.DB, p EventsParams) (*EventsResult, error) {
	limit := normalizeLimit(p.Limit)

	spids, err := normalizeSPIDs(p.SPIDs)
	if err != nil {
		return nil, err
	}
	eventNames, err := normalizeEventNames(p.EventNames)
	if err != nil {
		return nil, err
	}
	if err := validateTimeRange(p.TimeFrom, p.TimeTo); err != nil {
		return nil, err
	}
	if err := validateMinDuration(p.MinDurationMs); err != nil {
		return nil, err
	}
	if err := validateAfterID(p.AfterID); err != nil {
		return nil, err
	}
	short, err := normalizeEventsFormat(p.Format)
	if err != nil {
		return nil, err
	}

	f := trc.TRCEventFilter{
		SPIDs:         spids,
		Procedure:     p.Procedure,
		EventNames:    eventNames,
		TimeFrom:      p.TimeFrom,
		TimeTo:        p.TimeTo,
		MinDurationMs: p.MinDurationMs,
		AfterID:       p.AfterID,
	}
	result := &EventsResult{Short: short, Limit: limit}

	if p.Source.SessionID > 0 && db != nil {
		filteredCount, err := trc.LoadEventCountFiltered(ctx, db, p.Source.SessionID, f)
		if err != nil {
			return nil, err
		}
		result.FilteredCount = filteredCount
		if p.AfterID == nil {
			totalCount, err := trc.LoadEventCount(ctx, db, p.Source.SessionID)
			if err != nil {
				return nil, fmt.Errorf("failed to count events: %w", err)
			}
			result.TotalCount = &totalCount
		}
		if short {
			page, err := trc.LoadEventRowsFiltered(ctx, db, p.Source.SessionID, f, limit)
			if err != nil {
				return nil, err
			}
			result.Views = trcEventViewsFromRows(page.Rows)
			result.ReturnedCount = len(page.Rows)
			result.HasMore = page.HasMore
			result.NextAfterID = page.NextAfterID
			return result, nil
		}
		page, err := trc.LoadEventsFiltered(ctx, db, p.Source.SessionID, f, limit)
		if err != nil {
			return nil, err
		}
		result.Events = page.Events
		result.ReturnedCount = len(page.Events)
		result.HasMore = page.HasMore
		result.NextAfterID = page.NextAfterID
		return result, nil
	}

	// file-mode: логический ID события — позиция в потоке + 1.
	events, _, err := resolveSession(ctx, db, p.Source)
	if err != nil {
		return nil, err
	}
	matched := make([]int, 0)
	for i := range events {
		if eventMatchesFilter(events[i], f) {
			matched = append(matched, i)
		}
	}
	result.FilteredCount = len(matched)
	if p.AfterID == nil {
		total := len(events)
		result.TotalCount = &total
	}

	var after int64
	if p.AfterID != nil {
		after = *p.AfterID
	}
	pageIdx := make([]int, 0, limit+1)
	for _, i := range matched {
		if int64(i+1) <= after {
			continue
		}
		if len(pageIdx) > limit {
			break
		}
		pageIdx = append(pageIdx, i)
	}
	result.HasMore = len(pageIdx) > limit
	if result.HasMore {
		pageIdx = pageIdx[:limit]
		result.NextAfterID = int64(pageIdx[len(pageIdx)-1] + 1)
	}
	if short {
		for _, i := range pageIdx {
			result.Views = append(result.Views, eventView(events[i], int64(i+1)))
		}
	} else {
		for _, i := range pageIdx {
			ev := events[i]
			ev.StoreID = int64(i + 1)
			result.Events = append(result.Events, ev)
		}
	}
	result.ReturnedCount = len(pageIdx)
	return result, nil
}

func enrichProcedureAggregates(ctx context.Context, q trc.ProcedureLookup, aggs []trc.TRCProcAgg) {
	names := make([]string, 0, len(aggs))
	for _, agg := range aggs {
		names = append(names, agg.Procedure)
	}
	enrichMap := trc.EnrichProcedureNames(ctx, q, names)
	trc.EnrichAggregates(aggs, enrichMap)
}

// ExecuteProcedures агрегирует статистику по процедурам: saved-session —
// серверно (LoadProceduresAggregated), file-mode — в памяти с эквивалентными
// правилами (AggregateByProcedure).
func ExecuteProcedures(ctx context.Context, db *store.DB, p ProceduresParams) (*ProceduresResult, error) {
	spids, err := normalizeSPIDs(p.SPIDs)
	if err != nil {
		return nil, err
	}
	eventNames, err := normalizeEventNames(p.EventNames)
	if err != nil {
		return nil, err
	}
	top, err := normalizeTop(p.Top)
	if err != nil {
		return nil, err
	}
	sortBy, err := normalizeSortBy(p.SortBy)
	if err != nil {
		return nil, err
	}
	opts := trc.AggregateOptions{
		EventNames:  eventNames,
		SPIDs:       spids,
		GroupBySPID: p.GroupBySPID,
		SortBy:      sortBy,
		Top:         top,
	}

	if p.Source.SessionID > 0 && db != nil {
		aggs, err := trc.LoadProceduresAggregated(ctx, db, p.Source.SessionID, opts)
		if err != nil {
			return nil, err
		}
		if len(aggs) > 0 {
			enrichProcedureAggregates(ctx, query.New(db), aggs)
		}
		return &ProceduresResult{
			Procedures: aggs,
			Count:      len(aggs),
		}, nil
	}

	events, _, err := resolveSession(ctx, db, p.Source)
	if err != nil {
		return nil, err
	}
	aggs := trc.AggregateByProcedure(events, opts)
	if db != nil && len(aggs) > 0 {
		q := query.New(db)
		enrichProcedureAggregates(ctx, q, aggs)
	}
	return &ProceduresResult{
		Procedures: aggs,
		Count:      len(aggs),
	}, nil
}

// ExecuteTree строит деревья вызовов по SPID.
func ExecuteTree(ctx context.Context, db *store.DB, p TreeParams) (*TreeResult, error) {
	if p.Source.SessionID > 0 && db != nil {
		treeEvents, err := trc.LoadEventsForTree(ctx, db, p.Source.SessionID, p.SPID, p.MaxDepth, p.Limit, p.Procedure)
		if err != nil {
			return nil, err
		}
		trees := trc.BuildTrees(treeEvents)
		return &TreeResult{
			Trees:      trees,
			EventCount: len(treeEvents),
			SPID:       p.SPID,
		}, nil
	}

	events, _, err := resolveSession(ctx, db, p.Source)
	if err != nil {
		return nil, err
	}
	trees := trc.BuildTreesWithDepth(events, p.MaxDepth)
	if p.SPID > 0 {
		if t, ok := trees[p.SPID]; ok {
			trees = map[int][]*trc.TRCTreeNode{p.SPID: t}
		} else {
			trees = map[int][]*trc.TRCTreeNode{}
		}
	}
	trees = trc.FilterTreesByProcedure(trees, p.Procedure)
	trc.LimitTrees(trees, p.Limit)
	return &TreeResult{Trees: trees}, nil
}

// ExecuteErrors находит события с ошибками.
func ExecuteErrors(ctx context.Context, db *store.DB, p ErrorsParams) (*ErrorsResult, error) {
	limit := normalizeLimit(p.Limit)

	if p.Source.SessionID > 0 && db != nil {
		events, err := trc.LoadErrorEvents(ctx, db, p.Source.SessionID, limit)
		if err != nil {
			return nil, err
		}
		return &ErrorsResult{
			Events: events,
			Count:  len(events),
			Limit:  limit,
		}, nil
	}

	events, _, err := resolveSession(ctx, db, p.Source)
	if err != nil {
		return nil, err
	}
	var errs []trc.TRCEvent
	for _, ev := range events {
		if code, ok := ev.Columns[31].(int32); ok && code != 0 {
			errs = append(errs, ev)
		}
		if len(errs) >= limit {
			break
		}
	}
	return &ErrorsResult{
		Events: errs,
		Count:  len(errs),
		Limit:  limit,
	}, nil
}

// ExecuteSlow находит медленные события.
func ExecuteSlow(ctx context.Context, db *store.DB, p SlowParams) (*SlowResult, error) {
	threshold := p.ThresholdMs
	if threshold <= 0 {
		threshold = trc.GetSlowThresholdMs()
	}
	limit := normalizeLimit(p.Limit)

	if p.Source.SessionID > 0 && db != nil {
		events, err := trc.LoadSlowEvents(ctx, db, p.Source.SessionID, threshold, limit)
		if err != nil {
			return nil, err
		}
		return &SlowResult{
			Events:    events,
			Count:     len(events),
			Threshold: threshold,
			Limit:     limit,
		}, nil
	}

	events, _, err := resolveSession(ctx, db, p.Source)
	if err != nil {
		return nil, err
	}
	var slow []trc.TRCEvent
	for _, ev := range events {
		if ev.DurationMs >= int64(threshold) {
			slow = append(slow, ev)
		}
	}
	sort.Slice(slow, func(i, j int) bool { return slow[i].DurationMs > slow[j].DurationMs })
	if len(slow) > limit {
		slow = slow[:limit]
	}
	return &SlowResult{
		Events:    slow,
		Count:     len(slow),
		Threshold: threshold,
		Limit:     limit,
	}, nil
}

// ExecuteDelete удаляет сессию по ID.
func ExecuteDelete(ctx context.Context, db *store.DB, sessionID int64) (*DeleteResult, error) {
	if db == nil {
		return nil, fmt.Errorf("database not available")
	}
	session, err := trc.GetSession(ctx, db, sessionID)
	if err != nil {
		return nil, fmt.Errorf("session %d not found: %w", sessionID, err)
	}
	if err := trc.DeleteSession(ctx, db, sessionID); err != nil {
		return nil, err
	}
	return &DeleteResult{
		Deleted:   true,
		SessionID: sessionID,
		FilePath:  session.FilePath,
	}, nil
}

// ExecutePrune удаляет старые сессии, оставляя keepLast последних.
func ExecutePrune(ctx context.Context, db *store.DB, keepLast int) (*PruneResult, error) {
	if db == nil {
		return nil, fmt.Errorf("database not available")
	}
	if keepLast < 0 {
		return nil, fmt.Errorf("keep_last must be >= 0")
	}
	deleted, err := trc.PruneSessions(ctx, db, keepLast)
	if err != nil {
		return nil, err
	}
	return &PruneResult{
		DeletedCount: int(deleted),
		KeptLast:     keepLast,
	}, nil
}

// FileExists проверяет существование файла.
func FileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
