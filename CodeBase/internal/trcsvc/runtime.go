package trcsvc

import (
	"context"
	"fmt"
	"os"
	"sort"

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

// ExecuteEvents возвращает список событий с фильтрацией.
func ExecuteEvents(ctx context.Context, db *store.DB, p EventsParams) (*EventsResult, error) {
	limit := normalizeLimit(p.Limit)

	if p.Source.SessionID > 0 && db != nil {
		f := trc.TRCEventFilter{
			SPID:      p.SPID,
			Procedure: p.Procedure,
			EventName: p.EventName,
		}
		events, err := trc.LoadEventsFiltered(ctx, db, p.Source.SessionID, f, limit)
		if err != nil {
			return nil, err
		}
		totalCount, _ := trc.LoadEventCount(ctx, db, p.Source.SessionID)
		return &EventsResult{
			Events:        events,
			TotalCount:    totalCount,
			FilteredCount: len(events),
			Limit:         limit,
		}, nil
	}

	events, _, err := resolveSession(ctx, db, p.Source)
	if err != nil {
		return nil, err
	}
	var filtered []trc.TRCEvent
	for _, ev := range events {
		if p.SPID > 0 {
			if spid, ok := ev.Columns[12].(int32); !ok || int(spid) != p.SPID {
				continue
			}
		}
		if p.Procedure != "" && ev.Procedure != p.Procedure {
			continue
		}
		if p.EventName != "" && ev.EventName != p.EventName {
			continue
		}
		filtered = append(filtered, ev)
		if len(filtered) >= limit {
			break
		}
	}
	return &EventsResult{
		Events:        filtered,
		TotalCount:    len(events),
		FilteredCount: len(filtered),
		Limit:         limit,
	}, nil
}

// ExecuteProcedures агрегирует статистику по процедурам.
func ExecuteProcedures(ctx context.Context, db *store.DB, src SessionSource) (*ProceduresResult, error) {
	if src.SessionID > 0 && db != nil {
		aggs, err := trc.LoadProceduresAggregated(ctx, db, src.SessionID)
		if err != nil {
			return nil, err
		}
		if len(aggs) > 0 {
			q := query.New(db)
			sampleEvents, _ := trc.LoadEventsFiltered(ctx, db, src.SessionID, trc.TRCEventFilter{}, 1000)
			if len(sampleEvents) > 0 {
				enrichMap := trc.EnrichEvents(ctx, q, sampleEvents)
				trc.EnrichAggregates(aggs, enrichMap)
			}
		}
		return &ProceduresResult{
			Procedures: aggs,
			Count:      len(aggs),
		}, nil
	}

	events, _, err := resolveSession(ctx, db, src)
	if err != nil {
		return nil, err
	}
	aggs := trc.AggregateByProcedure(events)
	if db != nil && len(aggs) > 0 {
		q := query.New(db)
		enrichMap := trc.EnrichEvents(ctx, q, events)
		trc.EnrichAggregates(aggs, enrichMap)
	}
	return &ProceduresResult{
		Procedures: aggs,
		Count:      len(aggs),
	}, nil
}

// ExecuteTree строит деревья вызовов по SPID.
func ExecuteTree(ctx context.Context, db *store.DB, p TreeParams) (*TreeResult, error) {
	if p.Source.SessionID > 0 && db != nil {
		treeEvents, err := trc.LoadEventsForTree(ctx, db, p.Source.SessionID, p.SPID, p.MaxDepth, p.Limit)
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
