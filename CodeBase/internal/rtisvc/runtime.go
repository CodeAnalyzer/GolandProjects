package rtisvc

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/codebase/internal/query"
	"github.com/codebase/internal/rti"
	"github.com/codebase/internal/store"
	"github.com/codebase/internal/util"
)

// resolveSession загружает данные из БД (session_id > 0) или парсит файл.
// Возвращает calls, clientEvents, parseResult (для file-mode).
func resolveSession(ctx context.Context, db *store.DB, src SessionSource) (
	calls []*rti.RTICall,
	clientEvents []*rti.RTIClientEvent,
	parseResult *rti.RTIParseResult,
	err error,
) {
	if src.SessionID > 0 && db != nil {
		calls, err = rti.LoadCalls(ctx, db, src.SessionID)
		if err != nil {
			return nil, nil, nil, err
		}
		clientEvents, err = rti.LoadClientEvents(ctx, db, src.SessionID)
		if err != nil {
			return nil, nil, nil, err
		}
		return calls, clientEvents, nil, nil
	}
	if src.FilePath == "" {
		return nil, nil, nil, fmt.Errorf("either session_id or file_path is required")
	}
	parseResult, err = rti.ParseFile(src.FilePath)
	if err != nil {
		return nil, nil, nil, err
	}
	return parseResult.Calls, parseResult.ClientEvents, parseResult, nil
}

// toCallSlims преобразует []*RTICall в []RTICallSlim.
func toCallSlims(calls []*rti.RTICall) []RTICallSlim {
	if len(calls) == 0 {
		return nil
	}
	out := make([]RTICallSlim, len(calls))
	for i, c := range calls {
		out[i] = RTICallSlim{RTICall: c}
	}
	return out
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

// ExecuteParse парсит RTI-файл и сохраняет в БД (если db доступен).
// При повторном парсинге файла с тем же содержимым (SHA-256) той же версией
// парсера возвращается существующая сессия без повторного разбора.
func ExecuteParse(ctx context.Context, db *store.DB, filePath string, opts ParseOptions) (*ParseResult, error) {
	if filePath == "" {
		return nil, fmt.Errorf("file_path is required")
	}
	if db != nil {
		return executeParseDB(ctx, db, filePath, opts.Name)
	}
	result, err := rti.ParseFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to parse rti file: %w", err)
	}
	return &ParseResult{
		SessionID:  0,
		TotalCalls: result.Summary.TotalCalls,
		Summary:    result.Summary,
		Warning:    "database unavailable, session not saved",
	}, nil
}

// executeParseDB разбирает файл с сохранением в БД и reuse существующей
// сессии по ключу (file_hash, parser_version).
func executeParseDB(ctx context.Context, db *store.DB, filePath, explicitName string) (*ParseResult, error) {
	fileHash, err := util.FileHash(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to parse rti file: %w", err)
	}

	// Reuse: файл с тем же содержимым уже разобран этой версией парсера.
	// acquire атомарно инкрементирует use_count.
	if reusedID, useCount, err := rti.AcquireSessionByHash(ctx, db, fileHash, rti.ParserVersion); err != nil {
		return nil, err
	} else if reusedID > 0 {
		if err := renameOnReuse(ctx, db, reusedID, explicitName); err != nil {
			return nil, err
		}
		result, err := parseResultFromSession(ctx, db, reusedID)
		if err != nil {
			return nil, err
		}
		result.UseCount = useCount
		return result, nil
	}

	result, err := rti.ParseFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to parse rti file: %w", err)
	}
	sessionName := explicitName
	if sessionName == "" {
		sessionName = filepath.Base(filePath)
	}
	sessionID, err := rti.SaveSession(ctx, db, result, filePath, sessionName)
	if err != nil {
		return nil, fmt.Errorf("failed to save session: %w", err)
	}

	// Хеш и use_count = 1 проставляются последним шагом: прерванное сохранение
	// оставляет file_hash NULL — сессия не попадает в кэш reuse.
	if err := rti.SetSessionHash(ctx, db, sessionID, fileHash, rti.ParserVersion); err != nil {
		if !store.IsUniqueViolation(err) {
			return nil, err
		}
		// Гонка параллельных разборов одного файла: победитель уже записал
		// тот же ключ. Acquire инкрементирует use_count победителя (оба
		// вызова — держатели), проигравшую сессию удаляем явно (prune хранит
		// N свежих — проигравшая самая свежая).
		winnerID, winnerUse, findErr := rti.AcquireSessionByHash(ctx, db, fileHash, rti.ParserVersion)
		if findErr != nil {
			return nil, findErr
		}
		if winnerID > 0 {
			if delErr := rti.DeleteSession(ctx, db, sessionID); delErr != nil {
				return nil, fmt.Errorf("failed to delete duplicate rti session %d: %w", sessionID, delErr)
			}
			if err := renameOnReuse(ctx, db, winnerID, explicitName); err != nil {
				return nil, err
			}
			winnerResult, err := parseResultFromSession(ctx, db, winnerID)
			if err != nil {
				return nil, err
			}
			winnerResult.UseCount = winnerUse
			return winnerResult, nil
		}
		// Победитель удалён конкурентно между violation и повторным поиском:
		// один повтор записи хеша, затем ошибка.
		if retryErr := rti.SetSessionHash(ctx, db, sessionID, fileHash, rti.ParserVersion); retryErr != nil {
			return nil, retryErr
		}
	}

	return &ParseResult{
		SessionID:  sessionID,
		TotalCalls: result.Summary.TotalCalls,
		Summary:    result.Summary,
		UseCount:   1,
	}, nil
}

// renameOnReuse переименовывает найденную сессию, если имя передано явно.
func renameOnReuse(ctx context.Context, db *store.DB, sessionID int64, explicitName string) error {
	if explicitName == "" {
		return nil
	}
	session, err := rti.GetSession(ctx, db, sessionID)
	if err != nil {
		return err
	}
	if session.SessionName == explicitName {
		return nil
	}
	return rti.SetSessionName(ctx, db, sessionID, explicitName)
}

// parseResultFromSession собирает ParseResult по существующей сессии из БД
// (путь reuse: сводка эквивалентна первому парсингу).
func parseResultFromSession(ctx context.Context, db *store.DB, sessionID int64) (*ParseResult, error) {
	summary, err := rti.LoadSummary(ctx, db, sessionID)
	if err != nil {
		return nil, err
	}
	return &ParseResult{
		SessionID:  sessionID,
		TotalCalls: summary.TotalCalls,
		Summary:    *summary,
	}, nil
}

// ExecuteList возвращает список сессий из БД.
func ExecuteList(ctx context.Context, db *store.DB, limit int) (*ListResult, error) {
	if db == nil {
		return nil, fmt.Errorf("database not available")
	}
	sessions, err := rti.ListSessions(ctx, db, limit)
	if err != nil {
		return nil, err
	}
	return &ListResult{Sessions: sessions}, nil
}

// ExecuteSummary возвращает статистику по сессии.
func ExecuteSummary(ctx context.Context, db *store.DB, src SessionSource) (*SummaryResult, error) {
	if src.SessionID > 0 && db != nil {
		summary, err := rti.LoadSummary(ctx, db, src.SessionID)
		if err != nil {
			return nil, err
		}
		session, err := rti.GetSession(ctx, db, src.SessionID)
		if err != nil {
			return nil, err
		}
		return &SummaryResult{Summary: *summary, Session: session}, nil
	}
	_, _, parseResult, err := resolveSession(ctx, db, src)
	if err != nil {
		return nil, err
	}
	return &SummaryResult{Summary: parseResult.Summary}, nil
}

// ExecuteTree строит дерево вызовов.
func ExecuteTree(ctx context.Context, db *store.DB, p TreeParams) (*TreeResult, error) {
	var calls []*rti.RTICall
	if p.Source.SessionID > 0 && db != nil {
		var err error
		calls, err = rti.LoadCallsForTree(ctx, db, p.Source.SessionID, p.Procedure, p.MaxDepth, 5000)
		if err != nil {
			return nil, err
		}
	} else {
		_, _, parseResult, err := resolveSession(ctx, db, p.Source)
		if err != nil {
			return nil, err
		}
		calls = parseResult.Calls
	}

	tree := rti.BuildTree(calls, p.Procedure, p.MaxDepth)
	result := &TreeResult{Tree: tree}

	if db != nil && tree != nil {
		q := query.New(db)
		result.Enrichment = rti.EnrichCalls(ctx, q, calls)
	}

	return result, nil
}

// ExecuteErrors находит ошибки в сессии.
func ExecuteErrors(ctx context.Context, db *store.DB, p ErrorsParams) (*ErrorsResult, error) {
	limit := normalizeLimit(p.Limit)

	var errorCalls []*rti.RTICall
	var clientErrors []*rti.RTIClientEvent

	if p.Source.SessionID > 0 && db != nil {
		var err error
		errorCalls, err = rti.LoadErrorCalls(ctx, db, p.Source.SessionID, limit)
		if err != nil {
			return nil, err
		}
		clientErrors, err = rti.LoadClientErrors(ctx, db, p.Source.SessionID, limit)
		if err != nil {
			return nil, err
		}
	} else {
		calls, events, _, err := resolveSession(ctx, db, p.Source)
		if err != nil {
			return nil, err
		}
		for _, c := range calls {
			if c.RetVal != nil && *c.RetVal != 0 {
				errorCalls = append(errorCalls, c)
				if len(errorCalls) >= limit {
					break
				}
			}
		}
		for _, ev := range events {
			if ev.Kind == "error" && ev.ErrorText != "" {
				clientErrors = append(clientErrors, ev)
				if len(clientErrors) >= limit {
					break
				}
			}
		}
	}

	result := &ErrorsResult{
		ServerErrors:     toCallSlims(errorCalls),
		ServerErrorCount: len(errorCalls),
		ClientErrors:     clientErrors,
		ClientErrorCount: len(clientErrors),
		Limit:            limit,
	}

	if db != nil && (len(errorCalls) > 0 || len(clientErrors) > 0) {
		q := query.New(db)
		if len(errorCalls) > 0 {
			result.ServerEnrichment = rti.EnrichCalls(ctx, q, errorCalls)
			codes := make([]int64, 0, len(errorCalls))
			for _, c := range errorCalls {
				if c.RetVal != nil {
					codes = append(codes, int64(*c.RetVal))
				}
			}
			if len(codes) > 0 {
				retCodeMap, _ := db.LookupRetCodes(ctx, codes)
				for _, s := range result.ServerErrors {
					if s.RetVal != nil {
						if rc, ok := retCodeMap[int64(*s.RetVal)]; ok && rc != nil {
							s.RetValMeaning = rc.Message
							s.ErrorConstant = rc.ProcName
						}
					}
				}
			}
		}
		if len(clientErrors) > 0 {
			result.ClientEnrichment = rti.EnrichClientEvents(ctx, q, clientErrors)
		}
	}

	return result, nil
}

// ExecuteSlow находит медленные вызовы.
func ExecuteSlow(ctx context.Context, db *store.DB, p SlowParams) (*SlowResult, error) {
	threshold := p.ThresholdMs
	if threshold <= 0 {
		threshold = rti.GetSlowThresholdMs()
	}
	limit := normalizeLimit(p.Limit)

	var slowCalls []*rti.RTICall
	var slowClientSQL []*rti.RTIClientEvent

	if p.Source.SessionID > 0 && db != nil {
		var err error
		slowCalls, err = rti.LoadSlowCalls(ctx, db, p.Source.SessionID, threshold, limit)
		if err != nil {
			return nil, err
		}
		slowClientSQL, err = rti.LoadSlowClientSQL(ctx, db, p.Source.SessionID, threshold, limit)
		if err != nil {
			return nil, err
		}
	} else {
		calls, events, _, err := resolveSession(ctx, db, p.Source)
		if err != nil {
			return nil, err
		}
		for _, c := range calls {
			if c.ElapsedMs >= threshold {
				slowCalls = append(slowCalls, c)
			}
		}
		sort.Slice(slowCalls, func(i, j int) bool { return slowCalls[i].ElapsedMs > slowCalls[j].ElapsedMs })
		if len(slowCalls) > limit {
			slowCalls = slowCalls[:limit]
		}
		thresholdSec := float64(threshold) / 1000.0
		for _, ev := range events {
			if ev.Kind == "sql_block" && ev.SQL != nil && ev.SQL.DurationSec >= thresholdSec {
				slowClientSQL = append(slowClientSQL, ev)
			}
		}
		sort.Slice(slowClientSQL, func(i, j int) bool {
			return slowClientSQL[i].SQL.DurationSec > slowClientSQL[j].SQL.DurationSec
		})
		if len(slowClientSQL) > limit {
			slowClientSQL = slowClientSQL[:limit]
		}
	}

	result := &SlowResult{
		ServerCalls:     toCallSlims(slowCalls),
		ServerCallCount: len(slowCalls),
		ClientSQLBlocks: slowClientSQL,
		ClientSQLCount:  len(slowClientSQL),
		Threshold:       threshold,
		Limit:           limit,
	}

	if db != nil && (len(slowCalls) > 0 || len(slowClientSQL) > 0) {
		q := query.New(db)
		if len(slowCalls) > 0 {
			result.ServerEnrichment = rti.EnrichCalls(ctx, q, slowCalls)
		}
		if len(slowClientSQL) > 0 {
			result.ClientEnrichment = rti.EnrichClientEvents(ctx, q, slowClientSQL)
		}
	}

	return result, nil
}

// ExecuteDetails возвращает детали вызовов конкретной процедуры.
func ExecuteDetails(ctx context.Context, db *store.DB, p DetailsParams) (*DetailsResult, error) {
	limit := normalizeLimit(p.Limit)

	var calls []*rti.RTICall
	if p.Source.SessionID > 0 && db != nil {
		var err error
		calls, err = rti.LoadCallsByProcedure(ctx, db, p.Source.SessionID, p.Procedure, limit)
		if err != nil {
			return nil, err
		}
	} else {
		allCalls, _, _, err := resolveSession(ctx, db, p.Source)
		if err != nil {
			return nil, err
		}
		for _, c := range allCalls {
			if c.Procedure == p.Procedure {
				calls = append(calls, c)
				if len(calls) >= limit {
					break
				}
			}
		}
	}

	result := &DetailsResult{
		Procedure: p.Procedure,
		Calls:     calls,
		Count:     len(calls),
	}

	if db != nil && len(calls) > 0 {
		q := query.New(db)
		enrich, err := rti.EnrichProcedure(ctx, q, p.Procedure)
		if err == nil {
			result.Enrichment = enrich
		}
		codes := make([]int64, 0)
		for _, c := range calls {
			if c.RetVal != nil && *c.RetVal != 0 {
				codes = append(codes, int64(*c.RetVal))
			}
		}
		if len(codes) > 0 {
			retCodeMap, _ := db.LookupRetCodes(ctx, codes)
			for _, c := range calls {
				if c.RetVal != nil && *c.RetVal != 0 {
					if rc, ok := retCodeMap[int64(*c.RetVal)]; ok && rc != nil {
						c.RetValMeaning = rc.Message
						c.ErrorConstant = rc.ProcName
					}
				}
			}
		}
	}

	return result, nil
}

// ExecuteBlog возвращает business log для процедуры.
func ExecuteBlog(ctx context.Context, db *store.DB, p BlogParams) (*BlogResult, error) {
	limit := normalizeLimit(p.Limit)

	var calls []*rti.RTICall
	if p.Source.SessionID > 0 && db != nil {
		var err error
		calls, err = rti.LoadCallsByProcedure(ctx, db, p.Source.SessionID, p.Procedure, limit)
		if err != nil {
			return nil, err
		}
	} else {
		allCalls, _, _, err := resolveSession(ctx, db, p.Source)
		if err != nil {
			return nil, err
		}
		for _, c := range allCalls {
			if c.Procedure == p.Procedure {
				calls = append(calls, c)
				if len(calls) >= limit {
					break
				}
			}
		}
	}

	items := make([]BlogCallItem, 0, len(calls))
	for _, c := range calls {
		items = append(items, BlogCallItem{
			EnterLine:   c.EnterLine,
			ElapsedMs:   c.ElapsedMs,
			BLogBlocks:  c.BLogBlocks,
			Checkpoints: c.Checkpoints,
			BLogTables:  c.BLogTables,
		})
	}

	return &BlogResult{
		Procedure: p.Procedure,
		Count:     len(items),
		Calls:     items,
	}, nil
}

// ExecuteClientTree строит дерево клиентских событий.
func ExecuteClientTree(ctx context.Context, db *store.DB, p ClientTreeParams) (*ClientTreeResult, error) {
	limit := normalizeLimit(p.Limit)

	var events []*rti.RTIClientEvent
	if p.Source.SessionID > 0 && db != nil {
		var err error
		events, err = rti.LoadClientEventsFiltered(ctx, db, p.Source.SessionID, p.Filter, limit)
		if err != nil {
			return nil, err
		}
	} else {
		_, allEvents, _, err := resolveSession(ctx, db, p.Source)
		if err != nil {
			return nil, err
		}
		events = rti.FilterClientEvents(allEvents, p.Filter)
		if len(events) > limit {
			events = events[:limit]
		}
	}

	nodes := rti.BuildClientTree(events, 0)

	result := &ClientTreeResult{
		Nodes:               nodes,
		FilteredEventsCount: len(events),
		Limit:               limit,
	}

	if db != nil && len(events) > 0 {
		q := query.New(db)
		result.Enrichment = rti.EnrichClientEvents(ctx, q, events)
	}

	if strings.EqualFold(p.Filter.Format, "short") {
		shortNodes := make([]rti.RTIClientTreeNodeShort, 0, len(nodes))
		for _, n := range nodes {
			shortNodes = append(shortNodes, rti.ToShortClientTreeNode(n))
		}
		result.Nodes = shortNodes
	}

	return result, nil
}

// ExecuteTimeline возвращает единый timeline серверных и клиентских событий.
func ExecuteTimeline(ctx context.Context, db *store.DB, p TimelineParams) (*TimelineResult, error) {
	limit := normalizeLimit(p.Limit)

	var filteredCalls []*rti.RTICall
	var filteredEvents []*rti.RTIClientEvent

	if p.Source.SessionID > 0 && db != nil {
		var err error
		filteredCalls, err = rti.LoadTimelineCalls(ctx, db, p.Source.SessionID, p.Filter, limit)
		if err != nil {
			return nil, err
		}
		filteredEvents, err = rti.LoadTimelineClientEvents(ctx, db, p.Source.SessionID, p.Filter, limit)
		if err != nil {
			return nil, err
		}
	} else {
		calls, events, _, err := resolveSession(ctx, db, p.Source)
		if err != nil {
			return nil, err
		}
		filteredCalls, filteredEvents = rti.ApplyTimelineFilter(calls, events, p.Filter)
		if len(filteredCalls) > limit {
			filteredCalls = filteredCalls[:limit]
		}
		if len(filteredEvents) > limit {
			filteredEvents = filteredEvents[:limit]
		}
	}

	var clientEnrich map[string]*rti.ClientEnrichment
	if db != nil && len(filteredEvents) > 0 {
		q := query.New(db)
		clientEnrich = rti.EnrichClientEvents(ctx, q, filteredEvents)
	}

	var respCalls interface{} = filteredCalls
	var respEvents interface{} = filteredEvents
	if p.Filter.Format == "short" {
		shortCalls := make([]rti.RTICallShort, 0, len(filteredCalls))
		for _, c := range filteredCalls {
			shortCalls = append(shortCalls, rti.ToShortCall(c))
		}
		shortEvents := make([]rti.RTIClientEventShort, 0, len(filteredEvents))
		for _, e := range filteredEvents {
			shortEvents = append(shortEvents, rti.ToShortEvent(e))
		}
		respCalls = shortCalls
		respEvents = shortEvents
	}

	return &TimelineResult{
		Calls:               respCalls,
		ClientEvents:        respEvents,
		Enrichment:          clientEnrich,
		FilteredCallsCount:  len(filteredCalls),
		FilteredEventsCount: len(filteredEvents),
		Limit:               limit,
	}, nil
}

// ExecuteDelete удаляет сессию по ID либо все сессии с указанным именем.
func ExecuteDelete(ctx context.Context, db *store.DB, p DeleteParams) (*DeleteResult, error) {
	if p.SessionID > 0 && p.Name != "" {
		return nil, fmt.Errorf("session_id and name are mutually exclusive")
	}
	if p.SessionID <= 0 && p.Name == "" {
		return nil, fmt.Errorf("either session_id or name is required")
	}
	if db == nil {
		return nil, fmt.Errorf("database not available")
	}

	if p.SessionID > 0 {
		session, err := rti.GetSession(ctx, db, p.SessionID)
		if err != nil {
			return nil, fmt.Errorf("session %d not found: %w", p.SessionID, err)
		}
		if !p.Hard {
			// Общая сессия (use_count > 1): декремент, данные остаются.
			if remaining, ok, err := rti.ReleaseSession(ctx, db, p.SessionID); err != nil {
				return nil, err
			} else if ok {
				return &DeleteResult{
					Deleted:        false,
					Decremented:    true,
					SessionID:      p.SessionID,
					FilePath:       session.FilePath,
					DecrementedIDs: []int64{p.SessionID},
					UseCount:       remaining,
				}, nil
			}
		}
		if err := rti.DeleteSession(ctx, db, p.SessionID); err != nil {
			return nil, err
		}
		return &DeleteResult{
			Deleted:      true,
			SessionID:    p.SessionID,
			FilePath:     session.FilePath,
			DeletedCount: 1,
			SessionIDs:   []int64{p.SessionID},
		}, nil
	}

	ids, err := rti.FindSessionIDsByName(ctx, db, p.Name)
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, fmt.Errorf("session with name %q not found", p.Name)
	}
	var singleFilePath string
	if len(ids) == 1 {
		if session, err := rti.GetSession(ctx, db, ids[0]); err == nil {
			singleFilePath = session.FilePath
		}
	}
	var deletedIDs, decrementedIDs []int64
	for _, id := range ids {
		deleted, err := deleteOrRelease(ctx, db, id, p.Hard)
		if err != nil {
			return nil, err
		}
		if deleted {
			deletedIDs = append(deletedIDs, id)
		} else {
			decrementedIDs = append(decrementedIDs, id)
		}
	}
	result := &DeleteResult{
		Deleted:        len(deletedIDs) > 0,
		Decremented:    len(decrementedIDs) > 0,
		DeletedCount:   len(deletedIDs),
		SessionIDs:     deletedIDs,
		DecrementedIDs: decrementedIDs,
	}
	if len(ids) == 1 {
		result.SessionID = ids[0]
		result.FilePath = singleFilePath
		if len(decrementedIDs) == 1 {
			if session, err := rti.GetSession(ctx, db, ids[0]); err == nil {
				result.UseCount = session.UseCount
			}
		}
	}
	return result, nil
}

// deleteOrRelease обрабатывает одну сессию по правилам счётчика: без hard —
// декремент при use_count > 1 (deleted=false), иначе окончательное удаление
// (deleted=true).
func deleteOrRelease(ctx context.Context, db *store.DB, sessionID int64, hard bool) (bool, error) {
	if !hard {
		if _, ok, err := rti.ReleaseSession(ctx, db, sessionID); err != nil {
			return false, err
		} else if ok {
			return false, nil
		}
	}
	return true, rti.DeleteSession(ctx, db, sessionID)
}

// ExecutePrune удаляет старые сессии: последние KeepLast либо старше KeepHours.
func ExecutePrune(ctx context.Context, db *store.DB, p PruneParams) (*PruneResult, error) {
	if p.KeepLast != nil && p.KeepHours != nil {
		return nil, fmt.Errorf("keep_last and keep_hours are mutually exclusive")
	}
	if p.KeepHours != nil && *p.KeepHours <= 0 {
		return nil, fmt.Errorf("keep_hours must be > 0")
	}
	if p.KeepLast != nil && *p.KeepLast < 0 {
		return nil, fmt.Errorf("keep_last must be >= 0")
	}
	if db == nil {
		return nil, fmt.Errorf("database not available")
	}

	if p.KeepHours != nil {
		cutoff := time.Now().Add(-time.Duration(*p.KeepHours) * time.Hour)
		deleted, err := rti.PruneSessionsOlderThan(ctx, db, cutoff)
		if err != nil {
			return nil, err
		}
		return &PruneResult{
			DeletedCount: int(deleted),
			KeptHours:    p.KeepHours,
			Cutoff:       &cutoff,
		}, nil
	}

	keepLast := 0
	if p.KeepLast != nil {
		keepLast = *p.KeepLast
	}
	if keepLast < 0 {
		return nil, fmt.Errorf("keep_last must be >= 0")
	}
	deleted, err := rti.PruneSessions(ctx, db, keepLast)
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
