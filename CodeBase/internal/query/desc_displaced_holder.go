package query

import (
	"context"
	"fmt"
	"sync"
)

// descDisplacedHolder — кэш множества «вытесненных» процедур semantic-слоя
// desc-поиска (процедуры, реализующие контракты с непустым описанием): per-
// call SQL (~1 c на вытесняющих контрактах) заменяется кешем по ключу
// «поколение модели + последний завершённый scan_run id (эпоха индексации)»
// (change optimize-query-search-bottlenecks, B8/D5). Поколение одно не
// маркирует данные: переобучение может пропускаться (fingerprint неизменен)
// или откладываться (corpusDelta ниже порога), а relations и описания
// контрактов меняются каждым update — любой завершённый прогон создаёт новый
// scan_run. Актуальность проверяется на каждом semantic-вызове дёшево:
// MAX(id) по завершённым scan_run (PK; статусы completed/
// completed_with_errors — незавершённый running-прогон не эпоха, иначе вызов
// во время update привязал бы частичный снимок к будущей эпохе) +
// существующая проверка поколения в descSemanticHolder. Загрузка
// сериализована (singleflight) по образцу descSemanticHolder.
type descDisplacedHolder struct {
	mu      sync.RWMutex
	gen     string
	scanRun int64
	ids     map[int64]bool

	loadMu sync.Mutex
	loads  int // число перевычислений множества (диагностика/тесты)
}

// descDisplacedCache — глобальный держатель кеша displaced-множества
// (Query создаётся на каждый вызов инструмента, кеш — на процесс).
var descDisplacedCache descDisplacedHolder

func (h *descDisplacedHolder) snapshot() (string, int64, map[int64]bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.gen, h.scanRun, h.ids
}

func (h *descDisplacedHolder) store(gen string, scanRun int64, ids map[int64]bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.gen = gen
	h.scanRun = scanRun
	h.ids = ids
	h.loads++
}

// loadCount возвращает число выполненных перевычислений (для тестов).
func (h *descDisplacedHolder) loadCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.loads
}

// reset сбрасывает кеш и счётчик загрузок (для тестов).
func (h *descDisplacedHolder) reset() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.gen, h.scanRun, h.ids = "", 0, nil
	h.loads = 0
}

// freshLocked проверяет закешированное множество против текущей эпохи.
// Вызывается под loadMu (или до его взятия — тогда нестрого, двойная проверка).
func (h *descDisplacedHolder) fresh(ctx context.Context, q *Query, gen string) (bool, error) {
	cachedGen, cachedRun, ids := h.snapshot()
	if ids == nil || cachedGen != gen {
		return false, nil
	}
	latest, err := q.db.MaxScanRunID(ctx)
	if err != nil {
		return false, fmt.Errorf("check displaced epoch: %w", err)
	}
	return latest == cachedRun, nil
}

// get возвращает актуальное множество вытесненных процедур для поколения
// gen. Промах — единственный перевычислитель (singleflight); гонка с
// создающимся scan_run закрывается проверкой эпохи до и после выборки
// (при расхождении — один повтор). Возвращаемая карта — shared-снимок:
// потребители только читают, не мутируют (копия не делается намеренно,
// набор read-only по всему использованию).
func (h *descDisplacedHolder) get(ctx context.Context, q *Query, gen string) (map[int64]bool, error) {
	if fresh, err := h.fresh(ctx, q, gen); err != nil {
		return nil, err
	} else if fresh {
		_, _, ids := h.snapshot()
		return ids, nil
	}

	h.loadMu.Lock()
	defer h.loadMu.Unlock()

	if fresh, err := h.fresh(ctx, q, gen); err != nil {
		return nil, err
	} else if fresh {
		_, _, ids := h.snapshot()
		return ids, nil
	}

	for attempt := 0; attempt < 2; attempt++ {
		before, err := q.db.MaxScanRunID(ctx)
		if err != nil {
			return nil, fmt.Errorf("check displaced epoch: %w", err)
		}
		ids, err := q.loadDisplacedIDs(ctx)
		if err != nil {
			return nil, err
		}
		after, err := q.db.MaxScanRunID(ctx)
		if err != nil {
			return nil, fmt.Errorf("check displaced epoch: %w", err)
		}
		if before == after {
			h.store(gen, after, ids)
			return ids, nil
		}
		// Между выборкой и проверкой появился новый scan_run — состав мог
		// быть частично новым: повторяем один раз.
	}
	return nil, fmt.Errorf("desc displaced: scan epoch keeps changing")
}

// loadDisplacedIDs выполняет выборку вытесненных процедур — процедуры,
// реализующие контракты с непустым описанием (лёгкий запрос, ~тысячи строк).
func (q *Query) loadDisplacedIDs(ctx context.Context) (map[int64]bool, error) {
	displaced := make(map[int64]bool)
	rows, err := q.db.QueryContext(ctx, `
		SELECT r.source_id
		FROM relations r
		JOIN api_contracts dc ON dc.id = r.target_id AND r.target_type = 'api_contract'
		WHERE r.source_type = 'sql_procedure'
		  AND r.relation_type = 'implements_contract'
		  AND coalesce(dc.short_description, '') || coalesce(dc.full_description, '') <> ''`)
	if err != nil {
		return nil, fmt.Errorf("desc semantic displaced: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("desc semantic displaced scan: %w", err)
		}
		displaced[id] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("desc semantic displaced rows: %w", err)
	}
	return displaced, nil
}
