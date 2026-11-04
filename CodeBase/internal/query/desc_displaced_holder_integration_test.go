//go:build integration

package query

import (
	"context"
	"testing"
)

// TestDescriptionsDisplacedCache — кеш displaced-множества по ключу
// «поколение модели + эпоха индексации» (change optimize-query-search-
// bottlenecks, B8/D5):
//  1. повторные semantic-вызовы не перевычисляют множество (одна загрузка);
//  2. update без переобучения модели (новый scan_run, новые контракты)
//     перевычисляет кеш — окно устаревания из ревью F1 закрыто;
//  3. смена поколения перевычисляет;
//  4. состав кеша идентичен per-call выборке.
func TestDescriptionsDisplacedCache(t *testing.T) {
	db, ctx := setupDescLSAEnv(t)
	trainDescLSAForTest(t, db)
	descSemanticCache.reset()
	descDisplacedCache.reset()
	t.Cleanup(func() {
		descSemanticCache.reset()
		descDisplacedCache.reset()
	})
	q := New(db)

	var displacedProcID int64
	if err := db.QueryRow(`SELECT id FROM sql_procedures WHERE proc_name = 'API_CCred_BindClassifier'`).Scan(&displacedProcID); err != nil {
		t.Fatalf("read displaced proc: %v", err)
	}

	// 1. Два semantic-вызова — выборка вытеснения выполняется один раз.
	out1, err := q.SearchDescriptions(ctx, "привязка кредитных договоров", nil, 10)
	if err != nil {
		t.Fatalf("SearchDescriptions #1: %v", err)
	}
	loadsAfterFirst := descDisplacedCache.loadCount()
	if loadsAfterFirst != 1 {
		t.Fatalf("displaced loads after first call = %d, want 1", loadsAfterFirst)
	}
	if _, err := q.SearchDescriptions(ctx, "привязка кредитных договоров", nil, 10); err != nil {
		t.Fatalf("SearchDescriptions #2: %v", err)
	}
	if got := descDisplacedCache.loadCount(); got != loadsAfterFirst {
		t.Fatalf("displaced loads after second call = %d, want %d (cache reuse)", got, loadsAfterFirst)
	}
	// Вытесненная процедура не попадает в semantic-выдачу (контракт остаётся).
	for _, item := range out1.Items {
		if item.Source == "semantic" && item.Kind == "procedure" && item.Name == "API_CCred_BindClassifier" {
			t.Fatalf("displaced procedure leaked into semantic results: %+v", item)
		}
	}

	// 4. Состав кеша идентичен per-call выборке.
	_, _, cached := descDisplacedCache.snapshot()
	perCall, err := q.loadDisplacedIDs(ctx)
	if err != nil {
		t.Fatalf("loadDisplacedIDs: %v", err)
	}
	if len(cached) != len(perCall) {
		t.Fatalf("cached displaced = %d, per-call = %d", len(cached), len(perCall))
	}
	for id := range perCall {
		if !cached[id] {
			t.Fatalf("cached set misses per-call id %d", id)
		}
	}
	if !cached[displacedProcID] {
		t.Fatalf("cached set misses fixture displaced proc %d", displacedProcID)
	}

	// 2. Update без переобучения модели: новый завершённый scan_run + новая
	// вытесняемая пара (процедура → контракт с описанием) — кеш обязан
	// перевычислиться.
	var scanID int64
	if err := db.QueryRow(`INSERT INTO scan_runs (root_path, status) VALUES ('repo', 'completed') RETURNING id`).Scan(&scanID); err != nil {
		t.Fatalf("insert scan_run: %v", err)
	}
	var extraFileID int64
	if err := db.QueryRow(`SELECT id FROM files WHERE rel_path = 'Server/Misc/Unrelated_Proc.sql'`).Scan(&extraFileID); err != nil {
		t.Fatalf("read file: %v", err)
	}
	var newProcID int64
	if err := db.QueryRow(`
		INSERT INTO sql_procedures (file_id, proc_name, line_start, line_end, description)
		VALUES ($1, 'API_TEST_Displaced2', 1, 2, 'тестовое описание возврата денежных средств') RETURNING id
	`, extraFileID).Scan(&newProcID); err != nil {
		t.Fatalf("insert proc: %v", err)
	}
	var newContractID int64
	if err := db.QueryRow(`
		INSERT INTO api_contracts (file_id, contract_name, contract_kind, short_description, full_description, line_start, line_end)
		VALUES ($1, 'API_TEST_Displaced2', 'service', 'возврат денежных средств', '', 1, 1) RETURNING id
	`, extraFileID).Scan(&newContractID); err != nil {
		t.Fatalf("insert contract: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO relations (source_type, source_id, target_type, target_id, relation_type)
		VALUES ('sql_procedure', $1, 'api_contract', $2, 'implements_contract')
	`, newProcID, newContractID); err != nil {
		t.Fatalf("insert relation: %v", err)
	}

	if _, err := q.SearchDescriptions(ctx, "привязка кредитных договоров", nil, 10); err != nil {
		t.Fatalf("SearchDescriptions after update: %v", err)
	}
	if got := descDisplacedCache.loadCount(); got != loadsAfterFirst+1 {
		t.Fatalf("displaced loads after data-only update = %d, want %d (epoch invalidation)", got, loadsAfterFirst+1)
	}
	_, _, cachedAfterUpdate := descDisplacedCache.snapshot()
	if !cachedAfterUpdate[newProcID] {
		t.Fatalf("updated cache misses new displaced proc %d", newProcID)
	}

	// 3. Смена поколения перевычисляет кеш (модель переобучена — другой gen).
	if _, err := descDisplacedCache.get(context.Background(), q, "another-generation"); err != nil {
		t.Fatalf("get with new generation: %v", err)
	}
	if got := descDisplacedCache.loadCount(); got != loadsAfterFirst+2 {
		t.Fatalf("displaced loads after generation change = %d, want %d", got, loadsAfterFirst+2)
	}

	// 4. Незавершённый прогон — не эпоха (ревью-фикс: semantic-вызов во время
	// running-прогона не привязывает частичный снимок к будущей эпохе).
	// Сначала возвращаем кеш на реальное поколение модели (фаза 3 закешировала
	// фейковое — первый вызов с реальным поколением перезагружает).
	if _, err := q.SearchDescriptions(ctx, "привязка кредитных договоров", nil, 10); err != nil {
		t.Fatalf("SearchDescriptions back to real generation: %v", err)
	}
	runningLoads := descDisplacedCache.loadCount()
	// scan_run в статусе running не меняет MAX(id) завершённых и не
	// инвалидирует кеш; после завершения прогона (status→completed) кеш
	// перевычисляется.
	if _, err := db.Exec(`INSERT INTO scan_runs (root_path, status) VALUES ('repo', 'running')`); err != nil {
		t.Fatalf("insert running scan_run: %v", err)
	}
	if _, err := q.SearchDescriptions(ctx, "привязка кредитных договоров", nil, 10); err != nil {
		t.Fatalf("SearchDescriptions during running update: %v", err)
	}
	if got := descDisplacedCache.loadCount(); got != runningLoads {
		t.Fatalf("running scan_run must not invalidate cache: loads = %d, want %d", got, runningLoads)
	}
	if _, err := db.Exec(`UPDATE scan_runs SET status = 'completed' WHERE status = 'running'`); err != nil {
		t.Fatalf("complete running scan_run: %v", err)
	}
	if _, err := q.SearchDescriptions(ctx, "привязка кредитных договоров", nil, 10); err != nil {
		t.Fatalf("SearchDescriptions after completion: %v", err)
	}
	if got := descDisplacedCache.loadCount(); got != runningLoads+1 {
		t.Fatalf("completed scan_run must invalidate cache: loads = %d, want %d", got, runningLoads+1)
	}
}
