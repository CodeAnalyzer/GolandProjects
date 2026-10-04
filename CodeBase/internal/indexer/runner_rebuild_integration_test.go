//go:build integration

package indexer

import (
	"testing"
)

func countRowsIdx(t *testing.T, idx *Indexer, table string) int {
	t.Helper()
	var n int
	if err := idx.db.QueryRow(`SELECT count(*) FROM ` + table).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return n
}

// TestUpdateFullRebuild_SingleInstancesAndHistory — повторная пересборка не
// создаёт дублей, история scan_runs сохраняется, pre-filter отключён.
func TestUpdateFullRebuild_SingleInstancesAndHistory(t *testing.T) {
	idx, root := newTestIndexer(t)

	// «Старая» история прогонов до пересборки.
	if _, err := idx.db.Exec(`INSERT INTO scan_runs (root_path, status) VALUES ('repo-history', 'completed')`); err != nil {
		t.Fatalf("insert history scan_run: %v", err)
	}

	stats1, err := idx.Update(root, false, 1)
	if err != nil {
		t.Fatalf("rebuild #1: %v", err)
	}
	if stats1.PreFilteredFiles != 0 {
		t.Fatalf("rebuild #1 PreFilteredFiles = %d, want 0 (pre-filter off)", stats1.PreFilteredFiles)
	}

	baselineFiles := countRowsIdx(t, idx, "files")
	baselineProcs := countRowsIdx(t, idx, "sql_procedures")
	baselineSymbols := countRowsIdx(t, idx, "symbols")
	baselineSQLCalls := countRelation(t, idx, "calls_procedure", "sql_procedure")
	baselineJSCalls := countRelation(t, idx, "calls_procedure", "js_function")

	stats2, err := idx.Update(root, false, 1)
	if err != nil {
		t.Fatalf("rebuild #2: %v", err)
	}
	if stats2.PreFilteredFiles != 0 {
		t.Fatalf("rebuild #2 PreFilteredFiles = %d, want 0 (pre-filter off)", stats2.PreFilteredFiles)
	}

	for _, tc := range []struct {
		table string
		want  int
	}{
		{"files", baselineFiles},
		{"sql_procedures", baselineProcs},
		{"symbols", baselineSymbols},
	} {
		if got := countRowsIdx(t, idx, tc.table); got != tc.want {
			t.Fatalf("%s after rebuild #2 = %d, want %d (no duplicates)", tc.table, got, tc.want)
		}
	}
	if got := countRelation(t, idx, "calls_procedure", "sql_procedure"); got != baselineSQLCalls {
		t.Fatalf("sql calls after rebuild #2 = %d, want %d", got, baselineSQLCalls)
	}
	if got := countRelation(t, idx, "calls_procedure", "js_function"); got != baselineJSCalls {
		t.Fatalf("js calls after rebuild #2 = %d, want %d", got, baselineJSCalls)
	}

	// История: строка «до пересборки» + два rebuild-прогона.
	if got := countRowsIdx(t, idx, "scan_runs"); got != 3 {
		t.Fatalf("scan_runs = %d, want 3 (history preserved)", got)
	}
	var historyAlive bool
	if err := idx.db.QueryRow(`SELECT EXISTS(SELECT 1 FROM scan_runs WHERE root_path = 'repo-history')`).Scan(&historyAlive); err != nil {
		t.Fatalf("check history scan_run: %v", err)
	}
	if !historyAlive {
		t.Fatal("pre-rebuild scan_runs history row was removed")
	}
}

// TestUpdateFullRebuild_InterruptedResumedByRebuild — сценарий «Прерванная
// пересборка возобновляется повторным rebuild». Имитация прерванной пересборки:
// сущности обработанного подмножества на месте, у необработанных файлов строк
// нет (каскадное удаление), relations-граф пуст (постобработка не выполнялась).
func TestUpdateFullRebuild_InterruptedResumedByRebuild(t *testing.T) {
	idx, root := newTestIndexer(t)

	if _, err := idx.Update(root, false, 1); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	baselineFiles := countRowsIdx(t, idx, "files")
	baselineProcs := countRowsIdx(t, idx, "sql_procedures")
	baselineSQLCalls := countRelation(t, idx, "calls_procedure", "sql_procedure")
	baselineJSCalls := countRelation(t, idx, "calls_procedure", "js_function")

	// Имитация прерывания: a.sql не успел обработаться, relations ещё не строились.
	if _, err := idx.db.Exec(`DELETE FROM relations`); err != nil {
		t.Fatalf("wipe relations: %v", err)
	}
	if _, err := idx.db.Exec(`DELETE FROM files WHERE rel_path IN ('a.sql')`); err != nil {
		t.Fatalf("delete unprocessed file rows: %v", err)
	}

	// Повторный rebuild: полный рестарт, итог эквивалентен непрерывной пересборке.
	stats, err := idx.Update(root, false, 1)
	if err != nil {
		t.Fatalf("rebuild after interruption: %v", err)
	}
	if stats.PreFilteredFiles != 0 {
		t.Fatalf("rebuild after interruption PreFilteredFiles = %d, want 0", stats.PreFilteredFiles)
	}
	if got := countRowsIdx(t, idx, "files"); got != baselineFiles {
		t.Fatalf("files after re-rebuild = %d, want %d (no duplicates)", got, baselineFiles)
	}
	if got := countRowsIdx(t, idx, "sql_procedures"); got != baselineProcs {
		t.Fatalf("sql_procedures after re-rebuild = %d, want %d (no duplicates)", got, baselineProcs)
	}
	if got := countRelation(t, idx, "calls_procedure", "sql_procedure"); got != baselineSQLCalls {
		t.Fatalf("sql calls after re-rebuild = %d, want %d", got, baselineSQLCalls)
	}
	if got := countRelation(t, idx, "calls_procedure", "js_function"); got != baselineJSCalls {
		t.Fatalf("js calls after re-rebuild = %d, want %d", got, baselineJSCalls)
	}
}

// TestUpdateFullRebuild_IncrementalDoesNotRestoreRelations — документирующая
// проверка ограничения из спеки: инкрементальный run после прерванной пересборки
// восстанавливает сущности файлов, но не relations-граф пропущенных файлов
// (pending-ссылки постпроцессоров живут в памяти и не переживают перезапуск).
func TestUpdateFullRebuild_IncrementalDoesNotRestoreRelations(t *testing.T) {
	idx, root := newTestIndexer(t)

	if _, err := idx.Update(root, false, 1); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	baselineFiles := countRowsIdx(t, idx, "files")
	baselineSQLCalls := countRelation(t, idx, "calls_procedure", "sql_procedure")

	// Имитация прерывания: a.sql не обработан, relations пусты.
	if _, err := idx.db.Exec(`DELETE FROM relations`); err != nil {
		t.Fatalf("wipe relations: %v", err)
	}
	if _, err := idx.db.Exec(`DELETE FROM files WHERE rel_path IN ('a.sql')`); err != nil {
		t.Fatalf("delete unprocessed file rows: %v", err)
	}

	// Инкрементальный run: сущности восстановлены, relations-граф — нет.
	stats, err := idx.Update(root, true, 1)
	if err != nil {
		t.Fatalf("incremental resume: %v", err)
	}
	if stats.PreFilteredFiles == 0 {
		t.Fatal("incremental resume did not pre-filter already processed files")
	}
	if got := countRowsIdx(t, idx, "files"); got != baselineFiles {
		t.Fatalf("files after incremental resume = %d, want %d", got, baselineFiles)
	}
	if got := countRelation(t, idx, "calls_procedure", "sql_procedure"); got != baselineSQLCalls {
		t.Fatalf("sql calls of re-parsed file = %d, want %d (own pending is re-accumulated)", got, baselineSQLCalls)
	}
	if got := countRelation(t, idx, "calls_procedure", "js_function"); got != 0 {
		t.Fatalf("js calls after incremental resume = %d, want 0 (pre-filtered file's relations are not rebuilt)", got)
	}
}
