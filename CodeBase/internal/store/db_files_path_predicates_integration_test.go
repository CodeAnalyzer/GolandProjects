//go:build integration

package store_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/codebase/internal/store"
	"github.com/codebase/internal/store/testutil"
)

// seedFiles вставляет строки files и возвращает их id (path — как есть).
func seedFiles(t *testing.T, db *store.DB, paths ...string) {
	t.Helper()
	var scanID int64
	if err := db.QueryRow(`INSERT INTO scan_runs (root_path, status) VALUES ('/seed', 'done') RETURNING id`).Scan(&scanID); err != nil {
		t.Fatalf("insert scan run: %v", err)
	}
	for _, p := range paths {
		if _, err := db.Exec(`
			INSERT INTO files (scan_run_id, path, rel_path, extension, hash_sha256, modified_at, encoding, language)
			VALUES ($1, $2, $2, '.sql', 'h', NOW(), '', '')
		`, scanID, p); err != nil {
			t.Fatalf("insert file %s: %v", p, err)
		}
	}
}

func filePaths(t *testing.T, db *store.DB, rootPath string) map[string]bool {
	t.Helper()
	files, err := db.GetLatestFilesByRootPath(context.Background(), rootPath)
	if err != nil {
		t.Fatalf("GetLatestFilesByRootPath(%q): %v", rootPath, err)
	}
	set := make(map[string]bool, len(files))
	for p := range files {
		set[p] = true
	}
	return set
}

// TestRootPredicates_EscapedLikeScope (change update-startup-visibility):
// экранированный LIKE-префикс матчит только детей корня; корень с `%` и `_`
// не перематчивает sibling-каталоги (старый GetLatestFilesByRootPath
// подставлял корень без экранирования), ветка «path = root» мертва и убрана.
func TestRootPredicates_EscapedLikeScope(t *testing.T) {
	db := testutil.Open(t)

	root := `D:/repo_x/100%`
	under := []string{
		root + `/a.sql`,
		root + `/sub/b.sql`,
	}
	// Ловушки: % перематчивает «100<anything>», _ — любой символ.
	// Не должны попасть в результат.
	traps := []string{
		`D:/repo_x/100ABC/trap.sql`, // % без экранирования съел бы 100ABC
		`D:/repoYx/trap.sql`,        // _ без экранирования съел бы _
		root + `X/trap.sql`,         // % без экранирования съел бы / и хвост
	}
	seedFiles(t, db, append(append([]string{}, under...), traps...)...)

	got := filePaths(t, db, root)
	for _, p := range under {
		if !got[p] {
			t.Errorf("файл %s не попал в выборку корня (экранирование сломало матч?)", p)
		}
	}
	for _, p := range traps {
		if got[p] {
			t.Errorf("ловушка %s перематчилась корнем %q (нет экранирования LIKE)", p, root)
		}
	}
	if len(got) != len(under) {
		t.Fatalf("выборка корня = %d файлов, want %d: %v", len(got), len(under), got)
	}

	// FindDuplicateFileGenerations: дубликаты ищутся только внутри корня.
	// Второе поколение одного пути под корнем + дубль-ловушка снаружи.
	seedFiles(t, db, root+`/a.sql`, `D:/outside/a.sql`)
	seedFiles(t, db, `D:/outside/a.sql`)
	keepIDs, oldIDs, err := db.FindDuplicateFileGenerations(context.Background(), root)
	if err != nil {
		t.Fatalf("FindDuplicateFileGenerations: %v", err)
	}
	if len(keepIDs) != 1 {
		t.Fatalf("duplicate paths = %v, want ровно [корень/a.sql]", keysOf(keepIDs))
	}
	if _, ok := keepIDs[root+`/a.sql`]; !ok {
		t.Fatalf("keepIDs = %v, want %s", keysOf(keepIDs), root+"/a.sql")
	}
	if olds := oldIDs[root+`/a.sql`]; len(olds) != 1 {
		t.Fatalf("oldIDs[%s] = %v, want 1 старое поколение", root+`/a.sql`, olds)
	}
}

func keysOf(m map[string]int64) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// TestBackfillFilesFromSources_LikePredicate — sargable LIKE-предикат
// backfill'а эквивалентен прежнему left()-предикату: привязываются только
// строки под префиксом источника с границей разделителя; спецсимволы
// префикса литеральны; обратный слэш — тоже граница.
func TestBackfillFilesFromSources_LikePredicate(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()

	var productID int64
	if err := db.QueryRow(`INSERT INTO ds_products (product_name) VALUES ('fa-test') RETURNING id`).Scan(&productID); err != nil {
		t.Fatalf("insert product: %v", err)
	}
	var branchID int64
	if err := db.QueryRow(`
		INSERT INTO ds_products_branch (ds_product_id, branch_key, branch_name, path, commit_sha)
		VALUES ($1, 'develop', 'develop', '/src', 'sha') RETURNING id
	`, productID).Scan(&branchID); err != nil {
		t.Fatalf("insert branch: %v", err)
	}

	prefix := `D:/src_100%/fa-test` // спецсимволы в имени каталога источника
	under := []string{
		prefix + `/Proc.sql`,
		prefix + `/sub/Other.sql`,
	}
	traps := []string{
		`D:/src_100%/fa-testX/trap.sql`, // % съел бы границу
		`D:/srcX100Y/fa-test/trap.sql`,  // _ и % без экранирования
		`D:/src_100%/fa-tes/trap.sql`,   // sibling-префикс (короче)
		prefix + `-extra/trap.sql`,      // префикс без границы разделителя
	}
	var all []string
	all = append(all, under...)
	all = append(all, traps...)
	seedFiles(t, db, all...)

	n, err := db.BackfillFilesFromSources(ctx, []store.FileSourceBinding{
		{Path: prefix, ProductID: productID, BranchID: branchID},
	})
	if err != nil {
		t.Fatalf("BackfillFilesFromSources: %v", err)
	}
	if n != int64(len(under)) {
		t.Fatalf("backfilled = %d, want %d", n, len(under))
	}
	for _, p := range under {
		var pid, bid int64
		if err := db.QueryRow(`SELECT ds_product_id, ds_product_branch_id FROM files WHERE path = $1`, p).Scan(&pid, &bid); err != nil {
			t.Fatalf("row %s: %v", p, err)
		}
		if pid != productID || bid != branchID {
			t.Fatalf("row %s привязана к (%d,%d), want (%d,%d)", p, pid, bid, productID, branchID)
		}
	}
	for _, p := range traps {
		var nulls int
		if err := db.QueryRow(`SELECT (ds_product_id IS NULL)::int + (ds_product_branch_id IS NULL)::int FROM files WHERE path = $1`, p).Scan(&nulls); err != nil {
			t.Fatalf("row %s: %v", p, err)
		}
		if nulls != 2 {
			t.Fatalf("ловушка %s привязана источником (nulls=%d, want 2)", p, nulls)
		}
	}

	// Обратный слэш — тоже граница (симметрия с прежним substring-предикатом).
	bsPath := `D:/src_100%/fa-test\Backslash.sql`
	seedFiles(t, db, bsPath)
	n, err = db.BackfillFilesFromSources(ctx, []store.FileSourceBinding{
		{Path: prefix, ProductID: productID, BranchID: branchID},
	})
	if err != nil {
		t.Fatalf("BackfillFilesFromSources (backslash): %v", err)
	}
	if n != 1 {
		t.Fatalf("backfilled (backslash boundary) = %d, want 1", n)
	}
}

// TestIdxFilesPath_MigrationV6 — bump схемы v5→v6: существующая БД (маркер
// версии сброшен, индекс удалён — эмуляция v5) получает idx_files_path
// штатным InitSchema; opclass — text_pattern_ops.
func TestIdxFilesPath_MigrationV6(t *testing.T) {
	db := testutil.Open(t)

	if _, err := db.Exec(`DROP INDEX IF EXISTS idx_files_path`); err != nil {
		t.Fatalf("drop index: %v", err)
	}
	testutil.ResetSchemaVersion(t, db)
	if err := db.InitSchema(); err != nil {
		t.Fatalf("InitSchema (миграция v6): %v", err)
	}

	var indexdef string
	err := db.QueryRow(`
		SELECT indexdef FROM pg_indexes
		WHERE indexname = 'idx_files_path' AND tablename = 'files'
	`).Scan(&indexdef)
	if err != nil {
		t.Fatalf("idx_files_path не создан миграцией v6: %v", err)
	}
	if !strings.Contains(strings.ToLower(indexdef), "text_pattern_ops") {
		t.Fatalf("idx_files_path indexdef = %q, want opclass text_pattern_ops", indexdef)
	}
}

// TestIdxFilesPath_ExplainUsesIndex — структурная EXPLAIN-проверка: при
// отключённом seq scan узкопрефиксный LIKE-запрос обслуживается
// idx_files_path (доступность индекса планировщику, а не выбор по стоимости
// — на малых данных планировщик волен предпочесть Seq Scan).
func TestIdxFilesPath_ExplainUsesIndex(t *testing.T) {
	db := testutil.Open(t)

	// ~120 строк вне префикса, 2 внутри.
	paths := make([]string, 0, 122)
	for i := 0; i < 120; i++ {
		paths = append(paths, fmt.Sprintf("D:/bulk/dir%03d/file.sql", i))
	}
	paths = append(paths, "D:/bulk/narrow/a.sql", "D:/bulk/narrow/b.sql")
	seedFiles(t, db, paths...)

	if _, err := db.Exec(`SET enable_seqscan = off`); err != nil {
		t.Fatalf("SET enable_seqscan: %v", err)
	}
	defer func() { _, _ = db.Exec(`SET enable_seqscan = on`) }()

	var plan strings.Builder
	rows, err := db.Query(`
		EXPLAIN SELECT id FROM files WHERE path LIKE 'D:/bulk/narrow/%'
	`)
	if err != nil {
		t.Fatalf("EXPLAIN: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatalf("scan plan line: %v", err)
		}
		plan.WriteString(line)
		plan.WriteString("\n")
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("plan rows: %v", err)
	}

	if !strings.Contains(plan.String(), "idx_files_path") {
		t.Fatalf("план узкопрефиксного LIKE не использует idx_files_path:\n%s", plan.String())
	}
	if !strings.Contains(plan.String(), "Index") {
		t.Fatalf("план не содержит Index Scan/Bitmap Index Scan:\n%s", plan.String())
	}
}

// TestBackfillFilesFromSources_PlanNoIndexRange — канарейка плана
// production-формы bulk-backfill (code review update-startup-visibility, п.1):
// один UPDATE с N-строчным VALUES-джойном не использует idx_files_path — PG
// не извлекает индексный диапазон из LIKE-паттерна, вычисляемого из колонки
// джойна (для text_pattern_ops нужен план-константный префикс). Спека
// database-schema допускает для backfill Seq Scan (bulk-операция, префиксы
// источников покрывают всё дерево). Проверка при enable_seqscan=off: если бы
// диапазон был доступен планировщику, он взял бы индекс. Если будущий PG
// научится извлекать диапазон из VALUES-джойна — тест упадёт и спеку можно
// будет ужесточить.
func TestBackfillFilesFromSources_PlanNoIndexRange(t *testing.T) {
	db := testutil.Open(t)

	if _, err := db.Exec(`SET enable_seqscan = off`); err != nil {
		t.Fatalf("SET enable_seqscan: %v", err)
	}
	defer func() { _, _ = db.Exec(`SET enable_seqscan = on`) }()

	// Параметры для EXPLAIN без ANALYZE не выполняются — значения важны
	// только типами (text, bigint, bigint): структура плана от них не зависит.
	rows, err := db.Query(
		"EXPLAIN (COSTS OFF) "+store.BackfillFilesFromSourcesSQL(2),
		"D:/src/alpha", int64(1), int64(1), "D:/src/beta", int64(2), int64(2),
	)
	if err != nil {
		t.Fatalf("EXPLAIN production-формы backfill: %v", err)
	}
	defer rows.Close()
	var plan strings.Builder
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatalf("scan plan line: %v", err)
		}
		plan.WriteString(line)
		plan.WriteString("\n")
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("plan rows: %v", err)
	}

	if !strings.Contains(plan.String(), "Update on files") {
		t.Fatalf("план не является UPDATE по files:\n%s", plan.String())
	}
	if strings.Contains(plan.String(), "idx_files_path") {
		t.Fatalf("production-форма backfill использует idx_files_path — планировщик научился извлекать диапазон из VALUES-джойна, ужесточить спеку database-schema:\n%s", plan.String())
	}
}
