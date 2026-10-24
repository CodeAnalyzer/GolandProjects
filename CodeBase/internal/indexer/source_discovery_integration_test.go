//go:build integration

package indexer

import (
	"context"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/codebase/internal/config"
	"github.com/codebase/internal/store/testutil"
)

func newDiscoveryIndexer(t *testing.T, root string, patterns []string) *Indexer {
	t.Helper()
	return &Indexer{
		db:   testutil.Open(t),
		config: &config.Config{Indexer: config.IndexerConfig{
			Parallel:        1,
			BatchSize:       100,
			IncludePatterns: patterns,
		}},
		errorLogger: log.New(io.Discard, "", 0),
		shared:      newIndexerSharedState(),
	}
}

// seedGitSource создаёт внутри root git-репозиторий продукта с указанной
// веткой, remote origin (basename → продукт) и одним SQL-файлом.
func seedGitSource(t *testing.T, root, dirName, product, branch, content string) string {
	t.Helper()
	dir := filepath.Join(root, dirName)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir source: %v", err)
	}
	gitRun(t, dir, "init")
	gitRun(t, dir, "checkout", "-b", branch)
	gitRun(t, dir, "remote", "add", "origin", "git@example.com:diasoft/"+product+".git")
	if err := os.WriteFile(filepath.Join(dir, "Proc.sql"), []byte(content), 0o644); err != nil {
		t.Fatalf("write sql: %v", err)
	}
	gitRun(t, dir, "add", ".")
	gitRun(t, dir, "commit", "-m", "add proc")
	return dir
}

func gitRun(t *testing.T, dir string, args ...string) {
	t.Helper()
	full := append([]string{"-c", "user.email=test@example.com", "-c", "user.name=test"}, args...)
	cmd := exec.Command("git", full...)
	cmd.Dir = dir
	var stderr strings.Builder
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("git %v in %s: %v: %s", args, dir, err, stderr.String())
	}
}

type branchRowInfo struct {
	id          int64
	branchKey   string
	branchName  string
	isDefault   bool
}

func productBranchRows(t *testing.T, idx *Indexer, productName string) []branchRowInfo {
	t.Helper()
	rows, err := idx.db.Query(`
		SELECT b.id, b.branch_key, b.branch_name, b.is_default
		FROM ds_products_branch b
		JOIN ds_products p ON p.id = b.ds_product_id
		WHERE p.product_name = $1
		ORDER BY b.branch_key
	`, productName)
	if err != nil {
		t.Fatalf("query branches: %v", err)
	}
	defer rows.Close()
	var result []branchRowInfo
	for rows.Next() {
		var r branchRowInfo
		if err := rows.Scan(&r.id, &r.branchKey, &r.branchName, &r.isDefault); err != nil {
			t.Fatalf("scan branch: %v", err)
		}
		result = append(result, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("branches rows: %v", err)
	}
	return result
}

// TestDiscovery_TwoLinesOfOneProduct — develop + релизная линия одного
// продукта из двух каталогов: две строки ds_products_branch, develop
// помечен is_default, файлы привязаны к своим линиям, псевдопродуктов
// fa-contracts-11017 не возникает.
func TestDiscovery_TwoLinesOfOneProduct(t *testing.T) {
	if !gitAvailable() {
		t.Skip("git is not in PATH")
	}
	ctx := context.Background()
	root := t.TempDir()
	developDir := seedGitSource(t, root, "fa-contracts", "fa-contracts", "develop", "CREATE PROCEDURE DevelopProc AS RETURN 0\n")
	spDir := seedGitSource(t, root, "fa-contracts-11017", "fa-contracts", "FA-Contracts-11-01-7-SP", "CREATE PROCEDURE SpProc AS RETURN 1\n")

	idx := newDiscoveryIndexer(t, root, []string{"*.sql"})
	if _, err := idx.UpdateCtx(ctx, root, true, 1); err != nil {
		t.Fatalf("update: %v", err)
	}

	branches := productBranchRows(t, idx, "fa-contracts")
	if len(branches) != 2 {
		t.Fatalf("fa-contracts branches = %d rows (%+v), want 2", len(branches), branches)
	}
	if branches[0].branchKey != "develop" || !branches[0].isDefault {
		t.Fatalf("develop line: %+v, want is_default", branches[0])
	}
	if branches[1].branchKey != "fa-contracts-11-01-7" || branches[1].isDefault {
		t.Fatalf("sp line: %+v, want not default", branches[1])
	}

	// Псевдопродуктов нет: единственный продукт семейства fa-contracts-*.
	var pseudo int
	if err := idx.db.QueryRow(`SELECT count(*) FROM ds_products WHERE product_name LIKE 'fa-contracts-%' AND product_name <> 'fa-contracts'`).Scan(&pseudo); err != nil {
		t.Fatalf("count pseudoproducts: %v", err)
	}
	if pseudo != 0 {
		t.Fatalf("pseudoproducts created: %d", pseudo)
	}

	// Файлы привязаны к своим линиям (пути в БД — ToSlash, как в walker).
	fileBranch := func(path string) (string, int64) {
		t.Helper()
		var key string
		var fileID int64
		if err := idx.db.QueryRow(`
			SELECT f.id, b.branch_key
			FROM files f JOIN ds_products_branch b ON b.id = f.ds_product_branch_id
			WHERE f.path = $1
		`, filepath.ToSlash(path)).Scan(&fileID, &key); err != nil {
			t.Fatalf("file %s not bound to branch: %v", path, err)
		}
		return key, fileID
	}
	if key, _ := fileBranch(filepath.Join(developDir, "Proc.sql")); key != "develop" {
		t.Fatalf("develop file bound to %q", key)
	}
	if key, _ := fileBranch(filepath.Join(spDir, "Proc.sql")); key != "fa-contracts-11-01-7" {
		t.Fatalf("sp file bound to %q", key)
	}
}

// TestDiscovery_BackfillUnchangedRows — legacy-строка (псевдопродукт старой
// эвристики, без ветки) с неизменным mtime+size получает корректные продукт
// и линию через backfill, без переиндексации содержимого.
func TestDiscovery_BackfillUnchangedRows(t *testing.T) {
	if !gitAvailable() {
		t.Skip("git is not in PATH")
	}
	ctx := context.Background()
	root := t.TempDir()
	spDir := seedGitSource(t, root, "fa-contracts-11017", "fa-contracts", "FA-Contracts-11-01-7-SP", "CREATE PROCEDURE SpProc AS RETURN 1\n")
	sqlPath := filepath.Join(spDir, "Proc.sql")

	idx := newDiscoveryIndexer(t, root, []string{"*.sql"})

	// Legacy-состояние: строка файла существует с псевдопродуктом и без
	// ветки; mtime+size совпадают с диском → pre-filter пропустит файл.
	info, err := os.Stat(sqlPath)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	legacyProductID, err := idx.db.GetOrCreateDSProductIDByName(ctx, "fa-contracts-11017")
	if err != nil {
		t.Fatalf("legacy product: %v", err)
	}
	var scanRunID, legacyFileID int64
	if err := idx.db.QueryRow(`INSERT INTO scan_runs (root_path, status) VALUES ($1, 'completed') RETURNING id`, root).Scan(&scanRunID); err != nil {
		t.Fatalf("scan run: %v", err)
	}
	if err := idx.db.QueryRow(`
		INSERT INTO files (scan_run_id, ds_product_id, path, rel_path, extension, size_bytes, hash_sha256, modified_at, encoding, language)
		VALUES ($1, $2, $3, 'Proc.sql', '.sql', $4, 'legacy-hash', $5, '', '') RETURNING id
	`, scanRunID, legacyProductID, filepath.ToSlash(sqlPath), info.Size(), info.ModTime().UTC()).Scan(&legacyFileID); err != nil {
		t.Fatalf("legacy file: %v", err)
	}

	stats, err := idx.UpdateCtx(ctx, root, true, 1)
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	_ = stats

	// Строка файла (тот же id) перепривязана к реальному продукту и линии.
	var product, branchKey string
	if err := idx.db.QueryRow(`
		SELECT p.product_name, b.branch_key
		FROM files f
		JOIN ds_products p ON p.id = f.ds_product_id
		JOIN ds_products_branch b ON b.id = f.ds_product_branch_id
		WHERE f.id = $1
	`, legacyFileID).Scan(&product, &branchKey); err != nil {
		t.Fatalf("legacy row not backfilled: %v", err)
	}
	if product != "fa-contracts" || branchKey != "fa-contracts-11-01-7" {
		t.Fatalf("backfilled row: product=%q branch=%q, want fa-contracts / fa-contracts-11-01-7", product, branchKey)
	}
}

// TestDiscovery_SpecsTwoBranches — спеки в двух ветках одного продукта:
// spec_config'и разные (root_dir от checkout'а), продукт у обоих —
// fa-contracts (не псевдопродукт fa-contracts-11017), корпус LSA даёт
// одну capability (develop-копию).
func TestDiscovery_SpecsTwoBranches(t *testing.T) {
	if !gitAvailable() {
		t.Skip("git is not in PATH")
	}
	ctx := context.Background()
	root := t.TempDir()
	developDir := seedGitSource(t, root, "fa-contracts", "fa-contracts", "develop", "CREATE PROCEDURE DevelopProc AS RETURN 0\n")
	spDir := seedGitSource(t, root, "fa-contracts-11017", "fa-contracts", "FA-Contracts-11-01-7-SP", "CREATE PROCEDURE SpProc AS RETURN 1\n")

	writeSpec := func(dir, title string) {
		t.Helper()
		rootDir := filepath.Join(dir, "openspec")
		specDir := filepath.Join(rootDir, "specs", "query", "entity-queries")
		if err := os.MkdirAll(specDir, 0o755); err != nil {
			t.Fatalf("mkdir spec dir: %v", err)
		}
		configYAML := "schema: spec-driven\n\ncontext: |\n  Test product context " + title + "\n"
		if err := os.WriteFile(filepath.Join(rootDir, "config.yaml"), []byte(configYAML), 0o644); err != nil {
			t.Fatalf("write config.yaml: %v", err)
		}
		content := "# Entity queries\n\n## Purpose\n\n" + title + "\n\n## Requirements\n\n### Requirement: Req\n\nСистема SHALL " + title + ".\n\n#### Scenario: Sc\n\n- **GIVEN** x\n- **WHEN** y\n- **THEN** z\n"
		if err := os.WriteFile(filepath.Join(specDir, "spec.md"), []byte(content), 0o644); err != nil {
			t.Fatalf("write spec: %v", err)
		}
	}
	writeSpec(developDir, "Develop variant")
	writeSpec(spDir, "SP variant")
	gitRun(t, developDir, "add", ".")
	gitRun(t, developDir, "commit", "-m", "spec")
	gitRun(t, spDir, "add", ".")
	gitRun(t, spDir, "commit", "-m", "spec")

	idx := newDiscoveryIndexer(t, root, []string{"*.sql", "*.md", "*.yaml"})
	// ValidateSpecDrivenRoot читает config.yaml по RelPath (относительный
	// путь) — рабочий каталог теста должен быть корнем скана.
	t.Chdir(root)
	if _, err := idx.UpdateCtx(ctx, root, true, 1); err != nil {
		t.Fatalf("update: %v", err)
	}

	// Обе spec_config привязаны к продукту fa-contracts (не псевдопродукту).
	var configs int
	if err := idx.db.QueryRow(`
		SELECT count(*) FROM spec_configs c
		JOIN ds_products p ON p.id = c.ds_product_id
		WHERE p.product_name = 'fa-contracts'
	`).Scan(&configs); err != nil {
		t.Fatalf("count configs: %v", err)
	}
	if configs != 2 {
		t.Fatalf("spec_configs of fa-contracts = %d, want 2", configs)
	}

	// Capability существует в обоих config'ах — уникальность не нарушена,
	// продукт у capability — fa-contracts.
	var caps int
	if err := idx.db.QueryRow(`
		SELECT count(*) FROM spec_capabilities c
		JOIN ds_products p ON p.id = c.ds_product_id
		WHERE c.capability_name = 'query/entity-queries' AND p.product_name = 'fa-contracts'
	`).Scan(&caps); err != nil {
		t.Fatalf("count capabilities: %v", err)
	}
	if caps != 2 {
		t.Fatalf("capabilities in two branches = %d, want 2", caps)
	}

	// Корпус: одна capability из default-ветки.
	corpus, err := idx.db.LoadAllSpecCapabilitiesWithReqsForLSA(ctx)
	if err != nil {
		t.Fatalf("load corpus: %v", err)
	}
	var inCorpus int
	for _, c := range corpus {
		if strings.Contains(c.LSAText, "entity-queries") || strings.Contains(c.LSAText, "Develop variant") || strings.Contains(c.LSAText, "SP variant") {
			inCorpus++
			if strings.Contains(c.LSAText, "SP variant") {
				t.Fatalf("SP copy leaked into corpus: %q", c.LSAText)
			}
		}
	}
	if inCorpus == 0 {
		t.Fatal("spec capability missing from corpus")
	}
}

// TestDiscovery_SwitchBranchInDir — переключение каталога на другую ветку:
// строка линии обновляется, прежняя линия теряет файлы при следующем update.
func TestDiscovery_SwitchBranchInDir(t *testing.T) {
	if !gitAvailable() {
		t.Skip("git is not in PATH")
	}
	ctx := context.Background()
	root := t.TempDir()
	dir := seedGitSource(t, root, "fa-contracts", "fa-contracts", "develop", "CREATE PROCEDURE P AS RETURN 0\n")

	idx := newDiscoveryIndexer(t, root, []string{"*.sql"})
	if _, err := idx.UpdateCtx(ctx, root, true, 1); err != nil {
		t.Fatalf("update #1: %v", err)
	}
	if branches := productBranchRows(t, idx, "fa-contracts"); len(branches) != 1 || branches[0].branchKey != "develop" {
		t.Fatalf("after update #1: %+v", branches)
	}

	// Каталог переключается на релизную линию (контент файла меняется).
	gitRun(t, dir, "checkout", "-b", "FA-Contracts-11-01-6-SP")
	if err := os.WriteFile(filepath.Join(dir, "Proc.sql"), []byte("CREATE PROCEDURE P2 AS RETURN 6\n"), 0o644); err != nil {
		t.Fatalf("rewrite sql: %v", err)
	}
	gitRun(t, dir, "add", ".")
	gitRun(t, dir, "commit", "-m", "sp")

	if _, err := idx.UpdateCtx(ctx, root, true, 1); err != nil {
		t.Fatalf("update #2: %v", err)
	}
	branches := productBranchRows(t, idx, "fa-contracts")
	if len(branches) != 1 {
		t.Fatalf("after switch: %+v, want single line row (same path)", branches)
	}
	if branches[0].branchKey != "fa-contracts-11-01-6" {
		t.Fatalf("after switch branch_key = %q, want fa-contracts-11-01-6", branches[0].branchKey)
	}

	var bound int
	if err := idx.db.QueryRow(`SELECT count(*) FROM files f JOIN ds_products_branch b ON b.id = f.ds_product_branch_id WHERE b.branch_key = 'fa-contracts-11-01-6'`).Scan(&bound); err != nil {
		t.Fatalf("count bound files: %v", err)
	}
	if bound == 0 {
		t.Fatal("no files bound to switched line")
	}
}

// TestDiscovery_RemovedDirCleansBranch — исчезнувший каталог: файлы
// удаляются сканом, строка линии без файлов — cleanup'ом того же прогона.
func TestDiscovery_RemovedDirCleansBranch(t *testing.T) {
	if !gitAvailable() {
		t.Skip("git is not in PATH")
	}
	ctx := context.Background()
	root := t.TempDir()
	seedGitSource(t, root, "fa-cards", "fa-cards", "develop", "CREATE PROCEDURE C AS RETURN 0\n")
	removedDir := seedGitSource(t, root, "fa-cards-11017", "fa-cards", "FA-Cards-11-01-7-SP", "CREATE PROCEDURE C7 AS RETURN 7\n")

	idx := newDiscoveryIndexer(t, root, []string{"*.sql"})
	if _, err := idx.UpdateCtx(ctx, root, true, 1); err != nil {
		t.Fatalf("update #1: %v", err)
	}
	if branches := productBranchRows(t, idx, "fa-cards"); len(branches) != 2 {
		t.Fatalf("after update #1: %+v, want 2 lines", branches)
	}

	if err := os.RemoveAll(removedDir); err != nil {
		t.Fatalf("remove dir: %v", err)
	}
	if _, err := idx.UpdateCtx(ctx, root, true, 1); err != nil {
		t.Fatalf("update #2: %v", err)
	}

	branches := productBranchRows(t, idx, "fa-cards")
	if len(branches) != 1 || branches[0].branchKey != "develop" {
		t.Fatalf("after removal: %+v, want only develop", branches)
	}
}
