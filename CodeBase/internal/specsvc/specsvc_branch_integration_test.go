//go:build integration

package specsvc_test

import (
	"context"
	"testing"

	"github.com/codebase/internal/specsvc"
	"github.com/codebase/internal/store"
	"github.com/codebase/internal/store/testutil"
)

// seedSpecBranchFixture создаёт продукт fa-spec-br с двумя ветками
// (develop — default и релизная fa-spec-br-11-01-7), двумя spec-конфигурациями
// одного продукта; capability br-cap существует в обеих конфигурациях.
// Возвращает id develop- и SP-копий capability (develop, sp).
func seedSpecBranchFixture(t *testing.T, db *store.DB) (devCapID, spCapID int64) {
	t.Helper()

	var productID int64
	if err := db.QueryRow(
		`INSERT INTO ds_products (product_name) VALUES ('fa-spec-br') RETURNING id`,
	).Scan(&productID); err != nil {
		t.Fatalf("insert ds_product: %v", err)
	}

	insertBranch := func(name, key, path string, isDefault bool) int64 {
		var id int64
		if err := db.QueryRow(`
			INSERT INTO ds_products_branch (ds_product_id, branch_name, branch_key, path, is_default)
			VALUES ($1, $2, $3, $4, $5) RETURNING id`,
			productID, name, key, path, isDefault).Scan(&id); err != nil {
			t.Fatalf("insert branch %s: %v", key, err)
		}
		return id
	}
	devBranch := insertBranch("develop", "develop", "/repo/fa-spec-br/develop", true)
	spBranch := insertBranch("FA-Spec-BR-11-01-7-SP", "fa-spec-br-11-01-7", "/repo/fa-spec-br-11017", false)

	insertFile := func(path string, branchID int64) int64 {
		var scanID int64
		if err := db.QueryRow(
			`INSERT INTO scan_runs (root_path, status) VALUES ('/test', 'done') RETURNING id`,
		).Scan(&scanID); err != nil {
			t.Fatalf("insert scan_run: %v", err)
		}
		var fileID int64
		if err := db.QueryRow(`
			INSERT INTO files (scan_run_id, path, rel_path, extension, hash_sha256, modified_at, ds_product_branch_id)
			VALUES ($1, $2, $2, 'md', 'h', NOW(), $3) RETURNING id`,
			scanID, path, branchID).Scan(&fileID); err != nil {
			t.Fatalf("insert file %s: %v", path, err)
		}
		return fileID
	}

	// insertConfigWithCap вставляет конфигурацию + capability br-cap
	// (обе копии с одинаковым slug) + dependency-копию + code mention.
	insertConfigWithCap := func(cfgPath, capPath string, branchID int64, rootDir string, mentionLine int) int64 {
		cfgFile := insertFile(cfgPath, branchID)
		var cfgID int64
		if err := db.QueryRow(`
			INSERT INTO spec_configs (file_id, ds_product_id, product_name, root_dir)
			VALUES ($1, $2, 'fa-spec-br', $3) RETURNING id`,
			cfgFile, productID, rootDir).Scan(&cfgID); err != nil {
			t.Fatalf("insert spec_config %s: %v", rootDir, err)
		}
		capFile := insertFile(capPath, branchID)
		var capID int64
		if err := db.QueryRow(`
			INSERT INTO spec_capabilities (file_id, spec_config_id, ds_product_id, capability_name, title)
			VALUES ($1, $2, $3, 'br-cap', 'Жизненный цикл договора') RETURNING id`,
			capFile, cfgID, productID).Scan(&capID); err != nil {
			t.Fatalf("insert capability: %v", err)
		}
		depFile := insertFile(cfgPath+"-dep", branchID)
		var depID int64
		if err := db.QueryRow(`
			INSERT INTO spec_capabilities (file_id, spec_config_id, ds_product_id, capability_name, title)
			VALUES ($1, $2, $3, 'br-cap-dep', 'Зависимость') RETURNING id`,
			depFile, cfgID, productID).Scan(&depID); err != nil {
			t.Fatalf("insert dependency capability: %v", err)
		}
		insertRelationDirect(t, db, "spec_capability", capID, "spec_capability", depID, "depends_on_capability", "explicit")
		if _, err := db.Exec(`
			INSERT INTO spec_code_mentions (file_id, source_type, source_id, mention_name, mention_kind, line_number)
			VALUES ($1, 'spec_capability', $2, 'BrCodeProc', 'procedure', $3)`,
			capFile, capID, mentionLine); err != nil {
			t.Fatalf("insert code mention: %v", err)
		}
		// История изменений: копии change в обеих конфигурациях.
		changeFile := insertFile(cfgPath+"-change", branchID)
		var changeID int64
		if err := db.QueryRow(`
			INSERT INTO spec_changes (file_id, spec_config_id, change_name, status, dir_path)
			VALUES ($1, $2, 'br-change', 'active', 'openspec/changes/br-change') RETURNING id`,
			changeFile, cfgID).Scan(&changeID); err != nil {
			t.Fatalf("insert spec change: %v", err)
		}
		deltaFile := insertFile(cfgPath+"-delta", branchID)
		if _, err := db.Exec(`
			INSERT INTO spec_change_delta (file_id, change_id, section, capability_slug, requirement_name, body_text, line_start, line_end)
			VALUES ($1, $2, 'MODIFIED', 'br-cap', 'br-req', 'Изменённое требование', 1, 5)`,
			deltaFile, changeID); err != nil {
			t.Fatalf("insert spec change delta: %v", err)
		}
		insertRelationDirect(t, db, "spec_change", changeID, "spec_capability", capID, "change_modifies", "explicit")
		if err := db.EnsureSpecSearchVectors(context.Background(), capFile); err != nil {
			t.Fatalf("EnsureSpecSearchVectors: %v", err)
		}
		return capID
	}

	devCapID = insertConfigWithCap(
		"fa-spec-br/develop/openspec/config.yaml",
		"fa-spec-br/develop/openspec/specs/br-cap/spec.md",
		devBranch, "/repo/fa-spec-br/develop/openspec", 10)
	spCapID = insertConfigWithCap(
		"fa-spec-br-11017/openspec/config.yaml",
		"fa-spec-br-11017/openspec/specs/br-cap/spec.md",
		spBranch, "/repo/fa-spec-br-11017/openspec", 12)
	return devCapID, spCapID
}

// TestSpecSearchBranchDedup: exact-слой возвращает одну копию capability
// продукта (develop) с пометкой ветки.
func TestSpecSearchBranchDedup(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()
	seedSpecBranchFixture(t, db)

	res, err := specsvc.ExecuteSpecSearch(ctx, db, "договор", "", "", "exact", 50)
	if err != nil {
		t.Fatalf("ExecuteSpecSearch: %v", err)
	}
	count := 0
	for _, hit := range res.Exact {
		if hit.CapabilityName != "br-cap" || hit.Level != "capability" {
			continue
		}
		count++
		if hit.Product != "fa-spec-br" {
			t.Fatalf("product = %q, want fa-spec-br", hit.Product)
		}
		if hit.BranchUsed != "develop" {
			t.Fatalf("branch_used = %q, want develop", hit.BranchUsed)
		}
	}
	if count != 1 {
		t.Fatalf("capability hits = %d, want 1 (dedup): %+v", count, res.Exact)
	}
}

// TestSpecByCodeBranchDedup: упоминание из второй конфигурации не даёт
// дублирующей записи — одна develop-запись с пометкой ветки. Копии упоминания
// лежат на разных строках (10 и 12): line_number не входит в ключ дедупа
// (P2.3 — сдвиг строк не должен порождать дубль).
func TestSpecByCodeBranchDedup(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()
	seedSpecBranchFixture(t, db)

	res, err := specsvc.ExecuteSpecByCode(ctx, db, "BrCodeProc", 100)
	if err != nil {
		t.Fatalf("ExecuteSpecByCode: %v", err)
	}
	if !res.Resolved {
		t.Fatalf("expected resolved result")
	}
	if len(res.Hits) != 1 {
		t.Fatalf("hits = %d, want 1 (dedup): %+v", len(res.Hits), res.Hits)
	}
	hit := res.Hits[0]
	if hit.CapabilityName != "br-cap" || hit.Product != "fa-spec-br" || hit.BranchUsed != "develop" {
		t.Fatalf("hit = %+v, want br-cap / fa-spec-br / develop", hit)
	}
}

// TestSpecCoverageBranchDedup: capability встречается в покрытии один раз.
func TestSpecCoverageBranchDedup(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()
	seedSpecBranchFixture(t, db)

	res, err := specsvc.ExecuteSpecCoverage(ctx, db, "fa-spec-br", "", "")
	if err != nil {
		t.Fatalf("ExecuteSpecCoverage: %v", err)
	}
	count := 0
	for _, cap := range res.Capabilities {
		if cap.CapabilityName == "br-cap" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("br-cap occurrences = %d, want 1: %+v", count, res.Capabilities)
	}
}

// TestSpecConfigListsBranchConfigs: spec_config перечисляет все конфигурации
// продукта с пометкой default; профиль берётся из default-конфигурации.
func TestSpecConfigListsBranchConfigs(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()
	seedSpecBranchFixture(t, db)

	res, err := specsvc.ExecuteSpecConfig(ctx, db, "fa-spec-br", false, 2)
	if err != nil {
		t.Fatalf("ExecuteSpecConfig: %v", err)
	}
	if len(res.Configs) != 2 {
		t.Fatalf("configs = %d, want 2: %+v", len(res.Configs), res.Configs)
	}
	if !res.Configs[0].IsDefault || res.Configs[0].Branch != "develop" {
		t.Fatalf("first config = %+v, want default develop", res.Configs[0])
	}
	if res.Configs[1].IsDefault {
		t.Fatalf("second config must not be default: %+v", res.Configs[1])
	}
	if res.Profile.RootDir != "/repo/fa-spec-br/develop/openspec" {
		t.Fatalf("profile.root_dir = %q, want develop config", res.Profile.RootDir)
	}
}

// TestSpecDepsBranchDeterminism: lookup capability и зависимости берутся из
// default-ветки; рёбра из второй конфигурации не дублируются.
func TestSpecDepsBranchDeterminism(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()
	seedSpecBranchFixture(t, db)

	res, err := specsvc.ExecuteSpecDeps(ctx, db, "br-cap", "", 2)
	if err != nil {
		t.Fatalf("ExecuteSpecDeps: %v", err)
	}
	if res.CapabilityName != "br-cap" || res.BranchUsed != "develop" {
		t.Fatalf("root = %s / %s, want br-cap / develop", res.CapabilityName, res.BranchUsed)
	}
	if len(res.Dependencies) != 1 {
		t.Fatalf("dependencies = %d, want 1: %+v", len(res.Dependencies), res.Dependencies)
	}
	dep := res.Dependencies[0]
	if dep.CapabilityName != "br-cap-dep" || dep.BranchUsed != "develop" {
		t.Fatalf("dep = %+v, want br-cap-dep / develop", dep)
	}
}

// TestSpecHistoryPerEntryBranch: копии change из двух конфигураций
// схлопываются, каждая запись несёт branch_used своей конфигурации (P2.1).
func TestSpecHistoryPerEntryBranch(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()
	seedSpecBranchFixture(t, db)

	res, err := specsvc.ExecuteSpecHistory(ctx, db, "br-cap")
	if err != nil {
		t.Fatalf("ExecuteSpecHistory: %v", err)
	}
	if len(res.Changes) != 1 {
		t.Fatalf("history entries = %d, want 1 (dedup): %+v", len(res.Changes), res.Changes)
	}
	entry := res.Changes[0]
	if entry.ChangeName != "br-change" || entry.Section != "MODIFIED" {
		t.Fatalf("entry = %+v, want br-change/MODIFIED", entry)
	}
	if entry.BranchUsed != "develop" {
		t.Fatalf("entry branch_used = %q, want develop", entry.BranchUsed)
	}

	byChange, err := specsvc.ExecuteSpecHistory(ctx, db, "", "br-change")
	if err != nil {
		t.Fatalf("ExecuteSpecHistory by change: %v", err)
	}
	if len(byChange.Capabilities) != 1 {
		t.Fatalf("change capabilities = %d, want 1 (dedup): %+v", len(byChange.Capabilities), byChange.Capabilities)
	}
	if byChange.Capabilities[0].BranchUsed != "develop" {
		t.Fatalf("change capability branch_used = %q, want develop", byChange.Capabilities[0].BranchUsed)
	}
	if len(byChange.Changes) != 1 || byChange.Changes[0].BranchUsed != "develop" {
		t.Fatalf("change deltas = %+v, want one develop entry", byChange.Changes)
	}
}
