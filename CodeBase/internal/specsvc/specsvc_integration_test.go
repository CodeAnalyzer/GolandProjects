//go:build integration

package specsvc_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/codebase/internal/config"
	"github.com/codebase/internal/errs"
	"github.com/codebase/internal/model"
	"github.com/codebase/internal/specfts"
	"github.com/codebase/internal/specsvc"
	"github.com/codebase/internal/store"
	"github.com/codebase/internal/store/testutil"
)

// insertFileDirect вставляет scan_run + file, возвращает file_id.
func insertFileDirect(t *testing.T, db *store.DB, path string) int64 {
	t.Helper()
	var scanID int64
	if err := db.QueryRow(
		`INSERT INTO scan_runs (root_path, status) VALUES ('/test', 'done') RETURNING id`,
	).Scan(&scanID); err != nil {
		t.Fatalf("insert scan_run: %v", err)
	}
	var fileID int64
	if err := db.QueryRow(
		`INSERT INTO files (scan_run_id, path, rel_path, extension, hash_sha256, modified_at)
		 VALUES ($1, $2, $2, 'md', 'h', NOW()) RETURNING id`,
		scanID, path,
	).Scan(&fileID); err != nil {
		t.Fatalf("insert file %s: %v", path, err)
	}
	return fileID
}

func configureSemanticModel(t *testing.T, modelPath string) {
	t.Helper()
	oldConfigFile := config.GetConfigFile()
	oldCfg := config.Get()
	var oldCfgCopy config.Config
	if oldCfg != nil {
		oldCfgCopy = *oldCfg
	}
	config.CreateDefault(filepath.Dir(modelPath))
	cfg := config.Get()
	cfg.Spec.LSAModelPath = modelPath
	cfg.Spec.LSAEnabled = true
	config.SetConfigFile(filepath.Join(filepath.Dir(modelPath), "codebase.toml"))
	t.Cleanup(func() {
		config.SetConfigFile(oldConfigFile)
		if current := config.Get(); current != nil {
			if oldCfg != nil {
				*current = oldCfgCopy
			} else {
				current.Spec.LSAEnabled = false
			}
		}
	})
}

func writeSemanticTestModel(t *testing.T, path, generation string) {
	t.Helper()
	vocab := &specfts.Vocab{
		Terms:   []string{"арест"},
		Index:   map[string]int{"арест": 0},
		DocFreq: []int{1},
		IDF:     []float64{1},
	}
	vt := specfts.NewDenseMatrix(1, 1)
	vt.Set(0, 0, 1)
	model := &specfts.LSAModel{Generation: generation, Vocab: vocab, VT: vt, Singulars: []float64{1}, K: 1, NumDocs: 1, NumTerms: 1}
	if err := specfts.SaveLSAModel(model, path); err != nil {
		t.Fatalf("SaveLSAModel: %v", err)
	}
}

// insertSpecConfigAndCapabilityDirect вставляет spec_config + spec_capability, возвращает capability_id.
func insertSpecConfigAndCapabilityDirect(t *testing.T, db *store.DB, fileID int64, capName string) int64 {
	t.Helper()
	var cfgID int64
	if err := db.QueryRow(
		`INSERT INTO spec_configs (file_id, product_name) VALUES ($1, 'fa-contracts') RETURNING id`,
		fileID,
	).Scan(&cfgID); err != nil {
		t.Fatalf("insert spec_config: %v", err)
	}
	var capID int64
	if err := db.QueryRow(
		`INSERT INTO spec_capabilities (file_id, spec_config_id, capability_name, title)
		 VALUES ($1, $2, $3, 'Title') RETURNING id`,
		fileID, cfgID, capName,
	).Scan(&capID); err != nil {
		t.Fatalf("insert spec_capability %s: %v", capName, err)
	}
	return capID
}

// insertSpecChangeDirect вставляет spec_change, возвращает change_id.
func insertRequirementDirect(t *testing.T, db *store.DB, fileID, capabilityID int64, name, body string, lineStart, lineEnd int) int64 {
	t.Helper()
	var id int64
	if err := db.QueryRow(`
		INSERT INTO spec_requirements (file_id, capability_id, requirement_name, body_text, line_start, line_end)
		VALUES ($1, $2, $3, $4, $5, $6) RETURNING id`,
		fileID, capabilityID, name, body, lineStart, lineEnd,
	).Scan(&id); err != nil {
		t.Fatalf("insert requirement %s: %v", name, err)
	}
	return id
}

func insertScenarioDirect(t *testing.T, db *store.DB, fileID, requirementID int64, name, given, when, then string, lineStart, lineEnd int) int64 {
	t.Helper()
	var id int64
	if err := db.QueryRow(`
		INSERT INTO spec_scenarios (file_id, requirement_id, scenario_name, given_text, when_text, then_text, line_start, line_end)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING id`,
		fileID, requirementID, name, given, when, then, lineStart, lineEnd,
	).Scan(&id); err != nil {
		t.Fatalf("insert scenario %s: %v", name, err)
	}
	return id
}

func insertCodeMentionDirect(t *testing.T, db *store.DB, fileID int64, sourceType string, sourceID int64, name string, line int) {
	t.Helper()
	if _, err := db.Exec(`
		INSERT INTO spec_code_mentions (file_id, source_type, source_id, mention_name, mention_kind, line_number)
		VALUES ($1, $2, $3, $4, 'api', $5)`, fileID, sourceType, sourceID, name, line); err != nil {
		t.Fatalf("insert code mention %s: %v", name, err)
	}
}

func insertSpecChangeDirect(t *testing.T, db *store.DB, fileID int64, changeName, status string) int64 {
	t.Helper()
	var cfgID int64
	if err := db.QueryRow(
		`INSERT INTO spec_configs (file_id, product_name) VALUES ($1, 'fa-contracts') RETURNING id`,
		fileID,
	).Scan(&cfgID); err != nil {
		t.Fatalf("insert spec_config for change: %v", err)
	}
	var changeID int64
	if err := db.QueryRow(
		`INSERT INTO spec_changes (file_id, spec_config_id, change_name, status, dir_path)
		 VALUES ($1, $2, $3, $4, 'changes') RETURNING id`,
		fileID, cfgID, changeName, status,
	).Scan(&changeID); err != nil {
		t.Fatalf("insert spec_change %s: %v", changeName, err)
	}
	return changeID
}

// insertSpecChangeDeltaDirect вставляет delta-секцию change.
func insertSpecChangeDeltaDirect(t *testing.T, db *store.DB, fileID, changeID int64,
	capSlug, section, reqName, body string,
) {
	t.Helper()
	if _, err := db.Exec(
		`INSERT INTO spec_change_delta (file_id, change_id, section, capability_slug, requirement_name, body_text)
		 VALUES ($1, $2, $3, $4, $5, $6)`,
		fileID, changeID, section, capSlug, reqName, body,
	); err != nil {
		t.Fatalf("insert spec_change_delta: %v", err)
	}
}

// insertRelationDirect вставляет ребро relations.
func insertRelationDirect(t *testing.T, db *store.DB,
	srcType string, srcID int64, tgtType string, tgtID int64, relType, confidence string,
) {
	t.Helper()
	if _, err := db.Exec(
		`INSERT INTO relations (source_type, source_id, target_type, target_id, relation_type, confidence)
		 VALUES ($1, $2, $3, $4, $5, $6)`,
		srcType, srcID, tgtType, tgtID, relType, confidence,
	); err != nil {
		t.Fatalf("insert relation: %v", err)
	}
}

func TestExecuteSpecHistoryByChange_Delta(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()

	proposalFile := insertFileDirect(t, db, "changes/test-change/proposal.md")
	capFile := insertFileDirect(t, db, "specs/consumer-cession/purchase/spec.md")
	capID := insertSpecConfigAndCapabilityDirect(t, db, capFile, "consumer-cession/purchase")
	changeID := insertSpecChangeDirect(t, db, proposalFile, "test-change-delta", "archived")
	insertSpecChangeDeltaDirect(t, db, proposalFile, changeID, "consumer-cession/purchase", "ADDED",
		"Параллельная обработка начислений при покупке портфеля", "Тело требования")
	insertRelationDirect(t, db, "spec_change", changeID, "spec_capability", capID, "change_modifies", "delta")

	res, err := specsvc.ExecuteSpecHistory(ctx, db, "", "test-change-delta")
	if err != nil {
		t.Fatalf("ExecuteSpecHistory: %v", err)
	}
	if len(res.Changes) == 0 {
		t.Fatalf("expected non-empty Changes, got empty")
	}
	var found bool
	for _, e := range res.Changes {
		if e.CapabilityName == "consumer-cession/purchase" && e.Section == "ADDED" &&
			e.ReqName == "Параллельная обработка начислений при покупке портфеля" &&
			e.BodyText == "Тело требования" && !e.SkipSpecs {
			found = true
		}
	}
	if !found {
		t.Fatalf("delta entry not found in Changes: %+v", res.Changes)
	}
	if len(res.Capabilities) == 0 || res.Capabilities[0].CapabilityName != "consumer-cession/purchase" {
		t.Fatalf("Capabilities summary missing: %+v", res.Capabilities)
	}
}

func TestExecuteSpecHistoryByChange_SkipSpecs(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()

	proposalFile := insertFileDirect(t, db, "changes/skip-change/proposal.md")
	capFile := insertFileDirect(t, db, "specs/CORE/data/spec.md")
	capID := insertSpecConfigAndCapabilityDirect(t, db, capFile, "CORE/data")
	changeID := insertSpecChangeDirect(t, db, proposalFile, "test-change-skip", "archived")
	// Нет spec_change_delta — skip_specs. Связь из proposal: confidence = 'proposal'.
	insertRelationDirect(t, db, "spec_change", changeID, "spec_capability", capID, "change_modifies", "proposal")

	res, err := specsvc.ExecuteSpecHistory(ctx, db, "", "test-change-skip")
	if err != nil {
		t.Fatalf("ExecuteSpecHistory: %v", err)
	}
	if len(res.Changes) == 0 {
		t.Fatalf("expected non-empty Changes for skip_specs, got empty")
	}
	var found bool
	for _, e := range res.Changes {
		if e.CapabilityName == "CORE/data" && e.SkipSpecs && e.DeltaSource == "proposal" &&
			e.Section == "" && e.ReqName == "" {
			found = true
		}
	}
	if !found {
		t.Fatalf("skip_specs entry not found: %+v", res.Changes)
	}
}

func TestExecuteSpecHistoryByChange_EmptyChanges(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()

	proposalFile := insertFileDirect(t, db, "changes/empty-change/proposal.md")
	capFile := insertFileDirect(t, db, "specs/some/cap/spec.md")
	capID := insertSpecConfigAndCapabilityDirect(t, db, capFile, "some/cap")
	changeID := insertSpecChangeDirect(t, db, proposalFile, "test-change-empty", "active")
	// Нет delta, confidence пустой — не skip_specs. Changes не nil (пустой массив []).
	insertRelationDirect(t, db, "spec_change", changeID, "spec_capability", capID, "change_modifies", "")

	res, err := specsvc.ExecuteSpecHistory(ctx, db, "", "test-change-empty")
	if err != nil {
		t.Fatalf("ExecuteSpecHistory: %v", err)
	}
	// Changes должен быть не-nil (пустой массив [] в JSON, не null).
	if res.Changes == nil {
		t.Fatalf("expected non-nil Changes, got nil")
	}
	if len(res.Capabilities) == 0 {
		t.Fatalf("expected non-empty Capabilities, got empty")
	}
}

// insertDSProductDirect вставляет ds_product, возвращает id.
func insertDSProductDirect(t *testing.T, db *store.DB, name string) int64 {
	t.Helper()
	var id int64
	if err := db.QueryRow(
		`INSERT INTO ds_products (product_name) VALUES ($1) RETURNING id`, name,
	).Scan(&id); err != nil {
		t.Fatalf("insert ds_product %s: %v", name, err)
	}
	return id
}

// insertSpecConfigWithProductDirect вставляет spec_config, привязанный к ds_product.
func insertSpecConfigWithProductDirect(t *testing.T, db *store.DB, fileID, productID int64, productName string) int64 {
	t.Helper()
	var cfgID int64
	if err := db.QueryRow(
		`INSERT INTO spec_configs (file_id, ds_product_id, product_name, root_dir, schema_name,
		                          usecase_layout, id_style, cross_ref_style, normative_lang,
		                          traceability, has_changes, has_audit, has_adr,
		                          coverage_metrics, context_text)
		 VALUES ($1, $2, $3, 'openspec/root', 'schema',
		         'none', 'dir', 'mixed', 'ru',
		         'none', true, false, false,
		         false, 'Product context for tests') RETURNING id`,
		fileID, productID, productName,
	).Scan(&cfgID); err != nil {
		t.Fatalf("insert spec_config with product: %v", err)
	}
	return cfgID
}

// insertCapabilityWithParentDirect вставляет spec_capability с опциональным parent_id и title.
func insertCapabilityWithParentDirect(t *testing.T, db *store.DB, fileID, cfgID int64, capName, title string, parentID int64) int64 {
	t.Helper()
	var id int64
	if parentID > 0 {
		if err := db.QueryRow(
			`INSERT INTO spec_capabilities (file_id, spec_config_id, capability_name, title, parent_id)
			 VALUES ($1, $2, $3, $4, $5) RETURNING id`,
			fileID, cfgID, capName, title, parentID,
		).Scan(&id); err != nil {
			t.Fatalf("insert spec_capability %s: %v", capName, err)
		}
	} else {
		if err := db.QueryRow(
			`INSERT INTO spec_capabilities (file_id, spec_config_id, capability_name, title)
			 VALUES ($1, $2, $3, $4) RETURNING id`,
			fileID, cfgID, capName, title,
		).Scan(&id); err != nil {
			t.Fatalf("insert spec_capability %s: %v", capName, err)
		}
	}
	return id
}

func TestExecuteSpecConfig_FlatHierarchy(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()

	productID := insertDSProductDirect(t, db, "fa-test-flat")
	fileID := insertFileDirect(t, db, "openspec/config.yaml")
	cfgID := insertSpecConfigWithProductDirect(t, db, fileID, productID, "fa-test-flat")

	// 3 плоские capabilities (без parent_id)
	capFile := insertFileDirect(t, db, "specs/cap-a/spec.md")
	insertCapabilityWithParentDirect(t, db, capFile, cfgID, "cap-a", "Cap A", 0)
	capFile2 := insertFileDirect(t, db, "specs/cap-b/spec.md")
	insertCapabilityWithParentDirect(t, db, capFile2, cfgID, "cap-b", "Cap B", 0)
	capFile3 := insertFileDirect(t, db, "specs/cap-c/spec.md")
	insertCapabilityWithParentDirect(t, db, capFile3, cfgID, "cap-c", "Cap C", 0)

	res, err := specsvc.ExecuteSpecConfig(ctx, db, "fa-test-flat", true, 2)
	if err != nil {
		t.Fatalf("ExecuteSpecConfig: %v", err)
	}
	if res.Profile.ProductName != "fa-test-flat" {
		t.Fatalf("product_name = %q, want fa-test-flat", res.Profile.ProductName)
	}
	if res.Profile.UsecaseLayout != "none" {
		t.Fatalf("usecase_layout = %q, want none", res.Profile.UsecaseLayout)
	}
	if res.Profile.NormativeLang != "ru" {
		t.Fatalf("normative_lang = %q, want ru", res.Profile.NormativeLang)
	}
	if res.Profile.RootDir != "openspec/root" {
		t.Fatalf("root_dir = %q, want openspec/root", res.Profile.RootDir)
	}
	if res.Profile.HasChanges != true {
		t.Fatalf("has_changes = %v, want true", res.Profile.HasChanges)
	}
	if res.Profile.ContextText != "Product context for tests" {
		t.Fatalf("context_text = %q", res.Profile.ContextText)
	}
	if res.Stats.Capabilities != 3 {
		t.Fatalf("stats.capabilities = %d, want 3", res.Stats.Capabilities)
	}
	if len(res.Hierarchy) != 3 {
		t.Fatalf("hierarchy len = %d, want 3", len(res.Hierarchy))
	}
	for _, node := range res.Hierarchy {
		if node.ChildrenCount != 0 {
			t.Fatalf("children_count = %d, want 0 for flat", node.ChildrenCount)
		}
		if node.IsContainer {
			t.Fatalf("is_container = true, want false for %s", node.CapabilityName)
		}
	}
}

func TestExecuteSpecConfig_DeepHierarchy(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()

	productID := insertDSProductDirect(t, db, "fa-test-deep")
	fileID := insertFileDirect(t, db, "openspec/config.yaml")
	cfgID := insertSpecConfigWithProductDirect(t, db, fileID, productID, "fa-test-deep")

	// Дерево: root → child → grandchild
	rootFile := insertFileDirect(t, db, "specs/root/spec.md")
	rootID := insertCapabilityWithParentDirect(t, db, rootFile, cfgID, "root", "Root", 0)
	childFile := insertFileDirect(t, db, "specs/root/child/spec.md")
	childID := insertCapabilityWithParentDirect(t, db, childFile, cfgID, "root/child", "Child", rootID)
	grandFile := insertFileDirect(t, db, "specs/root/child/grand/spec.md")
	insertCapabilityWithParentDirect(t, db, grandFile, cfgID, "root/child/grand", "Grand", childID)

	// depth=2: root → child (раскрыт), grandchild — нет
	res, err := specsvc.ExecuteSpecConfig(ctx, db, "fa-test-deep", true, 2)
	if err != nil {
		t.Fatalf("ExecuteSpecConfig depth=2: %v", err)
	}
	if len(res.Hierarchy) != 1 {
		t.Fatalf("hierarchy len = %d, want 1 root", len(res.Hierarchy))
	}
	root := res.Hierarchy[0]
	if root.CapabilityName != "root" {
		t.Fatalf("root name = %q, want root", root.CapabilityName)
	}
	if root.ChildrenCount != 1 {
		t.Fatalf("root children_count = %d, want 1", root.ChildrenCount)
	}
	if len(root.Children) != 1 {
		t.Fatalf("root children len = %d, want 1", len(root.Children))
	}
	child := root.Children[0]
	if child.CapabilityName != "root/child" {
		t.Fatalf("child name = %q, want root/child", child.CapabilityName)
	}
	if child.ChildrenCount != 1 {
		t.Fatalf("child children_count = %d, want 1", child.ChildrenCount)
	}
	if len(child.Children) != 0 {
		t.Fatalf("child children len = %d, want 0 (depth=2 reached)", len(child.Children))
	}

	// depth=3: root → child → grandchild (все раскрыты)
	res3, err := specsvc.ExecuteSpecConfig(ctx, db, "fa-test-deep", true, 3)
	if err != nil {
		t.Fatalf("ExecuteSpecConfig depth=3: %v", err)
	}
	if len(res3.Hierarchy) != 1 {
		t.Fatalf("depth=3 hierarchy len = %d, want 1", len(res3.Hierarchy))
	}
	grand := res3.Hierarchy[0].Children[0].Children[0]
	if grand.CapabilityName != "root/child/grand" {
		t.Fatalf("grand name = %q, want root/child/grand", grand.CapabilityName)
	}
	if grand.ChildrenCount != 0 {
		t.Fatalf("grand children_count = %d, want 0", grand.ChildrenCount)
	}
}

func TestExecuteSpecConfig_ProductNotFound(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()

	_, err := specsvc.ExecuteSpecConfig(ctx, db, "nonexistent-product", true, 2)
	if !errors.Is(err, errs.ErrSpecNotFound) {
		t.Fatalf("error = %v, want ErrSpecNotFound", err)
	}
}

func TestExecuteSpecConfig_NoHierarchy(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()

	productID := insertDSProductDirect(t, db, "fa-test-no-hier")
	fileID := insertFileDirect(t, db, "openspec/config.yaml")
	cfgID := insertSpecConfigWithProductDirect(t, db, fileID, productID, "fa-test-no-hier")
	capFile := insertFileDirect(t, db, "specs/cap/spec.md")
	insertCapabilityWithParentDirect(t, db, capFile, cfgID, "cap", "Cap", 0)

	res, err := specsvc.ExecuteSpecConfig(ctx, db, "fa-test-no-hier", false, 2)
	if err != nil {
		t.Fatalf("ExecuteSpecConfig: %v", err)
	}
	if res.Hierarchy != nil {
		t.Fatalf("hierarchy = %v, want nil", res.Hierarchy)
	}
	if res.Stats.Capabilities != 1 {
		t.Fatalf("stats.capabilities = %d, want 1", res.Stats.Capabilities)
	}
}

func TestExecuteSpecConfig_ContainerNode(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()

	productID := insertDSProductDirect(t, db, "fa-test-container")
	fileID := insertFileDirect(t, db, "openspec/config.yaml")
	cfgID := insertSpecConfigWithProductDirect(t, db, fileID, productID, "fa-test-container")

	// Контейнер: title пустой, purpose/notes NULL
	containerFile := insertFileDirect(t, db, "specs/billing/")
	containerID := insertCapabilityWithParentDirect(t, db, containerFile, cfgID, "billing", "", 0)
	// Реальная capability: title непустой
	realFile := insertFileDirect(t, db, "specs/billing/invoicing/spec.md")
	insertCapabilityWithParentDirect(t, db, realFile, cfgID, "billing/invoicing", "Invoicing", containerID)

	res, err := specsvc.ExecuteSpecConfig(ctx, db, "fa-test-container", true, 2)
	if err != nil {
		t.Fatalf("ExecuteSpecConfig: %v", err)
	}
	if len(res.Hierarchy) != 1 {
		t.Fatalf("hierarchy len = %d, want 1", len(res.Hierarchy))
	}
	container := res.Hierarchy[0]
	if container.CapabilityName != "billing" {
		t.Fatalf("container name = %q, want billing", container.CapabilityName)
	}
	if !container.IsContainer {
		t.Fatalf("is_container = false, want true for billing")
	}
	if container.Title != "" {
		t.Fatalf("title = %q, want empty for container", container.Title)
	}
	if container.ChildrenCount != 1 {
		t.Fatalf("children_count = %d, want 1", container.ChildrenCount)
	}
	if len(container.Children) != 1 {
		t.Fatalf("children len = %d, want 1", len(container.Children))
	}
	real := container.Children[0]
	if real.CapabilityName != "billing/invoicing" {
		t.Fatalf("real name = %q, want billing/invoicing", real.CapabilityName)
	}
	if real.IsContainer {
		t.Fatalf("is_container = true, want false for invoicing")
	}
	if real.Title != "Invoicing" {
		t.Fatalf("title = %q, want Invoicing", real.Title)
	}
}

func TestExecuteSpecUsecase_NotFound(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()

	_, err := specsvc.ExecuteSpecUsecase(ctx, db, "nonexistent-usecase", "")
	if !errors.Is(err, errs.ErrSpecNotFound) {
		t.Fatalf("error = %v, want ErrSpecNotFound", err)
	}
}

// insertSpecUsecaseFullDirect вставляет spec_usecase со всеми полями, возвращает id.
func insertSpecUsecaseFullDirect(t *testing.T, db *store.DB, fileID, cfgID int64, name, title, desc, actors, pre, post, bv, sourceDir, kind string, pageID int64) int64 {
	t.Helper()
	var id int64
	if err := db.QueryRow(
		`INSERT INTO spec_usecases (file_id, spec_config_id, usecase_name, title, description, actors,
		                            preconditions, postconditions, business_value, source_dir, usecase_kind, page_id)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12) RETURNING id`,
		fileID, cfgID, name, title, desc, actors, pre, post, bv, sourceDir, kind, pageID,
	).Scan(&id); err != nil {
		t.Fatalf("insert spec_usecase %s: %v", name, err)
	}
	return id
}

func TestExecuteSpecUsecase_ByNameWithFullFields(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()

	productID := insertDSProductDirect(t, db, "fa-test-uc-full")
	fileID := insertFileDirect(t, db, "openspec/config.yaml")
	cfgID := insertSpecConfigWithProductDirect(t, db, fileID, productID, "fa-test-uc-full")

	ucFile := insertFileDirect(t, db, "usecases/fot/REQ-001-SC-001.md")
	insertSpecUsecaseFullDirect(t, db, ucFile, cfgID,
		"REQ-001-SC-001 — Расчет ставки",
		"Сценарий REQ-001/SC-001 — Расчет ставки",
		"Ставка налога по клиенту",
		"Система (автоматически)",
		"Дата выплаты >= 01.01.2026",
		"Применена льготная ставка 20%",
		"Автоматически применяет ставку",
		"usecases", "usecase", 467512912)

	// Вставляем шаги
	for i, step := range []struct{ flow, text string }{
		{"main", "Загрузка документа выплаты"},
		{"main", "Формирование НОВД"},
	} {
		if _, err := db.Exec(
			`INSERT INTO spec_usecase_steps (file_id, usecase_id, flow_kind, step_order, step_text, line_number)
			 VALUES ($1, (SELECT id FROM spec_usecases WHERE usecase_name = $2), $3, $4, $5, $6)`,
			ucFile, "REQ-001-SC-001 — Расчет ставки", step.flow, i+1, step.text, 0,
		); err != nil {
			t.Fatalf("insert step %d: %v", i, err)
		}
	}

	res, err := specsvc.ExecuteSpecUsecase(ctx, db, "REQ-001-SC-001 — Расчет ставки", "")
	if err != nil {
		t.Fatalf("ExecuteSpecUsecase: %v", err)
	}
	uc, ok := res.(*specsvc.SpecUsecaseResult)
	if !ok {
		t.Fatalf("expected *SpecUsecaseResult, got %T", res)
	}
	if uc.Description == "" || uc.Actors == "" || uc.BusinessValue == "" {
		t.Fatalf("fields empty: desc=%q actors=%q bv=%q", uc.Description, uc.Actors, uc.BusinessValue)
	}
	if uc.PageID != 467512912 {
		t.Fatalf("PageID = %d, want 467512912", uc.PageID)
	}
	if len(uc.Steps) != 2 {
		t.Fatalf("steps = %d, want 2", len(uc.Steps))
	}
}

func TestExecuteSpecSearchWeightedRequirementName(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()
	fileID := insertFileDirect(t, db, "specs/weighted/spec.md")
	capID := insertSpecConfigAndCapabilityDirect(t, db, fileID, "weighted")
	nameID := insertRequirementDirect(t, db, fileID, capID, "арест", "обычный текст", 3, 5)
	bodyID := insertRequirementDirect(t, db, fileID, capID, "обычное требование", "арест в описательном тексте требования", 6, 8)
	if err := db.EnsureSpecSearchVectors(ctx, fileID); err != nil {
		t.Fatalf("EnsureSpecSearchVectors: %v", err)
	}

	result, err := specsvc.ExecuteSpecSearch(ctx, db, "арест", "", "requirement", "exact", 10)
	if err != nil {
		t.Fatalf("ExecuteSpecSearch: %v", err)
	}
	var nameRank, bodyRank float64
	for _, hit := range result.Exact {
		switch hit.EntityID {
		case nameID:
			nameRank = hit.Rank
		case bodyID:
			bodyRank = hit.Rank
		}
	}
	if nameRank <= bodyRank || bodyRank == 0 {
		t.Fatalf("name rank = %v, body rank = %v, hits = %+v", nameRank, bodyRank, result.Exact)
	}
}

func TestExecuteSpecSearchTrgmMentionFindsLongScenario(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()
	fileID := insertFileDirect(t, db, "specs/mentions/spec.md")
	capID := insertSpecConfigAndCapabilityDirect(t, db, fileID, "mentions")
	reqID := insertRequirementDirect(t, db, fileID, capID, "API requirement", "requirement body", 3, 5)
	longText := strings.Repeat("длинный описательный текст сценария ", 80)
	scenarioID := insertScenarioDirect(t, db, fileID, reqID, "Long scenario", longText, longText, longText, 6, 20)
	insertCodeMentionDirect(t, db, fileID, "spec_scenario", scenarioID, "API_CCred_BindClassifier", 12)
	if err := db.EnsureSpecSearchVectors(ctx, fileID); err != nil {
		t.Fatalf("EnsureSpecSearchVectors: %v", err)
	}

	for _, query := range []string{"api_ccred_bindclassifier", "API_CCred_BindClassifie"} {
		result, err := specsvc.ExecuteSpecSearch(ctx, db, query, "", "scenario", "exact", 10)
		if err != nil {
			t.Fatalf("ExecuteSpecSearch %q: %v", query, err)
		}
		found := false
		for _, hit := range result.Exact {
			if hit.EntityID == scenarioID && hit.Level == "scenario" && hit.Source == "trgm" {
				found = true
			}
		}
		if !found {
			t.Fatalf("scenario mention not found for %q: %+v", query, result.Exact)
		}
	}
}

func TestExecuteSpecSearchTrgmMentionSourcesAndDedup(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()
	fileID := insertFileDirect(t, db, "specs/sources/spec.md")
	capID := insertSpecConfigAndCapabilityDirect(t, db, fileID, "sources")
	reqID := insertRequirementDirect(t, db, fileID, capID, "Requirement", "Requirement body", 10, 12)
	scenarioID := insertScenarioDirect(t, db, fileID, reqID, "Scenario", "given", "when", "then", 13, 16)
	ucConfigID := func() int64 {
		var id int64
		if err := db.QueryRow(`INSERT INTO spec_configs (file_id, product_name) VALUES ($1, 'sources') RETURNING id`, fileID).Scan(&id); err != nil {
			t.Fatalf("insert usecase config: %v", err)
		}
		return id
	}()
	ucFileID := insertFileDirect(t, db, "usecases/sources.md")
	var usecaseID int64
	if err := db.QueryRow(`
		INSERT INTO spec_usecases (file_id, spec_config_id, usecase_name, title, description, line_start, line_end)
		VALUES ($1, $2, 'UsecaseMention', 'Usecase title', 'Usecase description', 30, 40) RETURNING id`,
		ucFileID, ucConfigID).Scan(&usecaseID); err != nil {
		t.Fatalf("insert usecase: %v", err)
	}

	const mention = "API_SourceMention"
	insertCodeMentionDirect(t, db, fileID, "spec_capability", capID, mention, 4)
	insertCodeMentionDirect(t, db, fileID, "spec_requirement", reqID, mention, 11)
	insertCodeMentionDirect(t, db, fileID, "spec_scenario", scenarioID, mention, 14)
	insertCodeMentionDirect(t, db, fileID, "spec_scenario", scenarioID, mention, 15)
	insertCodeMentionDirect(t, db, ucFileID, "spec_usecase", usecaseID, mention, 35)

	result, err := specsvc.ExecuteSpecSearch(ctx, db, "api_sourcemention", "", "", "exact", 100)
	if err != nil {
		t.Fatalf("ExecuteSpecSearch: %v", err)
	}
	seen := map[string]specsvc.SpecSearchHit{}
	for _, hit := range result.Exact {
		key := hit.Level + ":" + fmt.Sprint(hit.EntityID)
		if hit.Source == "trgm" {
			seen[key] = hit
		}
	}
	for _, tc := range []struct {
		level string
		id    int64
		line  int
		capID int64
	}{
		{level: "capability", id: capID, line: 1, capID: capID},
		{level: "requirement", id: reqID, line: 10, capID: capID},
		{level: "scenario", id: scenarioID, line: 13, capID: capID},
		{level: "usecase", id: usecaseID, line: 30, capID: 0},
	} {
		hit, ok := seen[tc.level+":"+fmt.Sprint(tc.id)]
		if !ok || hit.LineStart != tc.line || hit.Snippet == "" || hit.CapabilityID != tc.capID {
			t.Fatalf("missing/invalid %s hit: %+v; all=%+v", tc.level, hit, result.Exact)
		}
	}
	countScenario := 0
	for _, hit := range result.Exact {
		if hit.Level == "scenario" && hit.EntityID == scenarioID {
			countScenario++
		}
	}
	if countScenario != 1 {
		t.Fatalf("scenario duplicate count = %d, want 1", countScenario)
	}
}

func explainPlan(t *testing.T, db *store.DB, query string) string {
	t.Helper()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`SET LOCAL enable_seqscan = off`); err != nil {
		t.Fatalf("disable seqscan: %v", err)
	}
	rows, err := tx.Query("EXPLAIN (COSTS OFF) " + query)
	if err != nil {
		t.Fatalf("EXPLAIN: %v", err)
	}
	defer rows.Close()
	var plan strings.Builder
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatal(err)
		}
		plan.WriteString(line)
		plan.WriteByte('\n')
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return plan.String()
}

func explainUsesIndex(t *testing.T, db *store.DB, query, indexName string) {
	t.Helper()
	plan := explainPlan(t, db, query)
	if !strings.Contains(plan, indexName) {
		t.Fatalf("plan does not use %s:\n%s", indexName, plan)
	}
}

func TestSpecSearchTrgmIndexes(t *testing.T) {
	db := testutil.Open(t)
	explainUsesIndex(t, db, `SELECT id FROM spec_capabilities WHERE related_code % 'API_CCred_BindClassifier'`, "idx_spec_capabilities_related_code_trgm")
	explainUsesIndex(t, db, `SELECT id FROM spec_requirements WHERE body_text % 'API_CCred_BindClassifier'`, "idx_spec_requirements_body_trgm")
	explainUsesIndex(t, db, `SELECT id FROM spec_scenarios WHERE (scenario_name || ' ' || COALESCE(given_text, '') || ' ' || COALESCE(when_text, '') || ' ' || COALESCE(then_text, '')) % 'API_CCred_BindClassifier'`, "idx_spec_scenarios_text_trgm")
	explainUsesIndex(t, db, `SELECT id FROM spec_usecases WHERE (usecase_name || ' ' || title || ' ' || COALESCE(description, '') || ' ' || COALESCE(actors, '') || ' ' || COALESCE(preconditions, '') || ' ' || COALESCE(postconditions, '') || ' ' || COALESCE(business_value, '') || ' ' || COALESCE(architecture, '') || ' ' || COALESCE(data_schema, '')) % 'API_CCred_BindClassifier'`, "idx_spec_usecases_text_trgm")
	explainUsesIndex(t, db, `SELECT id FROM spec_code_mentions WHERE mention_name % 'API_CCred_BindClassifier'`, "idx_spec_code_mentions_name_trgm")
	mentionPlan := explainPlan(t, db, `SELECT id FROM spec_code_mentions WHERE (LOWER(mention_name) = LOWER('API_CCred_BindClassifier') OR mention_name % 'API_CCred_BindClassifier')`)
	if strings.Contains(mentionPlan, "Seq Scan on spec_code_mentions") {
		t.Fatalf("production mention predicate uses sequential scan:\n%s", mentionPlan)
	}
	if !strings.Contains(mentionPlan, "idx_spec_code_mentions_name_lower") && !strings.Contains(mentionPlan, "idx_spec_code_mentions_name_trgm") {
		t.Fatalf("production mention predicate uses neither mention index:\n%s", mentionPlan)
	}
}

func TestExecuteSpecUsecase_ByPageId(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()

	productID := insertDSProductDirect(t, db, "fa-test-uc-pageid")
	fileID := insertFileDirect(t, db, "openspec/config.yaml")
	cfgID := insertSpecConfigWithProductDirect(t, db, fileID, productID, "fa-test-uc-pageid")

	ucFile := insertFileDirect(t, db, "usecases/fot/REQ-002.md")
	insertSpecUsecaseFullDirect(t, db, ucFile, cfgID,
		"REQ-002-SC-001", "Сценарий REQ-002", "", "", "", "", "",
		"usecases", "usecase", 999888777)

	res, err := specsvc.ExecuteSpecUsecase(ctx, db, "999888777", "")
	if err != nil {
		t.Fatalf("ExecuteSpecUsecase by pageId: %v", err)
	}
	uc, ok := res.(*specsvc.SpecUsecaseResult)
	if !ok {
		t.Fatalf("expected *SpecUsecaseResult, got %T", res)
	}
	if uc.UsecaseName != "REQ-002-SC-001" {
		t.Fatalf("usecase_name = %q, want REQ-002-SC-001", uc.UsecaseName)
	}
}

func TestExecuteSpecUsecase_ListByProduct(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()

	productID := insertDSProductDirect(t, db, "fa-test-uc-list")
	fileID := insertFileDirect(t, db, "openspec/config.yaml")
	cfgID := insertSpecConfigWithProductDirect(t, db, fileID, productID, "fa-test-uc-list")

	ucFile1 := insertFileDirect(t, db, "usecases/fot/REQ-001.md")
	insertSpecUsecaseFullDirect(t, db, ucFile1, cfgID,
		"REQ-001-SC-001", "Сценарий 1", "", "", "", "", "",
		"usecases", "usecase", 0)

	ucFile2 := insertFileDirect(t, db, "usecases/fot/REQ-002.md")
	insertSpecUsecaseFullDirect(t, db, ucFile2, cfgID,
		"REQ-002-SC-001", "Сценарий 2", "", "", "", "", "",
		"usecases", "usecase", 0)

	ucFile3 := insertFileDirect(t, db, "usecases/msfo/REQ-003.md")
	insertSpecUsecaseFullDirect(t, db, ucFile3, cfgID,
		"REQ-003-SC-001", "Сценарий 3", "", "", "", "", "",
		"usecases", "usecase", 0)

	res, err := specsvc.ExecuteSpecUsecase(ctx, db, "", "fa-test-uc-list")
	if err != nil {
		t.Fatalf("ExecuteSpecUsecase list: %v", err)
	}
	list, ok := res.(*specsvc.SpecUsecaseListResult)
	if !ok {
		t.Fatalf("expected *SpecUsecaseListResult, got %T", res)
	}
	if len(list.Usecases) != 3 {
		t.Fatalf("usecases count = %d, want 3", len(list.Usecases))
	}
	for _, item := range list.Usecases {
		if item.SourceDir != "usecases" {
			t.Fatalf("source_dir = %q, want usecases", item.SourceDir)
		}
		if item.UsecaseKind != "usecase" {
			t.Fatalf("usecase_kind = %q, want usecase", item.UsecaseKind)
		}
	}
}

// insertSpecChangeWithConfigDirect вставляет spec_change, привязанный к указанному spec_config_id.
func insertSpecChangeWithConfigDirect(t *testing.T, db *store.DB, fileID, cfgID int64, changeName, status string) int64 {
	t.Helper()
	var changeID int64
	if err := db.QueryRow(
		`INSERT INTO spec_changes (file_id, spec_config_id, change_name, status, dir_path)
		 VALUES ($1, $2, $3, $4, 'changes') RETURNING id`,
		fileID, cfgID, changeName, status,
	).Scan(&changeID); err != nil {
		t.Fatalf("insert spec_change %s: %v", changeName, err)
	}
	return changeID
}

// insertCapabilityWithProductDirect вставляет spec_capability с ds_product_id.
func insertCapabilityWithProductDirect(t *testing.T, db *store.DB, fileID, cfgID, productID int64, capName, title string) int64 {
	t.Helper()
	var id int64
	if err := db.QueryRow(
		`INSERT INTO spec_capabilities (file_id, spec_config_id, ds_product_id, capability_name, title)
		 VALUES ($1, $2, $3, $4, $5) RETURNING id`,
		fileID, cfgID, productID, capName, title,
	).Scan(&id); err != nil {
		t.Fatalf("insert spec_capability %s: %v", capName, err)
	}
	return id
}

func TestExecuteSpecHistoryByCapability_WithProductFilter(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()

	// Два продукта с одинаковым slug capability "card-limits".
	productCardsID := insertDSProductDirect(t, db, "fa-cards")
	productPaymentsID := insertDSProductDirect(t, db, "fa-payments")

	cardsFile := insertFileDirect(t, db, "specs/card-limits-cards/spec.md")
	cardsCfgID := insertSpecConfigWithProductDirect(t, db, cardsFile, productCardsID, "fa-cards")
	cardsCapID := insertCapabilityWithProductDirect(t, db, cardsFile, cardsCfgID, productCardsID, "card-limits", "Card Limits")

	paymentsFile := insertFileDirect(t, db, "specs/card-limits-payments/spec.md")
	paymentsCfgID := insertSpecConfigWithProductDirect(t, db, paymentsFile, productPaymentsID, "fa-payments")
	paymentsCapID := insertCapabilityWithProductDirect(t, db, paymentsFile, paymentsCfgID, productPaymentsID, "card-limits", "Card Limits")

	// Change в fa-cards.
	changeFile1 := insertFileDirect(t, db, "changes/change-cards/proposal.md")
	changeID1 := insertSpecChangeWithConfigDirect(t, db, changeFile1, cardsCfgID, "change-cards", "archived")
	insertRelationDirect(t, db, "spec_change", changeID1, "spec_capability", cardsCapID, "change_modifies", "delta")

	// Change в fa-payments.
	changeFile2 := insertFileDirect(t, db, "changes/change-payments/proposal.md")
	changeID2 := insertSpecChangeWithConfigDirect(t, db, changeFile2, paymentsCfgID, "change-payments", "archived")
	insertRelationDirect(t, db, "spec_change", changeID2, "spec_capability", paymentsCapID, "change_modifies", "delta")

	// Фильтр по продукту fa-cards — только change-cards.
	res, err := specsvc.ExecuteSpecHistory(ctx, db, "card-limits", "", "fa-cards")
	if err != nil {
		t.Fatalf("ExecuteSpecHistory with product: %v", err)
	}
	if len(res.Changes) != 1 {
		t.Fatalf("expected 1 change for fa-cards, got %d: %+v", len(res.Changes), res.Changes)
	}
	if res.Changes[0].ChangeName != "change-cards" {
		t.Fatalf("change_name = %q, want change-cards", res.Changes[0].ChangeName)
	}
	if res.Product != "fa-cards" {
		t.Fatalf("product = %q, want fa-cards", res.Product)
	}
}

func TestExecuteSpecHistoryByChange_WithProductFilter(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()

	// Два продукта.
	productCardsID := insertDSProductDirect(t, db, "fa-cards")
	productPaymentsID := insertDSProductDirect(t, db, "fa-payments")

	cardsFile := insertFileDirect(t, db, "specs/cap-cards/spec.md")
	cardsCfgID := insertSpecConfigWithProductDirect(t, db, cardsFile, productCardsID, "fa-cards")
	cardsCapID := insertCapabilityWithProductDirect(t, db, cardsFile, cardsCfgID, productCardsID, "cap-cards", "Cap Cards")

	paymentsFile := insertFileDirect(t, db, "specs/cap-payments/spec.md")
	paymentsCfgID := insertSpecConfigWithProductDirect(t, db, paymentsFile, productPaymentsID, "fa-payments")
	paymentsCapID := insertCapabilityWithProductDirect(t, db, paymentsFile, paymentsCfgID, productPaymentsID, "cap-payments", "Cap Payments")

	// Один change, затрагивающий capabilities в обоих продуктах.
	changeFile := insertFileDirect(t, db, "changes/multi-change/proposal.md")
	changeID := insertSpecChangeWithConfigDirect(t, db, changeFile, cardsCfgID, "multi-change", "archived")
	insertRelationDirect(t, db, "spec_change", changeID, "spec_capability", cardsCapID, "change_modifies", "delta")
	insertRelationDirect(t, db, "spec_change", changeID, "spec_capability", paymentsCapID, "change_modifies", "delta")

	// Фильтр по продукту fa-cards — только cap-cards.
	res, err := specsvc.ExecuteSpecHistory(ctx, db, "", "multi-change", "fa-cards")
	if err != nil {
		t.Fatalf("ExecuteSpecHistory by change with product: %v", err)
	}
	if len(res.Capabilities) != 1 {
		t.Fatalf("expected 1 capability for fa-cards, got %d: %+v", len(res.Capabilities), res.Capabilities)
	}
	if res.Capabilities[0].CapabilityName != "cap-cards" {
		t.Fatalf("capability = %q, want cap-cards", res.Capabilities[0].CapabilityName)
	}
	if res.Capabilities[0].Product != "fa-cards" {
		t.Fatalf("product = %q, want fa-cards", res.Capabilities[0].Product)
	}
}

func TestExecuteSpecHistoryByCapability_NoProductFilter(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()

	// Два продукта с одинаковым slug capability "card-limits".
	productCardsID := insertDSProductDirect(t, db, "fa-cards")
	productPaymentsID := insertDSProductDirect(t, db, "fa-payments")

	cardsFile := insertFileDirect(t, db, "specs/card-limits-cards2/spec.md")
	cardsCfgID := insertSpecConfigWithProductDirect(t, db, cardsFile, productCardsID, "fa-cards")
	cardsCapID := insertCapabilityWithProductDirect(t, db, cardsFile, cardsCfgID, productCardsID, "card-limits", "Card Limits")

	paymentsFile := insertFileDirect(t, db, "specs/card-limits-payments2/spec.md")
	paymentsCfgID := insertSpecConfigWithProductDirect(t, db, paymentsFile, productPaymentsID, "fa-payments")
	paymentsCapID := insertCapabilityWithProductDirect(t, db, paymentsFile, paymentsCfgID, productPaymentsID, "card-limits", "Card Limits")

	// Change в каждом продукте.
	changeFile1 := insertFileDirect(t, db, "changes/change-cards2/proposal.md")
	changeID1 := insertSpecChangeWithConfigDirect(t, db, changeFile1, cardsCfgID, "change-cards2", "archived")
	insertRelationDirect(t, db, "spec_change", changeID1, "spec_capability", cardsCapID, "change_modifies", "delta")

	changeFile2 := insertFileDirect(t, db, "changes/change-payments2/proposal.md")
	changeID2 := insertSpecChangeWithConfigDirect(t, db, changeFile2, paymentsCfgID, "change-payments2", "archived")
	insertRelationDirect(t, db, "spec_change", changeID2, "spec_capability", paymentsCapID, "change_modifies", "delta")

	// Без фильтра — оба change.
	res, err := specsvc.ExecuteSpecHistory(ctx, db, "card-limits", "")
	if err != nil {
		t.Fatalf("ExecuteSpecHistory without product: %v", err)
	}
	if len(res.Changes) != 2 {
		t.Fatalf("expected 2 changes without product filter, got %d: %+v", len(res.Changes), res.Changes)
	}
}

// insertSpecCodeMentionDirect вставляет spec_code_mention, возвращает id.
func insertSpecCodeMentionDirect(t *testing.T, db *store.DB, fileID int64, sourceType string, sourceID int64, mentionName, mentionKind string, lineNumber int) int64 {
	t.Helper()
	var id int64
	if err := db.QueryRow(
		`INSERT INTO spec_code_mentions (file_id, source_type, source_id, mention_name, mention_kind, line_number)
		 VALUES ($1, $2, $3, $4, $5, $6) RETURNING id`,
		fileID, sourceType, sourceID, mentionName, mentionKind, lineNumber,
	).Scan(&id); err != nil {
		t.Fatalf("insert spec_code_mention %s: %v", mentionName, err)
	}
	return id
}

// insertSpecUsecaseDirect вставляет spec_usecase, возвращает id.
func insertSpecUsecaseDirect(t *testing.T, db *store.DB, fileID, cfgID int64, usecaseName string) int64 {
	t.Helper()
	var id int64
	if err := db.QueryRow(
		`INSERT INTO spec_usecases (file_id, spec_config_id, usecase_name, title, source_dir, usecase_kind)
		 VALUES ($1, $2, $3, 'Test Usecase', 'scenarios', 'scenario') RETURNING id`,
		fileID, cfgID, usecaseName,
	).Scan(&id); err != nil {
		t.Fatalf("insert spec_usecase %s: %v", usecaseName, err)
	}
	return id
}

func TestExecuteSpecByCode_WithFilePath(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()

	specPath := "specs/billing/invoicing/spec.md"
	specFile := insertFileDirect(t, db, specPath)
	capID := insertSpecConfigAndCapabilityDirect(t, db, specFile, "billing/invoicing")
	insertSpecCodeMentionDirect(t, db, specFile, "spec_capability", capID, "PROC_TEST_INVOICE", "procedure", 10)

	res, err := specsvc.ExecuteSpecByCode(ctx, db, "PROC_TEST_INVOICE", 50)
	if err != nil {
		t.Fatalf("ExecuteSpecByCode: %v", err)
	}
	if !res.Resolved {
		t.Fatalf("expected Resolved=true, got false")
	}
	if len(res.Hits) == 0 {
		t.Fatalf("expected non-empty Hits, got 0")
	}
	var hit *specsvc.SpecByCodeHit
	for i := range res.Hits {
		if res.Hits[i].CapabilityName == "billing/invoicing" {
			hit = &res.Hits[i]
			break
		}
	}
	if hit == nil {
		t.Fatalf("capability billing/invoicing not found in hits: %+v", res.Hits)
	}
	if hit.File != specPath {
		t.Fatalf("hit.File = %q, want %q", hit.File, specPath)
	}
}

func TestExecuteSpecByCode_UsecaseMention(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()

	usecasePath := "scenarios/scenario-test-uc.md"
	specPath := "specs/billing/invoicing/spec.md"

	usecaseFile := insertFileDirect(t, db, usecasePath)
	specFile := insertFileDirect(t, db, specPath)

	capID := insertSpecConfigAndCapabilityDirect(t, db, specFile, "billing/invoicing")

	// spec_usecase привязан к usecase-файлу, но использует тот же spec_config.
	var cfgID int64
	if err := db.QueryRow(`SELECT spec_config_id FROM spec_capabilities WHERE id = $1`, capID).Scan(&cfgID); err != nil {
		t.Fatalf("get spec_config_id: %v", err)
	}
	ucID := insertSpecUsecaseDirect(t, db, usecaseFile, cfgID, "scenario-test-uc")

	// Связь usecase → capability (usecase_involves).
	insertRelationDirect(t, db, "spec_usecase", ucID, "spec_capability", capID, "usecase_involves", "explicit")

	// Упоминание код-сущности в usecase-файле.
	insertSpecCodeMentionDirect(t, db, usecaseFile, "spec_usecase", ucID, "PROC_TEST_USECASE", "procedure", 5)

	res, err := specsvc.ExecuteSpecByCode(ctx, db, "PROC_TEST_USECASE", 50)
	if err != nil {
		t.Fatalf("ExecuteSpecByCode: %v", err)
	}
	if !res.Resolved {
		t.Fatalf("expected Resolved=true, got false")
	}
	if len(res.Hits) == 0 {
		t.Fatalf("expected non-empty Hits, got 0")
	}
	var hit *specsvc.SpecByCodeHit
	for i := range res.Hits {
		if res.Hits[i].CapabilityName == "billing/invoicing" {
			hit = &res.Hits[i]
			break
		}
	}
	if hit == nil {
		t.Fatalf("capability billing/invoicing not found in hits: %+v", res.Hits)
	}
	if hit.File != usecasePath {
		t.Fatalf("hit.File = %q, want %q (usecase file, not spec.md)", hit.File, usecasePath)
	}
}

func TestExecuteSpecByCode_NotFound(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()

	res, err := specsvc.ExecuteSpecByCode(ctx, db, "NONEXISTENT_PROC_12345", 50)
	if err != nil {
		t.Fatalf("ExecuteSpecByCode: %v", err)
	}
	if res.Resolved {
		t.Fatalf("expected Resolved=false, got true")
	}
	if len(res.Hits) != 0 {
		t.Fatalf("expected 0 hits, got %d: %+v", len(res.Hits), res.Hits)
	}
}

func TestExecuteSpecSearchMissingModelClassification(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()
	fileID := insertFileDirect(t, db, "specs/semantic-missing/spec.md")
	capID := insertSpecConfigAndCapabilityDirect(t, db, fileID, "semantic-missing")
	insertRequirementDirect(t, db, fileID, capID, "арест", "exact body", 2, 4)
	if err := db.EnsureSpecSearchVectors(ctx, fileID); err != nil {
		t.Fatalf("EnsureSpecSearchVectors: %v", err)
	}
	configureSemanticModel(t, filepath.Join(t.TempDir(), "missing-model.bin"))

	both, err := specsvc.ExecuteSpecSearch(ctx, db, "арест", "", "", "both", 10)
	if err != nil {
		t.Fatalf("both search: %v", err)
	}
	if len(both.Exact) == 0 || both.SemanticMeta == nil || both.SemanticMeta.Reason != "unavailable" {
		t.Fatalf("unexpected both result: %+v", both)
	}
	if both.SemanticMeta.Hint == "" {
		t.Fatal("missing unavailable semantic hint")
	}
	_, err = specsvc.ExecuteSpecSearch(ctx, db, "арест", "", "", "semantic", 10)
	if !errors.Is(err, errs.ErrSpecModelNotFound) {
		t.Fatalf("semantic error = %v, want ErrSpecModelNotFound", err)
	}
}

func TestExecuteSpecSearchCorruptedModelIsError(t *testing.T) {
	db := testutil.Open(t)
	modelPath := filepath.Join(t.TempDir(), "corrupted-model.bin")
	if err := os.WriteFile(modelPath, []byte("not a gob model"), 0o644); err != nil {
		t.Fatal(err)
	}
	configureSemanticModel(t, modelPath)
	_, err := specsvc.ExecuteSpecSearch(context.Background(), db, "арест", "", "", "both", 10)
	if err == nil || errors.Is(err, errs.ErrSpecModelNotFound) {
		t.Fatalf("corrupted model error = %v, want non-model-not-found error", err)
	}
}

func TestExecuteSpecSearchMissingGenerationIsError(t *testing.T) {
	db := testutil.Open(t)
	modelPath := filepath.Join(t.TempDir(), "missing-generation-model.bin")
	writeSemanticTestModel(t, modelPath, "generation-without-rows")
	configureSemanticModel(t, modelPath)
	_, err := specsvc.ExecuteSpecSearch(context.Background(), db, "арест", "", "", "semantic", 10)
	if err == nil || !strings.Contains(err.Error(), "has no embeddings") {
		t.Fatalf("missing generation error = %v", err)
	}
}

func TestExecuteSpecSearchDBFailureIsPropagated(t *testing.T) {
	db := testutil.Open(t)
	fileID := insertFileDirect(t, db, "specs/semantic-db/spec.md")
	capID := insertSpecConfigAndCapabilityDirect(t, db, fileID, "semantic-db")
	modelPath := filepath.Join(t.TempDir(), "db-failure-model.bin")
	writeSemanticTestModel(t, modelPath, "generation-with-rows")
	configureSemanticModel(t, modelPath)
	if err := db.PublishSpecLSAGeneration(context.Background(), "generation-with-rows", nil, []model.SpecEmbedding{{
		SpecID: capID, EmbedLevel: "spec", EmbedText: "embedding", Embedding: []float64{1}, EmbedMethod: "tfidf-lsa", EmbedDim: 1,
	}}); err != nil {
		t.Fatalf("PublishSpecLSAGeneration: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := specsvc.ExecuteSpecSearch(ctx, db, "арест", "", "", "semantic", 10)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("DB error = %v, want context.Canceled", err)
	}
}

func TestExecuteSpecByCodeSourceSnippets(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()
	specFile := insertFileDirect(t, db, "specs/by-code/spec.md")
	capID := insertSpecConfigAndCapabilityDirect(t, db, specFile, "by-code")
	if _, err := db.Exec(`UPDATE spec_capabilities SET title = 'capability-marker', purpose = 'capability purpose' WHERE id = $1`, capID); err != nil {
		t.Fatal(err)
	}
	reqID := insertRequirementDirect(t, db, specFile, capID, "Requirement", "requirement-marker", 10, 12)
	scenarioID := insertScenarioDirect(t, db, specFile, reqID, "Scenario", "scenario-marker", "when", "then", 13, 16)

	ucFile := insertFileDirect(t, db, "usecases/by-code.md")
	var cfgID int64
	if err := db.QueryRow(`INSERT INTO spec_configs (file_id, product_name) VALUES ($1, 'by-code') RETURNING id`, ucFile).Scan(&cfgID); err != nil {
		t.Fatalf("insert usecase config: %v", err)
	}
	var usecaseID int64
	if err := db.QueryRow(`
		INSERT INTO spec_usecases (file_id, spec_config_id, usecase_name, title, description, line_start, line_end)
		VALUES ($1, $2, 'by-code-usecase', 'Usecase title', 'usecase-marker', 30, 40) RETURNING id`, ucFile, cfgID).Scan(&usecaseID); err != nil {
		t.Fatalf("insert usecase: %v", err)
	}
	insertRelationDirect(t, db, "spec_usecase", usecaseID, "spec_capability", capID, "usecase_involves", "test")

	const mention = "API_ByCodeMarker"
	insertCodeMentionDirect(t, db, specFile, "spec_capability", capID, mention, 4)
	insertCodeMentionDirect(t, db, specFile, "spec_requirement", reqID, mention, 11)
	insertCodeMentionDirect(t, db, specFile, "spec_scenario", scenarioID, mention, 14)
	insertCodeMentionDirect(t, db, ucFile, "spec_usecase", usecaseID, mention, 35)

	result, err := specsvc.ExecuteSpecByCode(ctx, db, mention, 20)
	if err != nil {
		t.Fatalf("ExecuteSpecByCode: %v", err)
	}
	want := map[string]struct {
		marker string
		file   string
	}{
		"spec_capability":  {marker: "capability-marker", file: "specs/by-code/spec.md"},
		"spec_requirement": {marker: "requirement-marker", file: "specs/by-code/spec.md"},
		"spec_scenario":    {marker: "scenario-marker", file: "specs/by-code/spec.md"},
		"spec_usecase":     {marker: "usecase-marker", file: "usecases/by-code.md"},
	}
	seen := map[string]bool{}
	for _, hit := range result.Hits {
		expected, ok := want[hit.SourceType]
		if !ok {
			continue
		}
		if hit.Snippet == "" || !strings.Contains(hit.Snippet, expected.marker) || hit.File != expected.file {
			t.Fatalf("invalid %s hit: %+v", hit.SourceType, hit)
		}
		seen[hit.SourceType] = true
	}
	for sourceType := range want {
		if !seen[sourceType] {
			t.Fatalf("missing %s source hit: %+v", sourceType, result.Hits)
		}
	}
}
