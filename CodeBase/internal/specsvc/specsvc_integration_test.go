//go:build integration

package specsvc_test

import (
	"context"
	"testing"

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
