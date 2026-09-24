//go:build integration

package indexer

import (
	"context"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/codebase/internal/config"
	"github.com/codebase/internal/store/testutil"
)

// insertMentionFile вставляет scan_run + file для spec_code_mentions.
func insertMentionFile(t *testing.T, idx *Indexer) int64 {
	t.Helper()
	var scanID int64
	if err := idx.db.QueryRow(
		`INSERT INTO scan_runs (root_path, status) VALUES ('/test', 'done') RETURNING id`,
	).Scan(&scanID); err != nil {
		t.Fatalf("insert scan_run: %v", err)
	}
	var fileID int64
	if err := idx.db.QueryRow(
		`INSERT INTO files (scan_run_id, path, rel_path, extension, hash_sha256, modified_at)
		 VALUES ($1, '/test/demo/spec.md', 'demo/spec.md', 'md', 'h', NOW()) RETURNING id`,
		scanID,
	).Scan(&fileID); err != nil {
		t.Fatalf("insert file: %v", err)
	}
	return fileID
}

func insertMention(t *testing.T, idx *Indexer, fileID int64, sourceID int64, name, kind string, line int) {
	t.Helper()
	if _, err := idx.db.Exec(
		`INSERT INTO spec_code_mentions (file_id, source_type, source_id, mention_name, mention_kind, line_number)
		 VALUES ($1, 'spec_capability', $2, $3, $4, $5)`,
		fileID, sourceID, name, kind, line,
	); err != nil {
		t.Fatalf("insert mention %s: %v", name, err)
	}
}

func countMentionRelations(t *testing.T, idx *Indexer, targetType string, targetID int64) int {
	t.Helper()
	var n int
	if err := idx.db.QueryRow(
		`SELECT count(*) FROM relations
		 WHERE relation_type = 'references_code' AND target_type = $1 AND target_id = $2`,
		targetType, targetID,
	).Scan(&n); err != nil {
		t.Fatalf("count relations %s/%d: %v", targetType, targetID, err)
	}
	return n
}

// TestSpecMentionPostProcessing_NewKinds проверяет резолв новых видов
// упоминаний: report → report_forms, event/api_table → api_contracts,
// .pas-фолбэк → dfm_forms, unknown second-chance → sql_procedures,
// и идемпотентность повторного запуска.
func TestSpecMentionPostProcessing_NewKinds(t *testing.T) {
	db := testutil.Open(t)
	idx := &Indexer{
		db:          db,
		config:      &config.Config{Indexer: config.IndexerConfig{BatchSize: 100}},
		errorLogger: log.New(io.Discard, "", 0),
		shared:      newIndexerSharedState(),
	}
	ctx := context.Background()
	fileID := insertMentionFile(t, idx)

	// Сущности-цели
	var reportID, eventContractID, contractA, contractB, procID, formID int64
	if err := idx.db.QueryRow(
		`INSERT INTO report_forms (file_id, report_name, report_type) VALUES ($1, 'form651', 'tpr') RETURNING id`,
		fileID,
	).Scan(&reportID); err != nil {
		t.Fatalf("insert report_form: %v", err)
	}
	if err := idx.db.QueryRow(
		`INSERT INTO api_contracts (file_id, contract_name, contract_kind) VALUES ($1, 'OnAfterPerson_Update', 'event') RETURNING id`,
		fileID,
	).Scan(&eventContractID); err != nil {
		t.Fatalf("insert event contract: %v", err)
	}
	for _, c := range []struct {
		id   *int64
		name string
	}{
		{&contractA, "CON_Accrual_Process"},
		{&contractB, "CON_AfterAccrual_Process"},
	} {
		if err := idx.db.QueryRow(
			`INSERT INTO api_contracts (file_id, contract_name, contract_kind) VALUES ($1, $2, 'callback_event') RETURNING id`,
			fileID, c.name,
		).Scan(c.id); err != nil {
			t.Fatalf("insert contract %s: %v", c.name, err)
		}
	}
	for _, contractID := range []int64{contractA, contractB} {
		if _, err := idx.db.Exec(
			`INSERT INTO api_contract_tables (contract_id, direction, table_name) VALUES ($1, 'input', 'pAPI_Accrual_ObjDate')`,
			contractID,
		); err != nil {
			t.Fatalf("insert api_contract_table: %v", err)
		}
	}
	if err := idx.db.QueryRow(
		`INSERT INTO sql_procedures (file_id, proc_name) VALUES ($1, 'r8938_prc') RETURNING id`,
		fileID,
	).Scan(&procID); err != nil {
		t.Fatalf("insert sql_procedure: %v", err)
	}
	if err := idx.db.QueryRow(
		`INSERT INTO dfm_forms (file_id, form_name, form_class) VALUES ($1, 'RPPortfolio_f', 'TfrmRPPortfolio_f') RETURNING id`,
		fileID,
	).Scan(&formID); err != nil {
		t.Fatalf("insert dfm_form: %v", err)
	}

	// Упоминания пяти видов + заведомо без цели
	const capID int64 = 777
	insertMention(t, idx, fileID, capID, "form651", "report", 10)
	insertMention(t, idx, fileID, capID, "OnAfterPerson_Update", "event", 11)
	insertMention(t, idx, fileID, capID, "pAPI_Accrual_ObjDate", "api_table", 12)
	insertMention(t, idx, fileID, capID, "RPPortfolio_f", "method", 13) // промах в pas_methods → dfm_forms
	insertMention(t, idx, fileID, capID, "r8938_prc", "unknown", 14)   // second-chance
	insertMention(t, idx, fileID, capID, "f123_proc", "unknown", 15)   // без цели

	run := func() {
		collector := &statsCollector{}
		idx.postProcessSpecCodeMentions(ctx, collector)
	}
	run()

	if got := countMentionRelations(t, idx, "report_form", reportID); got != 1 {
		t.Fatalf("report relations = %d, want 1", got)
	}
	if got := countMentionRelations(t, idx, "api_contract", eventContractID); got != 1 {
		t.Fatalf("event relations = %d, want 1", got)
	}
	for _, contractID := range []int64{contractA, contractB} {
		if got := countMentionRelations(t, idx, "api_contract", contractID); got != 1 {
			t.Fatalf("api_table owner %d relations = %d, want 1", contractID, got)
		}
	}
	if got := countMentionRelations(t, idx, "dfm_form", formID); got != 1 {
		t.Fatalf("method fallback form relations = %d, want 1", got)
	}
	if got := countMentionRelations(t, idx, "sql_procedure", procID); got != 1 {
		t.Fatalf("unknown second-chance relations = %d, want 1", got)
	}

	var total int
	if err := idx.db.QueryRow(
		`SELECT count(*) FROM relations WHERE relation_type = 'references_code' AND source_type = 'spec_capability'`,
	).Scan(&total); err != nil {
		t.Fatalf("count total: %v", err)
	}
	if total != 6 {
		t.Fatalf("total spec_capability references_code = %d, want 6 (f123_proc без цели)", total)
	}

	// Идемпотентность: повторный запуск не дублирует рёбра
	run()
	if err := idx.db.QueryRow(
		`SELECT count(*) FROM relations WHERE relation_type = 'references_code' AND source_type = 'spec_capability'`,
	).Scan(&total); err != nil {
		t.Fatalf("count total after rerun: %v", err)
	}
	if total != 6 {
		t.Fatalf("total after rerun = %d, want 6 (идемпотентность)", total)
	}
}

// TestSpecMentionGIVENScan проверяет извлечение inline-упоминаний из
// GIVEN-строк сценариев при полной индексации spec.md: mention из GIVEN,
// отсутствующий в WHEN/THEN, попадает в staging с корректным line_number.
func TestSpecMentionGIVENScan(t *testing.T) {
	db := testutil.Open(t)
	root := t.TempDir()
	specDir := root + "/openspec/specs/demo"
	if err := osWriteFile(specDir+"/spec.md", specGIVENContent); err != nil {
		t.Fatalf("write spec.md: %v", err)
	}
	if err := osWriteFile(root+"/openspec/config.yaml", "schema: spec-driven\n"); err != nil {
		t.Fatalf("write config.yaml: %v", err)
	}
	idx := &Indexer{
		db:     db,
		config: &config.Config{Indexer: config.IndexerConfig{Parallel: 1, BatchSize: 100, IncludePatterns: []string{"*.md", "*.yaml"}}},
		errorLogger: log.New(io.Discard, "", 0),
		shared:      newIndexerSharedState(),
	}
	if _, err := idx.Init(root, 1); err != nil {
		t.Fatalf("Init: %v", err)
	}

	type row struct {
		kind string
		line int
	}
	rows := map[string]row{}
	rs, err := idx.db.Query(
		`SELECT mention_name, mention_kind, line_number FROM spec_code_mentions`,
	)
	if err != nil {
		t.Fatalf("query mentions: %v", err)
	}
	defer rs.Close()
	for rs.Next() {
		var name string
		var r row
		if err := rs.Scan(&name, &r.kind, &r.line); err != nil {
			t.Fatalf("scan mention: %v", err)
		}
		rows[name] = r
	}
	if rs.Err() != nil {
		t.Fatalf("rows: %v", rs.Err())
	}

	// LineNumber упоминаний сценария считается от s.LineStart: GWT-поля
	// (given/when/then) не несут абсолютных офсетов строк файла — формула
	// s.LineStart + <строка в конкатенации Given+When+Then> - 1, та же
	// семантика, что у WHEN/THEN-упоминаний.
	scenarioLine := specGIVENLine("#### Scenario:")
	if scenarioLine < 0 {
		t.Fatalf("scenario header not found in test content")
	}

	given, ok := rows["Rpt_Sheme_Demo"]
	if !ok {
		t.Fatalf("GIVEN mention Rpt_Sheme_Demo not extracted: %+v", rows)
	}
	if given.kind != "procedure" {
		t.Fatalf("GIVEN mention kind = %s, want procedure", given.kind)
	}
	// given — строка 1 конкатенации Given+When+Then
	if want := scenarioLine; given.line != want {
		t.Fatalf("GIVEN mention line = %d, want %d (от LineStart сценария)", given.line, want)
	}

	report, ok := rows["demo"]
	if !ok {
		t.Fatalf("THEN report mention not extracted: %+v", rows)
	}
	if report.kind != "report" {
		t.Fatalf("report mention kind = %s, want report", report.kind)
	}
	// then — строка 3 конкатенации Given+When+Then (given=1, when=2, then=3)
	if want := scenarioLine + 2; report.line != want {
		t.Fatalf("report mention line = %d, want %d (от LineStart сценария)", report.line, want)
	}
}

const specGIVENContent = `# Demo

## Purpose

Демо-спека для проверки извлечения упоминаний из GIVEN-строк.

## Requirements

### Requirement: Расчёт по схеме

Система SHALL выполнять расчёт и формировать отчёт.

#### Scenario: GIVEN с упоминанием кода

- **GIVEN** расчёт выполняется по схеме ` + "`Rpt_Sheme_Demo.sql`" + `
- **WHEN** оператор запускает обработку
- **THEN** формируется отчетная форма demo.tpr
`

func specGIVENLine(marker string) int {
	line := 1
	for _, l := range strings.Split(specGIVENContent, "\n") {
		if strings.Contains(l, marker) {
			return line
		}
		line++
	}
	return -1
}

func osWriteFile(path, content string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, []byte(content), 0o644)
}
