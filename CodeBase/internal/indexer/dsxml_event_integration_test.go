//go:build integration

package indexer

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestInitEventXML_InfoPathEventContract — InfoPath <event>-форма (фикстура
// Bug-reports/OnAfterScr_MassDel.xml: ложная UTF-8 декларация поверх CP866)
// индексируется целиком: контракт kind=event с читаемой кириллицей,
// input-таблица pAPI_Scr_AfterDelScr с полями ScoreID/Date, ошибок нет.
func TestInitEventXML_InfoPathEventContract(t *testing.T) {
	idx, root := newTestIndexer(t)

	fixture, err := os.ReadFile(filepath.Join("..", "..", "Bug-reports", "OnAfterScr_MassDel.xml"))
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	eventPath := filepath.Join(root, "API_RTG", "DSArchitectData", "BObject", "Rating", "Event", "OnAfterScr_MassDel.xml")
	if err := os.MkdirAll(filepath.Dir(eventPath), 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(eventPath, fixture, 0644); err != nil {
		t.Fatalf("write fixture: %v", err)
	}

	stats, err := idx.Init(root, 1)
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	if stats.Errors != 0 {
		t.Fatalf("Init errors = %d, want 0", stats.Errors)
	}
	if stats.XMLSkipped != 0 {
		t.Fatalf("Init XMLSkipped = %d, want 0 (event file is indexed, not skipped)", stats.XMLSkipped)
	}

	var contractID int64
	var kind, ownerModule, businessObject, shortDescription string
	var implemented bool
	var archApproval int
	err = idx.db.QueryRow(`
		SELECT id, contract_kind, owner_module, business_object, implemented, arch_approval, short_description
		FROM api_contracts
		WHERE contract_name = 'OnAfterScr_MassDel'`,
	).Scan(&contractID, &kind, &ownerModule, &businessObject, &implemented, &archApproval, &shortDescription)
	if err != nil {
		t.Fatalf("select event contract: %v", err)
	}
	if kind != "event" {
		t.Fatalf("contract_kind = %q, want event", kind)
	}
	if ownerModule != "API_RTG" {
		t.Fatalf("owner_module = %q, want API_RTG", ownerModule)
	}
	if businessObject != "Rating" {
		t.Fatalf("business_object = %q, want Rating", businessObject)
	}
	if !implemented {
		t.Fatal("implemented = false, want true")
	}
	if archApproval != 1 {
		t.Fatalf("arch_approval = %d, want 1 (OK)", archApproval)
	}
	if !strings.Contains(shortDescription, "оценки") {
		t.Fatalf("short_description is not readable Russian: %q", shortDescription)
	}

	var tableID int64
	var direction, tableDescription string
	var tableRequired bool
	err = idx.db.QueryRow(`
		SELECT id, direction, required, description
		FROM api_contract_tables
		WHERE contract_id = $1 AND table_name = 'pAPI_Scr_AfterDelScr'`,
		contractID,
	).Scan(&tableID, &direction, &tableRequired, &tableDescription)
	if err != nil {
		t.Fatalf("select event input table: %v", err)
	}
	if direction != "input" {
		t.Fatalf("table direction = %q, want input", direction)
	}
	if !tableRequired {
		t.Fatal("table required = false, want true")
	}
	if !strings.Contains(tableDescription, "оценок") {
		t.Fatalf("table description is not readable Russian: %q", tableDescription)
	}

	type fieldRow struct {
		name, typeName, rusName string
		required                bool
	}
	rows, err := idx.db.Query(`
		SELECT field_name, type_name, required, rus_name
		FROM api_contract_table_fields
		WHERE contract_table_id = $1
		ORDER BY param_order`, tableID)
	if err != nil {
		t.Fatalf("select table fields: %v", err)
	}
	defer rows.Close()
	var fields []fieldRow
	for rows.Next() {
		var f fieldRow
		if err := rows.Scan(&f.name, &f.typeName, &f.required, &f.rusName); err != nil {
			t.Fatalf("scan field: %v", err)
		}
		fields = append(fields, f)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate fields: %v", err)
	}
	if len(fields) != 2 {
		t.Fatalf("fields = %+v, want 2 (ScoreID, Date)", fields)
	}
	scoreID, date := fields[0], fields[1]
	if scoreID.name != "ScoreID" || scoreID.typeName != "DSIDENTIFIER" || !scoreID.required || scoreID.rusName != "ИД оценки" {
		t.Fatalf("ScoreID field = %+v", scoreID)
	}
	if date.name != "Date" || date.typeName != "DSDATETIME" || date.required || date.rusName != "Дата события" {
		t.Fatalf("Date field = %+v", date)
	}

	var symbolCount int
	if err := idx.db.QueryRow(`
		SELECT count(*) FROM symbols
		WHERE symbol_name = 'OnAfterScr_MassDel' AND symbol_type = 'event' AND entity_type = 'xml'`,
	).Scan(&symbolCount); err != nil {
		t.Fatalf("count event symbol: %v", err)
	}
	if symbolCount != 1 {
		t.Fatalf("event symbols = %d, want 1", symbolCount)
	}
}

// TestInitXML_GracefulSkipCounted — message-XML вне DSArchitect-веток
// пропускается без ошибки: файл попадает в files, XMLSkipped растёт,
// errors остаётся нулём.
func TestInitXML_GracefulSkipCounted(t *testing.T) {
	idx, root := newTestIndexer(t)

	relPath := filepath.Join("API_X", "DSArchitectData", "Some", "Message", "note_message.xml")
	skipPath := filepath.Join(root, relPath)
	if err := os.MkdirAll(filepath.Dir(skipPath), 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	content := "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<message><subject>note</subject></message>\n"
	if err := os.WriteFile(skipPath, []byte(content), 0644); err != nil {
		t.Fatalf("write fixture: %v", err)
	}

	stats, err := idx.Init(root, 1)
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	if stats.Errors != 0 {
		t.Fatalf("Init errors = %d, want 0 (skip is not an error)", stats.Errors)
	}
	if stats.XMLSkipped != 1 {
		t.Fatalf("Init XMLSkipped = %d, want 1", stats.XMLSkipped)
	}

	var fileExists bool
	if err := idx.db.QueryRow(
		`SELECT EXISTS(SELECT 1 FROM files WHERE rel_path = $1)`,
		filepath.ToSlash(relPath),
	).Scan(&fileExists); err != nil {
		t.Fatalf("check skipped file row: %v", err)
	}
	if !fileExists {
		t.Fatal("skipped message XML must still be indexed in files")
	}
}
