//go:build integration

package review

import (
	"context"
	"testing"

	"github.com/codebase/internal/store/testutil"
)

// TestBatchLoadIndexCandidatesIsUnique: колонка уникальности в общем кэше
// индексов — is_unique для SQL-определений и index_type IN (2, 3) для
// API-таблиц; дубли имени индекса из разных файлов не размножают поля.
func TestBatchLoadIndexCandidatesIsUnique(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()

	var scanRunID int64
	if err := db.QueryRow(`INSERT INTO scan_runs (root_path, status) VALUES ('/review-fixture', 'finished') RETURNING id`).Scan(&scanRunID); err != nil {
		t.Fatalf("insert scan_run: %v", err)
	}
	newFile := func(path string) int64 {
		var id int64
		if err := db.QueryRow(`
			INSERT INTO files (scan_run_id, path, rel_path, extension, size_bytes, hash_sha256, modified_at)
			VALUES ($1, $2, $2, 'sql', 0, 'x', NOW()) RETURNING id`, scanRunID, path).Scan(&id); err != nil {
			t.Fatalf("insert file %s: %v", path, err)
		}
		return id
	}
	sqlFileID := newFile("/review/fixture/pMemUniq_TmpTbl.sql")
	sqlFile2ID := newFile("/review/fixture-branch/pMemUniq_TmpTbl.sql")
	apiFileID := newFile("/review/fixture/pMemApi.xml")

	// SQL-источник: уникальный XPK, неуникальный XIE и дубль XPK из другой ветки
	newSQLIndex := func(fileID int64, table, name string, isUnique bool, fields ...string) {
		var id int64
		if err := db.QueryRow(`
			INSERT INTO sql_index_definitions (file_id, table_name, index_name, index_fields, index_type, is_unique, definition_kind)
			VALUES ($1, $2, $3, $4, $5, $6, 'create_index') RETURNING id`,
			fileID, table, name, "", map[bool]string{true: "UNIQUE", false: ""}[isUnique], isUnique).Scan(&id); err != nil {
			t.Fatalf("insert sql_index_definitions %s: %v", name, err)
		}
		for i, f := range fields {
			if _, err := db.Exec(`INSERT INTO sql_index_definition_fields (table_index_id, field_name, field_order) VALUES ($1, $2, $3)`, id, f, i+1); err != nil {
				t.Fatalf("insert field %s: %v", f, err)
			}
		}
	}
	newSQLIndex(sqlFileID, "pMemUniq", "XPKpMemUniq", true, "SPID", "KeyCol")
	newSQLIndex(sqlFileID, "pMemUniq", "XIE1pMemUniq", false, "PlainCol")
	newSQLIndex(sqlFile2ID, "pMemUniq", "XPKpMemUniq", true, "SPID", "KeyCol")

	// API-источник: XIE type=0 (неуникальный), XAK type=2 и XPK type=3 (уникальные)
	var apiTableID int64
	if err := db.QueryRow(`
		INSERT INTO api_business_object_tables (file_id, business_object, table_name) VALUES ($1, 'TestObject', 'pMemApi') RETURNING id`, apiFileID).Scan(&apiTableID); err != nil {
		t.Fatalf("insert api_business_object_tables: %v", err)
	}
	newAPIIndex := func(name string, indexType int, fields ...string) {
		var id int64
		if err := db.QueryRow(`
			INSERT INTO api_business_object_table_indexes (business_table_id, index_name, index_type) VALUES ($1, $2, $3) RETURNING id`, apiTableID, name, indexType).Scan(&id); err != nil {
			t.Fatalf("insert api index %s: %v", name, err)
		}
		for i, f := range fields {
			if _, err := db.Exec(`INSERT INTO api_business_object_table_index_fields (table_index_id, field_name, field_order) VALUES ($1, $2, $3)`, id, f, i+1); err != nil {
				t.Fatalf("insert api field %s: %v", f, err)
			}
		}
	}
	newAPIIndex("XIE1pMemApi", 0, "PlainCol")
	newAPIIndex("XAK1pMemApi", 2, "KeyCol")
	newAPIIndex("XPKpMemApi", 3, "SPID", "TemplateID")

	r := NewRunner(db)
	if err := r.batchLoadIndexCandidates(ctx, []string{"pmemuniq", "pmemapi", "pnoindexes"}); err != nil {
		t.Fatalf("batchLoadIndexCandidates: %v", err)
	}

	sqlCands, err := r.lookupTableIndexCandidates(ctx, "pmemuniq")
	if err != nil {
		t.Fatalf("lookupTableIndexCandidates pmemuniq: %v", err)
	}
	// Три кандидата: XPK + XIE + дубль XPK из другой ветки (отдельная строка)
	if len(sqlCands) != 3 {
		t.Fatalf("pmemuniq candidates = %d (%+v), want 3", len(sqlCands), sqlCands)
	}
	byName := map[string][]tableIndexCandidate{}
	for _, c := range sqlCands {
		byName[c.Name] = append(byName[c.Name], c)
	}
	for _, c := range byName["XPKpMemUniq"] {
		if !c.IsUnique {
			t.Fatalf("XPKpMemUniq candidate %+v must be unique", c)
		}
		if len(c.Fields) != 2 || c.Fields[0] != "spid" || c.Fields[1] != "keycol" {
			t.Fatalf("XPKpMemUniq fields = %v, want [spid keycol] (duplicate must not multiply fields)", c.Fields)
		}
	}
	if len(byName["XIE1pMemUniq"]) != 1 || byName["XIE1pMemUniq"][0].IsUnique {
		t.Fatalf("XIE1pMemUniq = %+v, want single non-unique candidate", byName["XIE1pMemUniq"])
	}

	apiCands, err := r.lookupTableIndexCandidates(ctx, "pmemapi")
	if err != nil {
		t.Fatalf("lookupTableIndexCandidates pmemapi: %v", err)
	}
	apiByName := map[string]tableIndexCandidate{}
	for _, c := range apiCands {
		apiByName[c.Name] = c
	}
	if len(apiCands) != 3 {
		t.Fatalf("pmemapi candidates = %d (%+v), want 3", len(apiCands), apiCands)
	}
	if apiByName["XIE1pMemApi"].IsUnique {
		t.Fatalf("index_type=0 must not be unique: %+v", apiByName["XIE1pMemApi"])
	}
	if !apiByName["XAK1pMemApi"].IsUnique {
		t.Fatalf("index_type=2 must be unique: %+v", apiByName["XAK1pMemApi"])
	}
	if !apiByName["XPKpMemApi"].IsUnique {
		t.Fatalf("index_type=3 must be unique: %+v", apiByName["XPKpMemApi"])
	}
	if got := apiByName["XPKpMemApi"].Fields; len(got) != 2 || got[0] != "spid" || got[1] != "templateid" {
		t.Fatalf("XPKpMemApi fields = %v, want [spid templateid]", got)
	}

	// Negative cache: таблица без индексов — пустой кэш без повторного запроса
	empty, err := r.lookupTableIndexCandidates(ctx, "pnoindexes")
	if err != nil {
		t.Fatalf("lookupTableIndexCandidates pnoindexes: %v", err)
	}
	if len(empty) != 0 {
		t.Fatalf("pnoindexes candidates = %+v, want empty", empty)
	}

	// Пустая memory_tables (в фикстуре нет .lst): признак не установлен —
	// правило инертно; повторный вызов не должен менять результат
	if r.isMemoryTable(ctx, "pmemuniq") {
		t.Fatalf("isMemoryTable on empty memory_tables must be false")
	}
	if r.isMemoryTable(ctx, "pmemuniq") {
		t.Fatalf("isMemoryTable repeat call must stay false")
	}
}
