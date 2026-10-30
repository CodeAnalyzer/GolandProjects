//go:build integration

package indexer

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestInitXML_RecoveredTableIndexed — битая вложенность «element closed
// by» (фикстура Bug-reports/pAPI_DFA_BfrMassDelPmnt.xml: дублированный
// <Indexses> при незакрытом <Index>) не роняет индексацию: файл
// разбирается после автозакрытия, таблица с полями и индексом попадает
// в api_business_object_*, errors остаётся нулём.
func TestInitXML_RecoveredTableIndexed(t *testing.T) {
	idx, root := newTestIndexer(t)

	fixture, err := os.ReadFile(filepath.Join("..", "..", "Bug-reports", "pAPI_DFA_BfrMassDelPmnt.xml"))
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	relPath := filepath.Join("API_DFA", "DSArchitectData", "BObject", "DigitalFinancialAsset", "Table", "pAPI_DFA_BfrMassDelPmnt.xml")
	tablePath := filepath.Join(root, relPath)
	if err := os.MkdirAll(filepath.Dir(tablePath), 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(tablePath, fixture, 0644); err != nil {
		t.Fatalf("write fixture: %v", err)
	}

	stats, err := idx.Init(root, 1)
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	if stats.Errors != 0 {
		t.Fatalf("Init errors = %d, want 0 (recovered file is not an error)", stats.Errors)
	}
	if stats.XMLSkipped != 0 {
		t.Fatalf("Init XMLSkipped = %d, want 0 (recovered file is indexed)", stats.XMLSkipped)
	}

	var tableID int64
	var businessObject, rusName string
	err = idx.db.QueryRow(`
		SELECT id, business_object, rus_name
		FROM api_business_object_tables
		WHERE table_name = 'pAPI_DFA_BfrMassDelPmnt'`,
	).Scan(&tableID, &businessObject, &rusName)
	if err != nil {
		t.Fatalf("select business object table: %v", err)
	}
	if businessObject != "DigitalFinancialAsset" {
		t.Fatalf("business_object = %q, want DigitalFinancialAsset", businessObject)
	}
	if !strings.Contains(rusName, "выплат") {
		t.Fatalf("rus_name is not readable Russian: %q", rusName)
	}

	var fieldCount int
	if err := idx.db.QueryRow(`
		SELECT count(*) FROM api_business_object_table_fields
		WHERE business_table_id = $1`, tableID,
	).Scan(&fieldCount); err != nil {
		t.Fatalf("count table fields: %v", err)
	}
	if fieldCount != 2 {
		t.Fatalf("table fields = %d, want 2 (PaymentID, DigitalFinancialAssetID)", fieldCount)
	}

	var indexName, indexFields string
	err = idx.db.QueryRow(`
		SELECT index_name, index_fields
		FROM api_business_object_table_indexes
		WHERE business_table_id = $1`, tableID,
	).Scan(&indexName, &indexFields)
	if err != nil {
		t.Fatalf("select table index: %v", err)
	}
	if indexName != "XPKpAPI_DFA_BfrMassDelPmnt" {
		t.Fatalf("index_name = %q", indexName)
	}
	if !strings.Contains(indexFields, "PaymentID") {
		t.Fatalf("index_fields = %q, want SPID, PaymentID", indexFields)
	}
}
