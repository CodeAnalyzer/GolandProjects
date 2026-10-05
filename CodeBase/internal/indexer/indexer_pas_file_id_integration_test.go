//go:build integration

package indexer

import (
	"io"
	"log"
	"testing"

	"github.com/codebase/internal/config"
	"github.com/codebase/internal/store/testutil"
)

// TestInitIndexesPASFileFillsFileID — batch insert PAS-сущностей
// (pas_classes/pas_methods/pas_fields) заполняет file_id тем же значением,
// что и pas_units — прямая привязка к индексируемому файлу.
func TestInitIndexesPASFileFillsFileID(t *testing.T) {
	db := testutil.Open(t)
	root := t.TempDir()
	if err := osWriteFile(root+"/AdmCmd.pas", pasFileIDContent); err != nil {
		t.Fatalf("write .pas: %v", err)
	}
	idx := &Indexer{
		db:          db,
		config:      &config.Config{Indexer: config.IndexerConfig{Parallel: 1, BatchSize: 100, IncludePatterns: []string{"*.pas"}}},
		errorLogger: log.New(io.Discard, "", 0),
		shared:      newIndexerSharedState(),
	}
	if _, err := idx.Init(root, 1); err != nil {
		t.Fatalf("Init: %v", err)
	}

	var fileID int64
	if err := idx.db.QueryRow(`SELECT id FROM files WHERE path LIKE '%AdmCmd.pas'`).Scan(&fileID); err != nil {
		t.Fatalf("resolve file id: %v", err)
	}

	// Все строки PAS-сущностей несут file_id индексируемого файла
	for _, tc := range []struct {
		table  string
		filter string
	}{
		{table: "pas_units", filter: "unit_name = 'AdmCmd'"},
		{table: "pas_classes", filter: "class_name = 'TAdmCmd'"},
		{table: "pas_methods", filter: "method_name = 'Execute'"},
		{table: "pas_fields", filter: "field_name = 'FParamValue'"},
	} {
		var total, withFile int
		if err := idx.db.QueryRow(`SELECT count(*) FROM ` + tc.table + ` WHERE ` + tc.filter).Scan(&total); err != nil {
			t.Fatalf("count %s: %v", tc.table, err)
		}
		if total == 0 {
			t.Fatalf("%s: no rows for %s", tc.table, tc.filter)
		}
		if err := idx.db.QueryRow(`SELECT count(*) FROM ` + tc.table + ` WHERE ` + tc.filter + ` AND file_id = $1`, fileID).Scan(&withFile); err != nil {
			t.Fatalf("count %s with file_id: %v", tc.table, err)
		}
		if total != withFile {
			t.Fatalf("%s: rows = %d, with file_id = %d, want equal", tc.table, total, withFile)
		}
	}
}

const pasFileIDContent = `unit AdmCmd;

interface

type
  TAdmCmd = class(TObject)
  private
    FParamValue: Integer;
  public
    procedure Execute;
  end;

implementation

procedure TAdmCmd.Execute;
begin
end;

end.`
