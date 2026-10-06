//go:build integration

package indexer

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/text/encoding/charmap"
)

// TestInitContentEncodingDetection_CP1251SQL — CP1251 SQL-файл (стиль
// fa-administrator) индексируется с детекцией по содержимому: files.encoding
// хранит WIN1251, header-описание процедуры читаемо (без mojibake-рун),
// ScanStats.EncodingRefined учтён.
func TestInitContentEncodingDetection_CP1251SQL(t *testing.T) {
	idx, root := newTestIndexer(t)

	content := `DCL_PROC_BEGIN(CalcPercent_Admin)
                 @ContractID    DSIDENTIFIER
as
/*----------------------------------------------------------------------
CalcPercent_Admin.sql

CalcPercent_Admin - начисление процентов по договору

Возвращает 0 или код ошибки.
----------------------------------------------------------------------*/
__BEGIN_PROCEDURE__(CalcPercent_Admin)
  declare @RetVal int
  select @RetVal = 0
`
	cp1251Bytes, err := charmap.Windows1251.NewEncoder().Bytes([]byte(content))
	if err != nil {
		t.Fatalf("encode CP1251: %v", err)
	}
	fixturePath := filepath.Join(root, "admin", "CalcPercent_Admin.sql")
	if err := os.MkdirAll(filepath.Dir(fixturePath), 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(fixturePath, cp1251Bytes, 0644); err != nil {
		t.Fatalf("write fixture: %v", err)
	}

	stats, err := idx.Init(root, 1)
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	if stats.Errors != 0 {
		t.Fatalf("Init errors = %d", stats.Errors)
	}
	if stats.EncodingRefined < 1 {
		t.Fatalf("stats.EncodingRefined = %d, want >= 1", stats.EncodingRefined)
	}

	relPath := "admin/CalcPercent_Admin.sql"
	var encoding string
	if err := idx.db.QueryRow(`SELECT encoding FROM files WHERE rel_path = $1`, relPath).Scan(&encoding); err != nil {
		t.Fatalf("select files.encoding: %v", err)
	}
	if encoding != "WIN1251" {
		t.Fatalf("files.encoding = %q, want WIN1251", encoding)
	}

	var description string
	if err := idx.db.QueryRow(
		`SELECT description FROM sql_procedures WHERE proc_name = 'CalcPercent_Admin'`,
	).Scan(&description); err != nil {
		t.Fatalf("select procedure description: %v", err)
	}
	if !strings.Contains(description, "начисление процентов по договору") {
		t.Fatalf("description is not readable Russian: %q", description)
	}
	for _, r := range description {
		if r >= 0x2500 && r <= 0x259F {
			t.Fatalf("description contains mojibake box-drawing rune U+%04X: %q", r, description)
		}
	}
}
