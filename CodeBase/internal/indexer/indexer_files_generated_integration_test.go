//go:build integration

package indexer

import (
	"io"
	"log"
	"os"
	"path/filepath"
	"testing"

	"github.com/codebase/internal/config"
	"github.com/codebase/internal/store/testutil"
)

// TestInit_MarksGeneratedCopies проверяет, что после индексации тестового
// дерева файлы в каталогах UPLOAD и .t01 помечены is_generated = true,
// а канонические исходники — false (см. indexing/file-walking).
func TestInit_MarksGeneratedCopies(t *testing.T) {
	db := testutil.Open(t)
	root := t.TempDir()

	files := map[string]string{
		"Consumer/SERVER/Accrual/BaseAlg_ConsMinRest.sql":    "CREATE PROCEDURE BaseAlg_ConsMinRest AS BEGIN END",
		"LoanBureau/Server/UPLOAD/BaseAlg_ConsMinRest.sql":   "CREATE PROCEDURE BaseAlg_ConsMinRest AS BEGIN END",
		"API_Credit/Server/UPLOAD/BaseAlgAmrtCostSingle.t01": "CREATE PROCEDURE BaseAlgAmrtCostSingle AS BEGIN END",
	}
	for rel, content := range files {
		path := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			t.Fatalf("mkdir %s: %v", rel, err)
		}
		if err := os.WriteFile(path, []byte(content), 0644); err != nil {
			t.Fatalf("write %s: %v", rel, err)
		}
	}

	idx := &Indexer{
		db: db,
		config: &config.Config{Indexer: config.IndexerConfig{
			Parallel:        1,
			BatchSize:       100,
			IncludePatterns: []string{"*.sql", "*.t01"},
		}},
		errorLogger: log.New(io.Discard, "", 0),
		shared:      newIndexerSharedState(),
	}
	if _, err := idx.Init(root, 1); err != nil {
		t.Fatalf("Init: %v", err)
	}

	want := map[string]bool{
		"Consumer/SERVER/Accrual/BaseAlg_ConsMinRest.sql":    false,
		"LoanBureau/Server/UPLOAD/BaseAlg_ConsMinRest.sql":   true,
		"API_Credit/Server/UPLOAD/BaseAlgAmrtCostSingle.t01": true,
	}
	for rel, expected := range want {
		var got bool
		if err := db.QueryRow(`SELECT is_generated FROM files WHERE rel_path = $1`, rel).Scan(&got); err != nil {
			t.Fatalf("read is_generated for %s: %v", rel, err)
		}
		if got != expected {
			t.Fatalf("%s: is_generated = %v, want %v", rel, got, expected)
		}
	}
}
