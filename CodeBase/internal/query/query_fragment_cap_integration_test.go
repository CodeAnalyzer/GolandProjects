//go:build integration

package query

import (
	"context"
	"fmt"
	"testing"

	"github.com/codebase/internal/config"
	"github.com/codebase/internal/store/testutil"
)

// withSQLFragmentMatchCap подменяет query.sql_fragment_match_cap на время
// теста (создавая конфиг по умолчанию, если он не загружен) и восстанавливает
// прежнее состояние после.
func withSQLFragmentMatchCap(t *testing.T, matchCap int) {
	t.Helper()
	oldConfigFile := config.GetConfigFile()
	oldCfg := config.Get()
	var oldCfgCopy config.Config
	if oldCfg != nil {
		oldCfgCopy = *oldCfg
	}
	if config.Get() == nil {
		config.CreateDefault(t.TempDir())
	}
	cfg := config.Get()
	capValue := matchCap
	cfg.Query.SQLFragmentMatchCap = &capValue
	t.Cleanup(func() {
		config.SetConfigFile(oldConfigFile)
		if current := config.Get(); current != nil {
			if oldCfg != nil {
				*current = oldCfgCopy
			} else {
				current.Query.SQLFragmentMatchCap = nil
			}
		}
	})
}

// TestSearchQueryFragmentMatchCap — guard-потолок кандидатов (change
// optimize-query-search-bottlenecks, B5): совпадений меньше потолка —
// результат идентичен выдаче без ограничения (cap=0, прежний план), усечения
// нет; совпадений больше потолка — усечённая выдача с truncated=true и без
// ошибки, каждый возвращённый фрагмент входит в полный результат.
func TestSearchQueryFragmentMatchCap(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()
	scanID, err := db.CreateScanRun(ctx, "/repo-cap")
	if err != nil {
		t.Fatalf("CreateScanRun: %v", err)
	}
	var fileID int64
	if err := db.QueryRow(`
		INSERT INTO files (scan_run_id, path, rel_path, extension, size_bytes, hash_sha256, modified_at, encoding, language)
		VALUES ($1, '/repo-cap/cap.pas', '/repo-cap/cap.pas', 'pas', 1, 'hash', NOW(), 'UTF8', 'PAS')
		RETURNING id
	`, scanID).Scan(&fileID); err != nil {
		t.Fatalf("insert file: %v", err)
	}
	// 4 различных фрагмента с общим маркером TA_CAP.
	for i := 0; i < 4; i++ {
		text := fmt.Sprintf("SELECT %d FROM TA_CAP WHERE X = %d", i, i)
		if _, err := db.Exec(`
			INSERT INTO query_fragments (file_id, parent_type, parent_id, component_name, component_type, query_text, query_hash, context, line_number)
			VALUES ($1, 'pas_method', 1, 'cmp', 'pas', $2, $3, 'select', $4)
		`, fileID, text, fmt.Sprintf("frag-cap-%d", i), i+1); err != nil {
			t.Fatalf("insert fragment %d: %v", i, err)
		}
	}
	q := New(db)

	// cap=0 (явное «без ограничения») — прежний план: эталон полного результата.
	withSQLFragmentMatchCap(t, 0)
	uncapped, truncated, err := q.SearchQueryFragment(ctx, "TA_CAP", 100)
	if err != nil {
		t.Fatalf("SearchQueryFragment (cap=0): %v", err)
	}
	if truncated {
		t.Fatalf("cap=0 must never truncate")
	}
	if len(uncapped) != 4 {
		t.Fatalf("uncapped fragments = %d, want 4: %+v", len(uncapped), uncapped)
	}

	// Потолок выше числа совпадений: та же выдача байт-в-байт, truncated=false.
	withSQLFragmentMatchCap(t, 10)
	cappedHigh, truncatedHigh, err := q.SearchQueryFragment(ctx, "TA_CAP", 100)
	if err != nil {
		t.Fatalf("SearchQueryFragment (cap=10): %v", err)
	}
	if truncatedHigh {
		t.Fatalf("cap above match count must not truncate")
	}
	if len(cappedHigh) != len(uncapped) {
		t.Fatalf("cap=10 result = %d items, want %d", len(cappedHigh), len(uncapped))
	}
	for i := range uncapped {
		if cappedHigh[i].ID != uncapped[i].ID {
			t.Fatalf("cap=10 item %d differs from uncapped: %+v vs %+v", i, cappedHigh[i], uncapped[i])
		}
	}

	// Потолок ниже числа совпадений: усечённая выдача, truncated=true,
	// без ошибки; каждый фрагмент — подмножество полного результата.
	withSQLFragmentMatchCap(t, 2)
	cappedLow, truncatedLow, err := q.SearchQueryFragment(ctx, "TA_CAP", 100)
	if err != nil {
		t.Fatalf("SearchQueryFragment (cap=2): %v", err)
	}
	if !truncatedLow {
		t.Fatalf("cap=2 with 4 matches must be truncated")
	}
	fullIDs := map[int64]bool{}
	for _, f := range uncapped {
		fullIDs[f.ID] = true
	}
	if len(cappedLow) == 0 || len(cappedLow) > 3 {
		t.Fatalf("capped result = %d items, want 1..3 (chosen из ≤ cap+1 кандидатов): %+v", len(cappedLow), cappedLow)
	}
	for _, f := range cappedLow {
		if !fullIDs[f.ID] {
			t.Fatalf("capped item %d not part of full result", f.ID)
		}
	}

	// Регресс порядка аргументов: финальный LIMIT $2 — лимит выдачи, а не
	// потолок: при limit=1 возвращается ровно одна строка (перепутанные
	// $2/$3 дали бы до cap+1 строк).
	limited, _, err := q.SearchQueryFragment(ctx, "TA_CAP", 1)
	if err != nil {
		t.Fatalf("SearchQueryFragment (limit=1): %v", err)
	}
	if len(limited) != 1 {
		t.Fatalf("limit=1 must return exactly 1 row, got %d", len(limited))
	}
}
