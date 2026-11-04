//go:build integration

package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/codebase/internal/store/testutil"
)

// TestAnalyzeCodebaseTables — единый ANALYZE после init/update (change
// optimize-query-search-bottlenecks): после вызова last_analyze установлен
// у таблиц списка; отменённый контекст возвращает ошибку (вызывающий
// трактует её как warning, статус прогона не меняется).
func TestAnalyzeCodebaseTables(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()

	if err := db.AnalyzeCodebaseTables(ctx); err != nil {
		t.Fatalf("AnalyzeCodebaseTables: %v", err)
	}

	// pg_stat обновляется асинхронно — ждём появления last_analyze (≤2 c).
	deadline := time.Now().Add(2 * time.Second)
	analyzed := false
	for time.Now().Before(deadline) {
		var cnt int
		if err := db.QueryRowContext(ctx, `
			SELECT COUNT(*) FROM pg_stat_all_tables
			WHERE relname = 'symbols' AND last_analyze IS NOT NULL
		`).Scan(&cnt); err != nil {
			t.Fatalf("read pg_stat_all_tables: %v", err)
		}
		if cnt == 1 {
			analyzed = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !analyzed {
		t.Fatalf("last_analyze not set for symbols after ANALYZE (within 2s)")
	}

	// Отменённый контекст: ANALYZE не игнорирует отмену — метод возвращает
	// ошибку; в runner она превращается в предупреждение (analyzeAfterRun).
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := db.AnalyzeCodebaseTables(canceled); err == nil {
		t.Fatal("canceled context must produce error (treated as warning by caller)")
	}
}
