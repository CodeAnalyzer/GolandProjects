//go:build integration

package trcsvc

import (
	"context"
	"sync"
	"testing"

	"github.com/codebase/internal/trc"
	"github.com/codebase/internal/store/testutil"
)

// TestExecuteParse_ReusesSessionForSameFile: повторный ExecuteParse того же
// файла возвращает существующую сессию (тот же session_id, total_events
// совпадает, trc_events не удвоились); delete сбрасывает кэш.
func TestExecuteParse_ReusesSessionForSameFile_TRC(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()
	p := trcTestPath(t)

	r1, err := ExecuteParse(ctx, db, p)
	if err != nil {
		t.Fatalf("first ExecuteParse: %v", err)
	}
	if r1.SessionID == 0 {
		t.Fatal("first parse: SessionID = 0")
	}

	r2, err := ExecuteParse(ctx, db, p)
	if err != nil {
		t.Fatalf("second ExecuteParse: %v", err)
	}
	if r2.SessionID != r1.SessionID {
		t.Fatalf("reuse: SessionID = %d, want %d", r2.SessionID, r1.SessionID)
	}
	if r2.TotalEvents != r1.TotalEvents {
		t.Fatalf("reuse: TotalEvents = %d, want %d", r2.TotalEvents, r1.TotalEvents)
	}
	if r2.Warning != "" {
		t.Fatalf("reuse: Warning = %q, want empty", r2.Warning)
	}

	// Дублей событий нет
	var events int
	if err := db.QueryRow(`SELECT count(*) FROM trc_events WHERE session_id = $1`, r1.SessionID).Scan(&events); err != nil {
		t.Fatalf("count trc_events: %v", err)
	}
	if events != r1.TotalEvents {
		t.Fatalf("trc_events after reuse = %d, want %d", events, r1.TotalEvents)
	}

	// delete сбрасывает кэш: новый parse → новая сессия
	if err := trc.DeleteSession(ctx, db, r1.SessionID); err != nil {
		t.Fatalf("DeleteSession: %v", err)
	}
	r3, err := ExecuteParse(ctx, db, p)
	if err != nil {
		t.Fatalf("ExecuteParse after delete: %v", err)
	}
	if r3.SessionID == r1.SessionID {
		t.Fatalf("parse after delete reused deleted session %d", r3.SessionID)
	}
}

// TestExecuteParse_InterruptedSessionNotReused: сессия-обрывок без хеша
// (прерванный разбор) не переиспользуется и не мешает новому полному разбору.
func TestExecuteParse_InterruptedSessionNotReused(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()
	p := trcTestPath(t)

	// Имитация обрыва: сессия создана (ParseFileToDB вставляет до парсинга),
	// разбор не завершился — file_hash остался NULL, total_events = 0.
	var orphan int64
	if err := db.QueryRow(
		`INSERT INTO trc_sessions (file_path, file_size, total_events) VALUES ($1, 0, 0) RETURNING id`,
		p,
	).Scan(&orphan); err != nil {
		t.Fatalf("insert orphan session: %v", err)
	}

	r, err := ExecuteParse(ctx, db, p)
	if err != nil {
		t.Fatalf("ExecuteParse with orphan: %v", err)
	}
	if r.SessionID == orphan {
		t.Fatalf("orphan session %d was reused", orphan)
	}
	if r.TotalEvents == 0 {
		t.Fatal("expected full parse with events")
	}

	// Обрывок остался в БД (его чистит delete/prune), новая сессия — с хешем
	var orphanHashed, newHashed int
	if err := db.QueryRow(`SELECT count(*) FROM trc_sessions WHERE id = $1 AND file_hash IS NULL`, orphan).Scan(&orphanHashed); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT count(*) FROM trc_sessions WHERE id = $1 AND file_hash IS NOT NULL`, r.SessionID).Scan(&newHashed); err != nil {
		t.Fatal(err)
	}
	if orphanHashed != 1 || newHashed != 1 {
		t.Fatalf("orphanHashed=%d newHashed=%d, want 1/1", orphanHashed, newHashed)
	}
}

// TestExecuteParse_NewSessionForDifferentParserVersion_TRC: смена
// parser_version существующей сессии — тот же файл даёт новую сессию,
// старая остаётся.
func TestExecuteParse_NewSessionForDifferentParserVersion_TRC(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()
	p := trcTestPath(t)

	r1, err := ExecuteParse(ctx, db, p)
	if err != nil {
		t.Fatalf("first ExecuteParse: %v", err)
	}

	// Сессия разобрана другой (старой) версией парсера
	if _, err := db.Exec(`UPDATE trc_sessions SET parser_version = 0 WHERE id = $1`, r1.SessionID); err != nil {
		t.Fatalf("update parser_version: %v", err)
	}

	r2, err := ExecuteParse(ctx, db, p)
	if err != nil {
		t.Fatalf("second ExecuteParse: %v", err)
	}
	if r2.SessionID == r1.SessionID {
		t.Fatalf("version change: SessionID reused %d", r2.SessionID)
	}

	// Старая сессия осталась в БД
	var exists bool
	if err := db.QueryRow(`SELECT EXISTS(SELECT 1 FROM trc_sessions WHERE id = $1)`, r1.SessionID).Scan(&exists); err != nil {
		t.Fatalf("check old session: %v", err)
	}
	if !exists {
		t.Fatal("old-version session was removed")
	}
}

// TestExecuteParse_ConcurrentSameFileConverges_TRC: параллельные parse
// одного файла сходятся к одной сессии (гонка в SetSessionHash разрешается:
// победитель остаётся, проигравшая сессия удалена).
func TestExecuteParse_ConcurrentSameFileConverges_TRC(t *testing.T) {
	db := testutil.Open(t)
	p := trcTestPath(t)

	start := make(chan struct{})
	results := make([]*ParseResult, 2)
	errs := make([]error, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			results[i], errs[i] = ExecuteParse(context.Background(), db, p)
		}(i)
	}
	close(start)
	wg.Wait()

	for i := range errs {
		if errs[i] != nil {
			t.Fatalf("concurrent ExecuteParse[%d]: %v", i, errs[i])
		}
	}
	if results[0].SessionID != results[1].SessionID {
		t.Fatalf("concurrent parses diverged: %d vs %d", results[0].SessionID, results[1].SessionID)
	}

	// Проигравшая сессия удалена: с этим file_path осталась ровно одна
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM trc_sessions WHERE file_path = $1`, p).Scan(&n); err != nil {
		t.Fatalf("count sessions by path: %v", err)
	}
	if n != 1 {
		t.Fatalf("sessions for same file = %d, want 1 (loser must be deleted)", n)
	}
}
