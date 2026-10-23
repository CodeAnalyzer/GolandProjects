//go:build integration

package trcsvc

import (
	"context"
	"sync"
	"testing"

	"github.com/codebase/internal/store/testutil"
	"github.com/codebase/internal/trc"
)

// TestExecuteParse_ReusesSessionForSameFile: повторный ExecuteParse того же
// файла возвращает существующую сессию (тот же session_id, total_events
// совпадает, trc_events не удвоились); delete сбрасывает кэш.
func TestExecuteParse_ReusesSessionForSameFile_TRC(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()
	p := trcTestPath(t)

	r1, err := ExecuteParse(ctx, db, p, ParseOptions{})
	if err != nil {
		t.Fatalf("first ExecuteParse: %v", err)
	}
	if r1.SessionID == 0 {
		t.Fatal("first parse: SessionID = 0")
	}
	if r1.UseCount != 1 {
		t.Fatalf("first parse: UseCount = %d, want 1", r1.UseCount)
	}

	r2, err := ExecuteParse(ctx, db, p, ParseOptions{})
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
	if r2.UseCount != 2 {
		t.Fatalf("reuse: UseCount = %d, want 2", r2.UseCount)
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
	r3, err := ExecuteParse(ctx, db, p, ParseOptions{})
	if err != nil {
		t.Fatalf("ExecuteParse after delete: %v", err)
	}
	if r3.SessionID == r1.SessionID {
		t.Fatalf("parse after delete reused deleted session %d", r3.SessionID)
	}
}

// TestExecuteDelete_UseCountLifecycle_TRC: полный цикл счётчика через
// сервисный слой — fresh 1 → reuse 2 → декремент (deleted=false) → parse
// реюзит ту же сессию (счётчик снова 2) → hard-удаление → новый parse →
// последний delete без --hard удаляет окончательно.
func TestExecuteDelete_UseCountLifecycle_TRC(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()
	p := trcTestPath(t)

	r1, err := ExecuteParse(ctx, db, p, ParseOptions{})
	if err != nil {
		t.Fatalf("first ExecuteParse: %v", err)
	}
	r2, err := ExecuteParse(ctx, db, p, ParseOptions{})
	if err != nil {
		t.Fatalf("second ExecuteParse: %v", err)
	}
	if r2.UseCount != 2 {
		t.Fatalf("after 2 parses: UseCount = %d, want 2", r2.UseCount)
	}

	// Общая сессия: delete без --hard декрементирует, данные остаются
	d1, err := ExecuteDelete(ctx, db, DeleteParams{SessionID: r1.SessionID})
	if err != nil {
		t.Fatalf("ExecuteDelete shared: %v", err)
	}
	if d1.Deleted || !d1.Decremented || d1.UseCount != 1 || d1.DeletedCount != 0 {
		t.Fatalf("delete shared = %+v, want deleted=false decremented=true use_count=1", d1)
	}
	if _, err := trc.GetSession(ctx, db, r1.SessionID); err != nil {
		t.Fatalf("session must survive decrement: %v", err)
	}

	// Parse после декремента реюзит ту же сессию, счётчик снова 2
	r3, err := ExecuteParse(ctx, db, p, ParseOptions{})
	if err != nil {
		t.Fatalf("ExecuteParse after decrement: %v", err)
	}
	if r3.SessionID != r1.SessionID || r3.UseCount != 2 {
		t.Fatalf("parse after decrement = (%d, use=%d), want (%d, 2)", r3.SessionID, r3.UseCount, r1.SessionID)
	}

	// Hard-удаление общей сессии
	d2, err := ExecuteDelete(ctx, db, DeleteParams{SessionID: r1.SessionID, Hard: true})
	if err != nil {
		t.Fatalf("ExecuteDelete hard: %v", err)
	}
	if !d2.Deleted || d2.DeletedCount != 1 || len(d2.SessionIDs) != 1 || d2.SessionIDs[0] != r1.SessionID {
		t.Fatalf("delete hard = %+v, want deleted_count=1 [%d]", d2, r1.SessionID)
	}

	// Новый parse → новая сессия; последний delete без --hard удаляет окончательно
	r4, err := ExecuteParse(ctx, db, p, ParseOptions{})
	if err != nil {
		t.Fatalf("ExecuteParse after hard delete: %v", err)
	}
	if r4.SessionID == r1.SessionID || r4.UseCount != 1 {
		t.Fatalf("parse after hard delete = (%d, use=%d), want new session with use=1", r4.SessionID, r4.UseCount)
	}
	d3, err := ExecuteDelete(ctx, db, DeleteParams{SessionID: r4.SessionID})
	if err != nil {
		t.Fatalf("ExecuteDelete last holder: %v", err)
	}
	if !d3.Deleted || d3.DeletedCount != 1 {
		t.Fatalf("delete last holder = %+v, want deleted", d3)
	}
	if _, err := trc.GetSession(ctx, db, r4.SessionID); err == nil {
		t.Fatal("session must be gone after last delete")
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

	r, err := ExecuteParse(ctx, db, p, ParseOptions{})
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

	r1, err := ExecuteParse(ctx, db, p, ParseOptions{})
	if err != nil {
		t.Fatalf("first ExecuteParse: %v", err)
	}

	// Сессия разобрана другой (старой) версией парсера
	if _, err := db.Exec(`UPDATE trc_sessions SET parser_version = 0 WHERE id = $1`, r1.SessionID); err != nil {
		t.Fatalf("update parser_version: %v", err)
	}

	r2, err := ExecuteParse(ctx, db, p, ParseOptions{})
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
			results[i], errs[i] = ExecuteParse(context.Background(), db, p, ParseOptions{})
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

	// Победитель получил +1 за второй вызов: use_count = 2 (оба — держатели)
	var useCount int
	if err := db.QueryRow(`SELECT use_count FROM trc_sessions WHERE file_path = $1`, p).Scan(&useCount); err != nil {
		t.Fatalf("select use_count: %v", err)
	}
	if useCount != 2 {
		t.Fatalf("winner use_count = %d, want 2", useCount)
	}
}
