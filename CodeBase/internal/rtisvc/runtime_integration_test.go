//go:build integration

package rtisvc

import (
	"context"
	"sync"
	"testing"

	"github.com/codebase/internal/rti"
	"github.com/codebase/internal/store/testutil"
)

// TestExecuteParse_ReusesSessionForSameFile: повторный ExecuteParse того же
// файла возвращает существующую сессию (тот же session_id, сводка совпадает,
// rti_calls не удвоились); delete сбрасывает кэш — новый parse создаёт
// новую сессию.
func TestExecuteParse_ReusesSessionForSameFile(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()
	p := writeTempFile(t, ".rti", rtiSample)

	r1, err := ExecuteParse(ctx, db, p, ParseOptions{})
	if err != nil {
		t.Fatalf("first ExecuteParse: %v", err)
	}
	if r1.SessionID == 0 {
		t.Fatal("first parse: SessionID = 0")
	}

	r2, err := ExecuteParse(ctx, db, p, ParseOptions{})
	if err != nil {
		t.Fatalf("second ExecuteParse: %v", err)
	}
	if r2.SessionID != r1.SessionID {
		t.Fatalf("reuse: SessionID = %d, want %d", r2.SessionID, r1.SessionID)
	}
	if r2.TotalCalls != r1.TotalCalls {
		t.Fatalf("reuse: TotalCalls = %d, want %d", r2.TotalCalls, r1.TotalCalls)
	}
	if r2.Warning != "" {
		t.Fatalf("reuse: Warning = %q, want empty", r2.Warning)
	}

	// Сводка reuse эквивалентна первому парсингу
	summary, err := rti.LoadSummary(ctx, db, r1.SessionID)
	if err != nil {
		t.Fatalf("LoadSummary: %v", err)
	}
	if summary.TotalCalls != r1.TotalCalls || summary.ErrorsCount != r1.Summary.ErrorsCount {
		t.Fatalf("LoadSummary = %+v, want totals of %+v", summary, r1.Summary)
	}

	// Дублей вызовов нет
	var calls int
	if err := db.QueryRow(`SELECT count(*) FROM rti_calls WHERE session_id = $1`, r1.SessionID).Scan(&calls); err != nil {
		t.Fatalf("count rti_calls: %v", err)
	}
	if calls != r1.TotalCalls {
		t.Fatalf("rti_calls after reuse = %d, want %d", calls, r1.TotalCalls)
	}

	// delete сбрасывает кэш: новый parse → новая сессия
	if err := rti.DeleteSession(ctx, db, r1.SessionID); err != nil {
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

// TestExecuteParse_NewSessionForDifferentParserVersion: смена parser_version
// существующей сессии (имитация переразбора новой версией) — тот же файл даёт
// новую сессию, старая остаётся.
func TestExecuteParse_NewSessionForDifferentParserVersion(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()
	p := writeTempFile(t, ".rti", rtiSample)

	r1, err := ExecuteParse(ctx, db, p, ParseOptions{})
	if err != nil {
		t.Fatalf("first ExecuteParse: %v", err)
	}

	// Сессия разобрана другой (старой) версией парсера
	if _, err := db.Exec(`UPDATE rti_sessions SET parser_version = 0 WHERE id = $1`, r1.SessionID); err != nil {
		t.Fatalf("update parser_version: %v", err)
	}

	r2, err := ExecuteParse(ctx, db, p, ParseOptions{})
	if err != nil {
		t.Fatalf("second ExecuteParse: %v", err)
	}
	if r2.SessionID == r1.SessionID {
		t.Fatalf("same version change: SessionID reused %d", r2.SessionID)
	}

	// Старая сессия осталась в БД
	var exists bool
	if err := db.QueryRow(`SELECT EXISTS(SELECT 1 FROM rti_sessions WHERE id = $1)`, r1.SessionID).Scan(&exists); err != nil {
		t.Fatalf("check old session: %v", err)
	}
	if !exists {
		t.Fatal("old-version session was removed")
	}
}

// TestExecuteParse_ConcurrentSameFileConverges: параллельные parse одного
// файла сходятся к одной сессии (гонка в SetSessionHash разрешается:
// победитель остаётся, проигравшая сессия удалена).
func TestExecuteParse_ConcurrentSameFileConverges(t *testing.T) {
	db := testutil.Open(t)
	p := writeTempFile(t, ".rti", rtiSample)

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
	if err := db.QueryRow(`SELECT count(*) FROM rti_sessions WHERE file_path = $1`, p).Scan(&n); err != nil {
		t.Fatalf("count sessions by path: %v", err)
	}
	if n != 1 {
		t.Fatalf("sessions for same file = %d, want 1 (loser must be deleted)", n)
	}
}
