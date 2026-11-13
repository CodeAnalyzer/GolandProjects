//go:build integration

package review

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/codebase/internal/config"
	"github.com/codebase/internal/indexer"
	"github.com/codebase/internal/store/testutil"
)

// E2e-тесты правила debugProfileEnabled через полный production-путь
// RunSQLFileCtx: чтение файла, replaceMacros/stripMacroDeclarations (все
// #define вырезаются из expanded-контента) и централизованный ремап
// mapExpandedLineToOriginal. Прямой вызов check-функции в юнит-тестах эти
// фазы не покрывает (design Context).

// Ремап-ловушка F2: две #define-строки выше #define PROFILE — без
// LineInOriginalSpace ремап сдвинул бы строку finding.
const dbgProfilePositiveFixture = `#define VER 5
#define DBG 1
#define PROFILE
#include <macros.h>

create procedure Cons_MassOverdraft
as
begin
  PROFILE_BEGIN_EX('Cons_MassOverdraft - start')
  select 1
  PROFILE_END_EX(0, 'Cons_MassOverdraft')
end
`

// Cons_AimCnt_Delete-паттерн: вызовы без define — безопасное состояние.
const dbgProfileNegativeFixture = `#include <macros.h>

create procedure Cons_AimCnt_Delete
as
begin
  PROFILE_BEGIN_EX('Cons_AimCnt_Delete - start')
  PROFILE_END_EX(0, 'Cons_AimCnt_Delete')
end
`

// INS/FCT_AllSync_Reload_proc.h-паттерн: include-заголовок с активным
// define и точками профилирования.
const dbgProfileHeaderPositiveFixture = `#define PROFILE
#include <macros.h>
__BEGIN_PROCEDURE__
  select @v = 1
  PROFILE_TIME_VAR1('t', @v)
__END_PROCEDURE__
`

// dcutil.h-паттерн: активный define, но собственных точек нет.
const dbgProfileHeaderNegativeFixture = `#define PROFILE
#include <macros.h>
#define DC_SAFE(x) select @x = 1
`

func TestCheckDebugProfileEnabledE2E(t *testing.T) {
	db := testutil.Open(t)
	root := t.TempDir()

	posPath := writeMemUniqueFile(t, root, filepath.Join("Server", "Cons_MassOverdraft.sql"), dbgProfilePositiveFixture)
	negPath := writeMemUniqueFile(t, root, filepath.Join("Server", "Cons_AimCnt_Delete.sql"), dbgProfileNegativeFixture)
	hPosPath := writeMemUniqueFile(t, root, filepath.Join("Server", "INS_AllSync_Reload_proc.h"), dbgProfileHeaderPositiveFixture)
	hNegPath := writeMemUniqueFile(t, root, filepath.Join("Server", "dcutil.h"), dbgProfileHeaderNegativeFixture)

	idx := indexer.New(db, &config.Config{Indexer: config.IndexerConfig{
		Parallel:        1,
		BatchSize:       100,
		IncludePatterns: []string{"*.sql", "*.h"},
	}})
	if _, err := idx.InitCtx(context.Background(), root, 1); err != nil {
		t.Fatalf("InitCtx: %v", err)
	}

	// Review требует непустой ds_product_id у целевого файла.
	var productID int64
	if err := db.QueryRow(`INSERT INTO ds_products (product_name) VALUES ('fa-dbgprofile') RETURNING id`).Scan(&productID); err != nil {
		t.Fatalf("insert ds_products: %v", err)
	}
	if _, err := db.Exec(`UPDATE files SET ds_product_id = $1 WHERE path LIKE '%Cons_MassOverdraft.sql' OR path LIKE '%Cons_AimCnt_Delete.sql' OR path LIKE '%INS_AllSync_Reload_proc.h' OR path LIKE '%dcutil.h'`, productID); err != nil {
		t.Fatalf("update files: %v", err)
	}

	r := NewRunner(db)
	runOne := func(path string) *Result {
		t.Helper()
		res, err := r.RunSQLFileCtx(context.Background(), path, Options{Rules: []RuleID{RuleDebugProfileEnabled}})
		if err != nil {
			t.Fatalf("RunSQLFileCtx(%s): %v", path, err)
		}
		return res
	}

	// Positive: finding severity 1 с точной строкой #define PROFILE (3).
	// Строки 1-2 — другие #define, вырезанные из expanded: без
	// LineInOriginalSpace ремап сдвинул бы строку (ловушка F1/F2).
	res := runOne(posPath)
	if len(res.Findings) != 1 {
		t.Fatalf("positive: findings = %d, want 1: %+v", len(res.Findings), res.Findings)
	}
	f := res.Findings[0]
	if f.Rule != RuleDebugProfileEnabled || f.Severity != SeverityDeployStopper {
		t.Fatalf("positive: finding = %+v, want debugProfileEnabled/severity 1", f)
	}
	if f.Line != 3 {
		t.Fatalf("positive: line = %d, want 3 (строка #define PROFILE в исходном файле)", f.Line)
	}
	if !strings.Contains(f.Message, "PROFILE_BEGIN_EX") {
		t.Fatalf("positive: message = %q, want carrier PROFILE_BEGIN_EX", f.Message)
	}

	// Negative: вызовы профилирования без define — безопасное состояние.
	if res := runOne(negPath); len(res.Findings) != 0 {
		t.Fatalf("negative: findings = %d, want 0: %+v", len(res.Findings), res.Findings)
	}

	// .h через полный пайплайн (индексация, tolerant-парсинг, review).
	resH := runOne(hPosPath)
	if len(resH.Findings) != 1 {
		t.Fatalf(".h positive: findings = %d, want 1: %+v", len(resH.Findings), resH.Findings)
	}
	if resH.Findings[0].Severity != SeverityDeployStopper || resH.Findings[0].Line != 1 {
		t.Fatalf(".h positive: finding = %+v, want severity 1 / line 1", resH.Findings[0])
	}
	if res := runOne(hNegPath); len(res.Findings) != 0 {
		t.Fatalf(".h negative (dcutil.h): findings = %d, want 0: %+v", len(res.Findings), res.Findings)
	}
}
