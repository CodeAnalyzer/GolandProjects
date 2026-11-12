package review

import (
	"context"
	"os"
	"strings"
	"testing"

	sqlparser "github.com/codebase/internal/parser/sql"
	"github.com/codebase/internal/store"
)

// Тесты правил apiProcDirectCall и apiExecOutsideFacade
// (change add-review-api-proc-direct-call-rule).

// newAPIRuleRunner — Runner без БД с вручную заполненным кэшем владельцев
// процедур (cachedLookupProcedureScopeInfo читает prewarm-кэш до db).
func newAPIRuleRunner(t *testing.T, path, content string, owners map[string]store.ProcedureScopeInfo) *Runner {
	t.Helper()
	runner := &Runner{}
	runner.exec = &reviewExecContext{
		filePath:    normalizePath(path),
		content:     []byte(content),
		lines:       strings.Split(content, "\n"),
		macroResult: replaceMacros(content),
	}
	runner.procProductIDCache = owners
	return runner
}

func parseAPIRuleContent(t *testing.T, content string) *sqlparser.ParseResult {
	t.Helper()
	parsed, err := sqlparser.NewParser().ParseContent(content)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	return parsed
}

func tmpAPIRuleFile(t *testing.T, content string) string {
	t.Helper()
	tmpFile, err := os.CreateTemp("", "test_api_proc_direct_*.sql")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	t.Cleanup(func() { os.Remove(tmpFile.Name()) })
	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("failed to write temp file: %v", err)
	}
	tmpFile.Close()
	return tmpFile.Name()
}

// (a)-(f): чужой/свой/фасадный/неизвестный вызов, дедуп по имени на строке
// первого вхождения, регистронезависимость.
func TestCheckAPIProcDirectCall(t *testing.T) {
	content := `exec @RetVal = API_Foreign_Proc @ID = 1
exec API_Foreign_Proc @ID = 2
EXEC api_foreign_proc @ID = 3
exec API_Own_Proc @ID = 4
exec FCD_Con_Helper @ID = 5
exec API_Unknown_Proc @ID = 6`

	path := tmpAPIRuleFile(t, content)
	file := &indexedFile{Path: path, DsProductID: 1}
	runner := newAPIRuleRunner(t, path, content, map[string]store.ProcedureScopeInfo{
		"api_foreign_proc": {ProductID: 2},
		"api_own_proc":     {ProductID: 1},
		"fcd_con_helper":   {ProductID: 2},
	})

	findings, err := runner.checkAPIProcDirectCall(context.Background(), parseAPIRuleContent(t, content), file)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Ожидание: ровно один finding по API_Foreign_Proc (3 вызова одного имени,
	// в т.ч. lowercase — дедуп на имя); свой API_Own_Proc, фасадный
	// FCD_Con_Helper и отсутствующий в индексе API_Unknown_Proc — чисто.
	if len(findings) != 1 {
		t.Fatalf("expected exactly 1 finding, got %d: %+v", len(findings), findings)
	}
	fnd := findings[0]
	if fnd.Rule != RuleAPIProcDirectCall {
		t.Fatalf("rule = %s, want apiProcDirectCall", fnd.Rule)
	}
	if fnd.Severity != SeverityDeployStopper {
		t.Fatalf("severity = %d, want %d", fnd.Severity, SeverityDeployStopper)
	}
	if fnd.Object != "API_Foreign_Proc" {
		t.Fatalf("object = %s, want API_Foreign_Proc (исходный регистр первого вхождения)", fnd.Object)
	}
	if fnd.Line != 1 {
		t.Fatalf("line = %d, want 1 (первое вхождение)", fnd.Line)
	}
	if fnd.TargetProductID != 2 || fnd.CurrentProductID != 1 {
		t.Fatalf("products: current=%d target=%d, want 1/2", fnd.CurrentProductID, fnd.TargetProductID)
	}
}

// (f): foreignProcedureUsing не формируется для API_*-имён (в т.ч. lowercase),
// при этом сам apiProcDirectCall по тому же контенту срабатывает.
func TestForeignProceduresSkipsAPINamesCaseInsensitive(t *testing.T) {
	content := "exec api_Foreign_Util @ID = 1"

	path := tmpAPIRuleFile(t, content)
	file := &indexedFile{Path: path, DsProductID: 1}
	runner := newAPIRuleRunner(t, path, content, map[string]store.ProcedureScopeInfo{
		"api_foreign_util": {ProductID: 2},
	})

	foreignFindings, err := runner.checkForeignProcedures(context.Background(), parseAPIRuleContent(t, content), file)
	if err != nil {
		t.Fatalf("checkForeignProcedures: %v", err)
	}
	if len(foreignFindings) != 0 {
		t.Fatalf("foreignProcedureUsing должен исключать API_* (в т.ч. lowercase), got %+v", foreignFindings)
	}

	directFindings, err := runner.checkAPIProcDirectCall(context.Background(), parseAPIRuleContent(t, content), file)
	if err != nil {
		t.Fatalf("checkAPIProcDirectCall: %v", err)
	}
	if len(directFindings) != 1 || directFindings[0].Severity != SeverityDeployStopper {
		t.Fatalf("apiProcDirectCall: want 1 deploy-stopper, got %+v", directFindings)
	}
}

// (a)-(h) apiExecOutsideFacade: префиксы фасадов без учёта регистра,
// _fcd_-шаблон, комментарии и строковые литералы не учитываются.
func TestCheckAPIExecOutsideFacade(t *testing.T) {
	cases := []struct {
		name       string
		fileName   string
		content    string
		expectFind bool
		expectLine int
	}{
		{
			name:       "macro in plain script",
			fileName:   "CON_STP_SaveObject.sql",
			content:    "select 1\nAPI_EXEC(API_FinOper_FindLstByID, @RetVal)\n",
			expectFind: true,
			expectLine: 2,
		},
		{
			name:       "uppercase facade",
			fileName:   "FCD_Con_X.sql",
			content:    "API_EXEC(API_X, @RetVal)\n",
			expectFind: false,
		},
		{
			name:       "lowercase facade",
			fileName:   "fcd_log.sql",
			content:    "API_EXEC(API_X, @RetVal)\n",
			expectFind: false,
		},
		{
			name:       "facade_ prefix",
			fileName:   "Facade_Con_LoanData.sql",
			content:    "API_EXEC(API_X, @RetVal)\n",
			expectFind: false,
		},
		{
			name:       "api_ prefix",
			fileName:   "API_Credit_InitEvent.sql",
			content:    "API_EXEC(API_X, @RetVal)\n",
			expectFind: false,
		},
		{
			name:       "facade seed template",
			fileName:   "_fcd_tpl_seed.sql",
			content:    "API_EXEC(FCD_SaveRangeByBrief, @RetVal)\n",
			expectFind: false,
		},
		{
			name:       "macro in line comment",
			fileName:   "CON_STP_X.sql",
			content:    "-- API_EXEC(API_X)\nselect 1\n",
			expectFind: false,
		},
		{
			name:       "macro in block comment",
			fileName:   "CON_STP_X.sql",
			content:    "/* API_EXEC(API_X) */\nselect 1\n",
			expectFind: false,
		},
		{
			name:       "macro in string literal",
			fileName:   "CON_STP_X.sql",
			content:    "select 'text API_EXEC(API_X) tail'\n",
			expectFind: false,
		},
		{
			// R2-регресс: -- внутри литерала не должен вырезать хвост строки
			// и уводить последующий API_EXEC под «незакрытую строку».
			name:       "literal containing line comment marker",
			fileName:   "CON_STP_X.sql",
			content:    "select 'foo -- bar'\nAPI_EXEC(API_X, @RetVal)\n",
			expectFind: true,
			expectLine: 2,
		},
		{
			// R2-регресс: \n внутри литерала сохраняется — номер строки точен.
			name:       "multiline literal before macro",
			fileName:   "CON_STP_X.sql",
			content:    "select 'a\nb\nc'\nAPI_EXEC(API_X, @RetVal)\n",
			expectFind: true,
			expectLine: 4,
		},
		{
			// R2-регресс: апостроф в комментарии не открывает строку.
			name:       "line comment with apostrophe",
			fileName:   "CON_STP_X.sql",
			content:    "-- don't do this\nAPI_EXEC(API_X, @RetVal)\n",
			expectFind: true,
			expectLine: 2,
		},
		{
			name:       "block comment with apostrophe",
			fileName:   "CON_STP_X.sql",
			content:    "/* don't */\nAPI_EXEC(API_X, @RetVal)\n",
			expectFind: true,
			expectLine: 2,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runner := newAPIRuleRunner(t, tc.fileName, tc.content, nil)
			file := &indexedFile{Path: tc.fileName, DsProductID: 1}
			findings, err := runner.checkAPIExecOutsideFacade(context.Background(), file)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !tc.expectFind {
				if len(findings) != 0 {
					t.Fatalf("expected no findings, got %+v", findings)
				}
				return
			}
			if len(findings) != 1 {
				t.Fatalf("expected 1 finding, got %d: %+v", len(findings), findings)
			}
			fnd := findings[0]
			if fnd.Rule != RuleAPIExecOutsideFacade {
				t.Fatalf("rule = %s, want apiExecOutsideFacade", fnd.Rule)
			}
			if fnd.Severity != SeverityDeployStopper {
				t.Fatalf("severity = %d, want %d", fnd.Severity, SeverityDeployStopper)
			}
			if fnd.Line != tc.expectLine {
				t.Fatalf("line = %d, want %d", fnd.Line, tc.expectLine)
			}
			if fnd.CurrentProductID != 1 {
				t.Fatalf("current product = %d, want 1", fnd.CurrentProductID)
			}
		})
	}
}

// R6-кейсы: ProductID == 0 (владелец не определён) и нормализация
// schema-qualified/bracketed имён парсером.
func TestCheckAPIProcDirectCall_EdgeCases(t *testing.T) {
	ownerless := `exec API_Ownerless_Proc @ID = 1`
	path := tmpAPIRuleFile(t, ownerless)
	file := &indexedFile{Path: path, DsProductID: 1}
	runner := newAPIRuleRunner(t, path, ownerless, map[string]store.ProcedureScopeInfo{
		"api_ownerless_proc": {ProductID: 0},
	})
	findings, err := runner.checkAPIProcDirectCall(context.Background(), parseAPIRuleContent(t, ownerless), file)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(findings) != 0 {
		t.Fatalf("ProductID == 0: finding не нужен, got %+v", findings)
	}

	qualified := `exec dbo.API_Foreign_Proc @ID = 1
exec [API_Foreign_Proc] @ID = 2`
	path = tmpAPIRuleFile(t, qualified)
	file = &indexedFile{Path: path, DsProductID: 1}
	runner = newAPIRuleRunner(t, path, qualified, map[string]store.ProcedureScopeInfo{
		"api_foreign_proc": {ProductID: 2},
	})
	findings, err = runner.checkAPIProcDirectCall(context.Background(), parseAPIRuleContent(t, qualified), file)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Схема/скобки срезаны парсером → одно имя → дедуп даёт один finding.
	if len(findings) != 1 {
		t.Fatalf("qualified names: want 1 finding, got %d: %+v", len(findings), findings)
	}
	if findings[0].Object != "API_Foreign_Proc" || findings[0].Line != 1 {
		t.Fatalf("qualified names: object/line = %s/%d, want API_Foreign_Proc/1", findings[0].Object, findings[0].Line)
	}
}

// Регистронезависимость префиксов фасадов на уровне helper'а.
func TestIsFacadeScriptName(t *testing.T) {
	cases := []struct {
		path   string
		facade bool
	}{
		{"C:/x/FCD_Con.sql", true},
		{"C:/x/fcd_log.sql", true},
		{"C:/x/Facade_Con.sql", true},
		{"C:/x/facade_con.sql", true},
		{"C:/x/API_Credit.sql", true},
		{"C:/x/api_credit.sql", true},
		{"C:/x/_fcd_tpl_seed.sql", true},
		{"C:/x/_FCD_TPL_SEED.SQL", true},
		{"C:/x/CON_STP_SaveObject.sql", false},
		{"C:/x/Sp1.sql", false},
		{"C:/x/myfacade.sql", false},
	}
	for _, tc := range cases {
		if got := isFacadeScriptName(tc.path); got != tc.facade {
			t.Fatalf("isFacadeScriptName(%s) = %v, want %v", tc.path, got, tc.facade)
		}
	}
}
