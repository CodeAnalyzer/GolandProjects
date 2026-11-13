package review

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
)

// Тесты правила debugProfileEnabled
// (change add-review-debug-profile-disabled-rule).

// newDebugProfileRunner — Runner без БД с раздельными raw/expanded контентами:
// rawContent — исходный текст файла (директивы #define на месте), content —
// expanded-результат replaceMacros (все #define вырезаны stripMacroDeclarations).
// Дублирование исходника в rawContent обязательно: checkDebugProfileEnabled
// ищет директивы только по rawContent (design D2).
func newDebugProfileRunner(t *testing.T, path, raw, expanded string) *Runner {
	t.Helper()
	runner := &Runner{}
	runner.exec = &reviewExecContext{
		filePath:   normalizePath(path),
		content:    []byte(expanded),
		rawContent: []byte(raw),
		lines:      strings.Split(expanded, "\n"),
	}
	return runner
}

func tmpDebugProfileFile(t *testing.T, name string) string {
	t.Helper()
	// Файл на диске не нужен: правило читает rawContent из exec, а путь
	// используется только как идентификатор (basename, расширение).
	return filepath.Join(t.TempDir(), name)
}

// Табличные кейсы по сценариям дельта-спеки + регресс-кейсы лексера (3.2).
func TestCheckDebugProfileEnabled(t *testing.T) {
	cases := []struct {
		name      string
		fileName  string
		raw       string
		wantCount int
		wantLine  int
	}{
		{
			name:      "активный define + явный вызов",
			fileName:  "Cons_MassOverdraft.sql",
			raw:       "#define PROFILE\r\n#include <macros.h>\r\nPROFILE_BEGIN_EX('Cons_MassOverdraft - start')\r\nPROFILE_END_EX(0, 'Cons_MassOverdraft')",
			wantCount: 1,
			wantLine:  1,
		},
		{
			name:      "активный define + каркасный носитель без явных вызовов",
			fileName:  "CRD_Connect3DSAllMigCard.sql",
			raw:       "#define PROFILE\n#include <macros.h>\nDCL_PROC_BEGIN(CRD_Connect3DSAllMigCard)\nDCL_PROC_BODY\nDCL_PROC_END",
			wantCount: 1,
			wantLine:  1,
		},
		{
			name:      "закомментированный define",
			fileName:  "Safe_Proc.sql",
			raw:       "-- #define PROFILE\n#include <macros.h>\nPROFILE_BEGIN_EX('start')\nPROFILE_END_EX(0, 'Safe_Proc')",
			wantCount: 0,
		},
		{
			name:      "define отсутствует",
			fileName:  "Cons_AimCnt_Delete.sql",
			raw:       "#include <macros.h>\nPROFILE_BEGIN_EX('Cons_AimCnt_Delete - start')\nPROFILE_END_EX(0, 'Cons_AimCnt_Delete')",
			wantCount: 0,
		},
		{
			name:      "активный define без точек",
			fileName:  "NoPoints.sql",
			raw:       "#define PROFILE\n#include <macros.h>\nselect 1\nupdate t set a = 1",
			wantCount: 0,
		},
		{
			name:      "define в блочном комментарии",
			fileName:  "BlockComment.sql",
			raw:       "/*\n#define PROFILE\n*/\nPROFILE_TIME('checkpoint')",
			wantCount: 0,
		},
		{
			name:      "закомментированный define внутри блочного комментария",
			fileName:  "BlockCommentLine.sql",
			raw:       "/*\n-- #define PROFILE\n*/\nPROFILE_TIME('checkpoint')",
			wantCount: 0,
		},
		{
			name:      "define в строковом литерале при активном define выше",
			fileName:  "Literal.sql",
			raw:       "#define PROFILE\nselect '#define PROFILE'\nPROFILE_END_EX(0, 'Literal')",
			wantCount: 1,
			wantLine:  1,
		},
		{
			name:      "PROFILE_AVAILABLE не триггерит",
			fileName:  "Available.sql",
			raw:       "#define PROFILE_AVAILABLE\n#include <macros.h>\nPROFILE_BEGIN_EX('start')",
			wantCount: 0,
		},
		{
			name:      "undef PROFILE безопасен",
			fileName:  "Undef.sql",
			raw:       "#undef PROFILE\n#include <macros.h>\nPROFILE_BEGIN('start')",
			wantCount: 0,
		},
		{
			name:      "три активных define — один finding на первой строке",
			fileName:  "Triple.sql",
			raw:       "#define PROFILE\nselect 1\n#define PROFILE\nselect 2\n#define PROFILE\nPROFILE_TIME('checkpoint')",
			wantCount: 1,
			wantLine:  1,
		},
		{
			name:      "define после включения macros.h",
			fileName:  "tag13_PsExtendActions.sql",
			raw:       "#include <macros.h>\n\n#define PROFILE\n\n-- 105 строк ниже\nPROFILE_TIME_EX('update')",
			wantCount: 0,
		},
		{
			name:      "include macros.h с пробелами внутри угловых скобок",
			fileName:  "SpacedInclude.sql",
			raw:       "#include < macros.h >\n#define PROFILE\nPROFILE_TIME_EX('update')",
			wantCount: 0,
		},
		{
			name:      "расширение .h",
			fileName:  "INS_AllSync_Reload_proc.h",
			raw:       "#define PROFILE\n#include <macros.h>\n__BEGIN_PROCEDURE__\nPROFILE_TIME_VAR1('t', @v)\n__END_PROCEDURE__",
			wantCount: 1,
			wantLine:  1,
		},
		{
			name:      "расширение .SQL в верхнем регистре",
			fileName:  "LastDateCalc_New.SQL",
			raw:       "#define PROFILE\n#include <macros.h>\nDCL_PROC_BODY\nDCL_PROC_END",
			wantCount: 1,
			wantLine:  1,
		},
		{
			name:      "прочее расширение (.inc)",
			fileName:  "script.inc",
			raw:       "#define PROFILE\n#include <macros.h>\nPROFILE_TIME('checkpoint')",
			wantCount: 0,
		},
		{
			name:      "определение носителя не считается точкой",
			fileName:  "DefineCarrier.sql",
			raw:       "#define PROFILE\n#include <macros.h>\n#define PROFILE_BEGIN_EX(msg_) select @DebugProfile = msg_\nselect 1",
			wantCount: 0,
		},
		{
			name:      "helper-define с носителем в теле не считается точкой",
			fileName:  "HelperDefine.sql",
			raw:       "#define PROFILE\n#include <macros.h>\n#define DBG1(x) PROFILE_TIME_VAR1('t', x)\nselect 1",
			wantCount: 0,
		},
		{
			name:      "многострочный define-блок с носителем на продолжении",
			fileName:  "MultilineDefine.sql",
			raw:       "#define PROFILE\n#include <macros.h>\n#define M1(a) \\\nPROFILE_TIME_VAR1('t', a) \\\n\nselect 1",
			wantCount: 0,
		},
		{
			name:      "define PROFILE внутри чужого define-блока — не директива",
			fileName:  "DefineInsideDefineBlock.sql",
			raw:       "#define DBG(a) foo \\\n#define PROFILE bar \\\nbaz\n#include <macros.h>\nPROFILE_TIME_EX('t')",
			wantCount: 0,
		},
		{
			name:      "include внутри чужого define-блока не нарушает порядок",
			fileName:  "IncludeInsideDefineBlock.sql",
			raw:       "#define DBG(a) foo \\\n#include <macros.h> bar \\\nbaz\n#define PROFILE\nPROFILE_TIME_EX('t')",
			wantCount: 1,
			wantLine:  4,
		},
		{
			name:      "define PROFILE с многострочным телом — реальная директива",
			fileName:  "ProfileWithValue.sql",
			raw:       "#define PROFILE \\\n  1\n#include <macros.h>\nPROFILE_TIME_EX('t')",
			wantCount: 1,
			wantLine:  1,
		},
		{
			name:      "носитель в комментарии не точка",
			fileName:  "CommentCarrier.sql",
			raw:       "#define PROFILE\n-- DCL_PROC_BODY здесь только упоминание\nselect 1",
			wantCount: 0,
		},
		{
			name:      "носитель COMMIT_TRAN",
			fileName:  "CommitCarrier.sql",
			raw:       "#define PROFILE\n#include <macros.h>\nCOMMIT_TRAN",
			wantCount: 1,
			wantLine:  1,
		},
		{
			// Известное ограничение лексера (design Risks): вложенные блочные
			// комментарии T-SQL не поддерживаются — комментарий закрывается на
			// первом */, поэтому define строки 2 остаётся «активным».
			// Тест фиксирует фактическое поведение.
			name:      "вложенные блочные комментарии — известное ограничение лексера",
			fileName:  "NestedComments.sql",
			raw:       "/* outer /* inner */\n#define PROFILE\n*/\nPROFILE_TIME('checkpoint')",
			wantCount: 1,
			wantLine:  2,
		},
		{
			name:      "директива в литерале с -- внутри",
			fileName:  "LiteralWithComment.sql",
			raw:       "select 'x -- #define PROFILE'\n#define PROFILE\nPROFILE_TIME('checkpoint')",
			wantCount: 1,
			wantLine:  2,
		},
		{
			name:      "многострочный литерал с переносом строки после define",
			fileName:  "MultilineLiteral.sql",
			raw:       "#define PROFILE\nselect 'a\nb'\nPROFILE_TIME('checkpoint')",
			wantCount: 1,
			wantLine:  1,
		},
		{
			name:      "define без include macros.h с точками (транзитивный кейс teg13)",
			fileName:  "teg13_ConvertPaySchedule.sql",
			raw:       "#include <dcUtil.h>\nPROFILE_TIME_EX('update')\nPROFILE_END_EX(0, 'teg13')\n",
			wantCount: 0,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := tmpDebugProfileFile(t, tc.fileName)
			// expanded-контент в юнит-тестах не используется правилом;
			// передаю raw также в content (правило обязано читать rawContent).
			runner := newDebugProfileRunner(t, path, tc.raw, tc.raw)
			file := &indexedFile{Path: path, DsProductID: 1}

			findings, err := runner.checkDebugProfileEnabled(context.Background(), file)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(findings) != tc.wantCount {
				t.Fatalf("expected %d findings, got %d: %+v", tc.wantCount, len(findings), findings)
			}
			if tc.wantCount == 0 {
				return
			}
			f := findings[0]
			if f.Rule != RuleDebugProfileEnabled {
				t.Fatalf("expected rule %s, got %s", RuleDebugProfileEnabled, f.Rule)
			}
			if f.Severity != SeverityDeployStopper {
				t.Fatalf("expected severity %d, got %d", SeverityDeployStopper, f.Severity)
			}
			if f.Line != tc.wantLine {
				t.Fatalf("expected line %d, got %d", tc.wantLine, f.Line)
			}
			if !f.LineInOriginalSpace {
				t.Fatalf("expected LineInOriginalSpace=true")
			}
			if f.File != path {
				t.Fatalf("expected file %s, got %s", path, f.File)
			}
			if !strings.Contains(f.Message, "точка профилирования") {
				t.Fatalf("message must mention profiling point: %q", f.Message)
			}
		})
	}
}

// Контракт raw-контента (задача 3.3): правило находит директиву именно по
// rawContent, когда expanded-контент директив уже не содержит (production-путь
// replaceMacros/stripMacroDeclarations), и не видит директивы в expanded при
// пустом rawContent (стиль тест-хелпера newAPIRuleRunner).
func TestCheckDebugProfileRawContentContract(t *testing.T) {
	path := tmpDebugProfileFile(t, "RawContract.sql")
	raw := "#define PROFILE\n#include <macros.h>\nPROFILE_BEGIN_EX('start')\nPROFILE_END_EX(0, 'RawContract')"
	expanded := "\n\nselect @DebugProfile = 'start'\n"

	runner := newDebugProfileRunner(t, path, raw, expanded)
	file := &indexedFile{Path: path, DsProductID: 1}
	findings, err := runner.checkDebugProfileEnabled(context.Background(), file)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(findings) != 1 || findings[0].Line != 1 {
		t.Fatalf("expected 1 finding at line 1 from rawContent, got %+v", findings)
	}

	// Хелпер в стиле newAPIRuleRunner (только exec.content): директива в
	// expanded отсутствует физически — finding невозможен и не должен
	// появиться из expanded.
	legacy := &Runner{}
	legacy.exec = &reviewExecContext{
		filePath: normalizePath(path),
		content:  []byte(expanded),
		lines:    strings.Split(expanded, "\n"),
	}
	findings2, err := legacy.checkDebugProfileEnabled(context.Background(), file)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(findings2) != 0 {
		t.Fatalf("expected 0 findings without rawContent, got %+v", findings2)
	}
}

// Задача 2.1: accessor rawFileContent возвращает исходный текст с #define,
// fileContent — expanded без них; чужой путь даёт ошибку.
func TestRawFileContentAccessor(t *testing.T) {
	path := tmpDebugProfileFile(t, "Accessor.sql")
	raw := "#define PROFILE\nselect 1"
	expanded := "select 1"
	runner := newDebugProfileRunner(t, path, raw, expanded)

	gotRaw, err := runner.rawFileContent(context.Background(), path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(gotRaw) != raw {
		t.Fatalf("rawFileContent must return original text with #define, got %q", gotRaw)
	}
	gotExpanded, err := runner.fileContent(context.Background(), path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.Contains(string(gotExpanded), "#define") {
		t.Fatalf("fileContent must return expanded text without #define, got %q", gotExpanded)
	}
	if _, err := runner.rawFileContent(context.Background(), path+".other"); err == nil {
		t.Fatalf("expected error for non-active path")
	}
}

// Задача 1.2: правило зарегистрировано в каталоге с метаданными DeployStopper
// и входит в DefaultEnabledRules.
func TestDebugProfileRuleInCatalog(t *testing.T) {
	meta, ok := ruleCatalog[RuleDebugProfileEnabled]
	if !ok {
		t.Fatalf("rule %s must be registered in ruleCatalog", RuleDebugProfileEnabled)
	}
	if meta.Severity != SeverityDeployStopper {
		t.Fatalf("expected catalog severity %d, got %d", SeverityDeployStopper, meta.Severity)
	}
	if !DefaultEnabledRules()[RuleDebugProfileEnabled] {
		t.Fatalf("rule %s must be enabled by default", RuleDebugProfileEnabled)
	}
}
