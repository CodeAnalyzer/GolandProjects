package review

import (
	"context"
	"strings"
	"testing"
)

// Тесты маскирования dollar-quoted plpgsql-блоков (change
// mask-plpgsql-dollar-quoted-blocks). Ключевые инварианты — как у
// maskSQLComments: сохранение байтовой длины и позиций \n/\r.

func TestMaskDollarQuotedBlocks_Exact(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "dollar body masked",
			in:   "do $$\nbegin\nend\n$$;\nselect 2",
			want: "do   \n     \n   \n  ;\nselect 2",
		},
		{
			name: "tagged body masked",
			in:   "as $body$\nbegin\nend\n$body$;\nselect 2",
			want: "as       \n     \n   \n      ;\nselect 2",
		},
		{
			name: "code without dollars untouched",
			in:   "select 'a' from t\nupdate t set a = 1",
			want: "select 'a' from t\nupdate t set a = 1",
		},
		{
			name: "CRLF preserved inside body",
			in:   "do $$\r\nx = 1\r\n$$;\r\nselect 2",
			want: "do   \r\n     \r\n  ;\r\nselect 2",
		},
		{
			name: "create statement masked from header line",
			in:   "create function f(p int = null)\nas $$\nbegin\nend\n$$;\nselect 2",
			want: strings.Repeat(" ", 31) + "\n     \n     \n   \n  ;\nselect 2",
		},
		{
			name: "do block not extended backward",
			in:   "select 1;\ndo $$\nbegin\nend\n$$;\nselect 2",
			want: "select 1;\ndo   \n     \n   \n  ;\nselect 2",
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if got := maskDollarQuotedBlocks(tc.in); got != tc.want {
				t.Fatalf("maskDollarQuotedBlocks(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestMaskDollarQuotedBlocks_Properties(t *testing.T) {
	cases := []struct {
		name string
		in   string
		keep []string // фрагменты кода, которые обязаны сохраниться
		drop []string // фрагменты PG-кода, которые обязаны исчезнуть
	}{
		{
			name: "dollars inside single-quoted literal do not open",
			in:   "select '$$$$$$$$' from t\nselect a = null from t2",
			keep: []string{"select '$$$$$$$$' from t", "select a = null from t2"},
		},
		{
			name: "escaped quote keeps literal open across dollars",
			in:   "select 'it''s $$ body' from t\nselect 2",
			keep: []string{"'it''s $$ body'", "select 2"},
		},
		{
			name: "dollars inside line comment do not open",
			in:   "-- comment $$ here\nselect a = null from t",
			keep: []string{"-- comment $$ here", "select a = null from t"},
		},
		{
			name: "dollars inside block comment do not open",
			in:   "/* $$\nselect 1\n*/\nselect a = null from t",
			keep: []string{"/*", "select 1", "select a = null from t"},
		},
		{
			name: "macro marker $$$NAME not masked",
			in:   "$$$NPPBEGIN$$$\nselect a = null from t\n$$$FAKE_SECTION$$$",
			keep: []string{"$$$NPPBEGIN$$$", "select a = null from t", "$$$FAKE_SECTION$$$"},
		},
		{
			name: "unclosed tag fail-open keeps content",
			in:   "do $$\nbegin\nselect a = null from t\n",
			keep: []string{"do $$", "begin", "select a = null from t"},
		},
		{
			name: "region after failed open still masked",
			in:   "do $$\nbody\nselect 1;\ndo $x$\nselect a = null\n$x$;\nselect 2",
			keep: []string{"do $$", "body", "select 1;", "select 2"},
			drop: []string{"select a = null"},
		},
		{
			name: "macro series inside body does not close prematurely",
			in:   "do $$\n$$$INNER$$$\n$$;\nselect a = null from t",
			keep: []string{"select a = null from t"},
			drop: []string{"$$$INNER$$$"},
		},
		{
			name: "multiple independent regions",
			in:   "do $$ x $$;\nselect 1;\ndo $$ y $$;\nselect 2",
			keep: []string{"select 1;", "select 2"},
			drop: []string{" x ", " y "},
		},
		{
			name: "create header with semicolon before tag masks body only",
			in:   "create proc p1\nas\nbegin\nend\nselect 1;\ncreate or replace function f(p int = null) as $$\nbegin\nend\n$$;\nselect 2",
			keep: []string{"create proc p1", "begin", "end", "select 1;", "select 2"},
			drop: []string{"= null"},
		},
		{
			name: "PG signature defaults masked via create extension",
			in:   "create or replace function f(\n  p1 dsidentifier = null,\n  p2 dstype = null\n)\nlanguage plpgsql as $$\nbegin\nend\n$$;\nselect a = null from t",
			keep: []string{"select a = null from t"},
			drop: []string{"dsidentifier", "plpgsql"},
		},
		{
			name: "plain create procedure without dollars untouched",
			in:   "create procedure p\nas\nbegin\nselect a = null from t\nend\n",
			keep: []string{"create procedure p", "select a = null from t", "end"},
		},
		{
			name: "nested block comment keeps dollars as comment text",
			in:   "/* a /* b */ c $$ */\ndo $$\nbody\n$$;\nselect a = null from t",
			keep: []string{"/* a /* b */ c $$ */", "select a = null from t"},
			drop: []string{"body"},
		},
		{
			name: "unclosed nested comment consumes to EOF",
			in:   "/* a /* b $$\nselect a = null from t",
			keep: []string{"/* a /* b $$", "select a = null from t"},
		},
		{
			name: "positional $1$ pairs are not a dollar region",
			in:   "select $1$ x $1$ from t\nselect a = null from t2",
			keep: []string{"select $1$ x $1$ from t", "select a = null from t2"},
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got := maskDollarQuotedBlocks(tc.in)
			if len(got) != len(tc.in) {
				t.Fatalf("%s: byte len = %d, want %d (got %q)", tc.name, len(got), len(tc.in), got)
			}
			if strings.Count(got, "\n") != strings.Count(tc.in, "\n") {
				t.Fatalf("%s: newline count changed: %q", tc.name, got)
			}
			if strings.Count(got, "\r") != strings.Count(tc.in, "\r") {
				t.Fatalf("%s: CR count changed: %q", tc.name, got)
			}
			for _, k := range tc.keep {
				if !strings.Contains(got, k) {
					t.Fatalf("%s: want %q kept in result %q", tc.name, k, got)
				}
			}
			for _, d := range tc.drop {
				if strings.Contains(got, d) {
					t.Fatalf("%s: want %q masked in result %q", tc.name, d, got)
				}
			}
		})
	}
}

// TestMaskDollarQuotedBlocks_CreateExtensionSemantics — точная семантика
// обратного расширения: маска начинается с начала строки create-заголовка,
// барьер ';' между заголовком и тегом отключает расширение.
func TestMaskDollarQuotedBlocks_CreateExtensionSemantics(t *testing.T) {
	const withHeader = "select 1;\ncreate or replace function f(p int = null)\nlanguage plpgsql as $x$\nbody\n$x$;\nselect 2"
	got := maskDollarQuotedBlocks(withHeader)
	lines := strings.Split(got, "\n")
	if strings.TrimSpace(lines[1]) != "" {
		t.Fatalf("create header line not masked: %q", lines[1])
	}
	if strings.TrimSpace(lines[2]) != "" {
		t.Fatalf("language line not masked: %q", lines[2])
	}
	if strings.TrimSpace(lines[3]) != "" {
		t.Fatalf("body line not masked: %q", lines[3])
	}
	if strings.TrimSpace(lines[4]) != ";" {
		t.Fatalf("closing tag line: want trailing ';' only, got %q", lines[4])
	}
	if lines[0] != "select 1;" || lines[5] != "select 2" {
		t.Fatalf("outer code changed: %q / %q", lines[0], lines[5])
	}

	// Точка с запятой между заголовком и тегом: маска только тела.
	const withBarrier = "create or replace function f(p int = null)\nlanguage plpgsql;\nas $$\nbody\n$$;"
	got2 := maskDollarQuotedBlocks(withBarrier)
	if !strings.Contains(got2, "= null") {
		t.Fatalf("barrier case: signature masked, want body-only: %q", got2)
	}
	if strings.Contains(got2, "body") {
		t.Fatalf("barrier case: body not masked: %q", got2)
	}
}

// --- Пайплайн-тесты по сценариям дельта-спеки (task 3.1) ---

// ruleCheckFn — сигнатура file-правил (check(ctx, file)).
type ruleCheckFn = func(context.Context, *indexedFile) ([]Finding, error)

// runMaskedPipelineRule эмулирует препроцессинг runSQLFileCtx (mask →
// replaceMacros → правила → ремаппинг строк) и прогоняет одно правило на
// раннере с установленным exec.
func runMaskedPipelineRule(t *testing.T, rawContent string, mask bool, pick func(*Runner) ruleCheckFn) []Finding {
	t.Helper()
	preprocessed := rawContent
	if mask {
		preprocessed = maskDollarQuotedBlocks(rawContent)
	}
	macroResult := replaceMacros(preprocessed)
	file := newRuleTestFile(t, rawContent)
	r := &Runner{}
	r.exec = &reviewExecContext{
		filePath:    normalizePath(file.Path),
		content:     []byte(macroResult.Content),
		rawContent:  []byte(rawContent),
		lines:       strings.Split(macroResult.Content, "\n"),
		macroResult: macroResult,
	}
	findings, err := pick(r)(context.Background(), file)
	r.exec = nil
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for i := range findings {
		if !findings[i].LineInOriginalSpace {
			findings[i].Line = mapExpandedLineToOriginal(findings[i].Line, macroResult.SourceMap)
		}
	}
	return findings
}

// Сценарий «Чистый файл PL/pgSQL не даёт findings»: PG-код с дефолтами = null,
// return;, execute v_sql маскируется; без маскирования те же правила находят
// нарушения (контролируемое нарушение — маска действительно нужна).
func TestPipelineMask_PurePgFileNoFindings(t *testing.T) {
	content := `create or replace function adm_get_dbparam(a_param dsidentifier = null)
returns integer
language plpgsql
as $$
begin
  execute v_sql;
  if v_a = null then
    return;
  end if;
end
$$;

do $$
begin
  perform 1;
end
$$;
`
	for _, rule := range []struct {
		id   RuleID
		pick func(*Runner) ruleCheckFn
	}{
		{RuleNullComparison, func(r *Runner) ruleCheckFn { return r.checkNullComparison }},
		{RuleEmptyReturn, func(r *Runner) ruleCheckFn { return r.checkEmptyReturn }},
	} {
		unmasked := runMaskedPipelineRule(t, content, false, rule.pick)
		if len(unmasked) == 0 {
			t.Fatalf("%s: без маскирования нарушений нет — тест не показателен", rule.id)
		}
		masked := runMaskedPipelineRule(t, content, true, rule.pick)
		if len(masked) != 0 {
			t.Fatalf("%s: findings = %d, want 0: %#v", rule.id, len(masked), masked)
		}
	}
}

// Сценарий «Смешанный файл — MSSQL-ветка разбирается, PG-ветка маскируется» +
// «Якоря findings не смещаются»: finding MSSQL-ветки сохраняет исходную строку.
func TestPipelineMask_MixedFile_MssqlKept_PgMasked(t *testing.T) {
	content := `create proc RPT_Exec
as
  declare @a int
  if @a = null
    select 1
go

#ifdef PG_CODE
create or replace procedure rpt_exec(p1 dsidentifier = null)
language plpgsql
as $$
begin
  execute v_sql;
  return;
end
$$;
#endif
`
	const wantLine = 4
	findings := runMaskedPipelineRule(t, content, true, func(r *Runner) ruleCheckFn { return r.checkNullComparison })
	if len(findings) != 1 {
		t.Fatalf("nullComparison findings = %d, want 1: %#v", len(findings), findings)
	}
	if findings[0].Line != wantLine {
		t.Fatalf("finding line = %d, want %d (якорь сместился)", findings[0].Line, wantLine)
	}
	if !strings.Contains(findings[0].Object, "@a = null") {
		t.Fatalf("finding object = %q, want MSSQL-ветка", findings[0].Object)
	}

	emptyReturns := runMaskedPipelineRule(t, content, true, func(r *Runner) ruleCheckFn { return r.checkEmptyReturn })
	if len(emptyReturns) != 0 {
		t.Fatalf("emptyReturn findings = %d, want 0 (return; из PG-ветки): %#v", len(emptyReturns), emptyReturns)
	}
}

// Сценарий «Доллары внутри строкового литерала не открывают маску»: код после
// литерала с $$$$$$$$ всё ещё анализируется.
func TestPipelineMask_DollarsInLiteral_CodeAfterStillAnalyzed(t *testing.T) {
	content := "select '$$$$$$$$' from t\nselect a from t2 where a = null\n"
	findings := runMaskedPipelineRule(t, content, true, func(r *Runner) ruleCheckFn { return r.checkNullComparison })
	if len(findings) != 1 || findings[0].Line != 2 {
		t.Fatalf("findings = %#v, want 1 finding на строке 2", findings)
	}
}

// Сценарий «create без dollar-quote не маскируется»: обычная процедура
// разбирается как до изменения.
func TestPipelineMask_PlainProcAnalyzedAsBefore(t *testing.T) {
	content := `create procedure p1
as
begin
  if @a = null
    select 1
end
`
	masked := runMaskedPipelineRule(t, content, true, func(r *Runner) ruleCheckFn { return r.checkNullComparison })
	unmasked := runMaskedPipelineRule(t, content, false, func(r *Runner) ruleCheckFn { return r.checkNullComparison })
	if len(masked) != 1 || masked[0].Line != 4 {
		t.Fatalf("masked findings = %#v, want 1 finding на строке 4", masked)
	}
	if len(unmasked) != len(masked) {
		t.Fatalf("findings changed: masked=%d unmasked=%d", len(masked), len(unmasked))
	}
}

// Сценарий «Маркеры $$$ не затрагиваются маской»: findings совпадают с
// прогоном без маскирования (клиентские макро-маркеры в обычном коде).
func TestPipelineMask_MacroMarkers_SameFindings(t *testing.T) {
	content := `$$$NPPBEGIN$$$
create proc p1
as
begin
  if @a = null
    select 1
end
$$$FAKE_SECTION$$$
`
	masked := runMaskedPipelineRule(t, content, true, func(r *Runner) ruleCheckFn { return r.checkNullComparison })
	unmasked := runMaskedPipelineRule(t, content, false, func(r *Runner) ruleCheckFn { return r.checkNullComparison })
	if len(masked) != 1 || masked[0].Line != 5 {
		t.Fatalf("masked findings = %#v, want 1 finding на строке 5", masked)
	}
	if len(masked) != len(unmasked) || masked[0].Line != unmasked[0].Line {
		t.Fatalf("findings diverged: masked=%#v unmasked=%#v", masked, unmasked)
	}
}

// Сценарий «Незакрытый dollar-quote не маскируется»: нарушение ниже
// открывающего тега обнаруживается (fail-open).
func TestPipelineMask_UnclosedTag_FailOpen(t *testing.T) {
	content := `do $$
begin
  if v_a = null then
    perform 1;
`
	findings := runMaskedPipelineRule(t, content, true, func(r *Runner) ruleCheckFn { return r.checkNullComparison })
	if len(findings) != 1 || findings[0].Line != 3 {
		t.Fatalf("findings = %#v, want 1 finding на строке 3 (fail-open)", findings)
	}
}

// Сценарий «Вложенные блочные комментарии не открывают маску»: доллары внутри
// вложенного комментария — текст комментария; тело реального dollar-quote
// ниже маскируется, нарушение в MSSQL-коде сохраняется.
func TestPipelineMask_NestedBlockCommentDollars(t *testing.T) {
	content := "/* c1 /* c2 */ c3 $$ */\ndo $$\nif v = null then return; end if;\n$$;\nselect a = null from t\n"
	findings := runMaskedPipelineRule(t, content, true, func(r *Runner) ruleCheckFn { return r.checkNullComparison })
	if len(findings) != 1 || findings[0].Line != 5 {
		t.Fatalf("findings = %#v, want 1 finding на строке 5 (вложенный комментарий)", findings)
	}
}

// Сценарий «Псевдо-тег с цифрой в первой позиции не открывает маску»:
// пара $1$ (форма позиционного параметра) — не границы области, нарушение
// между ними обнаруживается.
func TestPipelineMask_DigitFirstTagNotOpened(t *testing.T) {
	content := "select $1$ where a = null select $1$\n"
	findings := runMaskedPipelineRule(t, content, true, func(r *Runner) ruleCheckFn { return r.checkNullComparison })
	if len(findings) != 1 || findings[0].Line != 1 {
		t.Fatalf("findings = %#v, want 1 finding на строке 1 ($1$ — не тег)", findings)
	}
}
