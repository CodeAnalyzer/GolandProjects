package review

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

// Тесты маскировки SQL-комментариев (change fix-review-block-comment-masking):
// хелпер maskSQLComments и регрессии raw-сканеров, читающих текст файла напрямую
// или через splitStatementsWithOffsets. Ключевой инвариант — сохранение байтовой
// длины и позиций \n/\r: контент декодирован CP866→UTF-8, offset-арифметика и
// подсчёт строк потребителей — байтовые.

// --- maskSQLComments (task 1.1): точные совпадения ---

func TestMaskSQLComments_Exact(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{name: "line comment full line", in: "-- x\ny", want: "    \ny"},
		{name: "block comment inline", in: "a/*b*/c", want: "a     c"},
		{name: "line comment ends at CR", in: "a -- c\r\nb", want: "a     \r\nb"},
		{name: "code without comments untouched", in: "select 'a' from t", want: "select 'a' from t"},
		{name: "double dash after digits starts comment", in: "select 5--3 from t", want: "select 5" + strings.Repeat(" ", len("--3 from t"))},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if got := maskSQLComments(tc.in); got != tc.want {
				t.Fatalf("maskSQLComments(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

// --- maskSQLComments (task 1.1): свойства (длина, строки, сохранение кода) ---

func TestMaskSQLComments_Properties(t *testing.T) {
	cases := []struct {
		name string
		in   string
		keep []string // фрагменты кода, которые обязаны сохраниться
		drop []string // фрагменты комментариев, которые обязаны исчезнуть
	}{
		{
			name: "multiline block",
			in:   "select 1\n/*\ninsert pFoo (A)\nselect 1, 2\n*/\nselect 2",
			keep: []string{"select 1", "\nselect 2"},
			drop: []string{"insert pFoo", "select 1, 2"},
		},
		{
			name: "adjacent blocks *//*",
			in:   "/* aaa *//* bbb */select 1",
			keep: []string{"select 1"},
			drop: []string{"aaa", "bbb"},
		},
		{
			// T-SQL не поддерживает вложенные /* */: блок закрывается первым */
			name: "nested open inside block closes at first closer",
			in:   "/* outer /* inner */ still code */select 1",
			keep: []string{"still code", "select 1"},
			drop: []string{"outer", "inner"},
		},
		{
			name: "double dash inside literal is not a comment",
			in:   "select 'a--b' from t",
			keep: []string{"'a--b'"},
		},
		{
			name: "block open inside literal is not a comment",
			in:   "select '/*x*/' from t",
			keep: []string{"'/*x*/'"},
		},
		{
			name: "escaped quote keeps literal open",
			in:   "select 'it''s -- ok' from t",
			keep: []string{"it''s -- ok"},
		},
		{
			// Состояние литерала переносится через \n (как в splitStatementsWithOffsets):
			// незакрытый литерал глушит распознавание комментариев до конца текста.
			name: "unterminated literal carries state over newline",
			in:   "select 'abc\ndef --ghi\nselect 2",
			keep: []string{"def --ghi", "select 2"},
		},
		{
			name: "unclosed block masks to EOF",
			in:   "select 1 /* tail\nselect 2",
			keep: []string{"select 1"},
			drop: []string{"tail", "select 2"},
		},
		{
			name: "CR inside block preserved",
			in:   "select 1 /* c1\r\nc2 */ select 2",
			keep: []string{"select 2"},
			drop: []string{"c1", "c2"},
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got := maskSQLComments(tc.in)
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
					t.Fatalf("%s: want %q dropped from result %q", tc.name, d, got)
				}
			}
		})
	}
}

// Кириллический комментарий перед триггером: байтовая длина сохранена
// (CP866→UTF-8, кириллица = 2 байта на символ) и Line-координаты совпадают.
func TestMaskSQLComments_CyrillicByteLength(t *testing.T) {
	in := "/* кириллический комментарий */\ninsert pFoo (A, B)\nselect 1, 2"
	got := maskSQLComments(in)
	if len(got) != len(in) {
		t.Fatalf("byte len = %d, want %d", len(got), len(in))
	}
	inLines := strings.Split(in, "\n")
	gotLines := strings.Split(got, "\n")
	if len(gotLines) != len(inLines) {
		t.Fatalf("line count = %d, want %d", len(gotLines), len(inLines))
	}
	for i := 1; i < len(inLines); i++ {
		if gotLines[i] != inLines[i] {
			t.Fatalf("line %d changed: got %q, want %q", i+1, gotLines[i], inLines[i])
		}
	}
	if strings.Contains(got, "кириллический") {
		t.Fatalf("comment text leaked: %q", got)
	}
}

// --- Регрессии raw-сканеров (tasks 2.2, 3.1-3.3) ---

func TestBlockCommentMasking_InsertRowLock(t *testing.T) {
	runner := &Runner{}
	runFileRuleCases(t, runner, RuleInsertRowLock, runner.checkInsertRowLock, []ruleCase{
		{name: "insert inside block comment (TmpTbl_GLReports case)", content: "/*\ninsert pGL_ReportFieldValues (ReportID, FieldID)\nselect 1, 2 --union all\nselect 3, 4\n*/\n", wantCount: 0},
		{name: "semicolon and union all inside comment do not split statements", content: "/*\ninsert pFoo (A, B);\nselect 1, 2\nunion all\nselect 3, 4\n*/\nselect 1", wantCount: 0},
		{name: "trailing line comment masks rest of line", content: "insert pFoo (A, B) -- insert pBar (C)\nselect 1, 2", wantCount: 1, wantLine: 1, wantObject: "pFoo"},
		{name: "real insert kept", content: "insert pFoo (A, B)\nselect 1, 2", wantCount: 1, wantLine: 1, wantObject: "pFoo"},
		{name: "comment between insert and select", content: "insert pFoo (A, B) /* cols */\nselect 1, 2", wantCount: 1, wantLine: 1, wantObject: "pFoo"},
	})
}

func TestBlockCommentMasking_UseSelectAll(t *testing.T) {
	runner := &Runner{}
	runFileRuleCases(t, runner, RuleUseSelectAll, runner.checkUseSelectAll, []ruleCase{
		{name: "select star inside block comment", content: "/*\nselect * from tFoo\n*/\nselect A from t", wantCount: 0},
		{name: "trailing line comment with select star", content: "select A, B from tContract -- было: select * from tContract", wantCount: 0},
		{name: "real select star kept", content: "select * from t", wantCount: 1, wantLine: 1},
	})
}

func TestBlockCommentMasking_TruncTbl(t *testing.T) {
	runner := &Runner{}
	runFileRuleCases(t, runner, RuleTruncTbl, runner.checkTruncTbl, []ruleCase{
		{name: "truncate inside block comment", content: "/*\ntruncate table pFoo\n*/\nselect 1", wantCount: 0},
		{name: "truncate inside line comment", content: "-- truncate table pFoo", wantCount: 0},
		{name: "real truncate kept", content: "truncate table pFoo", wantCount: 1, wantLine: 1, wantObject: "pFoo"},
		{name: "comment between truncate and table (FN fix)", content: "truncate /* batch */ table pFoo", wantCount: 1, wantLine: 1, wantObject: "pFoo"},
	})
}

func TestBlockCommentMasking_DeferredUpdate(t *testing.T) {
	cases := []ruleCase{
		{name: "update inside block comment", content: "/*\nupdate pFoo set KeyCol = @v from pFoo f M_UPDLOCK_INDEX(XPKpFoo) where f.SPID = 1\n*/\nselect 1", wantCount: 0},
		{name: "update inside line comment", content: "-- update pFoo set KeyCol = @v from pFoo f M_UPDLOCK_INDEX(XPKpFoo) where f.SPID = 1", wantCount: 0},
		{name: "real update kept", content: "update pFoo set KeyCol = @v from pFoo f M_UPDLOCK_INDEX(XPKpFoo) where f.SPID = 1", wantCount: 1, wantLine: 1},
		// offset-инвариант: кириллический блок (2 байта/символ) перед оператором
		// не смещает Line — защита от rune-регрессии маскировки.
		{name: "cyrillic block before real update keeps line", content: "/* кириллический комментарий перед оператором */\nupdate pFoo set KeyCol = @v from pFoo f M_UPDLOCK_INDEX(XPKpFoo) where f.SPID = 1", wantCount: 1, wantLine: 2},
	}
	if err := validateRuleCases(RuleDeferredUpdate, cases); err != nil {
		t.Fatal(err)
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			file := newRuleTestFile(t, tc.content)
			runner := &Runner{}
			runner.exec = newRuleTestExec(file.Path, tc.content)
			runner.indexFieldsCache = map[string][]string{"xpkpfoo": {"keycol", "spid"}}
			findings, err := runner.checkDeferredUpdate(context.Background(), file)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			assertRuleFindings(t, RuleDeferredUpdate, tc, findings)
		})
	}
}

func TestBlockCommentMasking_MemoryUniqueIndexUpdate(t *testing.T) {
	cases := []ruleCase{
		{name: "update inside block comment", content: "/*\nupdate pMem set KeyCol = 1\n*/\nselect 1", wantCount: 0},
		{name: "real update kept", content: "update pMem set KeyCol = 1", wantCount: 1, wantObject: "pmem"},
		{name: "cyrillic block before real update keeps line", content: "/* кириллический комментарий перед оператором */\nupdate pMem set KeyCol = 1", wantCount: 1, wantLine: 2, wantObject: "pmem"},
	}
	if err := validateRuleCases(RuleMemoryUniqueIndexUpdate, cases); err != nil {
		t.Fatal(err)
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			file := newRuleTestFile(t, tc.content)
			runner := &Runner{}
			runner.exec = newRuleTestExec(file.Path, tc.content)
			runner.memoryTableCache = map[string]struct{}{"pmem": {}}
			runner.memoryTablesLoaded = true
			runner.indexCandCache = map[string][]tableIndexCandidate{
				"pmem": {{Name: "XPKpMem", Fields: []string{"keycol"}, IsUnique: true}},
			}
			findings, err := runner.checkMemoryUniqueIndexUpdate(context.Background(), file)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			assertRuleFindings(t, RuleMemoryUniqueIndexUpdate, tc, findings)
		})
	}
}

func TestBlockCommentMasking_InSubQuery(t *testing.T) {
	runner := &Runner{}
	runFileRuleCases(t, runner, RuleInSubQuery, runner.checkInSubQuery, []ruleCase{
		{name: "in subquery inside block comment", content: "/*\nselect 1 from t where ID in (select ID from s)\n*/\nselect 2", wantCount: 0},
		{name: "in subquery inside line comment", content: "-- select 1 where ID in (select 1)", wantCount: 0},
		{name: "real in subquery kept", content: "select * from t where ID in (select ID from s)", wantCount: 1, wantLine: 1},
		{name: "cyrillic block before real in subquery keeps line", content: "/* кириллический комментарий перед оператором */\nselect * from t where ID in (select ID from s)", wantCount: 1, wantLine: 2},
	})
}

func TestBlockCommentMasking_TooManyJoins(t *testing.T) {
	// 13 JOIN при лимите 12 (MaxJoinsAllowed).
	var joins strings.Builder
	joins.WriteString("select 1 from t0")
	for i := 1; i <= 13; i++ {
		fmt.Fprintf(&joins, " join t%d on t0.a = t%d.b", i, i)
	}
	runner := &Runner{}
	runFileRuleCases(t, runner, RuleTooManyJoins, runner.checkTooManyJoins, []ruleCase{
		{name: "joins inside block comment", content: "/*\n" + joins.String() + "\n*/\nselect 1", wantCount: 0},
		{name: "real joins kept", content: joins.String(), wantCount: 1, wantLine: 1},
		{name: "cyrillic block before real joins keeps line", content: "/* кириллический комментарий перед оператором */\n" + joins.String(), wantCount: 1, wantLine: 2},
	})
}

// --- checkVarUseAfterCursor (task 4.1): закомментированные курсорные
// операторы не считаются операторами ---

func runVarUseAfterCursorContent(t *testing.T, content string) []Finding {
	t.Helper()
	r := &Runner{}
	path := normalizePath("test.sql")
	r.exec = &reviewExecContext{filePath: path, content: []byte(content), lines: strings.Split(content, "\n"), macroResult: replaceMacros(content)}
	findings, err := r.checkVarUseAfterCursor(context.Background(), &indexedFile{Path: path, DsProductID: 1})
	r.exec = nil
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	return findings
}

func TestBlockCommentMasking_VarUseAfterCursor_CommentedCursorOps_NoFinding(t *testing.T) {
	content := `create proc TestProc
as
  declare @a int
  /*
  declare cur1 cursor for select ID from tContract
  open cur1
  fetch next from cur1 into @a
  deallocate cur1
  */
  select @a
`
	if findings := runVarUseAfterCursorContent(t, content); len(findings) != 0 {
		t.Fatalf("findings count = %d, want 0: %#v", len(findings), findings)
	}
}

func TestBlockCommentMasking_VarUseAfterCursor_CommentedJunkAroundRealOps_Finding(t *testing.T) {
	content := `create proc TestProc
as
  declare @a int
  declare cur1 cursor for select ID from tContract
  open cur1
  fetch next from cur1 into @a
  /*
  deallocate cur1
  fetch next from cur1 into @a
  */
  deallocate cur1
  select @a
`
	findings := runVarUseAfterCursorContent(t, content)
	if len(findings) != 1 {
		t.Fatalf("findings count = %d, want 1: %#v", len(findings), findings)
	}
	if findings[0].Object != "@a" {
		t.Fatalf("object = %q, want @a", findings[0].Object)
	}
}
