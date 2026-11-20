package review

import (
	"context"
	"strings"
	"testing"

	sqlparser "github.com/codebase/internal/parser/sql"
)

const dashedHeader = "/*-------------------------------------------------------------------\n" +
	"  BPConsCoolingOff transfer\n" +
	"-------------------------------------------------------------------*/\n"

func TestCheckIndexWrong_DashedHeaderBlockComment(t *testing.T) {
	content := dashedHeader +
		"delete pBPConsCoolingOff\n" +
		"  from pBPConsCoolingOff M_ROWLOCK_INDEX(XPKpBPConsCoolingOff)\n" +
		" where SPID = @@spid\n" +
		"   and BankProductID = @BankProductID\n" +
		"M_KEEPPLAN\n"

	file := newRuleTestFile(t, content)
	runner := &Runner{}
	runner.exec = newRuleTestExec(file.Path, content)
	runner.indexCandCache = map[string][]tableIndexCandidate{
		"pbpconscoolingoff": {
			{Name: "XPKpBPConsCoolingOff", Fields: []string{"SPID", "AutoID"}, IsUnique: true},
			{Name: "XIE1pBPConsCoolingOff", Fields: []string{"SPID", "BankProductID", "AmountMin"}},
		},
	}

	findings, err := runner.checkIndexWrong(context.Background(), file)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(findings) != 1 {
		t.Fatalf("findings count = %d, want 1: %#v", len(findings), findings)
	}
	f := findings[0]
	if f.Rule != RuleIndexWrong {
		t.Fatalf("finding rule = %q, want %q", f.Rule, RuleIndexWrong)
	}
	if f.Line != 4 {
		t.Fatalf("finding line = %d, want 4", f.Line)
	}
	if !strings.EqualFold(f.Object, "pbpconscoolingoff.xpkpbpconscoolingoff") {
		t.Fatalf("finding object = %q, want %q", f.Object, "pbpconscoolingoff.xpkpbpconscoolingoff")
	}
	if !strings.Contains(f.Message, "XIE1pBPConsCoolingOff") {
		t.Fatalf("finding message = %q, want mention of XIE1pBPConsCoolingOff", f.Message)
	}
}

type blockCommentScannerCase struct {
	ruleCase
	rule  RuleID
	setup func(*Runner)
	run   func(context.Context, *Runner, *indexedFile, string) ([]Finding, error)
}

func TestBlockCommentHeaderScanners(t *testing.T) {
	fileRun := func(method func(context.Context, *Runner, *indexedFile) ([]Finding, error)) func(context.Context, *Runner, *indexedFile, string) ([]Finding, error) {
		return func(ctx context.Context, r *Runner, f *indexedFile, _ string) ([]Finding, error) {
			return method(ctx, r, f)
		}
	}

	cases := []blockCommentScannerCase{
		{
			ruleCase: ruleCase{name: "updateOnlyVar dashed header", content: dashedHeader + "update tContract set @a = 1, @b = col + 2 where id = 3\n", wantCount: 1},
			rule:     RuleUpdateOnlyVar,
			run: fileRun(func(ctx context.Context, r *Runner, f *indexedFile) ([]Finding, error) {
				return r.checkUpdateOnlyVar(ctx, f)
			}),
		},
		{
			ruleCase: ruleCase{name: "updateOnlyVar line comment", content: "-- update tContract set @a = 1, @b = col + 2 where id = 3\n", wantCount: 0},
			rule:     RuleUpdateOnlyVar,
			run: fileRun(func(ctx context.Context, r *Runner, f *indexedFile) ([]Finding, error) {
				return r.checkUpdateOnlyVar(ctx, f)
			}),
		},
		{
			ruleCase: ruleCase{name: "pTableSpid dashed header", content: dashedHeader + "select * from pTmpObject t where t.OtherCol = 1\n", wantCount: 1},
			rule:     RulePTableSpid,
			run: fileRun(func(ctx context.Context, r *Runner, f *indexedFile) ([]Finding, error) {
				return r.checkPTableSpid(ctx, f)
			}),
		},
		{
			ruleCase: ruleCase{name: "pTableSpid line comment", content: "-- select * from pTmpObject t where t.OtherCol = 1\n", wantCount: 0},
			rule:     RulePTableSpid,
			run: fileRun(func(ctx context.Context, r *Runner, f *indexedFile) ([]Finding, error) {
				return r.checkPTableSpid(ctx, f)
			}),
		},
		{
			ruleCase: ruleCase{name: "forceOrder2Tbl dashed header", content: dashedHeader + "select * from t1, t2 where t1.id = t2.id\n", wantCount: 1},
			rule:     RuleForceOrder2Tbl,
			run: fileRun(func(ctx context.Context, r *Runner, f *indexedFile) ([]Finding, error) {
				return r.checkForceOrder2Tbl(ctx, f)
			}),
		},
		{
			ruleCase: ruleCase{name: "forceOrder2Tbl line comment", content: "-- select * from t1, t2 where t1.id = t2.id\n", wantCount: 0},
			rule:     RuleForceOrder2Tbl,
			run: fileRun(func(ctx context.Context, r *Runner, f *indexedFile) ([]Finding, error) {
				return r.checkForceOrder2Tbl(ctx, f)
			}),
		},
		{
			ruleCase: ruleCase{name: "saveTran dashed header", content: dashedHeader + "save tran sp1\n", wantCount: 1},
			rule:     RuleSaveTran,
			run: fileRun(func(ctx context.Context, r *Runner, f *indexedFile) ([]Finding, error) {
				return r.checkSaveTran(ctx, f)
			}),
		},
		{
			ruleCase: ruleCase{name: "saveTran line comment", content: "-- save tran sp1\n", wantCount: 0},
			rule:     RuleSaveTran,
			run: fileRun(func(ctx context.Context, r *Runner, f *indexedFile) ([]Finding, error) {
				return r.checkSaveTran(ctx, f)
			}),
		},
		{
			ruleCase: ruleCase{name: "saveTran unclosed block hides code", content: "/* header\nsave tran sp1\n", wantCount: 0},
			rule:     RuleSaveTran,
			run: fileRun(func(ctx context.Context, r *Runner, f *indexedFile) ([]Finding, error) {
				return r.checkSaveTran(ctx, f)
			}),
		},
		{
			ruleCase: ruleCase{name: "saveTran inline block does not break analysis", content: "/* save tran hidden */\nsave tran sp1\n", wantCount: 1},
			rule:     RuleSaveTran,
			run: fileRun(func(ctx context.Context, r *Runner, f *indexedFile) ([]Finding, error) {
				return r.checkSaveTran(ctx, f)
			}),
		},
		{
			ruleCase: ruleCase{name: "saveTran code remainder after block close", content: "/* header\n----*/ save tran sp1\n", wantCount: 1, wantLine: 2},
			rule:     RuleSaveTran,
			run: fileRun(func(ctx context.Context, r *Runner, f *indexedFile) ([]Finding, error) {
				return r.checkSaveTran(ctx, f)
			}),
		},
		{
			ruleCase: ruleCase{name: "useDrop dashed header", content: dashedHeader + "create proc P as\nbegin\n  drop table #tmp\nend\n", wantCount: 1},
			rule:     RuleUseDrop,
			run: fileRun(func(ctx context.Context, r *Runner, f *indexedFile) ([]Finding, error) {
				return r.checkUseDrop(ctx, f)
			}),
		},
		{
			ruleCase: ruleCase{name: "useDrop line comment", content: "-- create proc P as\n-- begin\n--   drop table #tmp\n-- end\n", wantCount: 0},
			rule:     RuleUseDrop,
			run: fileRun(func(ctx context.Context, r *Runner, f *indexedFile) ([]Finding, error) {
				return r.checkUseDrop(ctx, f)
			}),
		},
		{
			ruleCase: ruleCase{name: "mathOperations dashed header", content: dashedHeader + "select * from t where\n  @limit > t.A + 1\n", wantCount: 1},
			rule:     RuleMathOperations,
			run: fileRun(func(ctx context.Context, r *Runner, f *indexedFile) ([]Finding, error) {
				return r.checkMathOperations(ctx, f)
			}),
		},
		{
			ruleCase: ruleCase{name: "mathOperations line comment", content: "-- select * from t where\n--   @limit > t.A + 1\n", wantCount: 0},
			rule:     RuleMathOperations,
			run: fileRun(func(ctx context.Context, r *Runner, f *indexedFile) ([]Finding, error) {
				return r.checkMathOperations(ctx, f)
			}),
		},
		{
			ruleCase: ruleCase{name: "existsWithAndInIf dashed header", content: dashedHeader + "if exists(select 1 from t where t.ID = 1) and @flag = 1\nbegin\nselect 1\nend\n", wantCount: 1},
			rule:     RuleExistsWithAndInIf,
			run: fileRun(func(ctx context.Context, r *Runner, f *indexedFile) ([]Finding, error) {
				return r.checkExistsWithAndInIf(ctx, f)
			}),
		},
		{
			ruleCase: ruleCase{name: "existsWithAndInIf line comment", content: "-- if exists(select 1 from t where t.ID = 1) and @flag = 1\n-- begin\n-- select 1\n-- end\n", wantCount: 0},
			rule:     RuleExistsWithAndInIf,
			run: fileRun(func(ctx context.Context, r *Runner, f *indexedFile) ([]Finding, error) {
				return r.checkExistsWithAndInIf(ctx, f)
			}),
		},
		{
			ruleCase: ruleCase{name: "indexExistsInDB dashed header", content: dashedHeader + "select * from pT t M_NOLOCK_INDEX(XPK_missing) where t.SPID = 1\n", wantCount: 1},
			rule:     RuleIndexExistsInDB,
			setup: func(r *Runner) {
				r.indexCandCache = map[string][]tableIndexCandidate{"pt": {{Name: "XPKpT", Fields: []string{"spid"}}}}
			},
			run: fileRun(func(ctx context.Context, r *Runner, f *indexedFile) ([]Finding, error) {
				return r.checkIndexExistsInDB(ctx, f)
			}),
		},
		{
			ruleCase: ruleCase{name: "indexExistsInDB line comment", content: "-- select * from pT t M_NOLOCK_INDEX(XPK_missing) where t.SPID = 1\n", wantCount: 0},
			rule:     RuleIndexExistsInDB,
			setup: func(r *Runner) {
				r.indexCandCache = map[string][]tableIndexCandidate{"pt": {{Name: "XPKpT", Fields: []string{"spid"}}}}
			},
			run: fileRun(func(ctx context.Context, r *Runner, f *indexedFile) ([]Finding, error) {
				return r.checkIndexExistsInDB(ctx, f)
			}),
		},
		{
			ruleCase: ruleCase{name: "varcharSize dashed header", content: dashedHeader + "declare @Var varchar\n", wantCount: 1, wantObject: "@Var"},
			rule:     RuleVarcharSize,
			run: func(ctx context.Context, r *Runner, f *indexedFile, _ string) ([]Finding, error) {
				return r.checkVarcharSize(ctx, &sqlparser.ParseResult{}, f)
			},
		},
		{
			ruleCase: ruleCase{name: "varcharSize line comment", content: "-- declare @Var varchar\n", wantCount: 0},
			rule:     RuleVarcharSize,
			run: func(ctx context.Context, r *Runner, f *indexedFile, _ string) ([]Finding, error) {
				return r.checkVarcharSize(ctx, &sqlparser.ParseResult{}, f)
			},
		},
		{
			ruleCase: ruleCase{name: "useFuncInIndCol dashed header", content: dashedHeader + "select * from pT t M_NOLOCK_INDEX(XPKpT) where abs(t.SPID) = 1\n", wantCount: 1},
			rule:     RuleUseFuncInIndCol,
			setup: func(r *Runner) {
				r.indexFieldsCache = map[string][]string{"xpkpt": {"spid"}}
			},
			run: fileRun(func(ctx context.Context, r *Runner, f *indexedFile) ([]Finding, error) {
				return r.checkUseFuncInIndCol(ctx, f)
			}),
		},
		{
			ruleCase: ruleCase{name: "useFuncInIndCol line comment", content: "-- select * from pT t M_NOLOCK_INDEX(XPKpT) where abs(t.SPID) = 1\n", wantCount: 0},
			rule:     RuleUseFuncInIndCol,
			setup: func(r *Runner) {
				r.indexFieldsCache = map[string][]string{"xpkpt": {"spid"}}
			},
			run: fileRun(func(ctx context.Context, r *Runner, f *indexedFile) ([]Finding, error) {
				return r.checkUseFuncInIndCol(ctx, f)
			}),
		},
		{
			ruleCase: ruleCase{name: "useFuncInIndCol unclosed block hides code", content: "/* header\nselect * from pT t M_NOLOCK_INDEX(XPKpT) where abs(t.SPID) = 1\n", wantCount: 0},
			rule:     RuleUseFuncInIndCol,
			setup: func(r *Runner) {
				r.indexFieldsCache = map[string][]string{"xpkpt": {"spid"}}
			},
			run: fileRun(func(ctx context.Context, r *Runner, f *indexedFile) ([]Finding, error) {
				return r.checkUseFuncInIndCol(ctx, f)
			}),
		},
		{
			ruleCase: ruleCase{name: "postgreLabelGotoLevel dashed header", content: dashedHeader + "MyLabel:\nselect 1\ngoto MyLabel\n", wantCount: 1, wantLine: 6, wantObject: "goto MyLabel"},
			rule:     RulePostgreLabelGotoLevel,
			run: fileRun(func(ctx context.Context, r *Runner, f *indexedFile) ([]Finding, error) {
				return r.checkPostgreLabelGotoLevel(ctx, f)
			}),
		},
		{
			ruleCase: ruleCase{name: "postgreLabelGotoLevel line comment", content: "-- MyLabel:\n-- select 1\n-- goto MyLabel\n", wantCount: 0},
			rule:     RulePostgreLabelGotoLevel,
			run: fileRun(func(ctx context.Context, r *Runner, f *indexedFile) ([]Finding, error) {
				return r.checkPostgreLabelGotoLevel(ctx, f)
			}),
		},
		{
			ruleCase: ruleCase{name: "postgreLabelGotoLevel active label with commented goto", content: "MyLabel:\nselect 1\n-- goto MyLabel\n", wantCount: 0},
			rule:     RulePostgreLabelGotoLevel,
			run: fileRun(func(ctx context.Context, r *Runner, f *indexedFile) ([]Finding, error) {
				return r.checkPostgreLabelGotoLevel(ctx, f)
			}),
		},
		{
			ruleCase: ruleCase{name: "postgreLabelGotoLevel unclosed block hides code", content: "/* header\nMyLabel:\nselect 1\ngoto MyLabel\n", wantCount: 0},
			rule:     RulePostgreLabelGotoLevel,
			run: fileRun(func(ctx context.Context, r *Runner, f *indexedFile) ([]Finding, error) {
				return r.checkPostgreLabelGotoLevel(ctx, f)
			}),
		},
	}

	byRule := make(map[RuleID][]ruleCase)
	for _, tc := range cases {
		byRule[tc.rule] = append(byRule[tc.rule], tc.ruleCase)
	}
	for rule, ruleCases := range byRule {
		if err := validateRuleCases(rule, ruleCases); err != nil {
			t.Fatal(err)
		}
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			file := newRuleTestFile(t, tc.content)
			runner := &Runner{}
			runner.exec = newRuleTestExec(file.Path, tc.content)
			if tc.setup != nil {
				tc.setup(runner)
			}
			findings, err := tc.run(context.Background(), runner, file, tc.content)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			assertRuleFindings(t, tc.rule, tc.ruleCase, findings)
		})
	}
}
