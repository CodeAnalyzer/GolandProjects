package cmd

import (
	"testing"

	"github.com/codebase/internal/review"
)

func TestParseReviewRules_AcceptsExecNotExistsProc(t *testing.T) {
	rules, raw, err := parseReviewRules("execNotExistsProc,datatype")
	if err != nil {
		t.Fatalf("parseReviewRules returned error: %v", err)
	}
	if len(rules) != 2 || rules[0] != review.RuleExecNotExistsProc || rules[1] != review.RuleDatatype {
		t.Fatalf("unexpected rules: %#v", rules)
	}
	if len(raw) != 2 || raw[0] != "execNotExistsProc" || raw[1] != "datatype" {
		t.Fatalf("unexpected raw rules: %#v", raw)
	}
}

func TestParseReviewRules_ProcDuplicate(t *testing.T) {
	rules, raw, err := parseReviewRules("procDuplicate")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rules) != 1 || rules[0] != review.RuleProcDuplicate {
		t.Fatalf("expected procDuplicate, got %v", rules)
	}
	if len(raw) != 1 || raw[0] != "procDuplicate" {
		t.Fatalf("expected raw procDuplicate, got %v", raw)
	}
}

func TestParseReviewRules_ProcParamDefValue(t *testing.T) {
	rules, raw, err := parseReviewRules("procParamDefValue")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rules) != 1 || rules[0] != review.RuleProcParamDefValue {
		t.Fatalf("expected procParamDefValue, got %v", rules)
	}
	if len(raw) != 1 || raw[0] != "procParamDefValue" {
		t.Fatalf("expected raw procParamDefValue, got %v", raw)
	}
}

func TestParseReviewRules_ProcElseCase(t *testing.T) {
	rules, raw, err := parseReviewRules("procElseCase")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rules) != 1 || rules[0] != review.RuleProcElseCase {
		t.Fatalf("expected procElseCase, got %v", rules)
	}
	if len(raw) != 1 || raw[0] != "procElseCase" {
		t.Fatalf("expected raw procElseCase, got %v", raw)
	}
}

func TestParseReviewRules_UseSelectAll(t *testing.T) {
	rules, raw, err := parseReviewRules("useSelectAll")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rules) != 1 || rules[0] != review.RuleUseSelectAll {
		t.Fatalf("expected useSelectAll, got %v", rules)
	}
	if len(raw) != 1 || raw[0] != "useSelectAll" {
		t.Fatalf("expected raw useSelectAll, got %v", raw)
	}
}

func TestParseReviewRules_TruncTbl(t *testing.T) {
	rules, raw, err := parseReviewRules("truncTbl")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rules) != 1 || rules[0] != review.RuleTruncTbl {
		t.Fatalf("expected truncTbl, got %v", rules)
	}
	if len(raw) != 1 || raw[0] != "truncTbl" {
		t.Fatalf("expected raw truncTbl, got %v", raw)
	}
}

func TestParseReviewRules_AnsiInJoin(t *testing.T) {
	rules, raw, err := parseReviewRules("ansiInJoin")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rules) != 1 || rules[0] != review.RuleAnsiInJoin {
		t.Fatalf("expected ansiInJoin, got %v", rules)
	}
	if len(raw) != 1 || raw[0] != "ansiInJoin" {
		t.Fatalf("expected raw ansiInJoin, got %v", raw)
	}
}

func TestParseReviewRules_InsertRowLock(t *testing.T) {
	rules, raw, err := parseReviewRules("insertRowLock")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rules) != 1 || rules[0] != review.RuleInsertRowLock {
		t.Fatalf("expected insertRowLock, got %v", rules)
	}
	if len(raw) != 1 || raw[0] != "insertRowLock" {
		t.Fatalf("expected raw insertRowLock, got %v", raw)
	}
}

func TestParseReviewRules_UnknownRule(t *testing.T) {
	_, _, err := parseReviewRules("unknownRule")
	if err == nil {
		t.Fatalf("expected error for unknown rule")
	}
}
