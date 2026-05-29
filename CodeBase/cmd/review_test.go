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

func TestParseReviewRules_UnknownRule(t *testing.T) {
	_, _, err := parseReviewRules("unknownRule")
	if err == nil {
		t.Fatalf("expected error for unknown rule")
	}
}
