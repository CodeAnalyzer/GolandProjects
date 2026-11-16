package review

import (
	"fmt"
	"testing"
)

func TestBatchErrorCode(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"not indexed", fmt.Errorf("%w: /tmp/a.sql", ErrFileNotIndexed), "not_indexed"},
		{"missing product", fmt.Errorf("%w: /tmp/b.sql", ErrProductNotSet), "missing_product"},
		{"other", fmt.Errorf("parse failed"), "review_failed"},
	}
	for _, tc := range cases {
		if got := BatchErrorCode(tc.err); got != tc.want {
			t.Fatalf("%s: BatchErrorCode = %q, want %q", tc.name, got, tc.want)
		}
	}
}

func TestMergeSummary(t *testing.T) {
	var dst Summary
	mergeSummary(&dst, Summary{
		Total:      2,
		ByRule:     map[RuleID]int{RuleUseSelectAll: 1, RuleTruncTbl: 1},
		BySeverity: map[int]int{SeverityDeployStopper: 1, SeverityFineCode: 1},
	})
	mergeSummary(&dst, Summary{
		Total:      1,
		ByRule:     map[RuleID]int{RuleUseSelectAll: 1},
		BySeverity: map[int]int{SeverityDeployStopper: 1},
	})

	if dst.Total != 3 {
		t.Fatalf("Total = %d, want 3", dst.Total)
	}
	if dst.ByRule[RuleUseSelectAll] != 2 || dst.ByRule[RuleTruncTbl] != 1 {
		t.Fatalf("ByRule = %#v", dst.ByRule)
	}
	if dst.BySeverity[SeverityDeployStopper] != 2 || dst.BySeverity[SeverityFineCode] != 1 {
		t.Fatalf("BySeverity = %#v", dst.BySeverity)
	}
}
