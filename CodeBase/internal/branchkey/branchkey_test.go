package branchkey

import "testing"

func TestNormalizeBranchKey(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"SP-ветка релизной линии", "FA-Contracts-11-01-7-SP", "fa-contracts-11-01-7"},
		{"линия без суффикса", "FA-Contracts-11-01-7", "fa-contracts-11-01-7"},
		{"develop", "develop", "develop"},
		{"HF-суффикс", "FA-Contracts-11-01-7-HF008", "fa-contracts-11-01-7"},
		{"повторные суффиксы", "FA-Contracts-11-01-7-SP-HF008", "fa-contracts-11-01-7"},
		{"только SP-SP", "FA-Payments-11-01-21-SP-SP", "fa-payments-11-01-21"},
		{"точки и подчёркивания", "FA.Contracts_11-01-7_SP", "fa-contracts-11-01-7"},
		{"регистр", "fa-agreement-9-01-1", "fa-agreement-9-01-1"},
		{"служебная ветка без автоматического матчинга", "sup/11-01-7", "sup/11-01-7"},
		{"feature-ветка", "prj/NOVIKOM-PERSFORM", "prj/novikom-persform"},
		{"пустая строка", "", ""},
		{"SP без линии", "hotfix-sp", "hotfix"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := NormalizeBranchKey(tc.in); got != tc.want {
				t.Fatalf("NormalizeBranchKey(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

// TestGitBranchMatchesManifest — git-ветка -SP и манифест-строка с -HF
// обязаны давать одинаковый branch_key: это условие сопоставления линий
// индекса с конфигурацией банка (dsinfo).
func TestGitBranchMatchesManifest(t *testing.T) {
	gitBranch := NormalizeBranchKey("FA-Contracts-11-01-7-SP")
	_, manifestBranch, hotfix := NormalizeProduct("FA-Contracts-11-01-7", "FA-Contracts-11-01-7-HF008")
	if gitBranch != manifestBranch {
		t.Fatalf("branch_key git-ветки (%q) != branch_key манифеста (%q)", gitBranch, manifestBranch)
	}
	if gitBranch != "fa-contracts-11-01-7" {
		t.Fatalf("branch_key = %q, want fa-contracts-11-01-7", gitBranch)
	}
	if hotfix != "FA-Contracts-11-01-7-HF008" {
		t.Fatalf("hotfixName = %q, want FA-Contracts-11-01-7-HF008", hotfix)
	}
}
