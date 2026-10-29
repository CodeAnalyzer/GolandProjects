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

func TestNumericTokens(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"sup/11-01-7", "11|01|7"},
		{"fa-contracts-11-01-7", "11|01|7"},
		{"develop", ""},
		{"prj/novikom-persform", ""},
		{"fa-contracts-ext-9-01-553-pkg", "9|01|553"},
		{"fa-ftfns440reqprocadapter-11-01-11", "11|01|11"},
		{"fa-ftfns440reqprocadapter", ""},
	}
	for _, tc := range cases {
		tokens := NumericTokens(tc.in)
		got := ""
		for i, token := range tokens {
			if i > 0 {
				got += "|"
			}
			got += token
		}
		if got != tc.want {
			t.Fatalf("NumericTokens(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestNumericTailMatches(t *testing.T) {
	cases := []struct {
		requested string
		candidate string
		want      bool
	}{
		{"fa-contracts-11-01-7", "sup/11-01-7", true},
		{"fa-contracts-11-01-7", "fa-contracts-11-01-7", true},
		{"fa-contracts-ext-9-01-553", "fa-contracts-ext-9-01-553", true},
		{"fa-contracts-11-01-7", "fa-contracts-11-01-6", false},
		{"fa-contracts-11-01-7", "develop", false},
		{"fa-commonfiles", "develop", false},
		{"fa-commonfiles", "sup/11-01-7", false},
		{"fa-contracts-11-01-7", "fa-contracts-11-01-7-hotfix-2", false},
		{"fa-ftfns440reqprocadapter-11-01-11", "sup/11-01-11", true},
		{"fa-ftfns440reqprocadapter-11-01-11", "sup/11-01-7", false},
	}
	for _, tc := range cases {
		if got := NumericTailMatches(tc.requested, tc.candidate); got != tc.want {
			t.Fatalf("NumericTailMatches(%q, %q) = %v, want %v", tc.requested, tc.candidate, got, tc.want)
		}
	}
}

func TestCompareVersion(t *testing.T) {
	cases := []struct {
		a, b string
		want int
	}{
		{"fa-contracts-11-01-7", "fa-contracts-11-01-6", 1},
		{"fa-contracts-11-01-7", "fa-contracts-11-01-7", 0},
		{"fa-contracts-9-01-553", "fa-contracts-11-01-7", -1},
		{"fa-contracts-11-01", "fa-contracts-11-01-7", -1},
		{"develop", "fa-contracts-11-01-7", -1},
		{"develop", "master", 0},
		{"fa-contracts-11-01-07", "fa-contracts-11-01-7", 0},
	}
	for _, tc := range cases {
		if got := CompareVersion(tc.a, tc.b); got != tc.want {
			t.Fatalf("CompareVersion(%q, %q) = %d, want %d", tc.a, tc.b, got, tc.want)
		}
	}
}
