package query

import (
	"strings"
	"testing"
)

type warningSample struct {
	Product         string
	BranchRequested string
	BranchUsed      string
}

func TestCollectBranchWarnings(t *testing.T) {
	cases := []struct {
		name  string
		value interface{}
		want  int
		parts []string
	}{
		{
			name:  "совпадение с единственной линией",
			value: warningSample{Product: "fa-x", BranchRequested: "fa-x-11-01-7", BranchUsed: "fa-x-11-01-7"},
			want:  0,
		},
		{
			name:  "совпадение с одной из нескольких линий",
			value: warningSample{Product: "fa-x", BranchRequested: "fa-x-7-02-2, fa-x-9-01-553", BranchUsed: "fa-x-9-01-553"},
			want:  0,
		},
		{
			name:  "fallback на другую ветку",
			value: warningSample{Product: "fa-x", BranchRequested: "fa-x-11-01-7", BranchUsed: "develop"},
			want:  1,
			parts: []string{"линия fa-x-11-01-7", "fa-x", "develop"},
		},
		{
			name:  "несколько запрошенных линий и fallback",
			value: warningSample{Product: "fa-x", BranchRequested: "fa-x-7-02-2, fa-x-9-01-553", BranchUsed: "develop"},
			want:  1,
			parts: []string{"линии fa-x-7-02-2, fa-x-9-01-553", "не проиндексированы"},
		},
		{
			name:  "копия без привязки к ветке",
			value: warningSample{Product: "fa-x", BranchRequested: "fa-x-11-01-7", BranchUsed: ""},
			want:  1,
			parts: []string{"без привязки к ветке"},
		},
		{
			name:  "продукт вне профиля",
			value: warningSample{Product: "fa-y", BranchRequested: "", BranchUsed: "develop"},
			want:  0,
		},
		{
			name: "дедупликация одинаковых предупреждений",
			value: []warningSample{
				{Product: "fa-x", BranchRequested: "fa-x-11-01-7", BranchUsed: "develop"},
				{Product: "fa-x", BranchRequested: "fa-x-11-01-7", BranchUsed: "develop"},
			},
			want: 1,
		},
		{
			name: "вложенные списки",
			value: struct {
				Items []warningSample
			}{Items: []warningSample{
				{Product: "fa-x", BranchRequested: "fa-x-11-01-7", BranchUsed: "fa-x-11-01-7"},
				{Product: "fa-y", BranchRequested: "fa-y-11-01-9", BranchUsed: "develop"},
			}},
			want:  1,
			parts: []string{"fa-y-11-01-9"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := CollectBranchWarnings(tc.value)
			if len(got) != tc.want {
				t.Fatalf("warnings = %v, want %d", got, tc.want)
			}
			for _, part := range tc.parts {
				found := false
				for _, warning := range got {
					if strings.Contains(warning, part) {
						found = true
						break
					}
				}
				if !found {
					t.Fatalf("warnings %v do not contain %q", got, part)
				}
			}
		})
	}
}

func TestBranchRequestedJoinsLines(t *testing.T) {
	q := &Query{profile: &ProfileContext{
		Requested: map[string][]string{"fa-x": {"fa-x-7-02-2", "fa-x-9-01-553"}},
	}}
	if got := q.branchRequested("fa-x"); got != "fa-x-7-02-2, fa-x-9-01-553" {
		t.Fatalf("branchRequested = %q", got)
	}
	if got := q.branchRequested("fa-y"); got != "" {
		t.Fatalf("branchRequested(fa-y) = %q, want empty", got)
	}
}
