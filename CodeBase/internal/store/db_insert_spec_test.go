package store

import (
	"strings"
	"testing"
)

func TestSpecSearchVectorUpdateStatements(t *testing.T) {
	statements := specSearchVectorUpdateStatements("WHERE file_id = $1 AND search_vector IS NULL")
	checks := []struct {
		name string
		want []string
	}{
		{name: "capability", want: []string{"capability_name", "title", "purpose", "notes", "'A'", "'B'"}},
		{name: "requirement", want: []string{"requirement_name", "body_text", "'A'", "'B'"}},
		{name: "scenario", want: []string{"scenario_name", "given_text", "when_text", "then_text", "'A'", "'B'"}},
		{name: "usecase", want: []string{"usecase_name", "title", "description", "'A'", "'B'"}},
	}
	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			var statement string
			for _, candidate := range statements {
				if strings.Contains(strings.ToLower(candidate), check.name) {
					statement = candidate
				}
			}
			if statement == "" {
				for _, candidate := range statements {
					if strings.Contains(candidate, check.want[0]) {
						statement = candidate
					}
				}
			}
			if statement == "" {
				t.Fatalf("statement for %s not found: %v", check.name, statements)
			}
			for _, want := range check.want {
				if !strings.Contains(statement, want) {
					t.Errorf("statement for %s lacks %q: %s", check.name, want, statement)
				}
			}
			if !strings.Contains(statement, "WHERE file_id = $1 AND search_vector IS NULL") {
				t.Errorf("statement for %s lacks file predicate: %s", check.name, statement)
			}
		})
	}

	backfill := specSearchVectorUpdateStatements("")
	for _, statement := range backfill {
		if strings.Contains(statement, "WHERE file_id") {
			t.Errorf("backfill statement contains file predicate: %s", statement)
		}
	}
}
