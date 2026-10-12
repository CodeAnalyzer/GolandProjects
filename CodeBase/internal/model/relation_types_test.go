package model

import "testing"

func TestRelationTypesVocabulary(t *testing.T) {
	if len(RelationTypes) != 21 {
		t.Fatalf("RelationTypes must contain 21 values, got %d", len(RelationTypes))
	}
	seen := make(map[string]struct{}, len(RelationTypes))
	for _, v := range RelationTypes {
		if _, dup := seen[v]; dup {
			t.Errorf("duplicate relation type %q", v)
		}
		seen[v] = struct{}{}
	}
	for _, want := range []string{
		"calls_procedure", "references_table", "inserts_into", "uses_param",
		"change_modifies", "depends_on_capability", "references_code", "usecase_involves",
	} {
		if _, ok := seen[want]; !ok {
			t.Errorf("RelationTypes missing %q", want)
		}
	}
}

func TestRelationEntityTypesVocabulary(t *testing.T) {
	if len(RelationEntityTypes) != 17 {
		t.Fatalf("RelationEntityTypes must contain 17 values, got %d", len(RelationEntityTypes))
	}
	seen := make(map[string]struct{}, len(RelationEntityTypes))
	for _, v := range RelationEntityTypes {
		if _, dup := seen[v]; dup {
			t.Errorf("duplicate relation entity type %q", v)
		}
		seen[v] = struct{}{}
	}
	for _, want := range []string{
		"sql_procedure", "sql_table", "query_fragment", "pas_method", "dfm_form",
		"js_function", "smf_instrument", "api_contract", "report_form", "report_field",
		"report_param", "vb_function", "spec_capability", "spec_change",
		"spec_requirement", "spec_scenario", "spec_usecase",
	} {
		if _, ok := seen[want]; !ok {
			t.Errorf("RelationEntityTypes missing %q", want)
		}
	}
}

func TestIsKnownRelationType(t *testing.T) {
	cases := []struct {
		raw  string
		want bool
	}{
		{"calls_procedure", true},
		{"depends_on_capability", true},
		{"  uses_param  ", true},
		{"", false},
		{"__no_such_relation__", false},
		{"inserts_into ", true},
		{"pas_class", false},
	}
	for _, tc := range cases {
		if got := IsKnownRelationType(tc.raw); got != tc.want {
			t.Errorf("IsKnownRelationType(%q) = %v, want %v", tc.raw, got, tc.want)
		}
	}
}

func TestIsKnownRelationEntityType(t *testing.T) {
	cases := []struct {
		raw  string
		want bool
	}{
		{"sql_procedure", true},
		{"dfm_form", true},
		{" spec_scenario ", true},
		{"", false},
		{"__no_such_type__", false},
		{"pas_class", false},
		{"dfm_component", false},
	}
	for _, tc := range cases {
		if got := IsKnownRelationEntityType(tc.raw); got != tc.want {
			t.Errorf("IsKnownRelationEntityType(%q) = %v, want %v", tc.raw, got, tc.want)
		}
	}
}
