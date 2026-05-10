package indexer

import (
	"reflect"
	"testing"

	"github.com/codebase/internal/model"
)

func TestCollectUniqueSQLCallCalleeNames(t *testing.T) {
	pending := []*PendingSQLCallFile{
		{
			Calls: []*model.SQLProcedureCall{
				{CalleeName: " ProcA "},
				{CalleeName: "proca"},
				{CalleeName: "ProcB"},
				{CalleeName: ""},
				nil,
			},
		},
		nil,
	}

	got := collectUniqueSQLCallCalleeNames(pending)
	want := []string{"ProcA", "ProcB"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("callee names = %#v, want %#v", got, want)
	}
}

func TestBuildSQLProcedureCallRelationsWithTargetIDs(t *testing.T) {
	pending := []*PendingSQLCallFile{
		nil,
		{
			Procedures: []*model.SQLProcedure{
				{ProcName: "CallerA", LineStart: 1, LineEnd: 20},
				{ProcName: "CallerB", LineStart: 21, LineEnd: 40},
			},
			ProcedureIDs: map[string]int64{
				"callera": 101,
				"callerb": 102,
			},
			Calls: []*model.SQLProcedureCall{
				{CallerName: "CallerA", CalleeName: "TargetA", LineNumber: 10},
				{CallerName: "CallerA", CalleeName: "targeta", LineNumber: 10},
				{CallerName: "CallerB", CalleeName: "TargetB", LineNumber: 30},
				{CallerName: "CallerB", CalleeName: "Missing", LineNumber: 31},
				{CallerName: "", CalleeName: "NoSource", LineNumber: 100},
				nil,
			},
		},
	}
	targetIDs := map[string]int64{
		"targeta": 201,
		"targetb": 202,
	}
	progress := 0

	relations := buildSQLProcedureCallRelationsWithTargetIDs(pending, targetIDs, func(count int) {
		progress += count
	})

	if progress != 2 {
		t.Fatalf("progress = %d, want 2", progress)
	}
	if len(relations) != 2 {
		t.Fatalf("relations count = %d, want 2: %+v", len(relations), relations)
	}
	if relations[0].SourceType != "sql_procedure" || relations[0].SourceID != 101 || relations[0].TargetType != "sql_procedure" || relations[0].TargetID != 201 || relations[0].RelationType != "calls_procedure" || relations[0].LineNumber != 10 {
		t.Fatalf("unexpected first relation: %+v", relations[0])
	}
	if relations[1].SourceID != 102 || relations[1].TargetID != 202 || relations[1].LineNumber != 30 {
		t.Fatalf("unexpected second relation: %+v", relations[1])
	}
}

func TestBuildSQLProcedureCallRelationsParallelDedups(t *testing.T) {
	pending := []*PendingSQLCallFile{
		{
			Procedures:   []*model.SQLProcedure{{ProcName: "CallerA", LineStart: 1, LineEnd: 20}},
			ProcedureIDs: map[string]int64{"callera": 101},
			Calls:        []*model.SQLProcedureCall{{CallerName: "CallerA", CalleeName: "TargetA", LineNumber: 10}},
		},
		{
			Procedures:   []*model.SQLProcedure{{ProcName: "CallerA", LineStart: 1, LineEnd: 20}},
			ProcedureIDs: map[string]int64{"callera": 101},
			Calls:        []*model.SQLProcedureCall{{CallerName: "CallerA", CalleeName: "TargetA", LineNumber: 10}},
		},
	}

	relations := buildSQLProcedureCallRelationsParallel(pending, map[string]int64{"targeta": 201}, 4, nil)
	if len(relations) != 1 {
		t.Fatalf("relations count = %d, want 1", len(relations))
	}
}

func TestRelationDedupKey(t *testing.T) {
	relation := &model.Relation{SourceType: "a", SourceID: 1, TargetType: "b", TargetID: 2, RelationType: "rel", LineNumber: 3}
	if got := relationDedupKey(relation); got != "a|1|b|2|rel|3" {
		t.Fatalf("dedup key = %q", got)
	}
}
