package indexer

import (
	dbsql "database/sql"
	"errors"
	"testing"

	"github.com/codebase/internal/model"
)

func TestBuildJSProcedureCallRelationsWithResolvers_BuildsAndDedups(t *testing.T) {
	calls := []*model.JSProcedureCall{
		{ProcName: "TestProc", LineNumber: 10},
		{ProcName: "TestProc", LineNumber: 10},
		{ProcName: "OtherProc", LineNumber: 11},
		{ProcName: "", LineNumber: 12},
		{ProcName: "NoLine", LineNumber: 0},
	}

	relations, err := buildJSProcedureCallRelationsWithResolvers(
		calls,
		func(lineNumber int) (int64, error) {
			switch lineNumber {
			case 10:
				return 101, nil
			case 11:
				return 102, nil
			default:
				return 0, dbsql.ErrNoRows
			}
		},
		func(procName string) (int64, error) {
			switch procName {
			case "TestProc":
				return 201, nil
			case "OtherProc":
				return 202, nil
			default:
				return 0, dbsql.ErrNoRows
			}
		},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(relations) != 2 {
		t.Fatalf("unexpected relations count: got=%d want=%d", len(relations), 2)
	}

	if relations[0].SourceType != "js_function" || relations[0].TargetType != "sql_procedure" || relations[0].RelationType != "calls_procedure" {
		t.Fatalf("unexpected first relation types: %+v", relations[0])
	}
	if relations[0].SourceID != 101 || relations[0].TargetID != 201 || relations[0].LineNumber != 10 {
		t.Fatalf("unexpected first relation payload: %+v", relations[0])
	}

	if relations[1].SourceID != 102 || relations[1].TargetID != 202 || relations[1].LineNumber != 11 {
		t.Fatalf("unexpected second relation payload: %+v", relations[1])
	}
}

func TestBuildJSProcedureCallRelationsWithResolvers_SkipsNotResolved(t *testing.T) {
	calls := []*model.JSProcedureCall{
		{ProcName: "MissingProc", LineNumber: 20},
		{ProcName: "NoSource", LineNumber: 21},
	}

	relations, err := buildJSProcedureCallRelationsWithResolvers(
		calls,
		func(lineNumber int) (int64, error) {
			if lineNumber == 21 {
				return 0, dbsql.ErrNoRows
			}
			return 1001, nil
		},
		func(procName string) (int64, error) {
			if procName == "MissingProc" {
				return 0, dbsql.ErrNoRows
			}
			return 2001, nil
		},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(relations) != 0 {
		t.Fatalf("expected no relations, got=%d", len(relations))
	}
}

func TestBuildJSProcedureCallRelationsWithResolvers_PropagatesResolverErrors(t *testing.T) {
	sourceErr := errors.New("source failed")
	targetErr := errors.New("target failed")

	_, err := buildJSProcedureCallRelationsWithResolvers(
		[]*model.JSProcedureCall{{ProcName: "P1", LineNumber: 1}},
		func(lineNumber int) (int64, error) {
			return 0, sourceErr
		},
		func(procName string) (int64, error) {
			return 1, nil
		},
	)
	if !errors.Is(err, sourceErr) {
		t.Fatalf("expected source error, got: %v", err)
	}

	_, err = buildJSProcedureCallRelationsWithResolvers(
		[]*model.JSProcedureCall{{ProcName: "P1", LineNumber: 1}},
		func(lineNumber int) (int64, error) {
			return 1, nil
		},
		func(procName string) (int64, error) {
			return 0, targetErr
		},
	)
	if !errors.Is(err, targetErr) {
		t.Fatalf("expected target error, got: %v", err)
	}
}
