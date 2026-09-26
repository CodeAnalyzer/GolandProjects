package querysvc

import (
	"database/sql"
	"errors"
	"testing"

	"github.com/codebase/internal/query"
)

func TestProcedureResultItems_NotFoundIsEmpty(t *testing.T) {
	items, err := ProcedureResultItems(nil, sql.ErrNoRows)
	if err != nil {
		t.Fatalf("not found must not error: %v", err)
	}
	result, ok := items.([]query.SQLProcedureResult)
	if !ok {
		t.Fatalf("items type = %T, want []query.SQLProcedureResult", items)
	}
	if len(result) != 0 {
		t.Fatalf("not found items = %d, want 0", len(result))
	}
}

func TestProcedureResultItems_FoundIsSingleItem(t *testing.T) {
	proc := &query.SQLProcedureResult{ProcName: "MyProc", File: "a.sql"}
	items, err := ProcedureResultItems(proc, nil)
	if err != nil {
		t.Fatalf("found must not error: %v", err)
	}
	result, ok := items.([]query.SQLProcedureResult)
	if !ok {
		t.Fatalf("items type = %T, want []query.SQLProcedureResult", items)
	}
	if len(result) != 1 || result[0].ProcName != "MyProc" {
		t.Fatalf("found items = %+v, want single MyProc", result)
	}
}

func TestProcedureResultItems_OtherErrorPropagates(t *testing.T) {
	boom := errors.New("boom")
	_, err := ProcedureResultItems(nil, boom)
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want boom", err)
	}
}
