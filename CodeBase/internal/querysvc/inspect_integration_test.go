//go:build integration

package querysvc

import (
	"context"
	"errors"
	"testing"

	"github.com/codebase/internal/errs"
	"github.com/codebase/internal/query"
	"github.com/codebase/internal/store/testutil"
)

type inspectBridgeFixture struct {
	q *query.Query
}

func seedInspectBridgeFixture(t *testing.T) inspectBridgeFixture {
	t.Helper()
	db := testutil.Open(t)
	ctx := context.Background()
	scanID, err := db.CreateScanRun(ctx, "/repo")
	if err != nil {
		t.Fatalf("CreateScanRun: %v", err)
	}
	var fileID int64
	err = db.QueryRow(`
		INSERT INTO files (scan_run_id, path, rel_path, extension, size_bytes, hash_sha256, modified_at, encoding, language)
		VALUES ($1, '/repo/bridge.sql', '/repo/bridge.sql', 'pas', 1, 'hash', NOW(), 'UTF8', 'Pascal')
		RETURNING id
	`, scanID).Scan(&fileID)
	if err != nil {
		t.Fatalf("insert file: %v", err)
	}

	insertRow := func(queryText string, args ...interface{}) int64 {
		t.Helper()
		var id int64
		if err := db.QueryRow(queryText, args...).Scan(&id); err != nil {
			t.Fatalf("insert (%s): %v", queryText, err)
		}
		return id
	}

	methodID := insertRow(`
		INSERT INTO pas_methods (file_id, method_name, line_number) VALUES ($1, 'DeleteLinkToPolicy', 10) RETURNING id
	`, fileID)
	fragmentID := insertRow(`
		INSERT INTO query_fragments (file_id, parent_type, parent_id, component_name, component_type, query_text, context, line_number)
		VALUES ($1, 'pas_method', $2, 'DeleteLinkToPolicy:btnOk', 'pas_method', 'SELECT 1', 'pas', 12)
		RETURNING id
	`, fileID, methodID)
	insertRow(`
		INSERT INTO symbols (file_id, symbol_name, symbol_type, entity_type, entity_id, line_number)
		VALUES ($1, 'DeleteLinkToPolicy', 'method', 'pas', $2, 10)
		RETURNING id
	`, fileID, methodID)
	insertRow(`
		INSERT INTO relations (source_type, source_id, target_type, target_id, relation_type, confidence, line_number)
		VALUES ('pas_method', $1, 'query_fragment', $2, 'builds_query', 'certain', 12)
		RETURNING id
	`, methodID, fragmentID)

	contractID := insertRow(`
		INSERT INTO api_contracts (file_id, contract_name, contract_kind) VALUES ($1, 'API_Bridge_MySvc', 'service') RETURNING id
	`, fileID)
	implProcID := insertRow(`
		INSERT INTO sql_procedures (file_id, proc_name, line_start, line_end) VALUES ($1, 'Bridge_MySvc_Impl', 1, 2) RETURNING id
	`, fileID)
	insertRow(`
		INSERT INTO symbols (file_id, symbol_name, symbol_type, entity_type, entity_id, line_number)
		VALUES ($1, 'API_Bridge_MySvc', 'service', 'xml', $2, 1)
		RETURNING id
	`, fileID, contractID)
	insertRow(`
		INSERT INTO symbols (file_id, symbol_name, symbol_type, entity_type, entity_id, line_number)
		VALUES ($1, 'Bridge_MySvc_Impl', 'procedure', 'sql', $2, 1)
		RETURNING id
	`, fileID, implProcID)
	insertRow(`
		INSERT INTO relations (source_type, source_id, target_type, target_id, relation_type, confidence, line_number)
		VALUES ('sql_procedure', $1, 'api_contract', $2, 'implements_contract', 'certain', 1)
		RETURNING id
	`, implProcID, contractID)

	return inspectBridgeFixture{q: query.New(db)}
}

func TestRunInspectQuery_PasMethodSeesBuildsQuery(t *testing.T) {
	fx := seedInspectBridgeFixture(t)

	results, err := RunInspectQuery(context.Background(), fx.q, "DeleteLinkToPolicy", "method", 50)
	if err != nil {
		t.Fatalf("RunInspectQuery: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected single inspect result, got %d", len(results))
	}
	if results[0].Symbol.Type != "method" {
		t.Fatalf("symbol type = %q, want method", results[0].Symbol.Type)
	}
	if len(results[0].Outgoing) == 0 {
		t.Fatal("PAS method inspect must return outgoing builds_query relations")
	}
	found := false
	for _, rel := range results[0].Outgoing {
		if rel.RelationType == "builds_query" {
			found = true
		}
	}
	if !found {
		t.Fatalf("outgoing relations must include builds_query, got %+v", results[0].Outgoing)
	}
}

func TestRunInspectQuery_ApiContractSeesImplementsContract(t *testing.T) {
	fx := seedInspectBridgeFixture(t)
	ctx := context.Background()

	for _, typeFilter := range []string{"", "service", "api_contract"} {
		results, err := RunInspectQuery(ctx, fx.q, "API_Bridge_MySvc", typeFilter, 50)
		if err != nil {
			t.Fatalf("RunInspectQuery(type=%q): %v", typeFilter, err)
		}
		if len(results) != 1 {
			t.Fatalf("type=%q: expected single inspect result, got %d", typeFilter, len(results))
		}
		if len(results[0].Incoming) == 0 {
			t.Fatalf("type=%q: API contract inspect must return incoming implements_contract relations", typeFilter)
		}
		found := false
		for _, rel := range results[0].Incoming {
			if rel.RelationType == "implements_contract" {
				found = true
			}
		}
		if !found {
			t.Fatalf("type=%q: incoming relations must include implements_contract, got %+v", typeFilter, results[0].Incoming)
		}
	}
}

func TestRunInspectQuery_UnknownTypePropagatesError(t *testing.T) {
	fx := seedInspectBridgeFixture(t)

	_, err := RunInspectQuery(context.Background(), fx.q, "DeleteLinkToPolicy", "pas", 50)
	if err == nil {
		t.Fatal("inspect with unknown type 'pas' must fail")
	}
	if !errors.Is(err, errs.ErrUnknownSymbolType) {
		t.Fatalf("error must wrap errs.ErrUnknownSymbolType, got %v", err)
	}
}
