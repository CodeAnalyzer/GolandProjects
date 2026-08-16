package rti

import (
	"context"
	"errors"
	"testing"

	"github.com/codebase/internal/query"
)

type mockClientLookup struct {
	methods       map[string][]query.MethodResult
	dfmForms      map[string][]query.DFMFormResult
	queryFragment map[string][]query.QueryFragmentResult
}

func (m *mockClientLookup) FindPASMethodsByName(ctx context.Context, methodName string, like bool, limit int) ([]query.MethodResult, error) {
	if res, ok := m.methods[methodName]; ok {
		return res, nil
	}
	return nil, errors.New("not found")
}

func (m *mockClientLookup) SearchDFMForm(ctx context.Context, name string, like bool, limit int) ([]query.DFMFormResult, error) {
	if res, ok := m.dfmForms[name]; ok {
		return res, nil
	}
	return nil, nil
}

func (m *mockClientLookup) SearchQueryFragment(ctx context.Context, text string, limit int) ([]query.QueryFragmentResult, error) {
	if res, ok := m.queryFragment[text]; ok {
		return res, nil
	}
	return nil, nil
}

func TestEnrichClientEvent_ClassMethodMatch(t *testing.T) {
	q := &mockClientLookup{
		methods: map[string][]query.MethodResult{
			"Open": {
				{MethodName: "Open", ClassName: "DsADORecordset", UnitName: "DsADO", File: "d5ntsys/DsADO.pas", LineNumber: 100},
			},
		},
	}
	ev := &RTIClientEvent{ClassName: "DsADORecordset", MethodName: "Open", Kind: "recordset_open"}

	enrich := EnrichClientEvent(context.Background(), q, ev)
	if !enrich.Found {
		t.Fatalf("expected Found=true")
	}
	if enrich.SourceFile != "d5ntsys/DsADO.pas" || enrich.LineNumber != 100 {
		t.Fatalf("unexpected enrichment: %+v", enrich)
	}
}

func TestEnrichClientEvent_NoMatch(t *testing.T) {
	q := &mockClientLookup{methods: map[string][]query.MethodResult{}}
	ev := &RTIClientEvent{ClassName: "UnknownClass", MethodName: "UnknownMethod", Kind: "generic"}

	enrich := EnrichClientEvent(context.Background(), q, ev)
	if enrich.Found {
		t.Fatalf("expected Found=false, got %+v", enrich)
	}
}

func TestEnrichClientEvent_WithDFMForm(t *testing.T) {
	q := &mockClientLookup{
		methods: map[string][]query.MethodResult{
			"OnClick": {
				{MethodName: "OnClick", ClassName: "TfrmConsumer", UnitName: "ConsumerForm", File: "Consumer/ConsumerForm.pas", LineNumber: 50},
			},
		},
		dfmForms: map[string][]query.DFMFormResult{
			"TfrmConsumer": {
				{FormName: "frmConsumer", FormClass: "TfrmConsumer", Caption: "Consumer Screen"},
			},
		},
	}
	ev := &RTIClientEvent{ClassName: "TfrmConsumer", MethodName: "OnClick", Kind: "generic"}

	enrich := EnrichClientEvent(context.Background(), q, ev)
	if !enrich.Found {
		t.Fatalf("expected Found=true")
	}
	if enrich.DFMFormName != "frmConsumer" || enrich.DFMCaption != "Consumer Screen" {
		t.Fatalf("unexpected DFM enrichment: %+v", enrich)
	}
}

func TestEnrichClientEvent_WithoutDFMForm(t *testing.T) {
	q := &mockClientLookup{
		methods: map[string][]query.MethodResult{
			"Open": {
				{MethodName: "Open", ClassName: "DsADORecordset", UnitName: "DsADO", File: "d5ntsys/DsADO.pas", LineNumber: 100},
			},
		},
		dfmForms: map[string][]query.DFMFormResult{},
	}
	ev := &RTIClientEvent{ClassName: "DsADORecordset", MethodName: "Open", Kind: "recordset_open"}

	enrich := EnrichClientEvent(context.Background(), q, ev)
	if enrich.DFMFormName != "" {
		t.Fatalf("expected no DFM form for infrastructure class, got %+v", enrich)
	}
}

func TestEnrichClientEvent_QueryFragmentMatchForExecBlock(t *testing.T) {
	q := &mockClientLookup{
		queryFragment: map[string][]query.QueryFragmentResult{
			"FCD_10_Log_SaveOption": {
				{
					QueryText:     "exec FCD_10_Log_SaveOption @UserID = :UserID, @HostName = :HostName",
					ComponentName: "SaveClientOptions",
					File:          "Consumer/ClientOptions.pas",
					LineNumber:    77,
				},
			},
		},
	}
	ev := &RTIClientEvent{
		Kind: "sql_block",
		SQL: &RTISQLBlock{
			ExecProcedure: "FCD_10_Log_SaveOption",
			Text:          "exec FCD_10_Log_SaveOption\n  @UserID = 0,\n  @HostName = 'AFEDOROV'",
		},
	}

	enrich := EnrichClientEvent(context.Background(), q, ev)
	if enrich.QueryFragmentFile != "Consumer/ClientOptions.pas" || enrich.QueryFragmentLine != 77 {
		t.Fatalf("expected query fragment match, got %+v", enrich)
	}
	if enrich.OriginMethod != "SaveClientOptions" {
		t.Fatalf("expected origin method SaveClientOptions, got %q", enrich.OriginMethod)
	}
}

func TestEnrichClientEvent_QueryFragmentNoMatch(t *testing.T) {
	q := &mockClientLookup{
		queryFragment: map[string][]query.QueryFragmentResult{
			"FCD_10_Log_SaveOption": {
				{
					QueryText:  "select 1", // не содержит имени процедуры
					File:       "somewhere.pas",
					LineNumber: 1,
				},
			},
		},
	}
	ev := &RTIClientEvent{
		Kind: "sql_block",
		SQL: &RTISQLBlock{
			ExecProcedure: "FCD_10_Log_SaveOption",
			Text:          "exec FCD_10_Log_SaveOption @UserID = 0",
		},
	}

	enrich := EnrichClientEvent(context.Background(), q, ev)
	if enrich.QueryFragmentFile != "" {
		t.Fatalf("expected no query fragment match, got %+v", enrich)
	}
}

func TestEnrichClientEvents_Deduplication(t *testing.T) {
	calls := 0
	q := &mockClientLookup{
		methods: map[string][]query.MethodResult{
			"Open": {{MethodName: "Open", ClassName: "DsADORecordset", File: "f.pas", LineNumber: 1}},
		},
	}
	events := []*RTIClientEvent{
		{ClassName: "DsADORecordset", MethodName: "Open", Kind: "recordset_open"},
		{ClassName: "DsADORecordset", MethodName: "Open", Kind: "recordset_open"},
		{ClassName: "DsADORecordset", MethodName: "Open", Kind: "recordset_open"},
	}
	result := EnrichClientEvents(context.Background(), q, events)
	_ = calls
	if len(result) != 1 {
		t.Fatalf("expected 1 unique enrichment key, got %d", len(result))
	}
	key := "DsADORecordset.Open"
	if _, ok := result[key]; !ok {
		t.Fatalf("expected key %q in result", key)
	}
}
