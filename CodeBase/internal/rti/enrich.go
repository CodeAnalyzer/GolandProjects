package rti

import (
	"context"
	"fmt"

	"github.com/codebase/internal/query"
)

// ProcedureLookup — узкий интерфейс для получения данных о процедуре из CodeBase.
// Реализуется *query.Query.
type ProcedureLookup interface {
	GetProcedureResult(ctx context.Context, name string) (*query.SQLProcedureResult, error)
}

// ProcedureEnrichment — результат обогащения процедуры данными из CodeBase
type ProcedureEnrichment struct {
	Procedure  string               `json:"procedure"`
	SourceFile string               `json:"source_file,omitempty"`
	LineNumber int                  `json:"line_number,omitempty"`
	LineStart  int                  `json:"line_start,omitempty"`
	LineEnd    int                  `json:"line_end,omitempty"`
	Params     []query.SQLParamResult `json:"params,omitempty"`
	Found      bool                 `json:"found"`
}

// EnrichCalls обогащает вызовы данными из CodeBase DB.
// Возвращает map: procedure name → enrichment.
func EnrichCalls(ctx context.Context, q ProcedureLookup, calls []*RTICall) map[string]*ProcedureEnrichment {
	result := make(map[string]*ProcedureEnrichment)
	for _, c := range calls {
		if _, ok := result[c.Procedure]; ok {
			continue
		}
		enrich, err := EnrichProcedure(ctx, q, c.Procedure)
		if err != nil {
			result[c.Procedure] = &ProcedureEnrichment{
				Procedure:  c.Procedure,
				Found:      false,
				SourceFile: "(not found)",
			}
			continue
		}
		result[c.Procedure] = enrich
	}
	return result
}

// EnrichProcedure ищет процедуру в CodeBase DB и возвращает enrichment.
func EnrichProcedure(ctx context.Context, q ProcedureLookup, procName string) (*ProcedureEnrichment, error) {
	proc, err := q.GetProcedureResult(ctx, procName)
	if err != nil {
		return nil, fmt.Errorf("procedure %q not found: %w", procName, err)
	}

	enrich := &ProcedureEnrichment{
		Procedure:  procName,
		SourceFile: proc.File,
		LineStart:  proc.LineStart,
		LineEnd:    proc.LineEnd,
		Params:     proc.Params,
		Found:      true,
	}

	return enrich, nil
}
