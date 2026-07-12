package trc

import (
	"fmt"
	"strings"

	"github.com/codebase/internal/query"
)

// ProcedureLookup — узкий интерфейс для получения данных о процедуре из
// CodeBase. Реализуется *query.Query (тот же интерфейс, что и
// internal/rti/enrich.go — переиспользуется отдельным определением, а не
// импортом пакета rti, чтобы не создавать межпакетную зависимость
// internal/trc -> internal/rti).
type ProcedureLookup interface {
	GetProcedureResult(name string) (*query.SQLProcedureResult, error)
}

// ProcedureEnrichment — результат обогащения процедуры данными из CodeBase.
type ProcedureEnrichment struct {
	Procedure  string                  `json:"procedure"`
	SourceFile string                  `json:"source_file,omitempty"`
	LineStart  int                     `json:"line_start,omitempty"`
	LineEnd    int                     `json:"line_end,omitempty"`
	Params     []query.SQLParamResult  `json:"params,omitempty"`
	Found      bool                    `json:"found"`
}

// EnrichProcedure ищет процедуру в CodeBase DB и возвращает enrichment.
// Отбрасывает завершающий суффикс "#" от имени процедуры (встречается в
// TextData для параметризованных вызовов) перед поиском.
func EnrichProcedure(q ProcedureLookup, procName string) (*ProcedureEnrichment, error) {
	name := trimHashSuffix(procName)
	proc, err := q.GetProcedureResult(name)
	if err != nil {
		return nil, fmt.Errorf("procedure %q not found: %w", procName, err)
	}
	return &ProcedureEnrichment{
		Procedure:  procName,
		SourceFile: proc.File,
		LineStart:  proc.LineStart,
		LineEnd:    proc.LineEnd,
		Params:     proc.Params,
		Found:      true,
	}, nil
}

// EnrichEvents обогащает события данными из CodeBase DB. Возвращает map:
// procedure name (как в TRCEvent.Procedure) → enrichment.
func EnrichEvents(q ProcedureLookup, events []TRCEvent) map[string]*ProcedureEnrichment {
	result := make(map[string]*ProcedureEnrichment)
	for _, ev := range events {
		if ev.Procedure == "" {
			continue
		}
		if _, ok := result[ev.Procedure]; ok {
			continue
		}
		enrich, err := EnrichProcedure(q, ev.Procedure)
		if err != nil {
			result[ev.Procedure] = &ProcedureEnrichment{
				Procedure:  ev.Procedure,
				Found:      false,
				SourceFile: "(not found)",
			}
			continue
		}
		result[ev.Procedure] = enrich
	}
	return result
}

// trimHashSuffix отбрасывает завершающий суффикс "#..." (маркер
// параметризованного/временного варианта имени), который может
// присутствовать в имени процедуры, извлечённом из TextData.
func trimHashSuffix(name string) string {
	if i := strings.IndexByte(name, '#'); i >= 0 {
		return name[:i]
	}
	return name
}
