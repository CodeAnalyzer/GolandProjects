package cmd

import (
	"github.com/codebase/internal/query"
	"github.com/spf13/cobra"
)

var (
	outputJSON              bool
	outputNDJSON            bool
	outputSummary           bool
	limit                   int
	symbolLikeSearch        bool
	jsFunctionLikeSearch    bool
	vbFunctionLikeSearch    bool
	smfInstrumentLikeSearch bool
	formLikeSearch          bool
	formComponentLikeSearch bool
	reportFormLikeSearch    bool
	reportFieldLikeSearch   bool
	reportParamLikeSearch   bool
	apiContractLikeSearch   bool
	apiTableLikeSearch      bool
	apiParamLikeSearch      bool
	tableLikeSearch         bool
	tableIndexLikeSearch    bool
	apiTableIndexLikeSearch bool
)

type queryResponseMeta struct {
	Limit   int               `json:"limit"`
	Filters map[string]string `json:"filters,omitempty"`
	Output  string            `json:"output,omitempty"`
}

type querySuccessResponse struct {
	Success       bool              `json:"success"`
	FormatVersion string            `json:"format_version"`
	Command       string            `json:"command"`
	Count         int               `json:"count"`
	Items         interface{}       `json:"items"`
	Summary       interface{}       `json:"summary,omitempty"`
	Meta          queryResponseMeta `json:"meta"`
}

type queryErrorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type queryErrorResponse struct {
	Success       bool           `json:"success"`
	FormatVersion string         `json:"format_version"`
	Command       string         `json:"command"`
	Error         queryErrorBody `json:"error"`
}

type queryCommandSpec struct {
	commandName string
	filters     map[string]string
	run         func(q *query.Query) (interface{}, error)
}

type querySummary struct {
	Count           int            `json:"count"`
	Kinds           map[string]int `json:"kinds,omitempty"`
	Files           int            `json:"files,omitempty"`
	RelationTypes   map[string]int `json:"relation_types,omitempty"`
	SourceTypes     map[string]int `json:"source_types,omitempty"`
	TargetTypes     map[string]int `json:"target_types,omitempty"`
	DistinctTargets int            `json:"distinct_targets,omitempty"`
}

type inspectResult struct {
	Symbol    query.SymbolResult     `json:"symbol"`
	Incoming  []query.RelationResult `json:"incoming"`
	Outgoing  []query.RelationResult `json:"outgoing"`
	Neighbors []query.SymbolResult   `json:"neighbors,omitempty"`
}

var queryCmd = &cobra.Command{
	Use:   "query",
	Short: "Point-in-time index queries",
	Long:  `Searches for entities in the index and outputs results.`,
}

var (
	symbolName         string
	symbolType         string
	tableName          string
	tableIndexName     string
	procedureName      string
	jsFuncName         string
	smfInstrName       string
	smfType            string
	formName           string
	formComponentName  string
	queryFragmentText  string
	reportFormName     string
	reportFieldName    string
	reportParamName    string
	vbFuncName         string
	relationSourceType string
	relationSourceName string
	relationTargetType string
	relationTargetName string
	relationType       string
	inspectName        string
	inspectType        string
)
