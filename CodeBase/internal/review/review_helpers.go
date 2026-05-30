package review

import (
	"path/filepath"
	"strings"
)

var sharedTTables = map[string]struct{}{
	"tcontract":   {},
	"tdeal":       {},
	"tmanynumber": {},
	"tseed":       {},
	"tdocmark":    {},
}

type tableRef struct {
	Name string
	Line int
}

func tableNames(items []tableRef) []string {
	names := make([]string, 0, len(items))
	for _, item := range items {
		names = append(names, item.Name)
	}
	return names
}

func isSharedTTable(tableName string) bool {
	_, exists := sharedTTables[normalizeIdentifier(tableName)]
	return exists
}

func normalizePath(path string) string {
	return filepath.ToSlash(strings.TrimSpace(path))
}

func normalizeIdentifier(value string) string {
	trimmed := strings.TrimSpace(strings.ToLower(value))
	trimmed = strings.Trim(trimmed, "[]\"")
	return trimmed
}

func normalizeDataType(value string) string {
	v := strings.ToLower(strings.TrimSpace(value))
	v = strings.Join(strings.Fields(v), " ")
	return v
}

func areEquivalentTypes(left, right string) bool {
	left = normalizeDataType(left)
	right = normalizeDataType(right)
	if left == right {
		return true
	}
	lg := typeGroup(left)
	rg := typeGroup(right)
	return lg != "" && lg == rg
}

func typeGroup(dataType string) string {
	v := normalizeDataType(dataType)
	switch {
	case strings.Contains(v, "int") || strings.HasPrefix(v, "dsidentifier"):
		return "int"
	case strings.Contains(v, "decimal") || strings.Contains(v, "numeric") || strings.Contains(v, "money") || strings.Contains(v, "float") || strings.Contains(v, "real"):
		return "number"
	case strings.Contains(v, "char") || strings.Contains(v, "text") || strings.HasPrefix(v, "dsbriefname") || strings.HasPrefix(v, "dscomment"):
		return "string"
	case strings.Contains(v, "date") || strings.Contains(v, "time"):
		return "datetime"
	case strings.Contains(v, "bit") || strings.Contains(v, "bool"):
		return "bool"
	default:
		return ""
	}
}

func enabledRuleSet(rules []RuleID) map[RuleID]bool {
	result := map[RuleID]bool{
		RuleForeignTablesUsing:    true,
		RuleForeignPTablesUsing:   true,
		RuleForeignProcedureUsing: true,
		RuleExecNotExistsProc:     true,
		RuleProcDuplicate:         true,
		RuleProcParamDefValue:     true,
		RuleProcElseCase:          true,
		RuleUseSelectAll:          true,
		RuleTruncTbl:              true,
		RuleDatatype:              true,
	}
	if len(rules) == 0 {
		return result
	}
	for key := range result {
		result[key] = false
	}
	for _, rule := range rules {
		rule = RuleID(strings.TrimSpace(string(rule)))
		if _, exists := result[rule]; exists {
			result[rule] = true
		}
	}
	return result
}

func buildSummary(findings []Finding) Summary {
	summary := Summary{
		Total:      len(findings),
		ByRule:     map[RuleID]int{},
		BySeverity: map[int]int{},
	}
	for _, finding := range findings {
		summary.ByRule[finding.Rule]++
		summary.BySeverity[finding.Severity]++
	}
	if len(summary.ByRule) == 0 {
		summary.ByRule = nil
	}
	if len(summary.BySeverity) == 0 {
		summary.BySeverity = nil
	}
	return summary
}
