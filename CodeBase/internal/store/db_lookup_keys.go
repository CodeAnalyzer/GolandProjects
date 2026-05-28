package store

import (
	"fmt"
	"strings"
)

func BuildHDefineLookupKey(defineName string, lineNumber int) string {
	return strings.ToLower(strings.TrimSpace(defineName)) + "|" + fmt.Sprintf("%d", lineNumber)
}

func BuildJSFunctionLookupKey(functionName string, lineStart int) string {
	return strings.ToLower(strings.TrimSpace(functionName)) + "|" + fmt.Sprintf("%d", lineStart)
}

func BuildDFMFormLookupKey(formName string, lineStart int) string {
	return strings.ToLower(strings.TrimSpace(formName)) + "|" + fmt.Sprintf("%d", lineStart)
}

func BuildDFMComponentLookupKey(componentName string, lineStart int) string {
	return strings.ToLower(strings.TrimSpace(componentName)) + "|" + fmt.Sprintf("%d", lineStart)
}

func BuildPASUnitLookupKey(unitName string, lineStart int) string {
	return strings.ToLower(strings.TrimSpace(unitName)) + "|" + fmt.Sprintf("%d", lineStart)
}

func BuildPASClassLookupKey(className string, lineStart int) string {
	return strings.ToLower(strings.TrimSpace(className)) + "|" + fmt.Sprintf("%d", lineStart)
}

func BuildPASMethodLookupKey(className string, methodName string, lineNumber int) string {
	return strings.ToLower(strings.TrimSpace(className)) + "|" + strings.ToLower(strings.TrimSpace(methodName)) + "|" + fmt.Sprintf("%d", lineNumber)
}

func BuildPASFieldLookupKey(className string, fieldName string, lineNumber int) string {
	return strings.ToLower(strings.TrimSpace(className)) + "|" + strings.ToLower(strings.TrimSpace(fieldName)) + "|" + fmt.Sprintf("%d", lineNumber)
}

func BuildSQLTableLookupKey(tableName, context string, lineNumber int) string {
	return strings.ToLower(strings.TrimSpace(tableName)) + "|" + strings.ToLower(strings.TrimSpace(context)) + "|" + fmt.Sprintf("%d", lineNumber)
}

func BuildSQLIndexDefinitionLookupKey(tableName, indexName string, lineNumber int) string {
	return strings.ToLower(strings.TrimSpace(tableName)) + "|" + strings.ToLower(strings.TrimSpace(indexName)) + "|" + fmt.Sprintf("%d", lineNumber)
}

func BuildQueryFragmentLookupKey(queryHash, context string, lineNumber int) string {
	return strings.TrimSpace(queryHash) + "|" + strings.ToLower(strings.TrimSpace(context)) + "|" + fmt.Sprintf("%d", lineNumber)
}

func BuildReportFieldLookupKey(fieldName string, lineNumber int) string {
	return strings.ToLower(strings.TrimSpace(fieldName)) + "|" + fmt.Sprintf("%d", lineNumber)
}

func BuildReportParamLookupKey(paramName string, lineNumber int) string {
	return strings.ToLower(strings.TrimSpace(paramName)) + "|" + fmt.Sprintf("%d", lineNumber)
}

func BuildVBFunctionLookupKey(functionName string, lineStart int) string {
	return strings.ToLower(strings.TrimSpace(functionName)) + "|" + fmt.Sprintf("%d", lineStart)
}

func BuildJSConstantLookupKey(constantName string, lineNumber int) string {
	return strings.ToLower(strings.TrimSpace(constantName)) + "|" + fmt.Sprintf("%d", lineNumber)
}

func BuildSQLColumnDefinitionLookupKey(tableName string, columnName string, lineNumber int, columnOrder int) string {
	return strings.ToLower(strings.TrimSpace(tableName)) + "|" + strings.ToLower(strings.TrimSpace(columnName)) + "|" + fmt.Sprintf("%d", lineNumber) + "|" + fmt.Sprintf("%d", columnOrder)
}
