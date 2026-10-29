package query

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
)

// CollectBranchWarnings собирает предупреждения о fallback'ах линий по
// фактическим элементам результата: для каждой структуры читаются поля
// Product, BranchRequested и BranchUsed. Предупреждение формируется, когда
// профиль запросил линии продукта, а выбранная ветка не входит в этот список
// (в том числе когда ветка не определена — копия без привязки к линии).
//
// Предупреждения агрегируются по тройке (продукт, запрошенные линии,
// использованная линия) и сортируются, поэтому один и тот же fallback,
// встретившийся в нескольких элементах, даёт одну строку. Обход рекурсивный:
// вложенные списки (параметры/таблицы API, узлы RTI-дерева) учитываются.
func CollectBranchWarnings(value interface{}) []string {
	collector := &branchWarningCollector{
		seen: make(map[string]string),
	}
	collector.walk(reflect.ValueOf(value), make(map[uintptr]bool))
	if len(collector.seen) == 0 {
		return nil
	}
	keys := make([]string, 0, len(collector.seen))
	for key := range collector.seen {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	warnings := make([]string, 0, len(keys))
	for _, key := range keys {
		warnings = append(warnings, collector.seen[key])
	}
	return warnings
}

type branchWarningCollector struct {
	seen map[string]string
}

func (c *branchWarningCollector) walk(v reflect.Value, visited map[uintptr]bool) {
	if !v.IsValid() {
		return
	}
	switch v.Kind() {
	case reflect.Interface:
		if v.IsNil() {
			return
		}
		c.walk(v.Elem(), visited)
	case reflect.Pointer:
		if v.IsNil() {
			return
		}
		ptr := v.Pointer()
		if visited[ptr] {
			return
		}
		visited[ptr] = true
		c.walk(v.Elem(), visited)
	case reflect.Struct:
		c.collectFromStruct(v)
		for i := 0; i < v.NumField(); i++ {
			c.walk(v.Field(i), visited)
		}
	case reflect.Slice, reflect.Array:
		for i := 0; i < v.Len(); i++ {
			c.walk(v.Index(i), visited)
		}
	case reflect.Map:
		iter := v.MapRange()
		for iter.Next() {
			c.walk(iter.Value(), visited)
		}
	}
}

func (c *branchWarningCollector) collectFromStruct(v reflect.Value) {
	productField := v.FieldByName("Product")
	requestedField := v.FieldByName("BranchRequested")
	usedField := v.FieldByName("BranchUsed")
	if !productField.IsValid() || !requestedField.IsValid() || !usedField.IsValid() {
		return
	}
	if productField.Kind() != reflect.String || requestedField.Kind() != reflect.String || usedField.Kind() != reflect.String {
		return
	}
	product := strings.TrimSpace(productField.String())
	requested := strings.TrimSpace(requestedField.String())
	used := strings.TrimSpace(usedField.String())
	if product == "" || requested == "" {
		return
	}
	keys := splitRequestedBranches(requested)
	if len(keys) == 0 {
		return
	}
	if used != "" && containsString(keys, used) {
		return
	}
	key := product + "\x00" + requested + "\x00" + used
	if _, exists := c.seen[key]; exists {
		return
	}
	c.seen[key] = branchFallbackWarning(keys, product, used)
}

// splitRequestedBranches разбирает branch_requested, склеенный через ", ".
func splitRequestedBranches(requested string) []string {
	parts := strings.Split(requested, ", ")
	keys := make([]string, 0, len(parts))
	for _, part := range parts {
		if key := strings.TrimSpace(part); key != "" {
			keys = append(keys, key)
		}
	}
	return keys
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func branchFallbackWarning(keys []string, product, used string) string {
	label := "линия"
	verb := "не проиндексирована"
	if len(keys) > 1 {
		label = "линии"
		verb = "не проиндексированы"
	}
	if used == "" {
		return fmt.Sprintf(
			"%s %s продукта %s %s; копия выбрана без привязки к ветке — тела могут отличаться от установленной в банке линии",
			label, strings.Join(keys, ", "), product, verb)
	}
	return fmt.Sprintf(
		"%s %s продукта %s %s, показан код ветки %s — тела могут отличаться от установленной в банке линии",
		label, strings.Join(keys, ", "), product, verb, used)
}
