package querysvc

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/codebase/internal/query"
)

type InspectResult struct {
	Symbol    query.SymbolResult     `json:"symbol"`
	Incoming  []query.RelationResult `json:"incoming"`
	Outgoing  []query.RelationResult `json:"outgoing"`
	Neighbors []query.SymbolResult   `json:"neighbors,omitempty"`
}

func RunInspectQuery(ctx context.Context, q *query.Query, name string, symbolType string, limit int) ([]InspectResult, error) {
	symbols, err := q.SearchSymbol(ctx, name, symbolType, false, limit)
	if err != nil {
		return nil, err
	}
	if len(symbols) == 0 {
		symbols, err = q.SearchSymbol(ctx, name, symbolType, true, limit)
		if err != nil {
			return nil, err
		}
	}
	ordered := PrioritizeExactSymbolMatches(symbols, name, symbolType)
	ordered = LimitInspectSymbols(ordered, limit)
	results := make([]InspectResult, 0, len(ordered))
	for _, symbol := range ordered {
		relationType := InspectRelationType(symbol)
		outgoing, err := q.SearchRelationsByEntity(ctx, relationType, symbol.EntityID, "", 0, "", limit)
		if err != nil && !strings.Contains(err.Error(), "at least one relation filter must be provided") {
			return nil, err
		}
		incoming, err := q.SearchRelationsByEntity(ctx, "", 0, relationType, symbol.EntityID, "", limit)
		if err != nil && !strings.Contains(err.Error(), "at least one relation filter must be provided") {
			return nil, err
		}
		neighbors := CollectInspectNeighbors(symbol, incoming, outgoing)
		results = append(results, InspectResult{
			Symbol:    symbol,
			Incoming:  incoming,
			Outgoing:  outgoing,
			Neighbors: neighbors,
		})
	}
	return results, nil
}

func LimitInspectSymbols(symbols []query.SymbolResult, limit int) []query.SymbolResult {
	maxInspectSymbols := 5
	if limit > 0 && limit < maxInspectSymbols {
		maxInspectSymbols = limit
	}
	if len(symbols) <= maxInspectSymbols {
		return symbols
	}
	return symbols[:maxInspectSymbols]
}

func InspectRelationType(symbol query.SymbolResult) string {
	if strings.TrimSpace(symbol.Type) == "" {
		return strings.TrimSpace(symbol.EntityType)
	}
	switch strings.ToLower(strings.TrimSpace(symbol.EntityType)) {
	case "sql":
		if strings.EqualFold(symbol.Type, "procedure") {
			return "sql_procedure"
		}
		if strings.EqualFold(symbol.Type, "table") {
			return "sql_table"
		}
	case "dfm":
		if strings.EqualFold(symbol.Type, "form") {
			return "dfm_form"
		}
		if strings.EqualFold(symbol.Type, "component") {
			return "dfm_component"
		}
	}
	return strings.TrimSpace(symbol.Type)
}

func PrioritizeExactSymbolMatches(symbols []query.SymbolResult, name string, symbolType string) []query.SymbolResult {
	ordered := append([]query.SymbolResult(nil), symbols...)
	needleName := strings.ToLower(strings.TrimSpace(name))
	needleType := strings.ToLower(strings.TrimSpace(symbolType))
	sort.SliceStable(ordered, func(i, j int) bool {
		left := inspectScore(ordered[i], needleName, needleType)
		right := inspectScore(ordered[j], needleName, needleType)
		if left != right {
			return left > right
		}
		return ordered[i].Name < ordered[j].Name
	})
	return ordered
}

func inspectScore(item query.SymbolResult, needleName string, needleType string) int {
	score := 0
	if strings.EqualFold(item.Name, needleName) {
		score += 10
	}
	if needleType != "" && strings.EqualFold(item.Type, needleType) {
		score += 5
	}
	if strings.EqualFold(item.EntityType, needleType) {
		score += 3
	}
	return score
}

func CollectInspectNeighbors(symbol query.SymbolResult, incoming []query.RelationResult, outgoing []query.RelationResult) []query.SymbolResult {
	neighbors := make([]query.SymbolResult, 0)
	seen := map[string]struct{}{}
	appendNeighbor := func(ref query.RelationEntityRef) {
		key := fmt.Sprintf("%s:%d", ref.Type, ref.ID)
		if ref.ID == 0 || key == fmt.Sprintf("%s:%d", symbol.EntityType, symbol.ID) {
			return
		}
		if _, exists := seen[key]; exists {
			return
		}
		seen[key] = struct{}{}
		neighbors = append(neighbors, query.SymbolResult{
			ID:         ref.ID,
			FileID:     ref.FileID,
			Name:       ref.Name,
			Type:       ref.Type,
			EntityType: ref.Type,
			File:       ref.File,
			LineNumber: ref.LineNumber,
		})
	}
	for _, relation := range incoming {
		appendNeighbor(relation.Source)
	}
	for _, relation := range outgoing {
		appendNeighbor(relation.Target)
	}
	return neighbors
}
