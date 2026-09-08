package specsvc

import (
	"math"
	"strconv"
	"strings"
)

// parsePGFloatArray парсит PostgreSQL array literal "{1.0,2.0,3.0}" в []float64.
func parsePGFloatArray(s string) []float64 {
	s = strings.TrimSpace(s)
	if len(s) < 2 || s[0] != '{' || s[len(s)-1] != '}' {
		return nil
	}
	inner := s[1 : len(s)-1]
	if inner == "" {
		return nil
	}
	parts := strings.Split(inner, ",")
	result := make([]float64, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		v, err := strconv.ParseFloat(p, 64)
		if err != nil {
			continue
		}
		result = append(result, v)
	}
	return result
}

func mergeSpecSearchHits(hits []SpecSearchHit, limit int) []SpecSearchHit {
	seen := make(map[string]struct{}, len(hits))
	result := make([]SpecSearchHit, 0, len(hits))
	for _, hit := range hits {
		key := hit.Level + "|" + strconv.FormatInt(hit.EntityID, 10)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		result = append(result, hit)
		if len(result) == limit {
			break
		}
	}
	return result
}

// cosineSim вычисляет cosine similarity между двумя векторами.
func cosineSim(a, b []float64) float64 {
	if len(a) != len(b) || len(a) == 0 {
		return 0
	}
	var dot, normA, normB float64
	for i := range a {
		dot += a[i] * b[i]
		normA += a[i] * a[i]
		normB += b[i] * b[i]
	}
	if normA == 0 || normB == 0 {
		return 0
	}
	return dot / (math.Sqrt(normA) * math.Sqrt(normB))
}
