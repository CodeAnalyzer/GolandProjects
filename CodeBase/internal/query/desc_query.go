package query

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/codebase/internal/config"
	"github.com/codebase/internal/specfts"
	"github.com/lib/pq"
)

// DescriptionSearchResult — запись выдачи поиска по описаниям процедур
// и API-контрактов.
type DescriptionSearchResult struct {
	Kind        string  `json:"kind"`                  // procedure | service | event | callback_event | used_service
	Name        string  `json:"name"`                  // имя процедуры или контракта
	File        string  `json:"file,omitempty"`        // rel_path исходника
	LineStart   int     `json:"line_start"`            // строка начала сущности
	Rank        float64 `json:"rank"`                  // exact: ts_rank (имя A / описание B); semantic: cosine
	Source      string  `json:"source"`                // exact | semantic
	Snippet     string  `json:"snippet"`               // exact: ts_headline; semantic: embed_text
	Description string  `json:"description,omitempty"` // полное описание (очищенное)
}

// descSearchKindValid — допустимые значения фильтра kind.
var descSearchKindValid = map[string]bool{
	"procedure":      true,
	"service":        true,
	"event":          true,
	"callback_event": true,
	"used_service":   true,
}

// ValidateDescriptionSearchKinds проверяет значения фильтра kind, возвращая
// нормализованный (lowercase) список.
func ValidateDescriptionSearchKinds(kinds []string) ([]string, error) {
	if len(kinds) == 0 {
		return nil, nil
	}
	normalized := make([]string, 0, len(kinds))
	for _, k := range kinds {
		lk := strings.ToLower(strings.TrimSpace(k))
		if lk == "" {
			continue
		}
		if !descSearchKindValid[lk] {
			return nil, fmt.Errorf("недопустимое значение kind: %q (ожидается procedure|service|event|callback_event|used_service)", k)
		}
		normalized = append(normalized, lk)
	}
	return normalized, nil
}

// descSearchDisplaceFilter — дедуп «контракт вытесняет процедуру»: процедура
// с реализованным контрактом, у которого есть хоть какое-то описание, из
// выдачи исключается (контракт уже представляет пару).
const descSearchDisplaceFilter = `AND NOT EXISTS (
    SELECT 1
    FROM relations r
    JOIN api_contracts dc ON dc.id = r.target_id AND r.target_type = 'api_contract'
    WHERE r.source_type = 'sql_procedure'
      AND r.source_id = {alias}.id
      AND r.relation_type = 'implements_contract'
      AND coalesce(dc.short_description, '') || coalesce(dc.full_description, '') <> ''
)`

// SearchDescriptions — гибридный поиск по описаниям: exact (FTS tsvector,
// имя = вес A / описание = вес B) + semantic (LSA desc-модели, перефразировки).
// Дедуп «контракт вытесняет процедуру» и kind-фильтр действуют на объединённую
// выдачу. Хиты помечаются источником (exact | semantic); без обученной desc-
// модели поиск возвращает только exact-хиты без ошибки.
func (q *Query) SearchDescriptions(ctx context.Context, text string, kinds []string, limit int) ([]DescriptionSearchResult, bool, error) {
	if limit <= 0 {
		limit = 20
	}
	normalizedKinds, err := ValidateDescriptionSearchKinds(kinds)
	if err != nil {
		return nil, false, err
	}

	exact, exactHasMore, err := q.searchDescriptionsExact(ctx, text, normalizedKinds, limit)
	if err != nil {
		return nil, false, err
	}
	semantic, err := q.searchDescriptionsSemantic(ctx, text, normalizedKinds, limit)
	if err != nil {
		return nil, false, err
	}

	// Объединение: exact-хиты первыми (по убыванию ранга), затем semantic
	// (по убыванию cosine); дубликаты одной сущности в разных слоях схлопываются.
	seen := make(map[string]bool, len(exact)+len(semantic))
	combined := make([]DescriptionSearchResult, 0, len(exact)+len(semantic))
	for _, group := range [][]DescriptionSearchResult{exact, semantic} {
		for _, item := range group {
			key := item.Kind + "\x00" + item.Name
			if seen[key] {
				continue
			}
			seen[key] = true
			if len(combined) >= limit {
				break
			}
			combined = append(combined, item)
		}
	}
	// Усечение детектируется, если хоть один слой вернул limit+1 строку
	// либо объединённая выдача заполнена до лимита при остатке в слоях.
	hasMore := exactHasMore || len(exact)+len(semantic) > limit
	return combined, hasMore, nil
}

// searchDescriptionsExact — лексический слой: ts_rank по векторам процедур
// и контрактов, ts_headline-сниппет.
func (q *Query) searchDescriptionsExact(ctx context.Context, text string, kinds []string, limit int) ([]DescriptionSearchResult, bool, error) {
	kindFilter := ""
	if len(kinds) > 0 {
		kindFilter = ` WHERE kind = ANY($3)`
	}

	// ts_headline дорог — применяется только к отсечённым LIMIT-ом топ-строкам
	// каждой ветки UNION (limit+1 на сторону для детекции усечения), а не ко
	// всем совпадениям.
	query := fmt.Sprintf(`
		SELECT kind, name, file, line_start, rank, source, description,
		       ts_headline('russian', description, tsq,
		                   'MaxWords=35, MinWords=15, MaxFragments=2, StartSel=<<, StopSel=>>') AS snippet
		FROM (
			(SELECT 'procedure'::text AS kind,
			       p.proc_name AS name,
			       f.rel_path AS file,
			       p.line_start AS line_start,
			       ts_rank(p.search_vector, tq.tsq) AS rank,
			       'exact'::text AS source,
			       coalesce(p.description, '') AS description,
			       tq.tsq AS tsq
			FROM sql_procedures p
			JOIN files f ON f.id = p.file_id
			CROSS JOIN (SELECT plainto_tsquery('russian', $1) AS tsq) tq
			WHERE p.search_vector @@ plainto_tsquery('russian', $1)
			  `+strings.ReplaceAll(descSearchDisplaceFilter, "{alias}", "p")+`
			ORDER BY rank DESC, name
			LIMIT $2)

			UNION ALL

			(SELECT lower(c.contract_kind) AS kind,
			       c.contract_name AS name,
			       f.rel_path AS file,
			       c.line_start AS line_start,
			       ts_rank(c.search_vector, tq.tsq) AS rank,
			       'exact'::text AS source,
			       coalesce(c.short_description, '') || ' ' || coalesce(c.full_description, '') AS description,
			       tq.tsq AS tsq
			FROM api_contracts c
			JOIN files f ON f.id = c.file_id
			CROSS JOIN (SELECT plainto_tsquery('russian', $1) AS tsq) tq
			WHERE c.search_vector @@ plainto_tsquery('russian', $1)
			ORDER BY rank DESC, name
			LIMIT $2)
		) res%s
		ORDER BY rank DESC, name
		LIMIT $4`, kindFilter)

	outerLimitParam := "$3"
	if kindFilter != "" {
		outerLimitParam = "$4"
	}
	query = strings.Replace(query, "LIMIT $4", "LIMIT "+outerLimitParam, 1)

	args := []interface{}{text, limit + 1}
	if kindFilter != "" {
		args = append(args, pq.Array(kinds))
	}
	args = append(args, 2*(limit+1))

	rows, err := q.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, false, fmt.Errorf("desc search query: %w", err)
	}
	defer rows.Close()

	items := make([]DescriptionSearchResult, 0, limit+1)
	for rows.Next() {
		var item DescriptionSearchResult
		if err := rows.Scan(&item.Kind, &item.Name, &item.File, &item.LineStart,
			&item.Rank, &item.Source, &item.Description, &item.Snippet); err != nil {
			return nil, false, fmt.Errorf("desc search scan: %w", err)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, false, fmt.Errorf("desc search rows: %w", err)
	}
	hasMore := false
	if len(items) > limit {
		hasMore = true
		items = items[:limit]
	}
	return items, hasMore, nil
}

// searchDescriptionsSemantic — семантический слой: LSA desc-модели поверх
// desc_embeddings. Без модели/поколения возвращает nil (graceful exact-only).
func (q *Query) searchDescriptionsSemantic(ctx context.Context, text string, kinds []string, limit int) ([]DescriptionSearchResult, error) {
	cfg := config.Get()
	minCosine := 0.15
	relativeCutoff := 0.5
	if cfg != nil {
		minCosine = cfg.DescLSA.MinCosine()
		relativeCutoff = cfg.DescLSA.RelativeCutoff()
	}

	model, err := specfts.LoadLSAModel(config.DescLSAModelPath())
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil // модель не обучена — exact-only
		}
		return nil, fmt.Errorf("load desc LSA model: %w", err)
	}
	if model.Vocab == nil || model.VT == nil || model.Generation == "" || model.K <= 0 {
		return nil, fmt.Errorf("invalid desc LSA model")
	}
	hasGeneration, err := q.db.HasDescLSAGeneration(ctx, model.Generation)
	if err != nil {
		return nil, fmt.Errorf("check desc LSA generation: %w", err)
	}
	if !hasGeneration {
		return nil, nil
	}
	queryVec := model.Vocab.ProjectQuery(text, model.VT)
	if len(queryVec) == 0 {
		return nil, nil
	}

	// Эмбеддинги — из бинарного кэша (фолбэк: БД); 54k × 512 через pq-текст
	// занимает ~6 c, кэш — ~0.2 c.
	rows, err := q.loadDescEmbeddingsCached(ctx, model.Generation)
	if err != nil {
		return nil, err
	}

	// Дедуп «контракт вытесняет процедуру» для semantic-стороны: процедуры,
	// реализующие контракты с непустым описанием (лёгкий запрос, ~тысячи строк).
	displaced := make(map[int64]bool)
	dRows, err := q.db.QueryContext(ctx, `
		SELECT r.source_id
		FROM relations r
		JOIN api_contracts dc ON dc.id = r.target_id AND r.target_type = 'api_contract'
		WHERE r.source_type = 'sql_procedure'
		  AND r.relation_type = 'implements_contract'
		  AND coalesce(dc.short_description, '') || coalesce(dc.full_description, '') <> ''`)
	if err != nil {
		return nil, fmt.Errorf("desc semantic displaced: %w", err)
	}
	for dRows.Next() {
		var id int64
		if err := dRows.Scan(&id); err != nil {
			dRows.Close()
			return nil, fmt.Errorf("desc semantic displaced scan: %w", err)
		}
		displaced[id] = true
	}
	dRows.Close()
	if err := dRows.Err(); err != nil {
		return nil, fmt.Errorf("desc semantic displaced rows: %w", err)
	}

	kindSet := make(map[string]bool, len(kinds))
	for _, k := range kinds {
		kindSet[k] = true
	}

	hits := make([]DescriptionSearchResult, 0, len(rows))
	for i := range rows {
		row := &rows[i]
		if displaced[row.EntityID] {
			continue
		}
		if len(kindSet) > 0 && !kindSet[row.Kind] {
			continue
		}
		if len(row.FloatsLE) != len(queryVec) {
			continue
		}
		vec := make([]float64, len(row.FloatsLE))
		for j, v := range row.FloatsLE {
			vec[j] = float64(v)
		}
		hits = append(hits, DescriptionSearchResult{
			Kind:      row.Kind,
			Name:      row.Name,
			File:      row.File,
			LineStart: row.LineStart,
			Rank:      specfts.CosineSimilarity(queryVec, vec),
			Source:    "semantic",
			Snippet:   row.SnippetLine,
		})
	}

	// Абсолютный порог → сортировка DESC → относительный cutoff (по образцу spec-слоя)
	filtered := make([]DescriptionSearchResult, 0, len(hits))
	for _, hit := range hits {
		if hit.Rank > minCosine {
			filtered = append(filtered, hit)
		}
	}
	sort.SliceStable(filtered, func(i, j int) bool {
		return filtered[i].Rank > filtered[j].Rank
	})
	if len(filtered) > 0 && relativeCutoff > 0 {
		cutoff := relativeCutoff * filtered[0].Rank
		kept := filtered[:0]
		for _, hit := range filtered {
			if hit.Rank >= cutoff {
				kept = append(kept, hit)
			}
		}
		filtered = kept
	}
	if limit > 0 && len(filtered) > limit+1 {
		filtered = filtered[:limit+1]
	}
	return filtered, nil
}

// firstDescriptionLine — сниппет semantic-хита: первая непустая строка описания.
func firstDescriptionLine(description string) string {
	for _, line := range strings.Split(description, "\n") {
		if trimmed := strings.TrimSpace(line); trimmed != "" {
			return trimmed
		}
	}
	return ""
}
