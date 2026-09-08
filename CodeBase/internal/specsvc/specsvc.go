package specsvc

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/codebase/internal/config"
	"github.com/codebase/internal/errs"
	"github.com/codebase/internal/specfts"
	"github.com/codebase/internal/store"
)

// SpecSearchResult — результат двухслойного поиска по спекам.
type SpecSearchResult struct {
	Exact    []SpecSearchHit `json:"exact"`
	Semantic []SpecSearchHit `json:"semantic,omitempty"`
}

type SpecSearchHit struct {
	CapabilityID   int64   `json:"capability_id"`
	CapabilityName string  `json:"capability_name"`
	EntityID       int64   `json:"entity_id"`
	Level          string  `json:"level"`
	Title          string  `json:"title"`
	Purpose        string  `json:"purpose,omitempty"`
	LineStart      int     `json:"line_start,omitempty"`
	LineEnd        int     `json:"line_end,omitempty"`
	Snippet        string  `json:"snippet,omitempty"`
	Rank           float64 `json:"rank,omitempty"`
	Source         string  `json:"source"` // tsvector | trgm | lsa
	Product        string  `json:"product,omitempty"`
}

// SpecByCodeResult — спеки, ссылающиеся на код-сущность.
type SpecByCodeResult struct {
	MentionName string          `json:"mention_name"`
	MentionKind string          `json:"mention_kind"`
	Resolved    bool            `json:"resolved"`
	Hits        []SpecByCodeHit `json:"hits"`
}

type SpecByCodeHit struct {
	CapabilityID   int64  `json:"capability_id"`
	CapabilityName string `json:"capability_name"`
	Title          string `json:"title"`
	SourceType     string `json:"source_type"`
	SourceID       int64  `json:"source_id"`
	Product        string `json:"product,omitempty"`
	LineNumber     int    `json:"line_number"`
	LineStart      int    `json:"line_start,omitempty"`
	LineEnd        int    `json:"line_end,omitempty"`
	Snippet        string `json:"snippet,omitempty"`
}

// SpecDepsResult — дерево зависимостей capability.
type SpecDepsResult struct {
	CapabilityID   int64         `json:"capability_id"`
	CapabilityName string        `json:"capability_name"`
	Title          string        `json:"title"`
	Product        string        `json:"product,omitempty"`
	Dependencies   []SpecDepNode `json:"dependencies"`
}

type SpecDepNode struct {
	CapabilityID   int64         `json:"capability_id"`
	CapabilityName string        `json:"capability_name"`
	Title          string        `json:"title"`
	Product        string        `json:"product,omitempty"`
	Confidence     string        `json:"confidence"`
	LineNumber     int           `json:"line_number,omitempty"`
	Depth          int           `json:"depth"`
	Children       []SpecDepNode `json:"children,omitempty"`
}

// SpecUsecaseResult — usecase со шагами.
type SpecUsecaseResult struct {
	ID             int64                   `json:"id"`
	UsecaseName    string                  `json:"usecase_name"`
	Title          string                  `json:"title"`
	Description    string                  `json:"description,omitempty"`
	Actors         string                  `json:"actors,omitempty"`
	Preconditions  string                  `json:"preconditions,omitempty"`
	Postconditions string                  `json:"postconditions,omitempty"`
	SourceDir      string                  `json:"source_dir"`
	UsecaseKind    string                  `json:"usecase_kind"`
	Steps          []SpecUsecaseStep       `json:"steps,omitempty"`
	Capabilities   []SpecUsecaseCapability `json:"capabilities,omitempty"`
}

type SpecUsecaseCapability struct {
	ID             int64  `json:"id"`
	CapabilityName string `json:"capability_name"`
	Title          string `json:"title"`
	Product        string `json:"product,omitempty"`
}

type SpecUsecaseStep struct {
	FlowKind  string `json:"flow_kind"`
	StepOrder int    `json:"step_order"`
	StepText  string `json:"step_text"`
}

// SpecCoverageResult — покрытие capability.
type SpecCoverageResult struct {
	CapabilityID   int64             `json:"capability_id"`
	CapabilityName string            `json:"capability_name"`
	Title          string            `json:"title"`
	Product        string            `json:"product,omitempty"`
	ApiTotal       int               `json:"api_total"`
	ApiCovered     int               `json:"api_covered"`
	CodeTotal      int               `json:"code_total"`
	CodeListed     int               `json:"code_listed"`
	Gaps           []SpecCoverageGap `json:"gaps,omitempty"`
}

type SpecCoverageGap struct {
	MentionName string `json:"mention_name"`
	MentionKind string `json:"mention_kind"`
	SourceType  string `json:"source_type"`
	LineNumber  int    `json:"line_number"`
}

// SpecHistoryResult — история изменения capability через changes.
type SpecHistoryResult struct {
	CapabilityName string                  `json:"capability_name,omitempty"`
	ChangeName     string                  `json:"change_name,omitempty"`
	Status         string                  `json:"status,omitempty"`
	Product        string                  `json:"product,omitempty"`
	Changes        []SpecHistoryEntry      `json:"changes"`
	Capabilities   []SpecHistoryCapability `json:"capabilities,omitempty"`
}

type SpecHistoryEntry struct {
	ChangeName string `json:"change_name"`
	Status     string `json:"status"`
	Section    string `json:"section"` // ADDED | MODIFIED | REMOVED
	ReqName    string `json:"requirement_name"`
	BodyText   string `json:"body_text,omitempty"`
	Source     string `json:"source,omitempty"`
}

type SpecHistoryCapability struct {
	CapabilityID   int64  `json:"capability_id"`
	CapabilityName string `json:"capability_name"`
	Title          string `json:"title"`
	Product        string `json:"product,omitempty"`
}

// ExecuteSpecSearch выполняет двухслойный поиск: exact (tsvector+trgm) + semantic (LSA).
func ExecuteSpecSearch(ctx context.Context, db *store.DB, query string, product string, level string, layer string, limit int) (*SpecSearchResult, error) {
	query = strings.TrimSpace(query)
	if query == "" {
		return nil, errs.ErrSpecSearchEmpty
	}
	if limit <= 0 {
		limit = 100
	}
	layer = strings.ToLower(strings.TrimSpace(layer))
	if layer == "" {
		layer = "both"
	}
	if layer != "exact" && layer != "semantic" && layer != "both" {
		return nil, fmt.Errorf("unsupported spec search layer %q", layer)
	}

	result := &SpecSearchResult{
		Exact:    make([]SpecSearchHit, 0),
		Semantic: make([]SpecSearchHit, 0),
	}

	if layer == "exact" || layer == "both" {
		// Слой 1: exact — tsvector по capabilities
		exact, err := searchSpecExact(ctx, db, query, product, level, limit)
		if err != nil {
			return nil, fmt.Errorf("spec search exact: %w", err)
		}
		result.Exact = exact

		// Слой 1b: trgm по техименам в capabilities
		trgm, err := searchSpecTrgm(ctx, db, query, product, level, limit)
		if err != nil {
			return nil, fmt.Errorf("spec search trgm: %w", err)
		}
		result.Exact = mergeSpecSearchHits(append(result.Exact, trgm...), limit)
	}

	// Слой 2: semantic — LSA через spec_embeddings
	if (layer == "semantic" || layer == "both") && (level == "" || strings.EqualFold(level, "capability")) {
		semantic, err := searchSpecSemantic(ctx, db, query, product, limit)
		if err == nil {
			result.Semantic = semantic
		}
	}

	return result, nil
}

// ExecuteSpecByCode находит спеки, ссылающиеся на код-сущность по имени.
func ExecuteSpecByCode(ctx context.Context, db *store.DB, name string, limit int) (*SpecByCodeResult, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return nil, errs.ErrSpecSearchEmpty
	}
	if limit <= 0 {
		limit = 100
	}

	result := &SpecByCodeResult{
		MentionName: name,
		Hits:        make([]SpecByCodeHit, 0),
	}

	rows, err := db.QueryContext(ctx, `
		SELECT m.source_type, m.source_id, m.mention_kind, m.line_number,
		       cap.id, cap.capability_name, cap.title, COALESCE(dp.product_name, ''),
		       COALESCE(c.line_start, req.line_start, s.line_start, uc.line_start, 0),
		       COALESCE(c.line_end, req.line_end, s.line_end, uc.line_end, 0),
		       COALESCE(
		           CONCAT_WS(' ', c.title, c.purpose, c.notes, c.related_code),
		           req.body_text,
		           CONCAT_WS(' ', s.given_text, s.when_text, s.then_text),
		           CONCAT_WS(' ', uc.description, uc.actors, uc.preconditions, uc.postconditions)
		       )
		FROM spec_code_mentions m
		LEFT JOIN spec_capabilities c ON m.source_type = 'spec_capability' AND c.id = m.source_id
		LEFT JOIN spec_requirements req ON m.source_type = 'spec_requirement' AND req.id = m.source_id
		LEFT JOIN spec_scenarios s ON m.source_type = 'spec_scenario' AND s.id = m.source_id
		LEFT JOIN spec_requirements req_s ON s.requirement_id = req_s.id
		LEFT JOIN spec_usecases uc ON m.source_type = 'spec_usecase' AND uc.id = m.source_id
		LEFT JOIN relations ui ON m.source_type = 'spec_usecase'
		     AND ui.relation_type = 'usecase_involves' AND ui.source_id = uc.id
		     AND ui.target_type = 'spec_capability'
		LEFT JOIN spec_capabilities cap ON cap.id = COALESCE(
		    c.id, req.capability_id, req_s.capability_id, ui.target_id
		)
		LEFT JOIN ds_products dp ON dp.id = cap.ds_product_id
		WHERE LOWER(m.mention_name) = LOWER($1) AND cap.id IS NOT NULL
		ORDER BY cap.capability_name, m.line_number
		LIMIT $2`, name, limit)
	if err != nil {
		return nil, fmt.Errorf("spec by-code query: %w", err)
	}
	defer rows.Close()

	seen := map[string]struct{}{}
	for rows.Next() {
		var hit SpecByCodeHit
		var mentionKind string
		if err := rows.Scan(&hit.SourceType, &hit.SourceID, &mentionKind, &hit.LineNumber,
			&hit.CapabilityID, &hit.CapabilityName, &hit.Title, &hit.Product,
			&hit.LineStart, &hit.LineEnd, &hit.Snippet); err != nil {
			return nil, fmt.Errorf("spec by-code scan: %w", err)
		}
		key := fmt.Sprintf("%s|%d|%d", hit.SourceType, hit.SourceID, hit.LineNumber)
		if _, exists := seen[key]; !exists {
			seen[key] = struct{}{}
			result.Hits = append(result.Hits, hit)
		}
		result.Resolved = true
		if result.MentionKind == "" || mentionKind == "api" {
			result.MentionKind = mentionKind
		}
	}

	return result, rows.Err()
}

// ExecuteSpecDeps строит дерево зависимостей capability.
func ExecuteSpecDeps(ctx context.Context, db *store.DB, capabilityName string, direction string, maxDepth int, product ...string) (*SpecDepsResult, error) {
	capabilityName = strings.TrimSpace(capabilityName)
	if capabilityName == "" {
		return nil, errs.ErrSpecSearchEmpty
	}
	if maxDepth <= 0 {
		maxDepth = 2
	}
	direction = strings.ToLower(strings.TrimSpace(direction))
	if direction == "" || direction == "outgoing" {
		direction = "depends_on"
	}
	if direction == "incoming" {
		direction = "depended_by"
	}
	if direction != "depends_on" && direction != "depended_by" {
		direction = "depends_on"
	}
	productFilter := ""
	if len(product) > 0 {
		productFilter = strings.TrimSpace(product[0])
	}

	var capID int64
	var actualName, title, productName string
	err := db.QueryRowContext(ctx, `
		SELECT c.id, c.capability_name, c.title, COALESCE(dp.product_name, '')
		FROM spec_capabilities c
		LEFT JOIN ds_products dp ON dp.id = c.ds_product_id
		WHERE LOWER(c.capability_name) = LOWER($1)
		  AND ($2 = '' OR dp.product_name = $2)
		LIMIT 1`, capabilityName, productFilter).Scan(&capID, &actualName, &title, &productName)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, errs.ErrSpecNotFound
		}
		return nil, fmt.Errorf("spec deps lookup: %w", err)
	}

	result := &SpecDepsResult{
		CapabilityID:   capID,
		CapabilityName: actualName,
		Title:          title,
		Product:        productName,
		Dependencies:   make([]SpecDepNode, 0),
	}

	visited := map[int64]bool{capID: true}
	deps, err := buildDepTree(ctx, db, capID, direction, productFilter, 1, maxDepth, visited)
	if err != nil {
		return nil, err
	}
	result.Dependencies = deps

	return result, nil
}

func buildDepTree(ctx context.Context, db *store.DB, capID int64, direction, product string, depth, maxDepth int, visited map[int64]bool) ([]SpecDepNode, error) {
	if depth > maxDepth {
		return nil, nil
	}

	var rows *sql.Rows
	var err error
	if direction == "depended_by" {
		rows, err = db.QueryContext(ctx, `
			SELECT r.source_id, c.capability_name, c.title, COALESCE(dp.product_name, ''),
			       r.confidence, r.line_number
			FROM relations r
			JOIN spec_capabilities c ON c.id = r.source_id
			LEFT JOIN ds_products dp ON dp.id = c.ds_product_id
			WHERE r.relation_type = 'depends_on_capability'
			  AND r.source_type = 'spec_capability' AND r.target_type = 'spec_capability'
			  AND r.target_id = $1 AND ($2 = '' OR dp.product_name = $2)
			ORDER BY c.capability_name`, capID, product)
	} else {
		rows, err = db.QueryContext(ctx, `
			SELECT r.target_id, c.capability_name, c.title, COALESCE(dp.product_name, ''),
			       r.confidence, r.line_number
			FROM relations r
			JOIN spec_capabilities c ON c.id = r.target_id
			LEFT JOIN ds_products dp ON dp.id = c.ds_product_id
			WHERE r.relation_type = 'depends_on_capability'
			  AND r.source_type = 'spec_capability' AND r.target_type = 'spec_capability'
			  AND r.source_id = $1 AND ($2 = '' OR dp.product_name = $2)
			ORDER BY c.capability_name`, capID, product)
	}
	if err != nil {
		return nil, fmt.Errorf("buildDepTree query: %w", err)
	}
	defer rows.Close()

	var nodes []SpecDepNode
	for rows.Next() {
		var node SpecDepNode
		var conf sql.NullString
		if err := rows.Scan(&node.CapabilityID, &node.CapabilityName, &node.Title,
			&node.Product, &conf, &node.LineNumber); err != nil {
			return nil, fmt.Errorf("buildDepTree scan: %w", err)
		}
		node.Depth = depth
		if conf.Valid {
			node.Confidence = conf.String
		}
		if !visited[node.CapabilityID] {
			visited[node.CapabilityID] = true
			children, err := buildDepTree(ctx, db, node.CapabilityID, direction, product, depth+1, maxDepth, visited)
			delete(visited, node.CapabilityID)
			if err != nil {
				return nil, err
			}
			node.Children = children
		}
		nodes = append(nodes, node)
	}

	return nodes, rows.Err()
}

// ExecuteSpecUsecase возвращает usecase со шагами по имени.
func ExecuteSpecUsecase(ctx context.Context, db *store.DB, usecaseName string) (*SpecUsecaseResult, error) {
	usecaseName = strings.TrimSpace(usecaseName)
	if usecaseName == "" {
		return nil, errs.ErrSpecSearchEmpty
	}

	var uc SpecUsecaseResult
	err := db.QueryRowContext(ctx, `
		SELECT id, usecase_name, title, COALESCE(description, ''), COALESCE(actors, ''),
		       COALESCE(preconditions, ''), COALESCE(postconditions, ''),
		       source_dir, usecase_kind
		FROM spec_usecases
		WHERE usecase_name = $1 OR LOWER(usecase_name) = LOWER($1)
		LIMIT 1`, usecaseName).Scan(
		&uc.ID, &uc.UsecaseName, &uc.Title, &uc.Description, &uc.Actors,
		&uc.Preconditions, &uc.Postconditions, &uc.SourceDir, &uc.UsecaseKind)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, errs.ErrSpecNotFound
		}
		return nil, fmt.Errorf("spec usecase lookup: %w", err)
	}

	// Шаги
	rows, err := db.QueryContext(ctx, `
		SELECT flow_kind, step_order, step_text
		FROM spec_usecase_steps
		WHERE usecase_id = $1
		ORDER BY flow_kind, step_order`, uc.ID)
	if err != nil {
		return nil, fmt.Errorf("spec usecase steps: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var step SpecUsecaseStep
		if err := rows.Scan(&step.FlowKind, &step.StepOrder, &step.StepText); err != nil {
			return nil, fmt.Errorf("spec usecase step scan: %w", err)
		}
		uc.Steps = append(uc.Steps, step)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}

	capRows, err := db.QueryContext(ctx, `
		SELECT c.id, c.capability_name, c.title, COALESCE(dp.product_name, '')
		FROM relations r
		JOIN spec_capabilities c ON c.id = r.target_id
		LEFT JOIN ds_products dp ON dp.id = c.ds_product_id
		WHERE r.relation_type = 'usecase_involves'
		  AND r.source_type = 'spec_usecase' AND r.source_id = $1
		  AND r.target_type = 'spec_capability'
		ORDER BY c.capability_name`, uc.ID)
	if err != nil {
		return nil, fmt.Errorf("spec usecase capabilities: %w", err)
	}
	defer capRows.Close()
	for capRows.Next() {
		var capability SpecUsecaseCapability
		if err := capRows.Scan(&capability.ID, &capability.CapabilityName,
			&capability.Title, &capability.Product); err != nil {
			return nil, fmt.Errorf("spec usecase capability scan: %w", err)
		}
		uc.Capabilities = append(uc.Capabilities, capability)
	}

	return &uc, capRows.Err()
}

// ExecuteSpecCoverage возвращает покрытие capability (сохранённые метрики или gaps).
func ExecuteSpecCoverage(ctx context.Context, db *store.DB, capabilityName string, mode string, product ...string) (*SpecCoverageResult, error) {
	capabilityName = strings.TrimSpace(capabilityName)
	if capabilityName == "" {
		return nil, errs.ErrSpecSearchEmpty
	}
	mode, err := normalizeCoverageMode(mode)
	if err != nil {
		return nil, err
	}
	productFilter := ""
	if len(product) > 0 {
		productFilter = strings.TrimSpace(product[0])
	}

	var cov SpecCoverageResult
	var dsProductID sql.NullInt64
	var apiTotal, apiCovered, codeTotal, codeListed sql.NullInt64
	err = db.QueryRowContext(ctx, `
		SELECT c.id, c.capability_name, c.title, c.ds_product_id, COALESCE(dp.product_name, ''),
		       c.api_total, c.api_covered, c.code_total, c.code_listed
		FROM spec_capabilities c
		LEFT JOIN ds_products dp ON dp.id = c.ds_product_id
		WHERE LOWER(c.capability_name) = LOWER($1)
		  AND ($2 = '' OR dp.product_name = $2)
		LIMIT 1`, capabilityName, productFilter).Scan(
		&cov.CapabilityID, &cov.CapabilityName, &cov.Title, &dsProductID, &cov.Product,
		&apiTotal, &apiCovered, &codeTotal, &codeListed)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, errs.ErrSpecNotFound
		}
		return nil, fmt.Errorf("spec coverage lookup: %w", err)
	}

	var computed SpecCoverageResult
	if !apiTotal.Valid || !apiCovered.Valid || !codeTotal.Valid || !codeListed.Valid {
		computed, err = computeSpecCoverage(ctx, db, cov.CapabilityID, dsProductID)
		if err != nil {
			return nil, err
		}
	}
	cov.ApiTotal = coverageMetric(apiTotal, computed.ApiTotal)
	cov.ApiCovered = coverageMetric(apiCovered, computed.ApiCovered)
	cov.CodeTotal = coverageMetric(codeTotal, computed.CodeTotal)
	cov.CodeListed = coverageMetric(codeListed, computed.CodeListed)

	if mode == "gaps" {
		rows, err := db.QueryContext(ctx, `
			WITH capability_sources AS (
				SELECT 'spec_capability'::text AS source_type, $1::bigint AS source_id
				UNION ALL
				SELECT 'spec_requirement', req.id
				FROM spec_requirements req
				WHERE req.capability_id = $1
				UNION ALL
				SELECT 'spec_scenario', s.id
				FROM spec_scenarios s
				JOIN spec_requirements req ON req.id = s.requirement_id
				WHERE req.capability_id = $1
			), resolved_mentions AS (
				SELECT r.source_type, r.source_id, 'procedure'::text AS mention_kind, LOWER(p.proc_name) AS mention_name
				FROM relations r JOIN sql_procedures p ON r.target_type = 'sql_procedure' AND p.id = r.target_id
				WHERE r.relation_type = 'references_code'
				UNION ALL
				SELECT r.source_type, r.source_id, 'table', LOWER(t.table_name)
				FROM relations r JOIN sql_tables t ON r.target_type = 'sql_table' AND t.id = r.target_id
				WHERE r.relation_type = 'references_code'
				UNION ALL
				SELECT r.source_type, r.source_id, 'form', LOWER(f.form_name)
				FROM relations r JOIN dfm_forms f ON r.target_type = 'dfm_form' AND f.id = r.target_id
				WHERE r.relation_type = 'references_code'
				UNION ALL
				SELECT r.source_type, r.source_id, 'smf', LOWER(s.instrument_name)
				FROM relations r JOIN smf_instruments s ON r.target_type = 'smf_instrument' AND s.id = r.target_id
				WHERE r.relation_type = 'references_code'
				UNION ALL
				SELECT r.source_type, r.source_id, 'method', LOWER(m.method_name)
				FROM relations r JOIN pas_methods m ON r.target_type = 'pas_method' AND m.id = r.target_id
				WHERE r.relation_type = 'references_code'
				UNION ALL
				SELECT r.source_type, r.source_id, 'api', LOWER(a.contract_name)
				FROM relations r JOIN api_contracts a ON r.target_type = 'api_contract' AND a.id = r.target_id
				WHERE r.relation_type = 'references_code'
				UNION ALL
				SELECT r.source_type, r.source_id, 'js_function', LOWER(j.function_name)
				FROM relations r JOIN js_functions j ON r.target_type = 'js_function' AND j.id = r.target_id
				WHERE r.relation_type = 'references_code'
				UNION ALL
				SELECT r.source_type, r.source_id, 'report_form', LOWER(rf.report_name)
				FROM relations r JOIN report_forms rf ON r.target_type = 'report_form' AND rf.id = r.target_id
				WHERE r.relation_type = 'references_code'
			)
			SELECT DISTINCT m.mention_name, m.mention_kind, m.source_type, m.line_number
			FROM spec_code_mentions m
			JOIN capability_sources src ON src.source_type = m.source_type AND src.source_id = m.source_id
			WHERE NOT EXISTS (
				SELECT 1
				FROM resolved_mentions resolved
				WHERE resolved.source_type = m.source_type
				  AND resolved.source_id = m.source_id
				  AND resolved.mention_kind = m.mention_kind
				  AND resolved.mention_name = LOWER(m.mention_name)
			)
			ORDER BY m.line_number, m.source_type, m.mention_name`, cov.CapabilityID)
		if err != nil {
			return nil, fmt.Errorf("spec coverage gaps: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var gap SpecCoverageGap
			if err := rows.Scan(&gap.MentionName, &gap.MentionKind, &gap.SourceType, &gap.LineNumber); err != nil {
				return nil, fmt.Errorf("spec coverage gap scan: %w", err)
			}
			cov.Gaps = append(cov.Gaps, gap)
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("spec coverage gaps: %w", err)
		}
	}

	return &cov, nil
}

func computeSpecCoverage(ctx context.Context, db *store.DB, capabilityID int64, dsProductID sql.NullInt64) (SpecCoverageResult, error) {
	var computed SpecCoverageResult
	if !dsProductID.Valid {
		return computed, nil
	}
	err := db.QueryRowContext(ctx, `
		WITH product_entities AS (
			SELECT 'sql_procedure'::text AS target_type, p.id AS target_id, false AS is_api
			FROM sql_procedures p JOIN files f ON f.id = p.file_id WHERE f.ds_product_id = $2
			UNION ALL
			SELECT 'sql_table', t.id, false FROM sql_tables t JOIN files f ON f.id = t.file_id WHERE f.ds_product_id = $2
			UNION ALL
			SELECT 'dfm_form', d.id, false FROM dfm_forms d JOIN files f ON f.id = d.file_id WHERE f.ds_product_id = $2
			UNION ALL
			SELECT 'smf_instrument', s.id, false FROM smf_instruments s JOIN files f ON f.id = s.file_id WHERE f.ds_product_id = $2
			UNION ALL
			SELECT 'pas_method', m.id, false FROM pas_methods m JOIN pas_units u ON u.id = m.unit_id JOIN files f ON f.id = u.file_id WHERE f.ds_product_id = $2
			UNION ALL
			SELECT 'js_function', j.id, false FROM js_functions j JOIN files f ON f.id = j.file_id WHERE f.ds_product_id = $2
			UNION ALL
			SELECT 'report_form', rf.id, false FROM report_forms rf JOIN files f ON f.id = rf.file_id WHERE f.ds_product_id = $2
			UNION ALL
			SELECT 'api_contract', a.id, true FROM api_contracts a JOIN files f ON f.id = a.file_id WHERE f.ds_product_id = $2
		), capability_sources AS (
			SELECT 'spec_capability'::text AS source_type, $1::bigint AS source_id
			UNION ALL SELECT 'spec_requirement', req.id FROM spec_requirements req WHERE req.capability_id = $1
			UNION ALL
			SELECT 'spec_scenario', s.id FROM spec_scenarios s
			JOIN spec_requirements req ON req.id = s.requirement_id WHERE req.capability_id = $1
		), covered_entities AS (
			SELECT DISTINCT pe.target_type, pe.target_id, pe.is_api
			FROM capability_sources src
			JOIN relations r ON r.source_type = src.source_type AND r.source_id = src.source_id
			JOIN product_entities pe ON pe.target_type = r.target_type AND pe.target_id = r.target_id
			WHERE r.relation_type = 'references_code'
		)
		SELECT
			COUNT(*) FILTER (WHERE is_api),
			(SELECT COUNT(*) FROM covered_entities WHERE is_api),
			COUNT(*) FILTER (WHERE NOT is_api),
			(SELECT COUNT(*) FROM covered_entities WHERE NOT is_api)
		FROM product_entities`, capabilityID, dsProductID.Int64).Scan(
		&computed.ApiTotal, &computed.ApiCovered, &computed.CodeTotal, &computed.CodeListed)
	if err != nil {
		return computed, fmt.Errorf("spec coverage compute: %w", err)
	}
	return computed, nil
}

func coverageMetric(saved sql.NullInt64, computed int) int {
	if saved.Valid {
		return int(saved.Int64)
	}
	return computed
}

func normalizeCoverageMode(mode string) (string, error) {
	mode = strings.ToLower(strings.TrimSpace(mode))
	if mode == "" {
		return "saved", nil
	}
	if mode != "saved" && mode != "gaps" {
		return "", fmt.Errorf("unsupported spec coverage mode %q", mode)
	}
	return mode, nil
}

// ExecuteSpecHistory возвращает историю изменений capability через changes.
func ExecuteSpecHistory(ctx context.Context, db *store.DB, capabilityName string, changeName ...string) (*SpecHistoryResult, error) {
	if len(changeName) == 0 && strings.TrimSpace(capabilityName) == "" {
		return nil, errs.ErrSpecSearchEmpty
	}
	change := ""
	productFilter := ""
	if len(changeName) > 0 {
		change = changeName[0]
	}
	if len(changeName) > 1 {
		productFilter = strings.TrimSpace(changeName[1])
	}
	capabilityName, change, err := normalizeHistorySelectors(capabilityName, change)
	if err != nil {
		return nil, err
	}
	if change != "" {
		return executeSpecHistoryByChange(ctx, db, change, productFilter)
	}

	result := &SpecHistoryResult{
		CapabilityName: capabilityName,
		Changes:        make([]SpecHistoryEntry, 0),
	}
	rows, err := db.QueryContext(ctx, `
		SELECT DISTINCT c.capability_name, COALESCE(dp.product_name, ''), sc.change_name, sc.status,
		       COALESCE(d.section, ''), COALESCE(d.requirement_name, ''), COALESCE(d.body_text, ''),
		       COALESCE(r.confidence, ''), COALESCE(d.line_start, 0), f.modified_at
		FROM relations r
		JOIN spec_changes sc ON r.source_type = 'spec_change' AND sc.id = r.source_id
		JOIN files f ON f.id = sc.file_id
		JOIN spec_capabilities c ON r.target_type = 'spec_capability' AND c.id = r.target_id
		LEFT JOIN ds_products dp ON dp.id = c.ds_product_id
		LEFT JOIN spec_change_delta d ON d.change_id = sc.id AND LOWER(d.capability_slug) = LOWER(c.capability_name)
		WHERE r.relation_type = 'change_modifies'
		  AND LOWER(c.capability_name) = LOWER($1)
		  AND ($2 = '' OR dp.product_name = $2)
		ORDER BY f.modified_at, sc.change_name, COALESCE(d.section, ''), COALESCE(d.line_start, 0)`, capabilityName, productFilter)
	if err != nil {
		return nil, fmt.Errorf("spec history query: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var entry SpecHistoryEntry
		var lineStart int
		var modifiedAt interface{}
		if err := rows.Scan(&result.CapabilityName, &result.Product, &entry.ChangeName, &entry.Status, &entry.Section,
			&entry.ReqName, &entry.BodyText, &entry.Source, &lineStart, &modifiedAt); err != nil {
			return nil, fmt.Errorf("spec history scan: %w", err)
		}
		result.Changes = append(result.Changes, entry)
	}
	return result, rows.Err()
}

func executeSpecHistoryByChange(ctx context.Context, db *store.DB, changeName, product string) (*SpecHistoryResult, error) {
	result := &SpecHistoryResult{
		ChangeName:   changeName,
		Product:      product,
		Capabilities: make([]SpecHistoryCapability, 0),
	}
	rows, err := db.QueryContext(ctx, `
		SELECT DISTINCT sc.change_name, sc.status, c.id, c.capability_name, c.title, COALESCE(dp.product_name, '')
		FROM relations r
		JOIN spec_changes sc ON r.source_type = 'spec_change' AND sc.id = r.source_id
		JOIN spec_capabilities c ON r.target_type = 'spec_capability' AND c.id = r.target_id
		LEFT JOIN ds_products dp ON dp.id = c.ds_product_id
		WHERE r.relation_type = 'change_modifies'
		  AND LOWER(sc.change_name) = LOWER($1)
		  AND ($2 = '' OR dp.product_name = $2)
		ORDER BY c.capability_name, c.id`, changeName, product)
	if err != nil {
		return nil, fmt.Errorf("spec history by change query: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var capability SpecHistoryCapability
		if err := rows.Scan(&result.ChangeName, &result.Status, &capability.CapabilityID,
			&capability.CapabilityName, &capability.Title, &capability.Product); err != nil {
			return nil, fmt.Errorf("spec history by change scan: %w", err)
		}
		result.Capabilities = append(result.Capabilities, capability)
	}
	return result, rows.Err()
}

func normalizeHistorySelectors(capabilityName, changeName string) (string, string, error) {
	capabilityName = strings.TrimSpace(capabilityName)
	changeName = strings.TrimSpace(changeName)
	if capabilityName == "" && changeName == "" {
		return "", "", fmt.Errorf("one of history selectors name or change is required")
	}
	if capabilityName != "" && changeName != "" {
		return "", "", fmt.Errorf("history selectors name and change are mutually exclusive")
	}
	return capabilityName, changeName, nil
}

// searchSpecExact — tsvector поиск по сущностям спек.
func searchSpecExact(ctx context.Context, db *store.DB, query, product, level string, limit int) ([]SpecSearchHit, error) {
	return searchSpecArtifacts(ctx, db, query, product, level, limit, false)
}

// searchSpecTrgm — trgm поиск по техименам.
func searchSpecTrgm(ctx context.Context, db *store.DB, query, product, level string, limit int) ([]SpecSearchHit, error) {
	return searchSpecArtifacts(ctx, db, query, product, level, limit, true)
}

func searchSpecArtifacts(ctx context.Context, db *store.DB, query, product, level string, limit int, trgm bool) ([]SpecSearchHit, error) {
	rankExpr := "ts_rank(a.search_vector, plainto_tsquery('russian', $1))"
	matchExpr := "a.search_vector @@ plainto_tsquery('russian', $1)"
	source := "tsvector"
	if trgm {
		rankExpr = "similarity(a.search_text, $1)"
		matchExpr = "similarity(a.search_text, $1) > 0.3"
		source = "trgm"
	}
	statement := fmt.Sprintf(`
		WITH artifacts AS (
			SELECT c.id AS entity_id, c.id AS capability_id, c.capability_name,
			       'capability'::text AS level, c.title, COALESCE(c.purpose, '') AS purpose,
			       c.line_start, c.line_end,
			       CONCAT_WS(' ', c.title, c.purpose, c.notes, c.related_code) AS search_text,
			       CONCAT_WS(' ', c.title, c.purpose, c.notes) AS snippet,
			       c.search_vector, c.ds_product_id
			FROM spec_capabilities c
			UNION ALL
			SELECT req.id, c.id, c.capability_name, 'requirement', req.requirement_name, '',
			       req.line_start, req.line_end, CONCAT_WS(' ', req.requirement_name, req.body_text),
			       req.body_text, req.search_vector, c.ds_product_id
			FROM spec_requirements req JOIN spec_capabilities c ON c.id = req.capability_id
			UNION ALL
			SELECT s.id, c.id, c.capability_name, 'scenario', s.scenario_name, '',
			       s.line_start, s.line_end,
			       CONCAT_WS(' ', s.scenario_name, s.given_text, s.when_text, s.then_text),
			       CONCAT_WS(' ', s.given_text, s.when_text, s.then_text), s.search_vector, c.ds_product_id
			FROM spec_scenarios s
			JOIN spec_requirements req ON req.id = s.requirement_id
			JOIN spec_capabilities c ON c.id = req.capability_id
			UNION ALL
			SELECT uc.id, 0, uc.usecase_name, 'usecase', uc.title, '',
			       uc.line_start, uc.line_end,
			       CONCAT_WS(' ', uc.usecase_name, uc.title, uc.description, uc.actors,
			                     uc.preconditions, uc.postconditions, uc.business_value,
			                     uc.architecture, uc.data_schema),
			       CONCAT_WS(' ', uc.description, uc.actors, uc.preconditions, uc.postconditions),
			       uc.search_vector, cfg.ds_product_id
			FROM spec_usecases uc JOIN spec_configs cfg ON cfg.id = uc.spec_config_id
		)
		SELECT a.capability_id, a.capability_name, a.entity_id, a.level, a.title, a.purpose,
		       a.line_start, a.line_end, a.snippet, %s AS rank, COALESCE(dp.product_name, '')
		FROM artifacts a
		LEFT JOIN ds_products dp ON dp.id = a.ds_product_id
		WHERE %s AND ($2 = '' OR dp.product_name = $2)
		  AND ($3 = '' OR LOWER(a.level) = LOWER($3))
		ORDER BY rank DESC, a.level, a.entity_id
		LIMIT $4`, rankExpr, matchExpr)
	rows, err := db.QueryContext(ctx, statement, query, product, level, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var hits []SpecSearchHit
	for rows.Next() {
		var hit SpecSearchHit
		if err := rows.Scan(&hit.CapabilityID, &hit.CapabilityName, &hit.EntityID, &hit.Level,
			&hit.Title, &hit.Purpose, &hit.LineStart, &hit.LineEnd, &hit.Snippet,
			&hit.Rank, &hit.Product); err != nil {
			return nil, err
		}
		hit.Source = source
		hits = append(hits, hit)
	}
	return hits, rows.Err()
}

// searchSpecSemantic — LSA поиск через spec_embeddings.
func searchSpecSemantic(ctx context.Context, db *store.DB, query, product string, limit int) ([]SpecSearchHit, error) {
	modelPath := filepath.Join(filepath.Dir(config.GetConfigFile()), "spec_lsa_model.bin")
	if cfg := config.Get(); cfg != nil && cfg.Spec.LSAModelPath != "" {
		modelPath = cfg.Spec.LSAModelPath
		if !filepath.IsAbs(modelPath) {
			modelPath = filepath.Join(filepath.Dir(config.GetConfigFile()), modelPath)
		}
	}
	model, err := specfts.LoadLSAModel(modelPath)
	if err != nil || model.Vocab == nil || model.VT == nil {
		return nil, errs.ErrSpecModelNotFound
	}
	queryVec := model.Vocab.ProjectQuery(query, model.VT)
	if len(queryVec) == 0 {
		return nil, errs.ErrSpecModelNotFound
	}

	rows, err := db.QueryContext(ctx, `
		SELECT se.spec_id, se.embedding, sc.capability_name, sc.title,
		       COALESCE(sc.purpose, ''), sc.line_start, sc.line_end, se.embed_text,
		       COALESCE(dp.product_name, '')
		FROM spec_embeddings se
		JOIN spec_capabilities sc ON se.spec_id = sc.id
		LEFT JOIN ds_products dp ON dp.id = sc.ds_product_id
		WHERE se.embed_level = 'spec' AND ($1 = '' OR dp.product_name = $1)
		ORDER BY se.spec_id`, product)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	hits := make([]SpecSearchHit, 0)
	for rows.Next() {
		var hit SpecSearchHit
		var embArr string
		if err := rows.Scan(&hit.CapabilityID, &embArr, &hit.CapabilityName, &hit.Title,
			&hit.Purpose, &hit.LineStart, &hit.LineEnd, &hit.Snippet, &hit.Product); err != nil {
			return nil, err
		}
		emb := parsePGFloatArray(embArr)
		if len(emb) != len(queryVec) {
			continue
		}
		hit.Rank = specfts.CosineSimilarity(queryVec, emb)
		if hit.Rank <= 0.01 {
			continue
		}
		hit.EntityID = hit.CapabilityID
		hit.Level = "capability"
		hit.Source = "lsa"
		hits = append(hits, hit)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	for i := 0; i < len(hits); i++ {
		for j := i + 1; j < len(hits); j++ {
			if hits[j].Rank > hits[i].Rank {
				hits[i], hits[j] = hits[j], hits[i]
			}
		}
	}
	if len(hits) > limit {
		hits = hits[:limit]
	}
	return hits, nil
}
