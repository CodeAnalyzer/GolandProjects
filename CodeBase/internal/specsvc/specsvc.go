package specsvc

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"

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
	File           string `json:"file"`
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

// SpecCoverageResult — перечень покрытых код-сущностей, сгруппированных по capability.
type SpecCoverageResult struct {
	Product      string                   `json:"product"`
	Capabilities []SpecCoverageCapability `json:"capabilities"`
}

// SpecCoverageCapability — capability с перечнем покрытых сущностей.
type SpecCoverageCapability struct {
	CapabilityName string               `json:"capability_name"`
	Title          string               `json:"title"`
	Covered        []SpecCoverageEntity `json:"covered"`
}

// SpecCoverageEntity — покрытая код-сущность.
type SpecCoverageEntity struct {
	Name string `json:"name"`
	Kind string `json:"kind"`
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
	// Поля режима по change: имя capability, к которому относится delta-секция,
	// и пометка skip_specs (связь извлечена из proposal, delta-требований нет).
	CapabilityName string `json:"capability_name"`
	SkipSpecs      bool   `json:"skip_specs,omitempty"`
	DeltaSource    string `json:"delta_source,omitempty"` // delta | proposal
}

type SpecHistoryCapability struct {
	CapabilityID   int64  `json:"capability_id"`
	CapabilityName string `json:"capability_name"`
	Title          string `json:"title"`
	Product        string `json:"product,omitempty"`
}

// SpecConfigResult — профиль продукта, статистика и опциональная иерархия capabilities.
type SpecConfigResult struct {
	Profile   SpecConfigProfile `json:"profile"`
	Stats     SpecConfigStats   `json:"stats"`
	Hierarchy []SpecConfigNode  `json:"hierarchy,omitempty"`
}

type SpecConfigProfile struct {
	ProductName     string `json:"product_name"`
	SchemaName      string `json:"schema_name"`
	RootDir         string `json:"root_dir"`
	UsecaseLayout   string `json:"usecase_layout"`
	IDStyle         string `json:"id_style"`
	CrossRefStyle   string `json:"cross_ref_style"`
	NormativeLang   string `json:"normative_lang"`
	Traceability    string `json:"traceability"`
	HasChanges      bool   `json:"has_changes"`
	HasAudit        bool   `json:"has_audit"`
	HasADR          bool   `json:"has_adr"`
	CoverageMetrics bool   `json:"coverage_metrics"`
	ContextText     string `json:"context_text"`
}

type SpecConfigStats struct {
	Capabilities int `json:"capabilities"`
	Requirements int `json:"requirements"`
	Scenarios    int `json:"scenarios"`
	Usecases     int `json:"usecases"`
	Changes      int `json:"changes"`
}

type SpecConfigNode struct {
	CapabilityName string           `json:"capability_name"`
	Title          string           `json:"title"`
	IsContainer    bool             `json:"is_container"`
	ChildrenCount  int              `json:"children_count"`
	Children       []SpecConfigNode `json:"children,omitempty"`
}

// ExecuteSpecConfig возвращает профиль продукта, статистику и опциональную иерархию capabilities.
func ExecuteSpecConfig(ctx context.Context, db *store.DB, product string, includeHierarchy bool, depth int) (*SpecConfigResult, error) {
	product = strings.TrimSpace(product)
	if product == "" {
		return nil, errs.ErrSpecSearchEmpty
	}
	if depth <= 0 {
		depth = 2
	}

	profileRow, err := db.LoadSpecConfigProfileByProduct(ctx, product)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, errs.ErrSpecNotFound
		}
		return nil, fmt.Errorf("spec config profile: %w", err)
	}

	statsRow, err := db.LoadSpecConfigStats(ctx, profileRow.ID)
	if err != nil {
		return nil, fmt.Errorf("spec config stats: %w", err)
	}

	result := &SpecConfigResult{
		Profile: SpecConfigProfile{
			ProductName:     profileRow.ProductName,
			SchemaName:      profileRow.SchemaName,
			RootDir:         profileRow.RootDir,
			UsecaseLayout:   profileRow.UsecaseLayout,
			IDStyle:         profileRow.IDStyle,
			CrossRefStyle:   profileRow.CrossRefStyle,
			NormativeLang:   profileRow.NormativeLang,
			Traceability:    profileRow.Traceability,
			HasChanges:      profileRow.HasChanges,
			HasAudit:        profileRow.HasAudit,
			HasADR:          profileRow.HasADR,
			CoverageMetrics: profileRow.CoverageMetrics,
			ContextText:     profileRow.ContextText,
		},
		Stats: SpecConfigStats{
			Capabilities: statsRow.Capabilities,
			Requirements: statsRow.Requirements,
			Scenarios:    statsRow.Scenarios,
			Usecases:     statsRow.Usecases,
			Changes:      statsRow.Changes,
		},
	}

	if !includeHierarchy {
		return result, nil
	}

	hierarchyRows, err := db.LoadSpecConfigHierarchy(ctx, profileRow.ID)
	if err != nil {
		return nil, fmt.Errorf("spec config hierarchy: %w", err)
	}

	result.Hierarchy = buildSpecConfigHierarchy(hierarchyRows, depth)
	return result, nil
}

// buildSpecConfigHierarchy строит дерево из плоской выборки с ограничением depth.
// is_container = true если title пустой AND purpose IS NULL AND notes IS NULL.
// children_count — общее количество прямых детей (без учёта depth).
func buildSpecConfigHierarchy(rows []store.SpecConfigHierarchyRow, depth int) []SpecConfigNode {
	byID := make(map[int64]*store.SpecConfigHierarchyRow, len(rows))
	childrenByParent := make(map[int64][]int64)
	var rootIDs []int64
	for i := range rows {
		r := &rows[i]
		byID[r.ID] = r
		if r.ParentID.Valid {
			childrenByParent[r.ParentID.Int64] = append(childrenByParent[r.ParentID.Int64], r.ID)
		} else {
			rootIDs = append(rootIDs, r.ID)
		}
	}

	var build func(id int64, currentDepth int) SpecConfigNode
	build = func(id int64, currentDepth int) SpecConfigNode {
		r := byID[id]
		node := SpecConfigNode{
			CapabilityName: r.CapabilityName,
			Title:          r.Title,
			IsContainer:    r.Title == "" && !r.Purpose.Valid && !r.Notes.Valid,
			ChildrenCount:  len(childrenByParent[id]),
		}
		if currentDepth < depth {
			childIDs := childrenByParent[id]
			node.Children = make([]SpecConfigNode, 0, len(childIDs))
			for _, childID := range childIDs {
				node.Children = append(node.Children, build(childID, currentDepth+1))
			}
		}
		return node
	}

	result := make([]SpecConfigNode, 0, len(rootIDs))
	for _, id := range rootIDs {
		result = append(result, build(id, 1))
	}
	return result
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
		       COALESCE(f.rel_path, ''),
		       COALESCE(c.line_start, req.line_start, s.line_start, uc.line_start, 0),
		       COALESCE(c.line_end, req.line_end, s.line_end, uc.line_end, 0),
		       COALESCE(
		           CONCAT_WS(' ', c.title, c.purpose, c.notes, c.related_code),
		           req.body_text,
		           CONCAT_WS(' ', s.given_text, s.when_text, s.then_text),
		           CONCAT_WS(' ', uc.description, uc.actors, uc.preconditions, uc.postconditions)
		       )
		FROM spec_code_mentions m
		JOIN files f ON f.id = m.file_id
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
			&hit.File, &hit.LineStart, &hit.LineEnd, &hit.Snippet); err != nil {
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

// ExecuteSpecCoverage возвращает перечень покрытых код-сущностей, сгруппированных по capability.
// product — обязательный фильтр продукта; name — опциональный фильтр capability; kind — опциональный фильтр типа сущности.
func ExecuteSpecCoverage(ctx context.Context, db *store.DB, product string, name string, kind string) (*SpecCoverageResult, error) {
	product = strings.TrimSpace(product)
	if product == "" {
		return nil, errs.ErrSpecSearchEmpty
	}
	name = strings.TrimSpace(name)
	kind = strings.TrimSpace(kind)

	// Шаг 1: получить список capability продукта (один лёгкий запрос).
	capQuery := `SELECT c.id, c.capability_name, c.title
		FROM spec_capabilities c
		JOIN ds_products dp ON dp.id = c.ds_product_id
		WHERE dp.product_name = $1`
	capArgs := []interface{}{product}
	if name != "" {
		capQuery += " AND LOWER(c.capability_name) = LOWER($2)"
		capArgs = append(capArgs, name)
	}
	capQuery += " ORDER BY c.capability_name"

	capRows, err := db.QueryContext(ctx, capQuery, capArgs...)
	if err != nil {
		return nil, fmt.Errorf("spec coverage capabilities: %w", err)
	}
	type capInfo struct {
		id    int64
		name  string
		title string
	}
	var capabilities []capInfo
	for capRows.Next() {
		var ci capInfo
		if err := capRows.Scan(&ci.id, &ci.name, &ci.title); err != nil {
			capRows.Close()
			return nil, fmt.Errorf("spec coverage capability scan: %w", err)
		}
		capabilities = append(capabilities, ci)
	}
	capRows.Close()
	if err := capRows.Err(); err != nil {
		return nil, fmt.Errorf("spec coverage capabilities: %w", err)
	}

	if len(capabilities) == 0 {
		return &SpecCoverageResult{Product: product, Capabilities: []SpecCoverageCapability{}}, nil
	}

	// Шаг 2: для каждой capability — параллельный запрос покрытых сущностей.
	// Каждый запрос строит capability_sources только для одной capability (маленький CTE),
	// затем JOIN relations → symbols/smf_instruments.

	// Динамический kind-фильтр: если kind задан, добавляем AND r.target_type = $2.
	kindCond := ""
	if kind != "" {
		kindCond = " AND r.target_type = $2"
	}

	// Финальный SELECT: UNION ALL по конкретным таблицам (PK-индексы работают быстро).
	// symbols не используется — нет индекса на entity_id, JOIN был медленным.
	type entityBranch struct {
		kind       string
		table      string
		nameColumn string
	}
	branches := []entityBranch{
		{"api_contract", "api_contracts", "contract_name"},
		{"sql_procedure", "sql_procedures", "proc_name"},
		{"sql_table", "sql_tables", "table_name"},
		{"dfm_form", "dfm_forms", "form_name"},
		{"smf_instrument", "smf_instruments", "instrument_name"},
		{"pas_method", "pas_methods", "method_name"},
		{"js_function", "js_functions", "function_name"},
		{"report_form", "report_forms", "report_name"},
	}

	// Фильтруем ветки по kind, если задан.
	selected := branches
	if kind != "" {
		selected = nil
		for _, b := range branches {
			if b.kind == kind {
				selected = []entityBranch{b}
				break
			}
		}
		if len(selected) == 0 {
			return &SpecCoverageResult{Product: product, Capabilities: []SpecCoverageCapability{}}, nil
		}
	}

	var selectParts []string
	for _, b := range selected {
		selectParts = append(selectParts, fmt.Sprintf(
			"SELECT e.%s AS entity_name, '%s'::text AS entity_kind\n"+
				"FROM covered c\n"+
				"JOIN %s e ON e.id = c.target_id\n"+
				"WHERE c.target_type = '%s'",
			b.nameColumn, b.kind, b.table, b.kind))
	}
	finalSelect := strings.Join(selectParts, "\nUNION ALL\n")

	perCapQuery := fmt.Sprintf(`
		WITH capability_sources AS (
			SELECT 'spec_capability'::text AS source_type, $1::bigint AS source_id
			UNION ALL
			SELECT 'spec_requirement', req.id FROM spec_requirements req WHERE req.capability_id = $1
			UNION ALL
			SELECT 'spec_scenario', s.id FROM spec_scenarios s
			JOIN spec_requirements req ON req.id = s.requirement_id WHERE req.capability_id = $1
		), covered AS (
			SELECT DISTINCT r.target_type, r.target_id
			FROM capability_sources cs
			JOIN relations r ON r.source_type = cs.source_type AND r.source_id = cs.source_id
			WHERE r.relation_type = 'references_code'%s
		)
		%s
		ORDER BY entity_kind, entity_name`, kindCond, finalSelect)

	type capResult struct {
		ci      capInfo
		covered []SpecCoverageEntity
		err     error
	}
	results := make([]capResult, len(capabilities))

	// Semaphore = размеру пула соединений БД, чтобы не плодить горутины,
	// которые всё равно ждут в очереди пула.
	concurrency := db.Stats().MaxOpenConnections
	if concurrency <= 0 {
		concurrency = runtime.NumCPU()
	}

	var wg sync.WaitGroup
	sem := make(chan struct{}, concurrency)
	for i, ci := range capabilities {
		wg.Add(1)
		sem <- struct{}{}
		go func(idx int, c capInfo) {
			defer wg.Done()
			defer func() { <-sem }()
			args := []interface{}{c.id}
			if kind != "" {
				args = append(args, kind)
			}
			rows, err := db.QueryContext(ctx, perCapQuery, args...)
			if err != nil {
				results[idx] = capResult{ci: c, err: fmt.Errorf("spec coverage query for %s: %w", c.name, err)}
				return
			}
			defer rows.Close()

			seen := map[string]bool{}
			var covered []SpecCoverageEntity
			for rows.Next() {
				var entityName, entityKind string
				if err := rows.Scan(&entityName, &entityKind); err != nil {
					results[idx] = capResult{ci: c, err: fmt.Errorf("spec coverage scan for %s: %w", c.name, err)}
					return
				}
				dedupKey := entityKind + "|" + entityName
				if seen[dedupKey] {
					continue
				}
				seen[dedupKey] = true
				covered = append(covered, SpecCoverageEntity{Name: entityName, Kind: entityKind})
			}
			if err := rows.Err(); err != nil {
				results[idx] = capResult{ci: c, err: fmt.Errorf("spec coverage rows for %s: %w", c.name, err)}
				return
			}
			if covered == nil {
				covered = []SpecCoverageEntity{}
			}
			results[idx] = capResult{ci: c, covered: covered}
		}(i, ci)
	}
	wg.Wait()

	// Шаг 3: агрегация.
	result := &SpecCoverageResult{
		Product:      product,
		Capabilities: make([]SpecCoverageCapability, 0, len(results)),
	}
	for _, r := range results {
		if r.err != nil {
			return nil, r.err
		}
		sort.Slice(r.covered, func(i, j int) bool {
			if r.covered[i].Kind != r.covered[j].Kind {
				return r.covered[i].Kind < r.covered[j].Kind
			}
			return r.covered[i].Name < r.covered[j].Name
		})
		result.Capabilities = append(result.Capabilities, SpecCoverageCapability{
			CapabilityName: r.ci.name,
			Title:          r.ci.title,
			Covered:        r.covered,
		})
	}

	return result, nil
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
		Changes:      make([]SpecHistoryEntry, 0),
	}
	// 1. Summary-список затронутых capability (без delta-join).
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
	for rows.Next() {
		var capability SpecHistoryCapability
		if err := rows.Scan(&result.ChangeName, &result.Status, &capability.CapabilityID,
			&capability.CapabilityName, &capability.Title, &capability.Product); err != nil {
			rows.Close()
			return nil, fmt.Errorf("spec history by change scan: %w", err)
		}
		result.Capabilities = append(result.Capabilities, capability)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("spec history by change rows: %w", err)
	}
	rows.Close()

	// 2. Delta-секции (ADDED/MODIFIED/REMOVED) по каждой capability.
	// LEFT JOIN spec_change_delta: для change со skip_specs (нет delta-файла)
	// возвращается одна строка с NULL delta-полями и confidence = 'proposal'.
	deltaRows, err := db.QueryContext(ctx, `
		SELECT sc.change_name, sc.status, c.capability_name,
		       COALESCE(d.section, ''), COALESCE(d.requirement_name, ''), COALESCE(d.body_text, ''),
		       COALESCE(r.confidence, '')
		FROM relations r
		JOIN spec_changes sc ON r.source_type = 'spec_change' AND sc.id = r.source_id
		JOIN spec_capabilities c ON r.target_type = 'spec_capability' AND c.id = r.target_id
		LEFT JOIN ds_products dp ON dp.id = c.ds_product_id
		LEFT JOIN spec_change_delta d ON d.change_id = sc.id AND LOWER(d.capability_slug) = LOWER(c.capability_name)
		WHERE r.relation_type = 'change_modifies'
		  AND LOWER(sc.change_name) = LOWER($1)
		  AND ($2 = '' OR dp.product_name = $2)
		ORDER BY c.capability_name, c.id, COALESCE(d.section, ''), COALESCE(d.line_start, 0)`, changeName, product)
	if err != nil {
		return nil, fmt.Errorf("spec history by change delta query: %w", err)
	}
	defer deltaRows.Close()

	for deltaRows.Next() {
		var entry SpecHistoryEntry
		var confidence string
		if err := deltaRows.Scan(&entry.ChangeName, &entry.Status, &entry.CapabilityName,
			&entry.Section, &entry.ReqName, &entry.BodyText, &confidence); err != nil {
			return nil, fmt.Errorf("spec history by change delta scan: %w", err)
		}
		// confidence хранит источник связи: 'delta' (из delta-файла) или
		// 'proposal' (из proposal references, change со skip_specs). Для старых
		// данных без confidence и с непустой delta-секцией — default 'delta'.
		entry.DeltaSource = confidence
		if entry.DeltaSource == "" && entry.Section != "" {
			entry.DeltaSource = "delta"
		}
		entry.SkipSpecs = entry.DeltaSource == "proposal"
		result.Changes = append(result.Changes, entry)
	}
	return result, deltaRows.Err()
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
