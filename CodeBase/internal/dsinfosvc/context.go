package dsinfosvc

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/codebase/internal/branchkey"
	"github.com/codebase/internal/store"
)

// EnvDsinfoID — переменная окружения с идентификатором профиля dsinfo для
// команд анализа (приоритет ниже явного флага, выше активного профиля).
const EnvDsinfoID = "CODEBASE_DSINFO_ID"

// Источники контекста анализа.
const (
	AnalysisSourceExplicit = "explicit" // --dsinfo / dsinfo_id
	AnalysisSourceEnv      = "env"      // CODEBASE_DSINFO_ID
	AnalysisSourceActive   = "active"   // dsinfo use
	AnalysisSourceNone     = "none"     // профиль не задан
)

// AnalysisContext — разрешённый контекст профиля для команд анализа:
// идентификатор профиля, источник, сопоставление манифеста с индексом и
// карты для выбора ветки.
//
// Branches — выбранная линия продукта. Если у продукта есть проиндексированные
// строки манифеста, предпочитается default-линия, затем старшая версия
// (11-01-7 старше 9-01-553), затем больший id; если ни одна строка не
// проиндексирована — default-ветка продукта (fallback на код без линии
// манифеста). Requested — все линии манифеста продукта, найденного в индексе
// (product_id → список branch_key), включая отсутствующие: по нему
// вычисляются branch_requested элемента и предупреждение, если выбранная
// линия не входит в запрошенные.
type AnalysisContext struct {
	ProfileID   int64            `json:"profile_id"`
	ProfileName string           `json:"profile_name,omitempty"`
	Source      string           `json:"source"`
	Matching    *MatchingResult  `json:"matching,omitempty"`
	Branches    map[int64]int64  `json:"-"`
	Requested   map[int64][]string `json:"-"`
}

// ResolveAnalysisContext разрешает контекст профиля по цепочке:
// явный id > CODEBASE_DSINFO_ID > активный профиль > отсутствие профиля.
// Несуществующий id из любого источника и некорректный формат env —
// ошибка (без молчаливого игнорирования).
func ResolveAnalysisContext(ctx context.Context, db *store.DB, explicitID int64) (*AnalysisContext, error) {
	if db == nil {
		return nil, fmt.Errorf("database not available")
	}

	profileID := int64(0)
	source := AnalysisSourceNone
	switch {
	case explicitID > 0:
		profileID, source = explicitID, AnalysisSourceExplicit
	case explicitID < 0:
		return nil, fmt.Errorf("invalid dsinfo id %d", explicitID)
	default:
		if raw := strings.TrimSpace(os.Getenv(EnvDsinfoID)); raw != "" {
			id, err := strconv.ParseInt(raw, 10, 64)
			if err != nil || id <= 0 {
				return nil, fmt.Errorf("invalid %s value %q: expected positive integer", EnvDsinfoID, raw)
			}
			profileID, source = id, AnalysisSourceEnv
		}
	}

	if profileID == 0 {
		active, err := db.GetDsInfoActiveProfile(ctx)
		if err != nil {
			return nil, err
		}
		if active != nil {
			profileID, source = active.ID, AnalysisSourceActive
		}
	}

	if profileID == 0 {
		return &AnalysisContext{Source: AnalysisSourceNone}, nil
	}

	profile, err := db.GetDsInfoProfile(ctx, profileID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("dsinfo profile %d not found", profileID)
		}
		return nil, err
	}
	matching, err := ResolveProfile(ctx, db, profileID)
	if err != nil {
		return nil, err
	}

	result := &AnalysisContext{
		ProfileID:   profileID,
		ProfileName: profile.SessionName,
		Source:      source,
		Matching:    matching,
		Branches:    make(map[int64]int64),
		Requested:   make(map[int64][]string),
	}

	// У продукта в манифесте может быть несколько строк/пакетов (например,
	// fa-contracts-ext с линиями 7-02-2 и 9-01-553). Собираем состояние по
	// продукту: все запрошенные линии и лучшую проиндексированную линию.
	type productState struct {
		name      string
		requested []string
		bestOK    *store.DsInfoProductMatch
		fallback  int64
	}
	states := make(map[int64]*productState)

	for i := range matching.Items {
		item := &matching.Items[i]
		if item.MatchedProductID == 0 {
			continue
		}
		st := states[item.MatchedProductID]
		if st == nil {
			st = &productState{}
			states[item.MatchedProductID] = st
		}
		if item.RequestedBranchKey != "" && !slices.Contains(st.requested, item.RequestedBranchKey) {
			st.requested = append(st.requested, item.RequestedBranchKey)
		}
		switch item.Status {
		case store.DsInfoMatchOK:
			if st.bestOK == nil || betterIndexedLine(*item, *st.bestOK) {
				st.bestOK = item
			}
		case store.DsInfoMatchBranchNotIndexed:
			if st.fallback == 0 && item.FallbackBranchID > 0 {
				st.fallback = item.FallbackBranchID
			}
		}
	}

	for productID, st := range states {
		sort.Strings(st.requested)
		result.Requested[productID] = st.requested
		switch {
		case st.bestOK != nil:
			result.Branches[productID] = st.bestOK.MatchedBranchID
		case st.fallback > 0:
			result.Branches[productID] = st.fallback
		}
	}
	return result, nil
}

// betterIndexedLine выбирает линию продукта среди проиндексированных строк
// манифеста: default-линия, затем старшая версия, затем больший id.
func betterIndexedLine(a, b store.DsInfoProductMatch) bool {
	if a.MatchedBranchDefault != b.MatchedBranchDefault {
		return a.MatchedBranchDefault
	}
	if c := branchkey.CompareVersion(a.MatchedBranchKey, b.MatchedBranchKey); c != 0 {
		return c > 0
	}
	return a.MatchedBranchID > b.MatchedBranchID
}
