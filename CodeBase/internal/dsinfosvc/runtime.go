// Package dsinfosvc — сервисный слой профилей DiasoftSystemInfo: общие
// операции для CLI (cmd/dsinfo.go) и MCP (internal/mcp).
package dsinfosvc

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"

	"github.com/codebase/internal/dsinfo"
	"github.com/codebase/internal/store"
	"github.com/codebase/internal/util"
)

// normalizeLimit нормализует лимит: default=100, max=1000.
func normalizeLimit(limit int) int {
	if limit <= 0 {
		return 100
	}
	if limit > 1000 {
		return 1000
	}
	return limit
}

// ExecuteParse разбирает DiasoftSystemInfo-файл и сохраняет профиль в БД.
// Повторный parse того же содержимого (SHA-256) той же версией парсера
// возвращает существующий профиль (reused=true) без повторного разбора.
func ExecuteParse(ctx context.Context, db *store.DB, p ParseParams) (*ParseResult, error) {
	if p.FilePath == "" {
		return nil, fmt.Errorf("file_path is required")
	}
	if db == nil {
		return nil, fmt.Errorf("database not available")
	}

	fileHash, err := util.FileHash(p.FilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	// Reuse: файл с тем же содержимым уже разобран этой версией парсера.
	if existing, err := db.FindDsInfoProfileByHash(ctx, fileHash, dsinfo.ParserVersion); err != nil {
		return nil, err
	} else if existing != nil {
		return parseResultFromSummary(existing, true), nil
	}

	profile, err := dsinfo.ParseFile(p.FilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to parse file: %w", err)
	}
	sessionName := p.Name
	if sessionName == "" {
		sessionName = filepath.Base(p.FilePath)
	}

	id, reused, err := db.InsertDsInfoProfile(ctx, profile, fileHash, sessionName)
	if err != nil {
		return nil, fmt.Errorf("failed to save dsinfo profile: %w", err)
	}
	if reused {
		// Гонка параллельных parse: победитель уже вставил профиль,
		// метаданные берём из БД.
		existing, err := db.GetDsInfoProfile(ctx, id)
		if err != nil {
			return nil, err
		}
		return parseResultFromSummary(existing, true), nil
	}

	return &ParseResult{
		ProfileID:       id,
		Reused:          false,
		FilePath:        profile.FilePath,
		SessionName:     sessionName,
		FileHash:        fileHash,
		ParserVersion:   profile.ParserVersion,
		DBServer:        profile.DBServer,
		DBName:          profile.DBName,
		ClientAlias:     profile.ClientAlias,
		ProductsCount:   profile.ProductsCount(),
		ModulesCount:    profile.ModulesCount(),
		ProceduresCount: profile.ProceduresCount(),
	}, nil
}

// parseResultFromSummary собирает ParseResult по существующему профилю.
func parseResultFromSummary(s *store.DsInfoProfileSummary, reused bool) *ParseResult {
	return &ParseResult{
		ProfileID:       s.ID,
		Reused:          reused,
		FilePath:        s.FilePath,
		SessionName:     s.SessionName,
		FileHash:        s.FileHash,
		ParserVersion:   s.ParserVersion,
		DBServer:        s.DBServer,
		DBName:          s.DBName,
		ClientAlias:     s.ClientAlias,
		ProductsCount:   s.ProductsCount,
		ModulesCount:    s.ModulesCount,
		ProceduresCount: s.ProceduresCount,
	}
}

// ExecuteList возвращает список профилей по убыванию parsed_at.
func ExecuteList(ctx context.Context, db *store.DB, limit int) (*ListResult, error) {
	if db == nil {
		return nil, fmt.Errorf("database not available")
	}
	profiles, err := db.ListDsInfoProfiles(ctx, normalizeLimit(limit))
	if err != nil {
		return nil, err
	}
	return &ListResult{Profiles: profiles}, nil
}

// ExecuteShow возвращает профиль с манифестом и модулями; процедуры — только
// при наличии фильтра (Module/Packet/Search), иначе только счётчик.
func ExecuteShow(ctx context.Context, db *store.DB, p ShowParams) (*ShowResult, error) {
	if db == nil {
		return nil, fmt.Errorf("database not available")
	}
	if p.ProfileID <= 0 {
		return nil, fmt.Errorf("profile_id is required")
	}
	limit := normalizeLimit(p.Limit)

	profile, err := db.GetDsInfoProfile(ctx, p.ProfileID)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("dsinfo profile %d not found", p.ProfileID)
	}
	if err != nil {
		return nil, err
	}

	products, err := db.LoadDsInfoProducts(ctx, p.ProfileID, limit)
	if err != nil {
		return nil, err
	}
	modules, err := db.LoadDsInfoModules(ctx, p.ProfileID, limit)
	if err != nil {
		return nil, err
	}

	result := &ShowResult{
		Profile:            profile,
		Products:           products,
		ProductsTruncated:  len(products) < profile.ProductsCount,
		Modules:            modules,
		ModulesTruncated:   len(modules) < profile.ModulesCount,
		ProceduresFiltered: false,
		Limit:              limit,
	}

	filter := store.DsInfoProcedureFilter{Module: p.Module, Packet: p.Packet, Search: p.Search}
	if filter.Empty() {
		// Без фильтров процедуры не выбираются: только общий счётчик
		// (62k строк не выпадают в вывод).
		count, err := db.CountDsInfoProcedures(ctx, p.ProfileID, filter)
		if err != nil {
			return nil, err
		}
		result.ProceduresCount = count
		return result, nil
	}

	procedures, err := db.LoadDsInfoProcedures(ctx, p.ProfileID, filter, limit)
	if err != nil {
		return nil, err
	}
	count, err := db.CountDsInfoProcedures(ctx, p.ProfileID, filter)
	if err != nil {
		return nil, err
	}
	result.Procedures = procedures
	result.ProceduresCount = count
	result.ProceduresFiltered = true
	return result, nil
}

// ExecuteDelete окончательно удаляет профиль (дочерние строки — CASCADE;
// если профиль был активным, dsinfo_state.active_dsinfo_id сбрасывается
// на NULL уровнем БД — ON DELETE SET NULL).
func ExecuteDelete(ctx context.Context, db *store.DB, p DeleteParams) (*DeleteResult, error) {
	if db == nil {
		return nil, fmt.Errorf("database not available")
	}
	if p.ProfileID <= 0 {
		return nil, fmt.Errorf("profile_id is required")
	}

	profile, err := db.GetDsInfoProfile(ctx, p.ProfileID)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("dsinfo profile %d not found", p.ProfileID)
	}
	if err != nil {
		return nil, err
	}

	deleted, err := db.DeleteDsInfoProfile(ctx, p.ProfileID)
	if err != nil {
		return nil, err
	}
	return &DeleteResult{
		Deleted:     deleted,
		ProfileID:   p.ProfileID,
		FilePath:    profile.FilePath,
		SessionName: profile.SessionName,
		WasActive:   profile.IsActive,
		ActiveReset: deleted && profile.IsActive,
	}, nil
}

// ExecuteUse назначает активный профиль (ProfileID > 0) или возвращает
// текущий активный (ProfileID == 0) без изменения состояния.
func ExecuteUse(ctx context.Context, db *store.DB, p UseParams) (*UseResult, error) {
	if db == nil {
		return nil, fmt.Errorf("database not available")
	}
	if p.ProfileID > 0 {
		ok, err := db.SetDsInfoActiveProfile(ctx, p.ProfileID)
		if err != nil {
			return nil, err
		}
		if !ok {
			// Неизвестный id — состояние не меняется.
			return nil, fmt.Errorf("dsinfo profile %d not found", p.ProfileID)
		}
	}
	active, err := db.GetDsInfoActiveProfile(ctx)
	if err != nil {
		return nil, err
	}
	return &UseResult{Active: active}, nil
}
