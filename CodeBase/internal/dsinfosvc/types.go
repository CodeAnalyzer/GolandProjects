package dsinfosvc

import (
	"github.com/codebase/internal/dsinfo"
	"github.com/codebase/internal/store"
)

// ParseParams — параметры dsinfo parse: FilePath обязателен, Name — явно
// заданное имя профиля (иначе base-имя файла).
type ParseParams struct {
	FilePath string
	Name     string
}

// ParseResult — результат парсинга DiasoftSystemInfo с сохранением профиля.
type ParseResult struct {
	ProfileID       int64  `json:"profile_id"`
	Reused          bool   `json:"reused"`
	FilePath        string `json:"file_path"`
	SessionName     string `json:"session_name"`
	FileHash        string `json:"file_hash"`
	ParserVersion   int    `json:"parser_version"`
	DBServer        string `json:"db_server"`
	DBName          string `json:"db_name"`
	ClientAlias     string `json:"client_alias"`
	ProductsCount   int    `json:"products_count"`
	ModulesCount    int    `json:"modules_count"`
	ProceduresCount int    `json:"procedures_count"`
}

// ListResult — список профилей (по убыванию parsed_at, активный помечен).
type ListResult struct {
	Profiles []store.DsInfoProfileSummary `json:"profiles"`
}

// ShowParams — параметры dsinfo show: метаданные профиля + манифест + модули
// всегда; процедуры — только при наличии хотя бы одного фильтра
// (Module/Packet/Search), иначе только счётчик.
type ShowParams struct {
	ProfileID int64
	Module    string
	Packet    string
	Search    string
	Limit     int
}

// ShowResult — содержимое профиля. Procedures nil, если фильтры не заданы
// (ProceduresCount — общее число процедур профиля).
type ShowResult struct {
	Profile            *store.DsInfoProfileSummary `json:"profile"`
	Products           []dsinfo.Product            `json:"products"`
	ProductsTruncated  bool                        `json:"products_truncated"`
	Modules            []dsinfo.Module             `json:"modules"`
	ModulesTruncated   bool                        `json:"modules_truncated"`
	Procedures         []dsinfo.Procedure          `json:"procedures,omitempty"`
	ProceduresCount    int                         `json:"procedures_count"`
	ProceduresFiltered bool                        `json:"procedures_filtered"`
	Limit              int                         `json:"limit"`
}

// DeleteParams — параметры dsinfo delete (окончательное удаление).
type DeleteParams struct {
	ProfileID int64
}

// DeleteResult — результат удаления. WasActive — удаляли активный профиль
// (ссылка сброшена на NULL уровнем БД).
type DeleteResult struct {
	Deleted     bool   `json:"deleted"`
	ProfileID   int64  `json:"profile_id"`
	FilePath    string `json:"file_path,omitempty"`
	SessionName string `json:"session_name,omitempty"`
	WasActive   bool   `json:"was_active"`
	ActiveReset bool   `json:"active_reset"`
}

// UseParams — параметры dsinfo use: ProfileID > 0 назначает активный профиль,
// 0 — показать текущий активный без изменения.
type UseParams struct {
	ProfileID int64
}

// UseResult — активный профиль (nil, если не назначен).
type UseResult struct {
	Active *store.DsInfoProfileSummary `json:"active"`
}
