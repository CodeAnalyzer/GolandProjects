package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/codebase/internal/dsinfo"
	"github.com/lib/pq"
)

// DsInfoProfileSummary — профиль DiasoftSystemInfo без дочерних данных
// (строка списка / метаданные show).
type DsInfoProfileSummary struct {
	ID              int64     `json:"id"`
	FilePath        string    `json:"file_path"`
	SessionName     string    `json:"session_name"`
	FileSize        int64     `json:"file_size"`
	FileHash        string    `json:"file_hash"`
	ParserVersion   int       `json:"parser_version"`
	DBServer        string    `json:"db_server"`
	DBName          string    `json:"db_name"`
	ClientAlias     string    `json:"client_alias"`
	ProductsCount   int       `json:"products_count"`
	ModulesCount    int       `json:"modules_count"`
	ProceduresCount int       `json:"procedures_count"`
	ParsedAt        time.Time `json:"parsed_at"`
	IsActive        bool      `json:"is_active"`
}

// DsInfoProcedureFilter — фильтры выборки процедур профиля (show --module/
// --packet/--search). Пустой фильтр не выбирает строки (show без фильтров
// возвращает только счётчик).
type DsInfoProcedureFilter struct {
	Module string // точное совпадение module_name
	Packet string // подстрока в packet_name (ILIKE)
	Search string // подстрока в object_name (ILIKE)
}

// Empty сообщает, что все фильтры пусты.
func (f DsInfoProcedureFilter) Empty() bool {
	return f.Module == "" && f.Packet == "" && f.Search == ""
}

// FindDsInfoProfileByHash находит профиль по паре (file_hash, parser_version)
// — ключу идемпотентности parse. nil, nil — профиль не найден.
func (db *DB) FindDsInfoProfileByHash(ctx context.Context, fileHash string, parserVersion int) (*DsInfoProfileSummary, error) {
	row := db.QueryRowContext(ctx, dsinfoProfileSelect+`
		WHERE ds.file_hash = $1 AND ds.parser_version = $2`, fileHash, parserVersion)
	s, err := scanDsInfoProfile(row)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return s, err
}

// InsertDsInfoProfile вставляет профиль с дочерними строками (манифест,
// модули, процедуры) одной транзакцией (pq.CopyIn) — либо всё, либо ничего.
// При гонке параллельных parse одного файла упирается в unique violation
// (idx_dsinfo_file_hash): транзакция откатывается, возвращается id
// существующего профиля и reused=true.
func (db *DB) InsertDsInfoProfile(ctx context.Context, profile *dsinfo.Profile, fileHash, sessionName string) (int64, bool, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return 0, false, fmt.Errorf("failed to begin dsinfo insert: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var id int64
	err = tx.QueryRowContext(ctx, `
		INSERT INTO dsinfo (file_path, session_name, file_size, file_hash, parser_version,
			db_server, db_name, client_alias, products_count, modules_count, procedures_count)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		RETURNING id`,
		profile.FilePath, sessionName, profile.FileSize, fileHash, profile.ParserVersion,
		profile.DBServer, profile.DBName, profile.ClientAlias,
		profile.ProductsCount(), profile.ModulesCount(), profile.ProceduresCount(),
	).Scan(&id)
	if err != nil {
		// Гонка: конкурентный parse того же содержимого уже вставил профиль.
		if IsUniqueViolation(err) {
			if existing, findErr := db.FindDsInfoProfileByHash(ctx, fileHash, profile.ParserVersion); findErr == nil && existing != nil {
				return existing.ID, true, nil
			}
		}
		return 0, false, fmt.Errorf("failed to insert dsinfo profile: %w", err)
	}

	if err := copyInDsInfoChildRows(ctx, tx, id, profile); err != nil {
		return 0, false, err
	}

	if err := tx.Commit(); err != nil {
		return 0, false, fmt.Errorf("failed to commit dsinfo profile: %w", err)
	}
	return id, false, nil
}

// copyInDsInfoChildRows батчит дочерние строки через pq.CopyIn.
func copyInDsInfoChildRows(ctx context.Context, tx *sql.Tx, id int64, profile *dsinfo.Profile) error {
	if len(profile.Products) > 0 {
		stmt, err := tx.PrepareContext(ctx, pq.CopyIn("dsinfo_products",
			"dsinfo_id", "line_no", "raw_name", "packet_ref", "product_name", "branch_key", "hotfix_name"))
		if err != nil {
			return fmt.Errorf("failed to prepare dsinfo_products copy: %w", err)
		}
		for _, p := range profile.Products {
			if _, err := stmt.Exec(id, p.LineNo, p.RawName, p.PacketRef, p.ProductName, p.BranchKey, p.HotfixName); err != nil {
				stmt.Close()
				return fmt.Errorf("failed to copy dsinfo_products row: %w", err)
			}
		}
		if _, err := stmt.Exec(); err != nil {
			stmt.Close()
			return fmt.Errorf("failed to flush dsinfo_products: %w", err)
		}
		stmt.Close()
	}

	if len(profile.Modules) > 0 {
		stmt, err := tx.PrepareContext(ctx, pq.CopyIn("dsinfo_modules",
			"dsinfo_id", "file_name", "file_description", "module_name",
			"product_version", "product_release", "product_issue", "product_build",
			"packet_name", "hotfix_name", "comments", "file_hash"))
		if err != nil {
			return fmt.Errorf("failed to prepare dsinfo_modules copy: %w", err)
		}
		for _, m := range profile.Modules {
			if _, err := stmt.Exec(id, m.FileName, m.FileDescription, m.ModuleName,
				m.ProductVersion, m.ProductRelease, m.ProductIssue, m.ProductBuild,
				m.PacketName, m.HotfixName, m.Comments, m.FileHash); err != nil {
				stmt.Close()
				return fmt.Errorf("failed to copy dsinfo_modules row: %w", err)
			}
		}
		if _, err := stmt.Exec(); err != nil {
			stmt.Close()
			return fmt.Errorf("failed to flush dsinfo_modules: %w", err)
		}
		stmt.Close()
	}

	if len(profile.Procedures) > 0 {
		stmt, err := tx.PrepareContext(ctx, pq.CopyIn("dsinfo_procedures",
			"dsinfo_id", "elem_schema", "object_name", "install_date", "module_name",
			"packet_name", "source_file", "state", "status", "comment"))
		if err != nil {
			return fmt.Errorf("failed to prepare dsinfo_procedures copy: %w", err)
		}
		for _, p := range profile.Procedures {
			if _, err := stmt.Exec(id, p.ElemSchema, p.ObjectName, p.InstallDate, p.ModuleName,
				p.PacketName, p.SourceFile, p.State, p.Status, p.Comment); err != nil {
				stmt.Close()
				return fmt.Errorf("failed to copy dsinfo_procedures row: %w", err)
			}
		}
		if _, err := stmt.Exec(); err != nil {
			stmt.Close()
			return fmt.Errorf("failed to flush dsinfo_procedures: %w", err)
		}
		stmt.Close()
	}
	return nil
}

const dsinfoProfileSelect = `
	SELECT ds.id, ds.file_path, ds.session_name, ds.file_size,
	       ds.file_hash, ds.parser_version, ds.db_server, ds.db_name, ds.client_alias,
	       ds.products_count, ds.modules_count, ds.procedures_count, ds.parsed_at,
	       COALESCE(st.active_dsinfo_id = ds.id, FALSE) AS is_active
	FROM dsinfo ds
	LEFT JOIN dsinfo_state st ON st.id = 1`

func scanDsInfoProfile(row interface{ Scan(...interface{}) error }) (*DsInfoProfileSummary, error) {
	var s DsInfoProfileSummary
	if err := row.Scan(&s.ID, &s.FilePath, &s.SessionName, &s.FileSize,
		&s.FileHash, &s.ParserVersion, &s.DBServer, &s.DBName, &s.ClientAlias,
		&s.ProductsCount, &s.ModulesCount, &s.ProceduresCount, &s.ParsedAt,
		&s.IsActive); err != nil {
		return nil, err
	}
	return &s, nil
}

// ListDsInfoProfiles возвращает профили по убыванию parsed_at; активный
// помечен IsActive (LIMIT применяется к числу профилей).
func (db *DB) ListDsInfoProfiles(ctx context.Context, limit int) ([]DsInfoProfileSummary, error) {
	if limit <= 0 {
		limit = 20
	}
	rows, err := db.QueryContext(ctx, dsinfoProfileSelect+`
		ORDER BY ds.parsed_at DESC LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to list dsinfo profiles: %w", err)
	}
	defer rows.Close()

	var profiles []DsInfoProfileSummary
	for rows.Next() {
		s, err := scanDsInfoProfile(rows)
		if err != nil {
			return nil, err
		}
		profiles = append(profiles, *s)
	}
	return profiles, rows.Err()
}

// GetDsInfoProfile возвращает метаданные профиля по id.
// sql.ErrNoRows при неизвестном id (сервис превращает в ошибку домена).
func (db *DB) GetDsInfoProfile(ctx context.Context, id int64) (*DsInfoProfileSummary, error) {
	row := db.QueryRowContext(ctx, dsinfoProfileSelect+` WHERE ds.id = $1`, id)
	return scanDsInfoProfile(row)
}

// DeleteDsInfoProfile окончательно удаляет профиль (дочерние строки —
// CASCADE; активная ссылка в dsinfo_state сбрасывается ON DELETE SET NULL).
// Возвращает false, если профиля не было.
func (db *DB) DeleteDsInfoProfile(ctx context.Context, id int64) (bool, error) {
	res, err := db.ExecContext(ctx, `DELETE FROM dsinfo WHERE id = $1`, id)
	if err != nil {
		return false, fmt.Errorf("failed to delete dsinfo profile: %w", err)
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// GetDsInfoActiveProfile возвращает активный профиль или nil, nil, если
// активный не назначен (или singleton-строка dsinfo_state ещё не создана).
func (db *DB) GetDsInfoActiveProfile(ctx context.Context) (*DsInfoProfileSummary, error) {
	row := db.QueryRowContext(ctx, dsinfoProfileSelect+`
		WHERE ds.id = (SELECT active_dsinfo_id FROM dsinfo_state WHERE id = 1)`)
	s, err := scanDsInfoProfile(row)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return s, err
}

// SetDsInfoActiveProfile назначает активный профиль (upsert singleton-строки
// dsinfo_state). Возвращает false без изменения состояния, если профиля с
// таким id не существует.
func (db *DB) SetDsInfoActiveProfile(ctx context.Context, id int64) (bool, error) {
	var exists bool
	if err := db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM dsinfo WHERE id = $1)`, id).Scan(&exists); err != nil {
		return false, fmt.Errorf("failed to check dsinfo profile %d: %w", id, err)
	}
	if !exists {
		return false, nil
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO dsinfo_state (id, active_dsinfo_id) VALUES (1, $1)
		ON CONFLICT (id) DO UPDATE SET active_dsinfo_id = EXCLUDED.active_dsinfo_id`, id); err != nil {
		return false, fmt.Errorf("failed to set active dsinfo profile: %w", err)
	}
	return true, nil
}

// LoadDsInfoProducts возвращает манифест профиля (по порядку строк файла).
func (db *DB) LoadDsInfoProducts(ctx context.Context, dsinfoID int64, limit int) ([]dsinfo.Product, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT line_no, raw_name, packet_ref, product_name, branch_key, hotfix_name
		FROM dsinfo_products WHERE dsinfo_id = $1 ORDER BY line_no LIMIT $2`,
		dsinfoID, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to load dsinfo products: %w", err)
	}
	defer rows.Close()

	var products []dsinfo.Product
	for rows.Next() {
		var p dsinfo.Product
		if err := rows.Scan(&p.LineNo, &p.RawName, &p.PacketRef, &p.ProductName, &p.BranchKey, &p.HotfixName); err != nil {
			return nil, err
		}
		products = append(products, p)
	}
	return products, rows.Err()
}

// LoadDsInfoModules возвращает модули профиля.
func (db *DB) LoadDsInfoModules(ctx context.Context, dsinfoID int64, limit int) ([]dsinfo.Module, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT file_name, file_description, module_name,
		       product_version, product_release, product_issue, product_build,
		       packet_name, hotfix_name, comments, file_hash
		FROM dsinfo_modules WHERE dsinfo_id = $1 ORDER BY id LIMIT $2`,
		dsinfoID, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to load dsinfo modules: %w", err)
	}
	defer rows.Close()

	var modules []dsinfo.Module
	for rows.Next() {
		var m dsinfo.Module
		if err := rows.Scan(&m.FileName, &m.FileDescription, &m.ModuleName,
			&m.ProductVersion, &m.ProductRelease, &m.ProductIssue, &m.ProductBuild,
			&m.PacketName, &m.HotfixName, &m.Comments, &m.FileHash); err != nil {
			return nil, err
		}
		modules = append(modules, m)
	}
	return modules, rows.Err()
}

// dsinfoProcedureWhere строит WHERE-условие по фильтру. Возвращает SQL-фрагмент
// (без WHERE) и аргументы, начиная со смещения start.
func dsinfoProcedureWhere(filter DsInfoProcedureFilter, start int) (string, []interface{}) {
	var parts []string
	var args []interface{}
	if filter.Module != "" {
		args = append(args, filter.Module)
		parts = append(parts, fmt.Sprintf("module_name = $%d", start+len(args)))
	}
	if filter.Packet != "" {
		args = append(args, "%"+escapeLike(filter.Packet)+"%")
		parts = append(parts, fmt.Sprintf("packet_name ILIKE $%d", start+len(args)))
	}
	if filter.Search != "" {
		args = append(args, "%"+escapeLike(filter.Search)+"%")
		parts = append(parts, fmt.Sprintf("object_name ILIKE $%d", start+len(args)))
	}
	return strings.Join(parts, " AND "), args
}

func escapeLike(s string) string {
	s = strings.ReplaceAll(s, "\\", "\\\\")
	return strings.ReplaceAll(s, "%", "\\%")
}

// LoadDsInfoProcedures возвращает процедуры профиля по фильтру, ограниченные
// limit (сортировка по имени объекта для стабильности постраничного вывода).
func (db *DB) LoadDsInfoProcedures(ctx context.Context, dsinfoID int64, filter DsInfoProcedureFilter, limit int) ([]dsinfo.Procedure, error) {
	where, args := dsinfoProcedureWhere(filter, 1)
	query := `
		SELECT elem_schema, object_name, install_date, module_name,
		       packet_name, source_file, state, status, comment
		FROM dsinfo_procedures WHERE dsinfo_id = $1`
	args = append([]interface{}{dsinfoID}, args...)
	if where != "" {
		query += " AND " + where
	}
	query += fmt.Sprintf(" ORDER BY object_name, id LIMIT %d", limit)

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to load dsinfo procedures: %w", err)
	}
	defer rows.Close()

	var procedures []dsinfo.Procedure
	for rows.Next() {
		var p dsinfo.Procedure
		if err := rows.Scan(&p.ElemSchema, &p.ObjectName, &p.InstallDate, &p.ModuleName,
			&p.PacketName, &p.SourceFile, &p.State, &p.Status, &p.Comment); err != nil {
			return nil, err
		}
		procedures = append(procedures, p)
	}
	return procedures, rows.Err()
}

// CountDsInfoProcedures возвращает число процедур профиля по фильтру
// (пустой фильтр — общее число: счётчик для show без фильтров).
func (db *DB) CountDsInfoProcedures(ctx context.Context, dsinfoID int64, filter DsInfoProcedureFilter) (int, error) {
	where, args := dsinfoProcedureWhere(filter, 1)
	query := `SELECT count(*) FROM dsinfo_procedures WHERE dsinfo_id = $1`
	args = append([]interface{}{dsinfoID}, args...)
	if where != "" {
		query += " AND " + where
	}
	var n int
	if err := db.QueryRowContext(ctx, query, args...).Scan(&n); err != nil {
		return 0, fmt.Errorf("failed to count dsinfo procedures: %w", err)
	}
	return n, nil
}
