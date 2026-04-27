package store

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/codebase/internal/config"
	"github.com/codebase/internal/model"
	"github.com/lib/pq"
)

// DB обёртка над sql.DB
type DB struct {
	*sql.DB
}

// Stats статистика индекса
type Stats struct {
	TotalFiles         int
	SQLFiles           int
	HFiles             int
	PASFiles           int
	INCFiles           int
	JSFiles            int
	XMLFiles           int
	SMFFiles           int
	DFMFiles           int
	TPRFiles           int
	RPTFiles           int
	Procedures         int
	Tables             int
	Columns            int
	Units              int
	Classes            int
	Methods            int
	Functions          int
	JSFunctions        int
	SMFInstruments     int
	Forms              int
	DFMQueries         int
	Defines            int
	Relations          int
	QueryFragments     int
	ReportForms        int
	ReportFields       int
	ReportParams       int
	VBFunctions        int
	APIBusinessObjects int
	APIContracts       int
	APIContractParams  int
	APIContractTables  int
	APIContractFields  int
	APIBusinessParams  int
	APIBusinessTables  int
	SQLTableIndexes    int
	APITableIndexes    int
	APIMacros          int
	Errors             int
	PASFields          int
	LastScanID         int64
	LastScanStarted    time.Time
	LastScanFinished   time.Time
	LastScanStatus     string
}

// NewDB создаёт подключение к БД и создаёт её если не существует
func NewDB(cfg config.DBConfig) (*DB, error) {
	// Подключение в два шага нужно потому, что целевая БД может ещё не существовать:
	// сначала идём в postgres/system database, затем в рабочую базу CodeBase.
	// Сначала подключаемся к default database для создания целевой БД
	dsnDefault := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=postgres sslmode=%s",
		cfg.Host, cfg.Port, cfg.User, cfg.Password, cfg.SSLMode,
	)

	dbDefault, err := sql.Open("postgres", dsnDefault)
	if err != nil {
		return nil, fmt.Errorf("failed to open default database: %w", err)
	}
	defer dbDefault.Close()

	// Проверяем подключение
	if err := dbDefault.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping default database: %w", err)
	}

	// Создаём БД если не существует
	if err := createDatabaseIfNotExists(dbDefault, cfg.Database); err != nil {
		return nil, fmt.Errorf("failed to create database: %w", err)
	}

	// Теперь подключаемся к целевой БД
	dsn := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		cfg.Host, cfg.Port, cfg.User, cfg.Password, cfg.Database, cfg.SSLMode,
	)

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Финальный ping подтверждает, что рабочая БД доступна уже после возможного создания.
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	// Пул соединений настраивается консервативно: CLI-процесс короткоживущий,
	// но indexer может параллельно держать несколько запросов.
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	return &DB{db}, nil
}

// createDatabaseIfNotExists создаёт БД если она не существует
func createDatabaseIfNotExists(db *sql.DB, dbName string) error {
	// Проверяем существование БД
	var exists bool
	err := db.QueryRow(`
		SELECT EXISTS(
			SELECT 1 FROM pg_database WHERE datname = $1
		)
	`, dbName).Scan(&exists)
	if err != nil {
		return fmt.Errorf("failed to check database existence: %w", err)
	}

	if !exists {
		// Создаём БД
		_, err = db.Exec(fmt.Sprintf("CREATE DATABASE %s", dbName))
		if err != nil {
			return fmt.Errorf("failed to create database: %w", err)
		}
	}

	return nil
}

// UpdatePASClassDFMForm обновляет dfm_form_id у PAS класса.
func (db *DB) UpdatePASClassDFMForm(classID int64, dfmFormID int64) error {
	_, err := db.Exec(`UPDATE pas_classes SET dfm_form_id = $1 WHERE id = $2`, NullableInt64(dfmFormID), classID)
	if err != nil {
		return fmt.Errorf("failed to update pas class dfm form: %w", err)
	}

	return nil
}

// UpdatePASFieldDFMComponent обновляет dfm_component_id у PAS поля.
func (db *DB) UpdatePASFieldDFMComponent(fieldID int64, dfmComponentID int64) error {
	_, err := db.Exec(`UPDATE pas_fields SET dfm_component_id = $1 WHERE id = $2`, NullableInt64(dfmComponentID), fieldID)
	if err != nil {
		return fmt.Errorf("failed to update pas field dfm component: %w", err)
	}

	return nil
}

// InitSchema создаёт схему БД если она не существует
func (db *DB) InitSchema() error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS scan_runs (
			id BIGSERIAL PRIMARY KEY,
			root_path TEXT NOT NULL,
			started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			finished_at TIMESTAMPTZ,
			status TEXT NOT NULL,
			files_scanned INTEGER NOT NULL DEFAULT 0,
			files_indexed INTEGER NOT NULL DEFAULT 0,
			errors_count INTEGER NOT NULL DEFAULT 0
		)`,
		`CREATE TABLE IF NOT EXISTS files (
			id BIGSERIAL PRIMARY KEY,
			scan_run_id BIGINT NOT NULL REFERENCES scan_runs(id) ON DELETE CASCADE,
			path TEXT NOT NULL,
			rel_path TEXT NOT NULL,
			extension TEXT NOT NULL,
			size_bytes BIGINT NOT NULL DEFAULT 0,
			hash_sha256 TEXT NOT NULL,
			modified_at TIMESTAMPTZ NOT NULL,
			encoding TEXT,
			language TEXT,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
		`CREATE TABLE IF NOT EXISTS symbols (
			id BIGSERIAL PRIMARY KEY,
			file_id BIGINT NOT NULL REFERENCES files(id) ON DELETE CASCADE,
			symbol_name TEXT NOT NULL,
			symbol_type TEXT NOT NULL,
			entity_type TEXT NOT NULL,
			entity_id BIGINT NOT NULL,
			line_number INTEGER NOT NULL DEFAULT 0,
			signature TEXT
		)`,
		`CREATE TABLE IF NOT EXISTS sql_procedures (
			id BIGSERIAL PRIMARY KEY,
			file_id BIGINT NOT NULL REFERENCES files(id) ON DELETE CASCADE,
			proc_name TEXT NOT NULL,
			parameters JSONB,
			line_start INTEGER NOT NULL DEFAULT 0,
			line_end INTEGER NOT NULL DEFAULT 0,
			body_hash TEXT
		)`,
		`CREATE TABLE IF NOT EXISTS sql_tables (
			id BIGSERIAL PRIMARY KEY,
			file_id BIGINT NOT NULL REFERENCES files(id) ON DELETE CASCADE,
			table_name TEXT NOT NULL,
			context TEXT NOT NULL,
			is_temporary BOOLEAN NOT NULL DEFAULT FALSE,
			line_number INTEGER NOT NULL DEFAULT 0,
			column_number INTEGER
		)`,
		`CREATE TABLE IF NOT EXISTS sql_columns (
			id BIGSERIAL PRIMARY KEY,
			file_id BIGINT NOT NULL REFERENCES files(id) ON DELETE CASCADE,
			table_name TEXT NOT NULL,
			column_name TEXT NOT NULL,
			line_number INTEGER NOT NULL DEFAULT 0,
			column_number INTEGER
		)`,
		`CREATE TABLE IF NOT EXISTS sql_column_definitions (
			id BIGSERIAL PRIMARY KEY,
			file_id BIGINT NOT NULL REFERENCES files(id) ON DELETE CASCADE,
			table_name TEXT NOT NULL,
			column_name TEXT NOT NULL,
			data_type TEXT NOT NULL,
			definition_kind TEXT NOT NULL DEFAULT 'create_table',
			line_number INTEGER NOT NULL DEFAULT 0,
			column_order INTEGER NOT NULL DEFAULT 0
		)`,
		`CREATE TABLE IF NOT EXISTS sql_index_definitions (
			id BIGSERIAL PRIMARY KEY,
			file_id BIGINT NOT NULL REFERENCES files(id) ON DELETE CASCADE,
			table_name TEXT NOT NULL,
			index_name TEXT NOT NULL,
			index_fields TEXT,
			index_type TEXT,
			is_unique BOOLEAN NOT NULL DEFAULT FALSE,
			definition_kind TEXT NOT NULL DEFAULT 'create_index',
			line_number INTEGER NOT NULL DEFAULT 0
		)`,
		`CREATE TABLE IF NOT EXISTS sql_index_definition_fields (
			id BIGSERIAL PRIMARY KEY,
			table_index_id BIGINT NOT NULL REFERENCES sql_index_definitions(id) ON DELETE CASCADE,
			field_name TEXT NOT NULL,
			field_order INTEGER NOT NULL DEFAULT 0,
			line_number INTEGER NOT NULL DEFAULT 0
		)`,
		`CREATE TABLE IF NOT EXISTS pas_units (
			id BIGSERIAL PRIMARY KEY,
			file_id BIGINT NOT NULL REFERENCES files(id) ON DELETE CASCADE,
			unit_name TEXT NOT NULL,
			interface_uses JSONB,
			implementation_uses JSONB,
			line_start INTEGER NOT NULL DEFAULT 0,
			line_end INTEGER NOT NULL DEFAULT 0
		)`,
		`CREATE TABLE IF NOT EXISTS h_files_defines (
			id BIGSERIAL PRIMARY KEY,
			file_id BIGINT NOT NULL REFERENCES files(id) ON DELETE CASCADE,
			define_name TEXT NOT NULL,
			define_value TEXT,
			define_type TEXT NOT NULL,
			line_number INTEGER NOT NULL DEFAULT 0
		)`,
		`CREATE TABLE IF NOT EXISTS js_functions (
			id BIGSERIAL PRIMARY KEY,
			file_id BIGINT NOT NULL REFERENCES files(id) ON DELETE CASCADE,
			function_name TEXT NOT NULL,
			signature TEXT,
			line_start INTEGER NOT NULL DEFAULT 0,
			line_end INTEGER NOT NULL DEFAULT 0,
			scenario_type TEXT,
			parent_object TEXT
		)`,
		`CREATE TABLE IF NOT EXISTS smf_instruments (
			id BIGSERIAL PRIMARY KEY,
			file_id BIGINT NOT NULL REFERENCES files(id) ON DELETE CASCADE,
			instrument_name TEXT NOT NULL,
			brief TEXT,
			deal_object_id BIGINT,
			ds_module_id BIGINT,
			start_state TEXT,
			scenario_type TEXT,
			states JSONB,
			actions JSONB,
			accounts JSONB
		)`,
		`CREATE TABLE IF NOT EXISTS dfm_forms (
			id BIGSERIAL PRIMARY KEY,
			file_id BIGINT NOT NULL REFERENCES files(id) ON DELETE CASCADE,
			form_name TEXT NOT NULL,
			form_class TEXT NOT NULL,
			caption TEXT,
			line_start INTEGER NOT NULL DEFAULT 0,
			line_end INTEGER NOT NULL DEFAULT 0
		)`,
		`CREATE TABLE IF NOT EXISTS dfm_components (
			id BIGSERIAL PRIMARY KEY,
			file_id BIGINT NOT NULL REFERENCES files(id) ON DELETE CASCADE,
			form_id BIGINT NOT NULL REFERENCES dfm_forms(id) ON DELETE CASCADE,
			component_name TEXT NOT NULL,
			component_type TEXT NOT NULL,
			parent_name TEXT,
			caption TEXT,
			line_start INTEGER NOT NULL DEFAULT 0,
			line_end INTEGER NOT NULL DEFAULT 0
		)`,
		`CREATE TABLE IF NOT EXISTS report_forms (
			id BIGSERIAL PRIMARY KEY,
			file_id BIGINT NOT NULL REFERENCES files(id) ON DELETE CASCADE,
			report_name TEXT NOT NULL,
			report_type TEXT NOT NULL,
			form_name TEXT,
			form_class TEXT,
			line_start INTEGER NOT NULL DEFAULT 0,
			line_end INTEGER NOT NULL DEFAULT 0
		)`,
		`CREATE TABLE IF NOT EXISTS report_fields (
			id BIGSERIAL PRIMARY KEY,
			report_form_id BIGINT NOT NULL REFERENCES report_forms(id) ON DELETE CASCADE,
			field_name TEXT NOT NULL,
			source_name TEXT,
			format_mask TEXT,
			options JSONB,
			line_number INTEGER NOT NULL DEFAULT 0,
			raw_text TEXT
		)`,
		`CREATE TABLE IF NOT EXISTS report_params (
			id BIGSERIAL PRIMARY KEY,
			report_form_id BIGINT NOT NULL REFERENCES report_forms(id) ON DELETE CASCADE,
			param_name TEXT NOT NULL,
			param_kind TEXT NOT NULL,
			component_type TEXT,
			data_type TEXT,
			lookup_form TEXT,
			lookup_table TEXT,
			lookup_column TEXT,
			key_column TEXT,
			required BOOLEAN NOT NULL DEFAULT FALSE,
			default_value TEXT,
			line_number INTEGER NOT NULL DEFAULT 0,
			raw_text TEXT
		)`,
		`CREATE TABLE IF NOT EXISTS vb_functions (
			id BIGSERIAL PRIMARY KEY,
			report_form_id BIGINT NOT NULL REFERENCES report_forms(id) ON DELETE CASCADE,
			function_name TEXT NOT NULL,
			function_type TEXT NOT NULL,
			signature TEXT,
			body_text TEXT,
			line_start INTEGER NOT NULL DEFAULT 0,
			line_end INTEGER NOT NULL DEFAULT 0,
			body_hash TEXT
		)`,
		`CREATE TABLE IF NOT EXISTS query_fragments (
			id BIGSERIAL PRIMARY KEY,
			file_id BIGINT NOT NULL REFERENCES files(id) ON DELETE CASCADE,
			parent_type TEXT NOT NULL,
			parent_id BIGINT NOT NULL,
			component_name TEXT NOT NULL,
			component_type TEXT NOT NULL,
			query_text TEXT NOT NULL,
			query_hash TEXT,
			tables_referenced JSONB,
			context TEXT NOT NULL,
			line_number INTEGER NOT NULL DEFAULT 0
		)`,
		`CREATE TABLE IF NOT EXISTS include_directives (
			id BIGSERIAL PRIMARY KEY,
			file_id BIGINT NOT NULL REFERENCES files(id) ON DELETE CASCADE,
			include_path TEXT NOT NULL,
			resolved_file_id BIGINT,
			line_number INTEGER NOT NULL DEFAULT 0
		)`,
		`CREATE TABLE IF NOT EXISTS pas_classes (
			id BIGSERIAL PRIMARY KEY,
			unit_id BIGINT,
			class_name TEXT NOT NULL,
			parent_class TEXT,
			dfm_form_id BIGINT,
			line_start INTEGER NOT NULL DEFAULT 0,
			line_end INTEGER NOT NULL DEFAULT 0
		)`,
		`CREATE TABLE IF NOT EXISTS pas_methods (
			id BIGSERIAL PRIMARY KEY,
			class_id BIGINT,
			unit_id BIGINT,
			method_name TEXT NOT NULL,
			signature TEXT,
			visibility TEXT,
			line_number INTEGER NOT NULL DEFAULT 0
		)`,
		`CREATE TABLE IF NOT EXISTS pas_fields (
			id BIGSERIAL PRIMARY KEY,
			class_id BIGINT,
			field_name TEXT NOT NULL,
			field_type TEXT,
			dfm_component_id BIGINT,
			visibility TEXT,
			line_number INTEGER NOT NULL DEFAULT 0
		)`,
		`CREATE TABLE IF NOT EXISTS api_business_objects (
			id BIGSERIAL PRIMARY KEY,
			file_id BIGINT NOT NULL REFERENCES files(id) ON DELETE CASCADE,
			business_object TEXT NOT NULL,
			module_name TEXT,
			line_start INTEGER NOT NULL DEFAULT 0,
			line_end INTEGER NOT NULL DEFAULT 0
		)`,
		`CREATE TABLE IF NOT EXISTS api_contracts (
			id BIGSERIAL PRIMARY KEY,
			file_id BIGINT NOT NULL REFERENCES files(id) ON DELETE CASCADE,
			business_object_id BIGINT,
			business_object TEXT,
			contract_name TEXT NOT NULL,
			contract_kind TEXT NOT NULL,
			object_type_id INTEGER,
			object_name_id BIGINT,
			api_version INTEGER,
			arch_approval INTEGER,
			implemented BOOLEAN NOT NULL DEFAULT FALSE,
			internal_use BOOLEAN NOT NULL DEFAULT FALSE,
			deprecated BOOLEAN NOT NULL DEFAULT FALSE,
			is_external BOOLEAN NOT NULL DEFAULT FALSE,
			owner_module TEXT,
			used_object_name TEXT,
			used_module_sys_name TEXT,
			short_description TEXT,
			full_description TEXT,
			line_start INTEGER NOT NULL DEFAULT 0,
			line_end INTEGER NOT NULL DEFAULT 0
		)`,
		`CREATE TABLE IF NOT EXISTS api_contract_params (
			id BIGSERIAL PRIMARY KEY,
			contract_id BIGINT NOT NULL REFERENCES api_contracts(id) ON DELETE CASCADE,
			direction TEXT NOT NULL,
			param_name TEXT NOT NULL,
			prm_sub_object TEXT,
			type_name TEXT,
			required BOOLEAN NOT NULL DEFAULT FALSE,
			rus_name TEXT,
			description TEXT,
			ws_param_name TEXT,
			param_order INTEGER NOT NULL DEFAULT 0,
			is_virtual_link BOOLEAN NOT NULL DEFAULT FALSE,
			line_number INTEGER NOT NULL DEFAULT 0
		)`,
		`CREATE TABLE IF NOT EXISTS api_contract_tables (
			id BIGSERIAL PRIMARY KEY,
			contract_id BIGINT NOT NULL REFERENCES api_contracts(id) ON DELETE CASCADE,
			direction TEXT NOT NULL,
			table_name TEXT NOT NULL,
			ws_param_name TEXT,
			required BOOLEAN NOT NULL DEFAULT FALSE,
			rus_name TEXT,
			description TEXT,
			param_order INTEGER NOT NULL DEFAULT 0,
			line_number INTEGER NOT NULL DEFAULT 0
		)`,
		`CREATE TABLE IF NOT EXISTS api_contract_table_fields (
			id BIGSERIAL PRIMARY KEY,
			contract_table_id BIGINT NOT NULL REFERENCES api_contract_tables(id) ON DELETE CASCADE,
			field_name TEXT NOT NULL,
			type_name TEXT,
			required BOOLEAN NOT NULL DEFAULT FALSE,
			rus_name TEXT,
			description TEXT,
			ws_param_name TEXT,
			param_order INTEGER NOT NULL DEFAULT 0,
			line_number INTEGER NOT NULL DEFAULT 0
		)`,
		`CREATE TABLE IF NOT EXISTS api_business_object_params (
			id BIGSERIAL PRIMARY KEY,
			file_id BIGINT NOT NULL REFERENCES files(id) ON DELETE CASCADE,
			business_object TEXT NOT NULL,
			param_name TEXT NOT NULL,
			prm_sub_object TEXT,
			type_name TEXT,
			ws_param_name TEXT,
			rus_name TEXT,
			description TEXT,
			line_number INTEGER NOT NULL DEFAULT 0
		)`,
		`CREATE TABLE IF NOT EXISTS api_business_object_tables (
			id BIGSERIAL PRIMARY KEY,
			file_id BIGINT NOT NULL REFERENCES files(id) ON DELETE CASCADE,
			business_object TEXT NOT NULL,
			table_name TEXT NOT NULL,
			type_name TEXT,
			ws_param_name TEXT,
			rus_name TEXT,
			description TEXT,
			line_number INTEGER NOT NULL DEFAULT 0
		)`,
		`CREATE TABLE IF NOT EXISTS api_business_object_table_fields (
			id BIGSERIAL PRIMARY KEY,
			business_table_id BIGINT NOT NULL REFERENCES api_business_object_tables(id) ON DELETE CASCADE,
			field_name TEXT NOT NULL,
			type_name TEXT,
			ws_param_name TEXT,
			rus_name TEXT,
			description TEXT,
			param_order INTEGER NOT NULL DEFAULT 0,
			line_number INTEGER NOT NULL DEFAULT 0
		)`,
		`CREATE TABLE IF NOT EXISTS api_business_object_table_indexes (
			id BIGSERIAL PRIMARY KEY,
			business_table_id BIGINT NOT NULL REFERENCES api_business_object_tables(id) ON DELETE CASCADE,
			index_name TEXT NOT NULL,
			index_fields TEXT,
			index_type INTEGER NOT NULL DEFAULT 0,
			is_clustered BOOLEAN NOT NULL DEFAULT FALSE,
			line_number INTEGER NOT NULL DEFAULT 0
		)`,
		`CREATE TABLE IF NOT EXISTS api_business_object_table_index_fields (
			id BIGSERIAL PRIMARY KEY,
			table_index_id BIGINT NOT NULL REFERENCES api_business_object_table_indexes(id) ON DELETE CASCADE,
			field_name TEXT NOT NULL,
			field_order INTEGER NOT NULL DEFAULT 0,
			line_number INTEGER NOT NULL DEFAULT 0
		)`,
		`CREATE TABLE IF NOT EXISTS api_contract_return_values (
			id BIGSERIAL PRIMARY KEY,
			contract_id BIGINT NOT NULL REFERENCES api_contracts(id) ON DELETE CASCADE,
			value TEXT,
			return_type INTEGER NOT NULL DEFAULT 0,
			description TEXT,
			line_number INTEGER NOT NULL DEFAULT 0
		)`,
		`CREATE TABLE IF NOT EXISTS api_contract_contexts (
			id BIGSERIAL PRIMARY KEY,
			contract_id BIGINT NOT NULL REFERENCES api_contracts(id) ON DELETE CASCADE,
			context_name TEXT,
			type_name TEXT,
			rus_name TEXT,
			description TEXT,
			context_order INTEGER NOT NULL DEFAULT 0,
			context_value TEXT,
			is_virtual_link BOOLEAN NOT NULL DEFAULT FALSE,
			line_number INTEGER NOT NULL DEFAULT 0
		)`,
		`CREATE TABLE IF NOT EXISTS api_macro_invocations (
			id BIGSERIAL PRIMARY KEY,
			file_id BIGINT NOT NULL REFERENCES files(id) ON DELETE CASCADE,
			procedure_name TEXT,
			macro_type TEXT NOT NULL,
			target_name TEXT NOT NULL,
			target_kind TEXT NOT NULL,
			line_number INTEGER NOT NULL DEFAULT 0,
			raw_text TEXT
		)`,
		`CREATE TABLE IF NOT EXISTS relations (
			id BIGSERIAL PRIMARY KEY,
			source_type TEXT NOT NULL,
			source_id BIGINT NOT NULL,
			target_type TEXT NOT NULL,
			target_id BIGINT NOT NULL,
			relation_type TEXT NOT NULL,
			confidence TEXT,
			line_number INTEGER NOT NULL DEFAULT 0
		)`,
		`CREATE EXTENSION IF NOT EXISTS pg_trgm`,
		`CREATE INDEX IF NOT EXISTS idx_files_scan_run_id ON files(scan_run_id)`,
		`CREATE INDEX IF NOT EXISTS idx_files_extension ON files(extension)`,
		`CREATE INDEX IF NOT EXISTS idx_symbols_file_id ON symbols(file_id)`,
		`CREATE INDEX IF NOT EXISTS idx_sql_procedures_file_id ON sql_procedures(file_id)`,
		`CREATE INDEX IF NOT EXISTS idx_sql_tables_file_id ON sql_tables(file_id)`,
		`CREATE INDEX IF NOT EXISTS idx_sql_columns_file_id ON sql_columns(file_id)`,
		`CREATE INDEX IF NOT EXISTS idx_sql_column_definitions_file_id ON sql_column_definitions(file_id)`,
		`CREATE INDEX IF NOT EXISTS idx_sql_column_definitions_table_name ON sql_column_definitions(table_name)`,
		`CREATE INDEX IF NOT EXISTS idx_sql_index_definitions_file_id ON sql_index_definitions(file_id)`,
		`CREATE INDEX IF NOT EXISTS idx_sql_index_definitions_table_name ON sql_index_definitions(table_name)`,
		`CREATE INDEX IF NOT EXISTS idx_sql_index_definitions_name ON sql_index_definitions(index_name)`,
		`CREATE INDEX IF NOT EXISTS idx_sql_index_definition_fields_index_id ON sql_index_definition_fields(table_index_id)`,
		`CREATE INDEX IF NOT EXISTS idx_pas_units_file_id ON pas_units(file_id)`,
		`CREATE INDEX IF NOT EXISTS idx_h_files_defines_file_id ON h_files_defines(file_id)`,
		`CREATE INDEX IF NOT EXISTS idx_js_functions_file_id ON js_functions(file_id)`,
		`CREATE INDEX IF NOT EXISTS idx_smf_instruments_file_id ON smf_instruments(file_id)`,
		`CREATE INDEX IF NOT EXISTS idx_dfm_forms_file_id ON dfm_forms(file_id)`,
		`CREATE INDEX IF NOT EXISTS idx_dfm_components_file_id ON dfm_components(file_id)`,
		`CREATE INDEX IF NOT EXISTS idx_dfm_components_form_id ON dfm_components(form_id)`,
		`CREATE INDEX IF NOT EXISTS idx_report_forms_file_id ON report_forms(file_id)`,
		`CREATE INDEX IF NOT EXISTS idx_report_fields_report_form_id ON report_fields(report_form_id)`,
		`CREATE INDEX IF NOT EXISTS idx_report_params_report_form_id ON report_params(report_form_id)`,
		`CREATE INDEX IF NOT EXISTS idx_vb_functions_report_form_id ON vb_functions(report_form_id)`,
		`CREATE INDEX IF NOT EXISTS idx_query_fragments_file_id ON query_fragments(file_id)`,
		`CREATE INDEX IF NOT EXISTS idx_query_fragments_query_text_trgm ON query_fragments USING GIN (query_text gin_trgm_ops)`,
		`CREATE INDEX IF NOT EXISTS idx_include_directives_file_id ON include_directives(file_id)`,
		`CREATE INDEX IF NOT EXISTS idx_api_business_objects_file_id ON api_business_objects(file_id)`,
		`CREATE INDEX IF NOT EXISTS idx_api_business_objects_name ON api_business_objects(business_object)`,
		`CREATE INDEX IF NOT EXISTS idx_api_contracts_file_id ON api_contracts(file_id)`,
		`CREATE INDEX IF NOT EXISTS idx_api_contracts_name_kind ON api_contracts(contract_name, contract_kind)`,
		`CREATE INDEX IF NOT EXISTS idx_api_contracts_business_object ON api_contracts(business_object)`,
		`CREATE INDEX IF NOT EXISTS idx_api_contract_params_contract_id ON api_contract_params(contract_id)`,
		`CREATE INDEX IF NOT EXISTS idx_api_contract_tables_contract_id ON api_contract_tables(contract_id)`,
		`CREATE INDEX IF NOT EXISTS idx_api_contract_table_fields_table_id ON api_contract_table_fields(contract_table_id)`,
		`CREATE INDEX IF NOT EXISTS idx_api_business_object_params_file_id ON api_business_object_params(file_id)`,
		`CREATE INDEX IF NOT EXISTS idx_api_business_object_tables_file_id ON api_business_object_tables(file_id)`,
		`CREATE INDEX IF NOT EXISTS idx_api_business_object_table_indexes_table_id ON api_business_object_table_indexes(business_table_id)`,
		`CREATE INDEX IF NOT EXISTS idx_api_business_object_table_indexes_name ON api_business_object_table_indexes(index_name)`,
		`CREATE INDEX IF NOT EXISTS idx_api_business_object_table_index_fields_index_id ON api_business_object_table_index_fields(table_index_id)`,
		`CREATE INDEX IF NOT EXISTS idx_api_macro_invocations_file_id ON api_macro_invocations(file_id)`,
		`CREATE INDEX IF NOT EXISTS idx_api_macro_invocations_target_name ON api_macro_invocations(target_name)`,
		`CREATE INDEX IF NOT EXISTS idx_relations_source_type_id ON relations(source_type, source_id)`,
		`CREATE INDEX IF NOT EXISTS idx_relations_target_type_id ON relations(target_type, target_id)`,
		`CREATE INDEX IF NOT EXISTS idx_relations_source_type_relation ON relations(source_type, relation_type, source_id)`,
		`CREATE INDEX IF NOT EXISTS idx_relations_target_type_relation ON relations(target_type, relation_type, target_id)`,
		`CREATE INDEX IF NOT EXISTS idx_relations_relation_type ON relations(relation_type)`,
	}

	for _, stmt := range statements {
		if _, err := db.Exec(stmt); err != nil {
			return fmt.Errorf("failed to initialize schema: %w", err)
		}
	}

	return nil
}

// GetStats возвращает статистику индекса
func (db *DB) GetStats() (*Stats, error) {
	stats := &Stats{}

	if err := db.QueryRow(`
		SELECT
			COUNT(*) AS total_files,
			COUNT(*) FILTER (WHERE UPPER(extension) = 'SQL') AS sql_files,
			COUNT(*) FILTER (WHERE UPPER(extension) = 'H') AS h_files,
			COUNT(*) FILTER (WHERE UPPER(extension) = 'PAS') AS pas_files,
			COUNT(*) FILTER (WHERE UPPER(extension) = 'INC') AS inc_files,
			COUNT(*) FILTER (WHERE UPPER(extension) = 'JS') AS js_files,
			COUNT(*) FILTER (WHERE UPPER(extension) = 'XML') AS xml_files,
			COUNT(*) FILTER (WHERE UPPER(extension) = 'SMF') AS smf_files,
			COUNT(*) FILTER (WHERE UPPER(extension) = 'DFM') AS dfm_files,
			COUNT(*) FILTER (WHERE UPPER(extension) = 'TPR') AS tpr_files,
			COUNT(*) FILTER (WHERE UPPER(extension) = 'RPT') AS rpt_files
		FROM files
	`).Scan(
		&stats.TotalFiles,
		&stats.SQLFiles,
		&stats.HFiles,
		&stats.PASFiles,
		&stats.INCFiles,
		&stats.JSFiles,
		&stats.XMLFiles,
		&stats.SMFFiles,
		&stats.DFMFiles,
		&stats.TPRFiles,
		&stats.RPTFiles,
	); err != nil {
		return nil, fmt.Errorf("failed to get file stats: %w", err)
	}

	aggregates := []struct {
		query  string
		target *int
		name   string
	}{
		{`SELECT COUNT(*) FROM sql_procedures`, &stats.Procedures, "sql procedures"},
		{`SELECT COUNT(*) FROM sql_tables`, &stats.Tables, "sql tables"},
		{`SELECT COUNT(*) FROM sql_columns`, &stats.Columns, "sql columns"},
		{`SELECT COUNT(*) FROM pas_units`, &stats.Units, "pas units"},
		{`SELECT COUNT(*) FROM pas_classes`, &stats.Classes, "pas classes"},
		{`SELECT COUNT(*) FROM pas_methods`, &stats.Methods, "pas methods"},
		{`SELECT COUNT(*) FROM pas_fields`, &stats.PASFields, "pas fields"},
		{`SELECT COUNT(*) FROM js_functions`, &stats.JSFunctions, "js functions"},
		{`SELECT COUNT(*) FROM smf_instruments`, &stats.SMFInstruments, "smf instruments"},
		{`SELECT COUNT(*) FROM dfm_forms`, &stats.Forms, "dfm forms"},
		{`SELECT COUNT(*) FROM h_files_defines`, &stats.Defines, "h defines"},
		{`SELECT COUNT(*) FROM report_forms`, &stats.ReportForms, "report forms"},
		{`SELECT COUNT(*) FROM report_fields`, &stats.ReportFields, "report fields"},
		{`SELECT COUNT(*) FROM report_params`, &stats.ReportParams, "report params"},
		{`SELECT COUNT(*) FROM vb_functions`, &stats.VBFunctions, "vb functions"},
		{`SELECT COUNT(*) FROM api_business_objects`, &stats.APIBusinessObjects, "api business objects"},
		{`SELECT COUNT(*) FROM api_contracts`, &stats.APIContracts, "api contracts"},
		{`SELECT COUNT(*) FROM api_contract_params`, &stats.APIContractParams, "api contract params"},
		{`SELECT COUNT(*) FROM api_contract_tables`, &stats.APIContractTables, "api contract tables"},
		{`SELECT COUNT(*) FROM api_contract_table_fields`, &stats.APIContractFields, "api contract table fields"},
		{`SELECT COUNT(*) FROM api_business_object_params`, &stats.APIBusinessParams, "api business params"},
		{`SELECT COUNT(*) FROM api_business_object_tables`, &stats.APIBusinessTables, "api business tables"},
		{`SELECT COUNT(*) FROM sql_index_definitions`, &stats.SQLTableIndexes, "sql table indexes"},
		{`SELECT COUNT(*) FROM api_business_object_table_indexes`, &stats.APITableIndexes, "api business table indexes"},
		{`SELECT COUNT(*) FROM api_macro_invocations`, &stats.APIMacros, "api macro invocations"},
		{`SELECT COUNT(*) FROM query_fragments`, &stats.QueryFragments, "query fragments"},
		{`SELECT COUNT(*) FROM relations`, &stats.Relations, "relations"},
	}

	for _, aggregate := range aggregates {
		if err := db.QueryRow(aggregate.query).Scan(aggregate.target); err != nil {
			return nil, fmt.Errorf("failed to get %s count: %w", aggregate.name, err)
		}
	}

	var finishedAt sql.NullTime
	var status sql.NullString
	var errorsCount sql.NullInt64
	if err := db.QueryRow(`
		SELECT id, started_at, finished_at, status, errors_count
		FROM scan_runs
		ORDER BY started_at DESC, id DESC
		LIMIT 1
	`).Scan(&stats.LastScanID, &stats.LastScanStarted, &finishedAt, &status, &errorsCount); err != nil {
		if err != sql.ErrNoRows {
			return nil, fmt.Errorf("failed to get last scan info: %w", err)
		}
		return stats, nil
	}

	if finishedAt.Valid {
		stats.LastScanFinished = finishedAt.Time
	}
	if status.Valid {
		stats.LastScanStatus = status.String
	}
	if errorsCount.Valid {
		stats.Errors = int(errorsCount.Int64)
	}

	return stats, nil
}

// GetLatestFilesByRootPath возвращает последнее известное состояние файлов для указанного root path.
func (db *DB) GetLatestFilesByRootPath(rootPath string) (map[string]*model.File, error) {
	normalizedRoot := strings.ReplaceAll(strings.TrimSpace(rootPath), `\`, "/")
	rows, err := db.Query(`
		SELECT DISTINCT ON (path)
			id, scan_run_id, path, rel_path, extension, size_bytes,
			hash_sha256, modified_at, encoding, language, created_at, updated_at
		FROM files
		WHERE path = $1 OR path LIKE $2
		ORDER BY path, id DESC
	`, normalizedRoot, normalizedRoot+"/%")
	if err != nil {
		return nil, fmt.Errorf("failed to query indexed files: %w", err)
	}
	defer rows.Close()

	files := make(map[string]*model.File)
	for rows.Next() {
		var f model.File
		if err := rows.Scan(
			&f.ID,
			&f.ScanRunID,
			&f.Path,
			&f.RelPath,
			&f.Extension,
			&f.SizeBytes,
			&f.HashSHA256,
			&f.ModifiedAt,
			&f.Encoding,
			&f.Language,
			&f.CreatedAt,
			&f.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan indexed file row: %w", err)
		}
		files[f.Path] = &f
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate indexed files: %w", err)
	}

	return files, nil
}

// DeleteFilesByPath удаляет все записи файла по path вместе с зависимыми сущностями.
func (db *DB) DeleteFilesByPath(path string) error {
	_, err := db.Exec(`DELETE FROM files WHERE path = $1`, path)
	if err != nil {
		return fmt.Errorf("failed to delete file by path: %w", err)
	}
	return nil
}

// DeleteFilesByPathExcept удаляет все записи файла по path, кроме указанной.
func (db *DB) DeleteFilesByPathExcept(path string, keepID int64) error {
	_, err := db.Exec(`DELETE FROM files WHERE path = $1 AND id <> $2`, path, keepID)
	if err != nil {
		return fmt.Errorf("failed to delete outdated file rows: %w", err)
	}
	return nil
}

// FindLatestFileIDByPaths возвращает последний file id, найденный по одному из path/rel_path кандидатов.
func (db *DB) FindLatestFileIDByPaths(paths []string) (int64, error) {
	if len(paths) == 0 {
		return 0, sql.ErrNoRows
	}

	seen := make(map[string]struct{}, len(paths))
	uniquePaths := make([]string, 0, len(paths))
	for _, path := range paths {
		normalized := strings.ToLower(strings.ReplaceAll(strings.TrimSpace(path), `\`, "/"))
		if normalized == "" {
			continue
		}
		if _, exists := seen[normalized]; exists {
			continue
		}
		seen[normalized] = struct{}{}
		uniquePaths = append(uniquePaths, normalized)
	}

	if len(uniquePaths) == 0 {
		return 0, sql.ErrNoRows
	}

	var id int64
	err := db.QueryRow(`
		SELECT id
		FROM files
		WHERE LOWER(path) = ANY($1) OR LOWER(rel_path) = ANY($1)
		ORDER BY id DESC
		LIMIT 1
	`, pq.Array(uniquePaths)).Scan(&id)
	if err != nil {
		return 0, err
	}

	return id, nil
}

// FindLatestDFMFormIDByClassName возвращает последний id DFM формы по имени класса формы.
func (db *DB) FindLatestDFMFormIDByClassName(className string) (int64, error) {
	var id int64
	err := db.QueryRow(`
		SELECT id
		FROM dfm_forms
		WHERE LOWER(form_class) = LOWER($1)
		ORDER BY id DESC
		LIMIT 1
	`, className).Scan(&id)
	if err != nil {
		return 0, err
	}

	return id, nil
}

type UnlinkedPASClass struct {
	ID        int64
	ClassName string
}

// FindUnlinkedPASClasses возвращает PAS классы без привязанной DFM формы.
func (db *DB) FindUnlinkedPASClasses() ([]UnlinkedPASClass, error) {
	rows, err := db.Query(`
		SELECT id, class_name
		FROM pas_classes
		WHERE dfm_form_id IS NULL
		ORDER BY id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]UnlinkedPASClass, 0)
	for rows.Next() {
		var item UnlinkedPASClass
		if err := rows.Scan(&item.ID, &item.ClassName); err != nil {
			return nil, err
		}
		result = append(result, item)
	}

	return result, rows.Err()
}

type PASFieldDFMLinkCandidate struct {
	FieldID   int64
	FieldName string
	DFMFormID int64
}

// FindPASFieldDFMLinkCandidates возвращает PAS поля, которые можно связать с DFM-компонентами через уже привязанный класс.
func (db *DB) FindPASFieldDFMLinkCandidates() ([]PASFieldDFMLinkCandidate, error) {
	rows, err := db.Query(`
		SELECT pf.id, pf.field_name, pc.dfm_form_id
		FROM pas_fields pf
		JOIN pas_classes pc ON pf.class_id = pc.id
		WHERE pf.dfm_component_id IS NULL
		  AND pc.dfm_form_id IS NOT NULL
		ORDER BY pf.id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]PASFieldDFMLinkCandidate, 0)
	for rows.Next() {
		var item PASFieldDFMLinkCandidate
		if err := rows.Scan(&item.FieldID, &item.FieldName, &item.DFMFormID); err != nil {
			return nil, err
		}
		result = append(result, item)
	}

	return result, rows.Err()
}

// FindLatestDFMComponentIDByFormAndName возвращает последний id DFM-компонента по форме и имени компонента.
func (db *DB) FindLatestDFMComponentIDByFormAndName(formID int64, componentName string) (int64, error) {
	var id int64
	err := db.QueryRow(`
		SELECT id
		FROM dfm_components
		WHERE form_id = $1
		  AND LOWER(component_name) = LOWER($2)
		ORDER BY id DESC
		LIMIT 1
	`, formID, componentName).Scan(&id)
	if err != nil {
		return 0, err
	}

	return id, nil
}

// FindLatestHFileIDByNameLike возвращает последний file id H-файла по имени include через LIKE.
func (db *DB) FindLatestHFileIDByNameLike(fileName string) (int64, error) {
	normalized := strings.ToLower(strings.ReplaceAll(strings.TrimSpace(fileName), `\`, "/"))
	if normalized == "" {
		return 0, sql.ErrNoRows
	}

	if idx := strings.LastIndex(normalized, "/"); idx >= 0 {
		normalized = normalized[idx+1:]
	}

	var id int64
	err := db.QueryRow(`
		SELECT id
		FROM files
		WHERE LOWER(extension) = 'h'
		  AND (LOWER(path) LIKE $1 OR LOWER(rel_path) LIKE $1)
		ORDER BY id DESC
		LIMIT 1
	`, "%/"+normalized).Scan(&id)
	if err != nil {
		return 0, err
	}

	return id, nil
}

// FindLatestSQLProcedureIDByName возвращает последний id SQL процедуры по имени.
func (db *DB) FindLatestSQLProcedureIDByName(procName string) (int64, error) {
	var id int64
	err := db.QueryRow(`
		SELECT id
		FROM sql_procedures
		WHERE LOWER(proc_name) = LOWER($1)
		ORDER BY id DESC
		LIMIT 1
	`, strings.TrimSpace(procName)).Scan(&id)
	if err != nil {
		return 0, err
	}

	return id, nil
}

// FindSQLProcedureIDsByFile возвращает id процедур файла по имени.
func (db *DB) FindSQLProcedureIDsByFile(fileID int64) (map[string]int64, error) {
	rows, err := db.Query(`
		SELECT id, proc_name
		FROM sql_procedures
		WHERE file_id = $1
		ORDER BY id DESC
	`, fileID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]int64)
	for rows.Next() {
		var id int64
		var procName string
		if err := rows.Scan(&id, &procName); err != nil {
			return nil, err
		}
		key := strings.ToLower(strings.TrimSpace(procName))
		if key == "" {
			continue
		}
		if _, exists := result[key]; !exists {
			result[key] = id
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

// FindPASUnitIDsByFile возвращает id PAS юнитов файла по имени и line_start.
func (db *DB) FindPASUnitIDsByFile(fileID int64) (map[string]int64, error) {
	rows, err := db.Query(`
		SELECT id, unit_name, line_start
		FROM pas_units
		WHERE file_id = $1
		ORDER BY id DESC
	`, fileID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]int64)
	for rows.Next() {
		var id int64
		var unitName string
		var lineStart int
		if err := rows.Scan(&id, &unitName, &lineStart); err != nil {
			return nil, err
		}
		key := BuildPASUnitLookupKey(unitName, lineStart)
		if _, exists := result[key]; !exists {
			result[key] = id
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

// FindPASClassIDsByUnit возвращает id PAS классов юнита по имени и line_start.
func (db *DB) FindPASClassIDsByUnit(unitID int64) (map[string]int64, error) {
	rows, err := db.Query(`
		SELECT id, class_name, line_start
		FROM pas_classes
		WHERE unit_id = $1
		ORDER BY id DESC
	`, unitID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]int64)
	for rows.Next() {
		var id int64
		var className string
		var lineStart int
		if err := rows.Scan(&id, &className, &lineStart); err != nil {
			return nil, err
		}
		key := BuildPASClassLookupKey(className, lineStart)
		if _, exists := result[key]; !exists {
			result[key] = id
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

// FindPASMethodIDsByUnit возвращает id PAS методов юнита по классу/имени/строке.
func (db *DB) FindPASMethodIDsByUnit(unitID int64) (map[string]int64, error) {
	rows, err := db.Query(`
		SELECT pm.id, COALESCE(pc.class_name, ''), pm.method_name, pm.line_number
		FROM pas_methods pm
		LEFT JOIN pas_classes pc ON pm.class_id = pc.id
		WHERE pm.unit_id = $1
		ORDER BY pm.id DESC
	`, unitID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]int64)
	for rows.Next() {
		var id int64
		var className string
		var methodName string
		var lineNumber int
		if err := rows.Scan(&id, &className, &methodName, &lineNumber); err != nil {
			return nil, err
		}
		key := BuildPASMethodLookupKey(className, methodName, lineNumber)
		if _, exists := result[key]; !exists {
			result[key] = id
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

// FindPASFieldIDsByClassNames возвращает id PAS полей файла по классу/имени/строке.
func (db *DB) FindPASFieldIDsByUnit(unitID int64) (map[string]int64, error) {
	rows, err := db.Query(`
		SELECT pf.id, COALESCE(pc.class_name, ''), pf.field_name, pf.line_number
		FROM pas_fields pf
		LEFT JOIN pas_classes pc ON pf.class_id = pc.id
		LEFT JOIN pas_methods pm ON 1 = 0
		WHERE pc.unit_id = $1 OR pf.class_id IS NULL
		ORDER BY pf.id DESC
	`, unitID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]int64)
	for rows.Next() {
		var id int64
		var className string
		var fieldName string
		var lineNumber int
		if err := rows.Scan(&id, &className, &fieldName, &lineNumber); err != nil {
			return nil, err
		}
		key := BuildPASFieldLookupKey(className, fieldName, lineNumber)
		if _, exists := result[key]; !exists {
			result[key] = id
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

// FindDFMComponentIDsByForm возвращает id компонентов формы по имени и line_start.
func (db *DB) FindDFMComponentIDsByForm(formID int64) (map[string]int64, error) {
	rows, err := db.Query(`
		SELECT id, component_name, line_start
		FROM dfm_components
		WHERE form_id = $1
		ORDER BY id DESC
	`, formID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]int64)
	for rows.Next() {
		var id int64
		var componentName string
		var lineStart int
		if err := rows.Scan(&id, &componentName, &lineStart); err != nil {
			return nil, err
		}
		key := BuildDFMComponentLookupKey(componentName, lineStart)
		if _, exists := result[key]; !exists {
			result[key] = id
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

func BuildHDefineLookupKey(defineName string, lineNumber int) string {
	return strings.ToLower(strings.TrimSpace(defineName)) + "|" + fmt.Sprintf("%d", lineNumber)
}

func BuildJSFunctionLookupKey(functionName string, lineStart int) string {
	return strings.ToLower(strings.TrimSpace(functionName)) + "|" + fmt.Sprintf("%d", lineStart)
}

func BuildDFMFormLookupKey(formName string, lineStart int) string {
	return strings.ToLower(strings.TrimSpace(formName)) + "|" + fmt.Sprintf("%d", lineStart)
}

func BuildDFMComponentLookupKey(componentName string, lineStart int) string {
	return strings.ToLower(strings.TrimSpace(componentName)) + "|" + fmt.Sprintf("%d", lineStart)
}

func BuildPASUnitLookupKey(unitName string, lineStart int) string {
	return strings.ToLower(strings.TrimSpace(unitName)) + "|" + fmt.Sprintf("%d", lineStart)
}

func BuildPASClassLookupKey(className string, lineStart int) string {
	return strings.ToLower(strings.TrimSpace(className)) + "|" + fmt.Sprintf("%d", lineStart)
}

func BuildPASMethodLookupKey(className string, methodName string, lineNumber int) string {
	return strings.ToLower(strings.TrimSpace(className)) + "|" + strings.ToLower(strings.TrimSpace(methodName)) + "|" + fmt.Sprintf("%d", lineNumber)
}

func BuildPASFieldLookupKey(className string, fieldName string, lineNumber int) string {
	return strings.ToLower(strings.TrimSpace(className)) + "|" + strings.ToLower(strings.TrimSpace(fieldName)) + "|" + fmt.Sprintf("%d", lineNumber)
}

// FindSQLTableIDsByFileAndLine возвращает id таблиц файла по имени, контексту и строке.
func (db *DB) FindSQLTableIDsByFileAndLine(fileID int64) (map[string]int64, error) {
	rows, err := db.Query(`
		SELECT id, table_name, context, line_number
		FROM sql_tables
		WHERE file_id = $1
		ORDER BY id DESC
	`, fileID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]int64)
	for rows.Next() {
		var id int64
		var tableName string
		var context string
		var lineNumber int
		if err := rows.Scan(&id, &tableName, &context, &lineNumber); err != nil {
			return nil, err
		}
		key := BuildSQLTableLookupKey(tableName, context, lineNumber)
		if _, exists := result[key]; !exists {
			result[key] = id
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

// FindLatestSQLTableIDByName возвращает последний id SQL таблицы по имени.
func (db *DB) FindLatestSQLTableIDByName(tableName string) (int64, error) {
	var id int64
	err := db.QueryRow(`
		SELECT id
		FROM sql_tables
		WHERE LOWER(table_name) = LOWER($1)
		ORDER BY id DESC
		LIMIT 1
	`, strings.TrimSpace(tableName)).Scan(&id)
	if err != nil {
		return 0, err
	}

	return id, nil
}

// FindQueryFragmentIDsByFileAndHash возвращает id query fragments файла по hash/context/line.
func (db *DB) FindQueryFragmentIDsByFileAndHash(fileID int64) (map[string]int64, error) {
	rows, err := db.Query(`
		SELECT id, COALESCE(query_hash, ''), context, line_number
		FROM query_fragments
		WHERE file_id = $1
		ORDER BY id DESC
	`, fileID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]int64)
	for rows.Next() {
		var id int64
		var queryHash string
		var context string
		var lineNumber int
		if err := rows.Scan(&id, &queryHash, &context, &lineNumber); err != nil {
			return nil, err
		}
		key := BuildQueryFragmentLookupKey(queryHash, context, lineNumber)
		if _, exists := result[key]; !exists {
			result[key] = id
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

func BuildSQLTableLookupKey(tableName, context string, lineNumber int) string {
	return strings.ToLower(strings.TrimSpace(tableName)) + "|" + strings.ToLower(strings.TrimSpace(context)) + "|" + fmt.Sprintf("%d", lineNumber)
}

func BuildSQLIndexDefinitionLookupKey(tableName, indexName string, lineNumber int) string {
	return strings.ToLower(strings.TrimSpace(tableName)) + "|" + strings.ToLower(strings.TrimSpace(indexName)) + "|" + fmt.Sprintf("%d", lineNumber)
}

func BuildQueryFragmentLookupKey(queryHash, context string, lineNumber int) string {
	return strings.TrimSpace(queryHash) + "|" + strings.ToLower(strings.TrimSpace(context)) + "|" + fmt.Sprintf("%d", lineNumber)
}

func BuildReportFieldLookupKey(fieldName string, lineNumber int) string {
	return strings.ToLower(strings.TrimSpace(fieldName)) + "|" + fmt.Sprintf("%d", lineNumber)
}

func BuildReportParamLookupKey(paramName string, lineNumber int) string {
	return strings.ToLower(strings.TrimSpace(paramName)) + "|" + fmt.Sprintf("%d", lineNumber)
}

func BuildVBFunctionLookupKey(functionName string, lineStart int) string {
	return strings.ToLower(strings.TrimSpace(functionName)) + "|" + fmt.Sprintf("%d", lineStart)
}

// FindHDefineIDsByFile возвращает id define-ов файла по имени и строке.
func (db *DB) FindHDefineIDsByFile(fileID int64) (map[string]int64, error) {
	rows, err := db.Query(`
		SELECT id, define_name, line_number
		FROM h_files_defines
		WHERE file_id = $1
		ORDER BY id DESC
	`, fileID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]int64)
	for rows.Next() {
		var id int64
		var defineName string
		var lineNumber int
		if err := rows.Scan(&id, &defineName, &lineNumber); err != nil {
			return nil, err
		}
		key := BuildHDefineLookupKey(defineName, lineNumber)
		if _, exists := result[key]; !exists {
			result[key] = id
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

// FindJSFunctionIDsByFile возвращает id JS функций файла по имени и line_start.
func (db *DB) FindJSFunctionIDsByFile(fileID int64) (map[string]int64, error) {
	rows, err := db.Query(`
		SELECT id, function_name, line_start
		FROM js_functions
		WHERE file_id = $1
		ORDER BY id DESC
	`, fileID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]int64)
	for rows.Next() {
		var id int64
		var functionName string
		var lineStart int
		if err := rows.Scan(&id, &functionName, &lineStart); err != nil {
			return nil, err
		}
		key := BuildJSFunctionLookupKey(functionName, lineStart)
		if _, exists := result[key]; !exists {
			result[key] = id
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

// FindDFMFormIDsByFile возвращает id DFM форм файла по имени и line_start.
func (db *DB) FindDFMFormIDsByFile(fileID int64) (map[string]int64, error) {
	rows, err := db.Query(`
		SELECT id, form_name, line_start
		FROM dfm_forms
		WHERE file_id = $1
		ORDER BY id DESC
	`, fileID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]int64)
	for rows.Next() {
		var id int64
		var formName string
		var lineStart int
		if err := rows.Scan(&id, &formName, &lineStart); err != nil {
			return nil, err
		}
		key := BuildDFMFormLookupKey(formName, lineStart)
		if _, exists := result[key]; !exists {
			result[key] = id
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

// FindJSFunctionIDByFileAndLine возвращает id JS функции, в диапазон которой попадает строка.
func (db *DB) FindJSFunctionIDByFileAndLine(fileID int64, lineNumber int) (int64, error) {
	var id int64
	err := db.QueryRow(`
		SELECT id
		FROM js_functions
		WHERE file_id = $1
		  AND line_start <= $2
		  AND line_end >= $2
		ORDER BY line_start DESC, id DESC
		LIMIT 1
	`, fileID, lineNumber).Scan(&id)
	if err != nil {
		return 0, err
	}

	return id, nil
}

// FindReportFormIDByFileAndLine возвращает id report form, диапазон которой включает строку.
func (db *DB) FindReportFormIDByFileAndLine(fileID int64, lineNumber int) (int64, error) {
	var id int64
	err := db.QueryRow(`
		SELECT id
		FROM report_forms
		WHERE file_id = $1
		  AND line_start <= $2
		  AND line_end >= $2
		ORDER BY line_start DESC, id DESC
		LIMIT 1
	`, fileID, lineNumber).Scan(&id)
	if err != nil {
		return 0, err
	}

	return id, nil
}

// FindReportFieldIDsByForm возвращает id report fields по форме.
func (db *DB) FindReportFieldIDsByForm(reportFormID int64) (map[string]int64, error) {
	rows, err := db.Query(`
		SELECT id, field_name, line_number
		FROM report_fields
		WHERE report_form_id = $1
		ORDER BY id DESC
	`, reportFormID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]int64)
	for rows.Next() {
		var id int64
		var fieldName string
		var lineNumber int
		if err := rows.Scan(&id, &fieldName, &lineNumber); err != nil {
			return nil, err
		}
		key := BuildReportFieldLookupKey(fieldName, lineNumber)
		if _, exists := result[key]; !exists {
			result[key] = id
		}
	}
	return result, rows.Err()
}

// FindReportParamIDsByForm возвращает id report params по форме.
func (db *DB) FindReportParamIDsByForm(reportFormID int64) (map[string]int64, error) {
	rows, err := db.Query(`
		SELECT id, param_name, line_number
		FROM report_params
		WHERE report_form_id = $1
		ORDER BY id DESC
	`, reportFormID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]int64)
	for rows.Next() {
		var id int64
		var paramName string
		var lineNumber int
		if err := rows.Scan(&id, &paramName, &lineNumber); err != nil {
			return nil, err
		}
		key := BuildReportParamLookupKey(paramName, lineNumber)
		if _, exists := result[key]; !exists {
			result[key] = id
		}
	}
	return result, rows.Err()
}

// FindVBFunctionIDsByForm возвращает id VB functions по форме.
func (db *DB) FindVBFunctionIDsByForm(reportFormID int64) (map[string]int64, error) {
	rows, err := db.Query(`
		SELECT id, function_name, line_start
		FROM vb_functions
		WHERE report_form_id = $1
		ORDER BY id DESC
	`, reportFormID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]int64)
	for rows.Next() {
		var id int64
		var functionName string
		var lineStart int
		if err := rows.Scan(&id, &functionName, &lineStart); err != nil {
			return nil, err
		}
		key := BuildVBFunctionLookupKey(functionName, lineStart)
		if _, exists := result[key]; !exists {
			result[key] = id
		}
	}
	return result, rows.Err()
}

// FindLatestSMFInstrumentIDByFile возвращает последний id SMF инструмента файла.
func (db *DB) FindLatestSMFInstrumentIDByFile(fileID int64) (int64, error) {
	var id int64
	err := db.QueryRow(`
		SELECT id
		FROM smf_instruments
		WHERE file_id = $1
		ORDER BY id DESC
		LIMIT 1
	`, fileID).Scan(&id)
	if err != nil {
		return 0, err
	}

	return id, nil
}

// FindDFMFormIDByFileAndLine возвращает id DFM формы, диапазон которой включает строку.
func (db *DB) FindDFMFormIDByFileAndLine(fileID int64, lineNumber int) (int64, error) {
	var id int64
	err := db.QueryRow(`
		SELECT id
		FROM dfm_forms
		WHERE file_id = $1
		  AND line_start <= $2
		  AND line_end >= $2
		ORDER BY line_start DESC, id DESC
		LIMIT 1
	`, fileID, lineNumber).Scan(&id)
	if err != nil {
		return 0, err
	}

	return id, nil
}

// FindLatestPASClassIDByName возвращает последний id класса по имени.
func (db *DB) FindLatestPASClassIDByName(className string) (int64, error) {
	var id int64
	err := db.QueryRow(`
		SELECT id
		FROM pas_classes
		WHERE LOWER(class_name) = LOWER($1)
		ORDER BY id DESC
		LIMIT 1
	`, strings.TrimSpace(className)).Scan(&id)
	if err != nil {
		return 0, err
	}

	return id, nil
}

// UpdatePASMethodClass обновляет class_id у PAS метода.
func (db *DB) UpdatePASMethodClass(methodID int64, classID int64) error {
	_, err := db.Exec(`UPDATE pas_methods SET class_id = $1 WHERE id = $2`, classID, methodID)
	if err != nil {
		return fmt.Errorf("failed to update pas method class: %w", err)
	}
	return nil
}

// UpdatePASFieldClass обновляет class_id у PAS поля.
func (db *DB) UpdatePASFieldClass(fieldID int64, classID int64) error {
	_, err := db.Exec(`UPDATE pas_fields SET class_id = $1 WHERE id = $2`, classID, fieldID)
	if err != nil {
		return fmt.Errorf("failed to update pas field class: %w", err)
	}
	return nil
}

// CreateScanRun создаёт запись о запуске сканирования
func (db *DB) CreateScanRun(rootPath string) (int64, error) {
	var id int64
	err := db.QueryRow(`
		INSERT INTO scan_runs (root_path, status) 
		VALUES ($1, 'running') 
		RETURNING id
	`, rootPath).Scan(&id)
	return id, err
}

// HasCompletedInit проверяет, была ли уже завершена первичная инициализация индекса.
func (db *DB) HasCompletedInit() (bool, error) {
	var exists bool
	err := db.QueryRow(`
		SELECT EXISTS (
			SELECT 1
			FROM scan_runs
			WHERE status IN ('completed', 'completed_with_errors')
		)
	`).Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists, nil
}

// UpdateScanRun обновляет статус сканирования
func (db *DB) UpdateScanRun(id int64, filesScanned, filesIndexed, errorsCount int, status string) error {
	_, err := db.Exec(`
		UPDATE scan_runs 
		SET finished_at = NOW(),
		    status = $4,
		    files_scanned = $1,
		    files_indexed = $2,
		    errors_count = $3
		WHERE id = $5
	`, filesScanned, filesIndexed, errorsCount, status, id)
	return err
}

func (db *DB) withCopyInTx(fn func(tx *sql.Tx) error) (err error) {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
			return
		}
		err = tx.Commit()
	}()
	return fn(tx)
}

// BatchInsertSymbols пакетная вставка символов
func (db *DB) BatchInsertSymbols(symbols []*model.Symbol, batchSize int) error {
	if len(symbols) == 0 {
		return nil
	}

	if len(symbols) <= batchSize {
		return db.insertSymbolsBatch(symbols)
	}

	for i := 0; i < len(symbols); i += batchSize {
		end := i + batchSize
		if end > len(symbols) {
			end = len(symbols)
		}

		batch := symbols[i:end]
		if err := db.insertSymbolsBatch(batch); err != nil {
			return err
		}
	}
	return nil
}

// insertSymbolsBatch вставляет одну пачку символов
func (db *DB) insertSymbolsBatch(symbols []*model.Symbol) error {
	if len(symbols) == 0 {
		return nil
	}

	return db.withCopyInTx(func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(pq.CopyIn("symbols", "file_id", "symbol_name", "symbol_type", "entity_type", "entity_id", "line_number", "signature"))
		if err != nil {
			return err
		}
		defer stmt.Close()

		for _, symbol := range symbols {
			_, err := stmt.Exec(
				symbol.FileID,
				sanitizeUTF8String(symbol.SymbolName),
				sanitizeUTF8String(symbol.SymbolType),
				sanitizeUTF8String(symbol.EntityType),
				symbol.EntityID,
				symbol.LineNumber,
				sanitizeUTF8String(symbol.Signature),
			)
			if err != nil {
				return err
			}
		}

		_, err = stmt.Exec()
		return err
	})
}

// BatchInsertDFMComponents пакетная вставка DFM компонентов
func (db *DB) BatchInsertDFMComponents(components []*model.DFMComponent, batchSize int) error {
	if len(components) == 0 {
		return nil
	}

	if len(components) <= batchSize {
		return db.insertDFMComponentsBatch(components)
	}

	for i := 0; i < len(components); i += batchSize {
		end := i + batchSize
		if end > len(components) {
			end = len(components)
		}

		batch := components[i:end]
		if err := db.insertDFMComponentsBatch(batch); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) insertDFMComponentsBatch(components []*model.DFMComponent) error {
	if len(components) == 0 {
		return nil
	}

	return db.withCopyInTx(func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(pq.CopyIn("dfm_components", "file_id", "form_id", "component_name", "component_type", "parent_name", "caption", "line_start", "line_end"))
		if err != nil {
			return err
		}
		defer stmt.Close()

		for _, component := range components {
			_, err := stmt.Exec(
				component.FileID,
				component.FormID,
				sanitizeUTF8String(component.ComponentName),
				sanitizeUTF8String(component.ComponentType),
				NullableString(component.ParentName),
				NullableString(component.Caption),
				component.LineStart,
				component.LineEnd,
			)
			if err != nil {
				return err
			}
		}

		_, err = stmt.Exec()
		return err
	})
}

// BatchInsertSQLProcedures пакетная вставка SQL процедур
func (db *DB) BatchInsertSQLProcedures(procedures []*model.SQLProcedure, batchSize int) error {
	if len(procedures) == 0 {
		return nil
	}

	if len(procedures) <= batchSize {
		return db.insertSQLProceduresBatch(procedures)
	}

	for i := 0; i < len(procedures); i += batchSize {
		end := i + batchSize
		if end > len(procedures) {
			end = len(procedures)
		}

		batch := procedures[i:end]
		if err := db.insertSQLProceduresBatch(batch); err != nil {
			return err
		}
	}
	return nil
}

// insertSQLProceduresBatch вставляет одну пачку SQL процедур
func (db *DB) insertSQLProceduresBatch(procedures []*model.SQLProcedure) error {
	if len(procedures) == 0 {
		return nil
	}

	return db.withCopyInTx(func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(pq.CopyIn("sql_procedures", "file_id", "proc_name", "parameters", "line_start", "line_end", "body_hash"))
		if err != nil {
			return err
		}
		defer stmt.Close()

		for _, proc := range procedures {
			var paramsJSON interface{}
			if len(proc.Params) > 0 {
				data, err := json.Marshal(proc.Params)
				if err != nil {
					return err
				}
				paramsJSON = string(data)
			}

			_, err := stmt.Exec(
				proc.FileID,
				sanitizeUTF8String(proc.ProcName),
				sanitizeNullableJSON(paramsJSON),
				proc.LineStart,
				proc.LineEnd,
				NullableString(proc.BodyHash),
			)
			if err != nil {
				return err
			}
		}

		_, err = stmt.Exec()
		return err
	})
}

// BatchInsertSQLTables пакетная вставка SQL таблиц
func (db *DB) BatchInsertSQLTables(tables []*model.SQLTable, batchSize int) error {
	if len(tables) == 0 {
		return nil
	}

	if len(tables) <= batchSize {
		return db.insertSQLTablesBatch(tables)
	}

	for i := 0; i < len(tables); i += batchSize {
		end := i + batchSize
		if end > len(tables) {
			end = len(tables)
		}

		batch := tables[i:end]
		if err := db.insertSQLTablesBatch(batch); err != nil {
			return err
		}
	}
	return nil
}

// insertSQLTablesBatch вставляет одну пачку SQL таблиц
func (db *DB) insertSQLTablesBatch(tables []*model.SQLTable) error {
	if len(tables) == 0 {
		return nil
	}

	return db.withCopyInTx(func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(pq.CopyIn("sql_tables", "file_id", "table_name", "context", "is_temporary", "line_number", "column_number"))
		if err != nil {
			return err
		}
		defer stmt.Close()

		for _, table := range tables {
			_, err := stmt.Exec(
				table.FileID,
				sanitizeUTF8String(table.TableName),
				sanitizeUTF8String(table.Context),
				table.IsTemporary,
				table.LineNumber,
				NullableInt(table.ColNumber),
			)
			if err != nil {
				return err
			}
		}

		_, err = stmt.Exec()
		return err
	})
}

// BatchInsertSQLColumns пакетная вставка SQL колонок
func (db *DB) BatchInsertSQLColumns(columns []*model.SQLColumn, batchSize int) error {
	if len(columns) == 0 {
		return nil
	}

	if len(columns) <= batchSize {
		return db.insertSQLColumnsBatch(columns)
	}

	for i := 0; i < len(columns); i += batchSize {
		end := i + batchSize
		if end > len(columns) {
			end = len(columns)
		}

		batch := columns[i:end]
		if err := db.insertSQLColumnsBatch(batch); err != nil {
			return err
		}
	}
	return nil
}

// insertSQLColumnsBatch вставляет одну пачку SQL колонок
func (db *DB) insertSQLColumnsBatch(columns []*model.SQLColumn) error {
	if len(columns) == 0 {
		return nil
	}

	return db.withCopyInTx(func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(pq.CopyIn("sql_columns", "file_id", "table_name", "column_name", "line_number", "column_number"))
		if err != nil {
			return err
		}
		defer stmt.Close()

		for _, column := range columns {
			_, err := stmt.Exec(
				column.FileID,
				sanitizeUTF8String(column.TableName),
				sanitizeUTF8String(column.ColumnName),
				column.LineNumber,
				column.ColNumber,
			)
			if err != nil {
				return err
			}
		}

		_, err = stmt.Exec()
		return err
	})
}

// BatchInsertSQLColumnDefinitions пакетная вставка определений SQL колонок
func (db *DB) BatchInsertSQLColumnDefinitions(columns []*model.SQLColumnDefinition, batchSize int) error {
	if len(columns) == 0 {
		return nil
	}

	if len(columns) <= batchSize {
		return db.insertSQLColumnDefinitionsBatch(columns)
	}

	for i := 0; i < len(columns); i += batchSize {
		end := i + batchSize
		if end > len(columns) {
			end = len(columns)
		}

		batch := columns[i:end]
		if err := db.insertSQLColumnDefinitionsBatch(batch); err != nil {
			return err
		}
	}
	return nil
}

// insertSQLColumnDefinitionsBatch вставляет одну пачку определений SQL колонок
func (db *DB) insertSQLColumnDefinitionsBatch(columns []*model.SQLColumnDefinition) error {
	if len(columns) == 0 {
		return nil
	}

	return db.withCopyInTx(func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(pq.CopyIn("sql_column_definitions", "file_id", "table_name", "column_name", "data_type", "definition_kind", "line_number", "column_order"))
		if err != nil {
			return err
		}
		defer stmt.Close()

		for _, column := range columns {
			_, err := stmt.Exec(
				column.FileID,
				sanitizeUTF8String(column.TableName),
				sanitizeUTF8String(column.ColumnName),
				sanitizeUTF8String(column.DataType),
				sanitizeUTF8String(column.DefinitionKind),
				column.LineNumber,
				column.ColumnOrder,
			)
			if err != nil {
				return err
			}
		}

		_, err = stmt.Exec()
		return err
	})
}

func (db *DB) FindLatestSQLColumnDefinitionType(tableName string, columnName string) (string, error) {
	var dataType string
	err := db.QueryRow(`
		SELECT data_type
		FROM sql_column_definitions
		WHERE LOWER(table_name) = LOWER($1)
		  AND LOWER(column_name) = LOWER($2)
		  AND TRIM(COALESCE(data_type, '')) <> ''
		ORDER BY id DESC
		LIMIT 1
	`, strings.TrimSpace(tableName), strings.TrimSpace(columnName)).Scan(&dataType)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(dataType), nil
}

// FindSQLIndexDefinitionIDsByFile возвращает id SQL-индексов по file_id.
func (db *DB) FindSQLIndexDefinitionIDsByFile(fileID int64) (map[string]int64, error) {
	rows, err := db.Query(`
		SELECT id, table_name, index_name, line_number
		FROM sql_index_definitions
		WHERE file_id = $1
		ORDER BY id DESC
	`, fileID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]int64)
	for rows.Next() {
		var id int64
		var tableName string
		var indexName string
		var lineNumber int
		if err := rows.Scan(&id, &tableName, &indexName, &lineNumber); err != nil {
			return nil, err
		}
		key := BuildSQLIndexDefinitionLookupKey(tableName, indexName, lineNumber)
		if _, exists := result[key]; !exists {
			result[key] = id
		}
	}
	return result, rows.Err()
}

// BatchInsertSQLIndexDefinitions пакетная вставка определений SQL индексов.
func (db *DB) BatchInsertSQLIndexDefinitions(items []*model.SQLIndexDefinition, batchSize int) error {
	if len(items) == 0 {
		return nil
	}
	if len(items) <= batchSize {
		return db.insertSQLIndexDefinitionsBatch(items)
	}
	for i := 0; i < len(items); i += batchSize {
		end := i + batchSize
		if end > len(items) {
			end = len(items)
		}
		if err := db.insertSQLIndexDefinitionsBatch(items[i:end]); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) insertSQLIndexDefinitionsBatch(items []*model.SQLIndexDefinition) error {
	if len(items) == 0 {
		return nil
	}
	return db.withCopyInTx(func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(pq.CopyIn("sql_index_definitions", "file_id", "table_name", "index_name", "index_fields", "index_type", "is_unique", "definition_kind", "line_number"))
		if err != nil {
			return err
		}
		defer stmt.Close()
		for _, item := range items {
			if _, err := stmt.Exec(item.FileID, sanitizeUTF8String(item.TableName), sanitizeUTF8String(item.IndexName), NullableString(item.IndexFields), NullableString(item.IndexType), item.IsUnique, sanitizeUTF8String(item.DefinitionKind), item.LineNumber); err != nil {
				return err
			}
		}
		_, err = stmt.Exec()
		return err
	})
}

// BatchInsertSQLIndexDefinitionFields пакетная вставка полей SQL индексов.
func (db *DB) BatchInsertSQLIndexDefinitionFields(items []*model.SQLIndexDefinitionField, batchSize int) error {
	if len(items) == 0 {
		return nil
	}
	if len(items) <= batchSize {
		return db.insertSQLIndexDefinitionFieldsBatch(items)
	}
	for i := 0; i < len(items); i += batchSize {
		end := i + batchSize
		if end > len(items) {
			end = len(items)
		}
		if err := db.insertSQLIndexDefinitionFieldsBatch(items[i:end]); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) insertSQLIndexDefinitionFieldsBatch(items []*model.SQLIndexDefinitionField) error {
	if len(items) == 0 {
		return nil
	}
	return db.withCopyInTx(func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(pq.CopyIn("sql_index_definition_fields", "table_index_id", "field_name", "field_order", "line_number"))
		if err != nil {
			return err
		}
		defer stmt.Close()
		for _, item := range items {
			if _, err := stmt.Exec(item.TableIndexID, sanitizeUTF8String(item.FieldName), item.FieldOrder, item.LineNumber); err != nil {
				return err
			}
		}
		_, err = stmt.Exec()
		return err
	})
}

// BatchInsertPASUnits пакетная вставка PAS юнитов
func (db *DB) BatchInsertPASUnits(units []*model.PASUnit, batchSize int) error {
	if len(units) == 0 {
		return nil
	}

	if len(units) <= batchSize {
		return db.insertPASUnitsBatch(units)
	}

	for i := 0; i < len(units); i += batchSize {
		end := i + batchSize
		if end > len(units) {
			end = len(units)
		}

		batch := units[i:end]
		if err := db.insertPASUnitsBatch(batch); err != nil {
			return err
		}
	}
	return nil
}

// insertPASUnitsBatch вставляет одну пачку PAS юнитов
func (db *DB) insertPASUnitsBatch(units []*model.PASUnit) error {
	if len(units) == 0 {
		return nil
	}

	return db.withCopyInTx(func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(pq.CopyIn("pas_units", "file_id", "unit_name", "interface_uses", "implementation_uses", "line_start", "line_end"))
		if err != nil {
			return err
		}
		defer stmt.Close()

		for _, unit := range units {
			var interfaceUsesJSON, implementationUsesJSON interface{}

			if len(unit.InterfaceUses) > 0 {
				data, err := json.Marshal(unit.InterfaceUses)
				if err != nil {
					return err
				}
				interfaceUsesJSON = string(data)
			}
			if len(unit.ImplementationUses) > 0 {
				data, err := json.Marshal(unit.ImplementationUses)
				if err != nil {
					return err
				}
				implementationUsesJSON = string(data)
			}

			_, err := stmt.Exec(
				unit.FileID,
				sanitizeUTF8String(unit.UnitName),
				sanitizeNullableJSON(interfaceUsesJSON),
				sanitizeNullableJSON(implementationUsesJSON),
				unit.LineStart,
				unit.LineEnd,
			)
			if err != nil {
				return err
			}
		}

		_, err = stmt.Exec()
		return err
	})
}

// BatchInsertPASClasses пакетная вставка PAS классов
func (db *DB) BatchInsertPASClasses(classes []*model.PASClass, batchSize int) error {
	if len(classes) == 0 {
		return nil
	}

	if len(classes) <= batchSize {
		return db.insertPASClassesBatch(classes)
	}

	for i := 0; i < len(classes); i += batchSize {
		end := i + batchSize
		if end > len(classes) {
			end = len(classes)
		}

		batch := classes[i:end]
		if err := db.insertPASClassesBatch(batch); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) insertPASClassesBatch(classes []*model.PASClass) error {
	if len(classes) == 0 {
		return nil
	}

	return db.withCopyInTx(func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(pq.CopyIn("pas_classes", "unit_id", "class_name", "parent_class", "dfm_form_id", "line_start", "line_end"))
		if err != nil {
			return err
		}
		defer stmt.Close()

		for _, class := range classes {
			_, err := stmt.Exec(
				NullableInt64(class.UnitID),
				sanitizeUTF8String(class.ClassName),
				NullableString(class.ParentClass),
				NullableInt64(class.DFMFormID),
				class.LineStart,
				class.LineEnd,
			)
			if err != nil {
				return err
			}
		}

		_, err = stmt.Exec()
		return err
	})
}

// BatchInsertPASMethods пакетная вставка PAS методов
func (db *DB) BatchInsertPASMethods(methods []*model.PASMethod, batchSize int) error {
	if len(methods) == 0 {
		return nil
	}

	if len(methods) <= batchSize {
		return db.insertPASMethodsBatch(methods)
	}

	for i := 0; i < len(methods); i += batchSize {
		end := i + batchSize
		if end > len(methods) {
			end = len(methods)
		}

		batch := methods[i:end]
		if err := db.insertPASMethodsBatch(batch); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) insertPASMethodsBatch(methods []*model.PASMethod) error {
	if len(methods) == 0 {
		return nil
	}

	return db.withCopyInTx(func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(pq.CopyIn("pas_methods", "class_id", "unit_id", "method_name", "signature", "visibility", "line_number"))
		if err != nil {
			return err
		}
		defer stmt.Close()

		for _, method := range methods {
			_, err := stmt.Exec(
				NullableInt64(method.ClassID),
				NullableInt64(method.UnitID),
				sanitizeUTF8String(method.MethodName),
				NullableString(method.Signature),
				NullableString(method.Visibility),
				method.LineNumber,
			)
			if err != nil {
				return err
			}
		}

		_, err = stmt.Exec()
		return err
	})
}

// BatchInsertPASFields пакетная вставка PAS полей
func (db *DB) BatchInsertPASFields(fields []*model.PASField, batchSize int) error {
	if len(fields) == 0 {
		return nil
	}

	if len(fields) <= batchSize {
		return db.insertPASFieldsBatch(fields)
	}

	for i := 0; i < len(fields); i += batchSize {
		end := i + batchSize
		if end > len(fields) {
			end = len(fields)
		}

		batch := fields[i:end]
		if err := db.insertPASFieldsBatch(batch); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) insertPASFieldsBatch(fields []*model.PASField) error {
	if len(fields) == 0 {
		return nil
	}

	return db.withCopyInTx(func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(pq.CopyIn("pas_fields", "class_id", "field_name", "field_type", "dfm_component_id", "visibility", "line_number"))
		if err != nil {
			return err
		}
		defer stmt.Close()

		for _, field := range fields {
			_, err := stmt.Exec(
				NullableInt64(field.ClassID),
				sanitizeUTF8String(field.FieldName),
				NullableString(field.FieldType),
				NullableInt64(field.DFMComponentID),
				NullableString(field.Visibility),
				field.LineNumber,
			)
			if err != nil {
				return err
			}
		}

		_, err = stmt.Exec()
		return err
	})
}

// BatchInsertHDefines пакетная вставка H define-ов
func (db *DB) BatchInsertHDefines(defines []*model.HDefine, batchSize int) error {
	if len(defines) == 0 {
		return nil
	}

	if len(defines) <= batchSize {
		return db.insertHDefinesBatch(defines)
	}

	for i := 0; i < len(defines); i += batchSize {
		end := i + batchSize
		if end > len(defines) {
			end = len(defines)
		}

		batch := defines[i:end]
		if err := db.insertHDefinesBatch(batch); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) insertHDefinesBatch(defines []*model.HDefine) error {
	if len(defines) == 0 {
		return nil
	}

	return db.withCopyInTx(func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(pq.CopyIn("h_files_defines", "file_id", "define_name", "define_value", "define_type", "line_number"))
		if err != nil {
			return err
		}
		defer stmt.Close()

		for _, define := range defines {
			_, err := stmt.Exec(
				define.FileID,
				sanitizeUTF8String(define.DefineName),
				sanitizeUTF8String(define.DefineValue),
				sanitizeUTF8String(define.DefineType),
				define.LineNumber,
			)
			if err != nil {
				return err
			}
		}

		_, err = stmt.Exec()
		return err
	})
}

// BatchInsertAPIMacroInvocations пакетная вставка APIMacroInvocations
func (db *DB) BatchInsertAPIMacroInvocations(invocations []*model.APIMacroInvocation, batchSize int) error {
	if len(invocations) == 0 {
		return nil
	}

	if len(invocations) <= batchSize {
		return db.insertAPIMacroInvocationsBatch(invocations)
	}

	for i := 0; i < len(invocations); i += batchSize {
		end := i + batchSize
		if end > len(invocations) {
			end = len(invocations)
		}

		batch := invocations[i:end]
		if err := db.insertAPIMacroInvocationsBatch(batch); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) insertAPIMacroInvocationsBatch(invocations []*model.APIMacroInvocation) error {
	if len(invocations) == 0 {
		return nil
	}

	return db.withCopyInTx(func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(pq.CopyIn("api_macro_invocations", "file_id", "procedure_name", "macro_type", "target_name", "target_kind", "line_number", "raw_text"))
		if err != nil {
			return err
		}
		defer stmt.Close()

		for _, invocation := range invocations {
			_, err := stmt.Exec(
				invocation.FileID,
				NullableString(invocation.ProcedureName),
				sanitizeUTF8String(invocation.MacroType),
				sanitizeUTF8String(invocation.TargetName),
				sanitizeUTF8String(invocation.TargetKind),
				invocation.LineNumber,
				NullableString(invocation.RawText),
			)
			if err != nil {
				return err
			}
		}

		_, err = stmt.Exec()
		return err
	})
}

// BatchInsertJSFunctions пакетная вставка JS функций
func (db *DB) BatchInsertJSFunctions(functions []*model.JSFunction, batchSize int) error {
	if len(functions) == 0 {
		return nil
	}

	if len(functions) <= batchSize {
		return db.insertJSFunctionsBatch(functions)
	}

	for i := 0; i < len(functions); i += batchSize {
		end := i + batchSize
		if end > len(functions) {
			end = len(functions)
		}

		batch := functions[i:end]
		if err := db.insertJSFunctionsBatch(batch); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) insertJSFunctionsBatch(functions []*model.JSFunction) error {
	if len(functions) == 0 {
		return nil
	}

	return db.withCopyInTx(func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(pq.CopyIn("js_functions", "file_id", "function_name", "signature", "line_start", "line_end", "scenario_type", "parent_object"))
		if err != nil {
			return err
		}
		defer stmt.Close()

		for _, fn := range functions {
			_, err := stmt.Exec(
				fn.FileID,
				sanitizeUTF8String(fn.FunctionName),
				NullableString(fn.Signature),
				fn.LineStart,
				fn.LineEnd,
				NullableString(fn.ScenarioType),
				NullableString(fn.ParentObject),
			)
			if err != nil {
				return err
			}
		}

		_, err = stmt.Exec()
		return err
	})
}

// BatchInsertReportForms пакетная вставка report forms.
func (db *DB) BatchInsertReportForms(forms []*model.ReportForm, batchSize int) error {
	if len(forms) == 0 {
		return nil
	}

	if len(forms) <= batchSize {
		return db.insertReportFormsBatch(forms)
	}

	for i := 0; i < len(forms); i += batchSize {
		end := i + batchSize
		if end > len(forms) {
			end = len(forms)
		}

		batch := forms[i:end]
		if err := db.insertReportFormsBatch(batch); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) insertReportFormsBatch(forms []*model.ReportForm) error {
	if len(forms) == 0 {
		return nil
	}

	return db.withCopyInTx(func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(pq.CopyIn("report_forms", "file_id", "report_name", "report_type", "form_name", "form_class", "line_start", "line_end"))
		if err != nil {
			return err
		}
		defer stmt.Close()

		for _, form := range forms {
			_, err := stmt.Exec(
				form.FileID,
				sanitizeUTF8String(form.ReportName),
				sanitizeUTF8String(form.ReportType),
				NullableString(form.FormName),
				NullableString(form.FormClass),
				form.LineStart,
				form.LineEnd,
			)
			if err != nil {
				return err
			}
		}

		_, err = stmt.Exec()
		return err
	})
}

// BatchInsertReportFields пакетная вставка report fields.
func (db *DB) BatchInsertReportFields(fields []*model.ReportField, batchSize int) error {
	if len(fields) == 0 {
		return nil
	}

	if len(fields) <= batchSize {
		return db.insertReportFieldsBatch(fields)
	}

	for i := 0; i < len(fields); i += batchSize {
		end := i + batchSize
		if end > len(fields) {
			end = len(fields)
		}

		batch := fields[i:end]
		if err := db.insertReportFieldsBatch(batch); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) insertReportFieldsBatch(fields []*model.ReportField) error {
	if len(fields) == 0 {
		return nil
	}

	return db.withCopyInTx(func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(pq.CopyIn("report_fields", "report_form_id", "field_name", "source_name", "format_mask", "options", "line_number", "raw_text"))
		if err != nil {
			return err
		}
		defer stmt.Close()

		for _, field := range fields {
			var optionsJSON interface{}
			if len(field.Options) > 0 {
				data, err := json.Marshal(field.Options)
				if err != nil {
					return err
				}
				optionsJSON = string(data)
			}

			_, err := stmt.Exec(
				field.ReportFormID,
				sanitizeUTF8String(field.FieldName),
				NullableString(field.SourceName),
				NullableString(field.FormatMask),
				sanitizeNullableJSON(optionsJSON),
				field.LineNumber,
				NullableString(field.RawText),
			)
			if err != nil {
				return err
			}
		}

		_, err = stmt.Exec()
		return err
	})
}

// BatchInsertReportParams пакетная вставка report params.
func (db *DB) BatchInsertReportParams(params []*model.ReportParam, batchSize int) error {
	if len(params) == 0 {
		return nil
	}

	if len(params) <= batchSize {
		return db.insertReportParamsBatch(params)
	}

	for i := 0; i < len(params); i += batchSize {
		end := i + batchSize
		if end > len(params) {
			end = len(params)
		}

		batch := params[i:end]
		if err := db.insertReportParamsBatch(batch); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) insertReportParamsBatch(params []*model.ReportParam) error {
	if len(params) == 0 {
		return nil
	}

	return db.withCopyInTx(func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(pq.CopyIn("report_params", "report_form_id", "param_name", "param_kind", "component_type", "data_type", "lookup_form", "lookup_table", "lookup_column", "key_column", "required", "default_value", "line_number", "raw_text"))
		if err != nil {
			return err
		}
		defer stmt.Close()

		for _, param := range params {
			_, err := stmt.Exec(
				param.ReportFormID,
				sanitizeUTF8String(param.ParamName),
				sanitizeUTF8String(param.ParamKind),
				NullableString(param.ComponentType),
				NullableString(param.DataType),
				NullableString(param.LookupForm),
				NullableString(param.LookupTable),
				NullableString(param.LookupColumn),
				NullableString(param.KeyColumn),
				param.Required,
				NullableString(param.DefaultValue),
				param.LineNumber,
				NullableString(param.RawText),
			)
			if err != nil {
				return err
			}
		}

		_, err = stmt.Exec()
		return err
	})
}

// BatchInsertVBFunctions пакетная вставка vb functions.
func (db *DB) BatchInsertVBFunctions(functions []*model.VBFunction, batchSize int) error {
	if len(functions) == 0 {
		return nil
	}

	if len(functions) <= batchSize {
		return db.insertVBFunctionsBatch(functions)
	}

	for i := 0; i < len(functions); i += batchSize {
		end := i + batchSize
		if end > len(functions) {
			end = len(functions)
		}

		batch := functions[i:end]
		if err := db.insertVBFunctionsBatch(batch); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) insertVBFunctionsBatch(functions []*model.VBFunction) error {
	if len(functions) == 0 {
		return nil
	}

	return db.withCopyInTx(func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(pq.CopyIn("vb_functions", "report_form_id", "function_name", "function_type", "signature", "body_text", "line_start", "line_end", "body_hash"))
		if err != nil {
			return err
		}
		defer stmt.Close()

		for _, fn := range functions {
			_, err := stmt.Exec(
				fn.ReportFormID,
				sanitizeUTF8String(fn.FunctionName),
				sanitizeUTF8String(fn.FunctionType),
				NullableString(fn.Signature),
				NullableString(fn.BodyText),
				fn.LineStart,
				fn.LineEnd,
				NullableString(fn.BodyHash),
			)
			if err != nil {
				return err
			}
		}

		_, err = stmt.Exec()
		return err
	})
}

// BatchInsertSMFInstruments пакетная вставка SMF инструментов
func (db *DB) BatchInsertSMFInstruments(instruments []*model.SMFInstrument, batchSize int) error {
	if len(instruments) == 0 {
		return nil
	}

	if len(instruments) <= batchSize {
		return db.insertSMFInstrumentsBatch(instruments)
	}

	for i := 0; i < len(instruments); i += batchSize {
		end := i + batchSize
		if end > len(instruments) {
			end = len(instruments)
		}

		batch := instruments[i:end]
		if err := db.insertSMFInstrumentsBatch(batch); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) insertSMFInstrumentsBatch(instruments []*model.SMFInstrument) error {
	if len(instruments) == 0 {
		return nil
	}

	return db.withCopyInTx(func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(pq.CopyIn("smf_instruments", "file_id", "instrument_name", "brief", "deal_object_id", "ds_module_id", "start_state", "scenario_type", "states", "actions", "accounts"))
		if err != nil {
			return err
		}
		defer stmt.Close()

		for _, instrument := range instruments {
			statesJSON, err := json.Marshal(instrument.States)
			if err != nil {
				return err
			}
			actionsJSON, err := json.Marshal(instrument.Actions)
			if err != nil {
				return err
			}
			accountsJSON, err := json.Marshal(instrument.Accounts)
			if err != nil {
				return err
			}

			_, err = stmt.Exec(
				instrument.FileID,
				sanitizeUTF8String(instrument.InstrumentName),
				NullableString(instrument.Brief),
				NullableInt64(instrument.DealObjectID),
				NullableInt64(instrument.DsModuleID),
				NullableString(instrument.StartState),
				NullableString(instrument.ScenarioType),
				sanitizeUTF8String(string(statesJSON)),
				sanitizeUTF8String(string(actionsJSON)),
				sanitizeUTF8String(string(accountsJSON)),
			)
			if err != nil {
				return err
			}
		}

		_, err = stmt.Exec()
		return err
	})
}

// BatchInsertDFMForms пакетная вставка DFM форм
func (db *DB) BatchInsertDFMForms(forms []*model.DFMForm, batchSize int) error {
	if len(forms) == 0 {
		return nil
	}

	if len(forms) <= batchSize {
		return db.insertDFMFormsBatch(forms)
	}

	for i := 0; i < len(forms); i += batchSize {
		end := i + batchSize
		if end > len(forms) {
			end = len(forms)
		}

		batch := forms[i:end]
		if err := db.insertDFMFormsBatch(batch); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) insertDFMFormsBatch(forms []*model.DFMForm) error {
	if len(forms) == 0 {
		return nil
	}

	return db.withCopyInTx(func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(pq.CopyIn("dfm_forms", "file_id", "form_name", "form_class", "caption", "line_start", "line_end"))
		if err != nil {
			return err
		}
		defer stmt.Close()

		for _, form := range forms {
			_, err := stmt.Exec(
				form.FileID,
				sanitizeUTF8String(form.FormName),
				sanitizeUTF8String(form.FormClass),
				NullableString(form.Caption),
				form.LineStart,
				form.LineEnd,
			)
			if err != nil {
				return err
			}
		}

		_, err = stmt.Exec()
		return err
	})
}

// BatchInsertQueryFragments пакетная вставка SQL фрагментов
func (db *DB) BatchInsertQueryFragments(fragments []*model.QueryFragment, batchSize int) error {
	if len(fragments) == 0 {
		return nil
	}

	if len(fragments) <= batchSize {
		return db.insertQueryFragmentsBatch(fragments)
	}

	for i := 0; i < len(fragments); i += batchSize {
		end := i + batchSize
		if end > len(fragments) {
			end = len(fragments)
		}

		batch := fragments[i:end]
		if err := db.insertQueryFragmentsBatch(batch); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) insertQueryFragmentsBatch(fragments []*model.QueryFragment) error {
	if len(fragments) == 0 {
		return nil
	}

	return db.withCopyInTx(func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(pq.CopyIn("query_fragments", "file_id", "parent_type", "parent_id", "component_name", "component_type", "query_text", "query_hash", "tables_referenced", "context", "line_number"))
		if err != nil {
			return err
		}
		defer stmt.Close()

		for _, fragment := range fragments {
			var tablesReferencedJSON interface{}
			if len(fragment.TablesReferenced) > 0 {
				data, err := json.Marshal(fragment.TablesReferenced)
				if err != nil {
					return err
				}
				tablesReferencedJSON = string(data)
			}

			_, err := stmt.Exec(
				fragment.FileID,
				sanitizeUTF8String(fragment.ParentType),
				fragment.ParentID,
				sanitizeUTF8String(fragment.ComponentName),
				sanitizeUTF8String(fragment.ComponentType),
				sanitizeUTF8String(fragment.QueryText),
				NullableString(fragment.QueryHash),
				sanitizeNullableJSON(tablesReferencedJSON),
				sanitizeUTF8String(fragment.Context),
				fragment.LineNumber,
			)
			if err != nil {
				return err
			}
		}

		_, err = stmt.Exec()
		return err
	})
}

// BatchInsertRelations пакетная вставка связей между сущностями.
func (db *DB) BatchInsertRelations(relations []*model.Relation, batchSize int) error {
	if len(relations) == 0 {
		return nil
	}

	if len(relations) <= batchSize {
		return db.insertRelationsBatch(relations)
	}

	for i := 0; i < len(relations); i += batchSize {
		end := i + batchSize
		if end > len(relations) {
			end = len(relations)
		}

		batch := relations[i:end]
		if err := db.insertRelationsBatch(batch); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) insertRelationsBatch(relations []*model.Relation) error {
	if len(relations) == 0 {
		return nil
	}

	return db.withCopyInTx(func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(pq.CopyIn("relations", "source_type", "source_id", "target_type", "target_id", "relation_type", "confidence", "line_number"))
		if err != nil {
			return err
		}
		defer stmt.Close()

		for _, relation := range relations {
			_, err := stmt.Exec(
				relation.SourceType,
				relation.SourceID,
				relation.TargetType,
				relation.TargetID,
				relation.RelationType,
				NullableString(relation.Confidence),
				relation.LineNumber,
			)
			if err != nil {
				return err
			}
		}

		_, err = stmt.Exec()
		return err
	})
}

// Вспомогательные функции для NULL значений
func sanitizeUTF8String(value string) string {
	if value == "" {
		return value
	}
	return strings.ToValidUTF8(value, "")
}

func sanitizeNullableJSON(value interface{}) interface{} {
	if value == nil {
		return nil
	}
	text, ok := value.(string)
	if !ok {
		return value
	}
	trimmed := strings.TrimSpace(text)
	if trimmed == "" {
		return nil
	}
	return sanitizeUTF8String(text)
}

func NullableString(value string) interface{} {
	value = sanitizeUTF8String(value)
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return value
}

func NullableInt(value int) interface{} {
	if value == 0 {
		return nil
	}
	return value
}

func NullableInt64(value int64) interface{} {
	if value == 0 {
		return nil
	}
	return value
}

func NullableProcID(procID int64) interface{} {
	if procID == 0 {
		return nil
	}
	return procID
}
