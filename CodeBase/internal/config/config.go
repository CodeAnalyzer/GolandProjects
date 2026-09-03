package config

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/pelletier/go-toml/v2"
)

// DBConfig конфигурация подключения к PostgreSQL
type DBConfig struct {
	Host            string `toml:"host"`
	Port            int    `toml:"port"`
	Database        string `toml:"database"`
	User            string `toml:"user"`
	Password        string `toml:"password"`
	SSLMode         string `toml:"sslmode"`
	ConnectTimeout  int    `toml:"connect_timeout"` // секунды
	MaxOpenConns    int    `toml:"max_open_conns"`
	MaxIdleConns    int    `toml:"max_idle_conns"`
	ConnMaxLifetime string `toml:"conn_max_lifetime"` // Go duration: "5m", "30s", "1h"
}

// Config полная конфигурация приложения
type Config struct {
	RootPath string        `toml:"root_path"`
	DB       DBConfig      `toml:"database"`
	Indexer  IndexerConfig `toml:"indexer"`
	Query    QueryConfig   `toml:"query"`
	RTI      RTIConfig     `toml:"rti"`
	TRC      TRCConfig     `toml:"trc"`
	Logging  LoggingConfig `toml:"logging"`
	MCP      MCPConfig     `toml:"mcp"`
}

// IndexerConfig конфигурация индексатора
type IndexerConfig struct {
	Parallel            int      `toml:"parallel"`
	BatchSize           int      `toml:"batch_size"`
	BatchInsertSize     int      `toml:"batch_insert_size"`
	ProgressIntervalMs  int      `toml:"progress_interval_ms"`
	IncludePatterns     []string `toml:"include_patterns"`
	ExcludePatterns     []string `toml:"exclude_patterns"`
}

// QueryConfig лимиты для query/rti/trc вызовов
type QueryConfig struct {
	DefaultLimit int `toml:"default_limit"`
	MaxLimit     int `toml:"max_limit"`
}

// RTIConfig настройки RTI-анализатора
type RTIConfig struct {
	SlowThresholdMs int `toml:"slow_threshold_ms"`
	TopSlowCount    int `toml:"top_slow_count"`
	ParseTimeoutSec int `toml:"parse_timeout_sec"` // таймаут MCP codebase_rti_parse (0 = без таймаута)
}

// TRCConfig настройки TRC-анализатора
type TRCConfig struct {
	SlowThresholdMs           int `toml:"slow_threshold_ms"`
	MaxEnrichWorkers          int `toml:"max_enrich_workers"`
	MinProcsForParallelEnrich int `toml:"min_procs_for_parallel_enrich"`
	ParseTimeoutSec           int `toml:"parse_timeout_sec"` // таймаут MCP codebase_trc_parse (0 = без таймаута)
}

type LoggingConfig struct {
	CommandEnabled *bool `toml:"command_enabled"`
}

// MCPConfig конфигурация MCP-сервера
type MCPConfig struct {
	PaginationChunkSize   int    `toml:"pagination_chunk_size"`
	PaginationTTL         string `toml:"pagination_ttl"` // Go duration: "15m", "30m"
	QueryTimeoutSec       int    `toml:"query_timeout_sec"`  // default 30
	ReviewTimeoutSec      int    `toml:"review_timeout_sec"` // default 120
	RegexpCacheMaxEntries int    `toml:"regexp_cache_max_entries"`
}

var (
	cfg        *Config
	configFile string
)

// SetConfigFile устанавливает путь к файлу конфигурации
func SetConfigFile(path string) {
	configFile = path
}

// GetConfigFile возвращает путь к файлу конфигурации
func GetConfigFile() string {
	return configFile
}

// Load загружает конфигурацию из файла
func Load() error {
	if configFile == "" {
		// Если путь не задан явно, пробуем стандартное имя рядом с исполняемым файлом.
		if executablePath, err := os.Executable(); err == nil {
			executableConfigPath := filepath.Join(filepath.Dir(executablePath), "codebase.toml")
			if _, err := os.Stat(executableConfigPath); err == nil {
				configFile = executableConfigPath
			}
		}
	}

	if configFile == "" {
		return os.ErrNotExist
	}

	data, err := os.ReadFile(configFile)
	if err != nil {
		return err
	}

	cfg = &Config{}
	if err := toml.Unmarshal(data, cfg); err != nil {
		return fmt.Errorf("failed to parse config: %w", err)
	}

	cfg.RootPath = filepath.FromSlash(cfg.RootPath)

	// Дефолты заполняют только отсутствующие значения, не затирая явно заданную конфигурацию.
	if cfg.DB.Host == "" {
		cfg.DB.Host = "localhost"
	}
	if cfg.DB.Port == 0 {
		cfg.DB.Port = 5435
	}
	if cfg.DB.Database == "" {
		cfg.DB.Database = "codebase"
	}
	if cfg.DB.User == "" {
		cfg.DB.User = "postgres"
	}
	if cfg.DB.SSLMode == "" {
		cfg.DB.SSLMode = "disable"
	}
	if cfg.DB.ConnectTimeout <= 0 {
		cfg.DB.ConnectTimeout = 10
	}
	if cfg.DB.MaxOpenConns <= 0 {
		cfg.DB.MaxOpenConns = 25
	}
	if cfg.DB.MaxIdleConns <= 0 {
		cfg.DB.MaxIdleConns = 25
	}
	if cfg.DB.ConnMaxLifetime == "" {
		cfg.DB.ConnMaxLifetime = "5m"
	}
	if cfg.Indexer.Parallel == 0 {
		cfg.Indexer.Parallel = 4
	}
	if cfg.Indexer.BatchSize == 0 {
		cfg.Indexer.BatchSize = 100
	}
	if cfg.Indexer.BatchInsertSize <= 0 {
		cfg.Indexer.BatchInsertSize = 50000
	}
	if cfg.Indexer.ProgressIntervalMs <= 0 {
		cfg.Indexer.ProgressIntervalMs = 250
	}
	if cfg.Query.DefaultLimit <= 0 {
		cfg.Query.DefaultLimit = 100
	}
	if cfg.Query.MaxLimit <= 0 {
		cfg.Query.MaxLimit = 1000
	}
	if cfg.RTI.SlowThresholdMs <= 0 {
		cfg.RTI.SlowThresholdMs = 100
	}
	if cfg.RTI.TopSlowCount <= 0 {
		cfg.RTI.TopSlowCount = 10
	}
	if cfg.RTI.ParseTimeoutSec <= 0 {
		cfg.RTI.ParseTimeoutSec = 300
	}
	if cfg.TRC.SlowThresholdMs <= 0 {
		cfg.TRC.SlowThresholdMs = 100
	}
	if cfg.TRC.MaxEnrichWorkers <= 0 {
		cfg.TRC.MaxEnrichWorkers = 16
	}
	if cfg.TRC.MinProcsForParallelEnrich <= 0 {
		cfg.TRC.MinProcsForParallelEnrich = 16
	}
	if cfg.TRC.ParseTimeoutSec <= 0 {
		cfg.TRC.ParseTimeoutSec = 300
	}
	if len(cfg.Indexer.IncludePatterns) == 0 {
		cfg.Indexer.IncludePatterns = []string{
			"*.sql", "*.h", "*.pas", "*.inc", "*.js", "*.smf", "*.dfm", "*.tpr", "*.rpt",
		}
	}
	if len(cfg.Indexer.ExcludePatterns) == 0 {
		cfg.Indexer.ExcludePatterns = []string{
			"*/.*", "*~", "*.bak", "*.old",
		}
	}
	if cfg.Logging.CommandEnabled == nil {
		enabled := true
		cfg.Logging.CommandEnabled = &enabled
	}
	if cfg.MCP.PaginationChunkSize == 0 {
		cfg.MCP.PaginationChunkSize = 8_000
	}
	if cfg.MCP.RegexpCacheMaxEntries <= 0 {
		cfg.MCP.RegexpCacheMaxEntries = 2048
	}
	if cfg.MCP.PaginationTTL == "" {
		cfg.MCP.PaginationTTL = "15m"
	}
	if cfg.MCP.QueryTimeoutSec <= 0 {
		cfg.MCP.QueryTimeoutSec = 30
	}
	if cfg.MCP.ReviewTimeoutSec <= 0 {
		cfg.MCP.ReviewTimeoutSec = 120
	}

	return nil
}

// Get возвращает текущую конфигурацию
func Get() *Config {
	return cfg
}

// Save сохраняет конфигурацию в файл
func Save(path string) error {
	if cfg == nil {
		cfg = &Config{}
	}

	// Сериализация идёт из текущего in-memory состояния cfg,
	// поэтому вызывающий код может предварительно модифицировать объект через Get().
	data, err := toml.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	// Создаём каталог назначения заранее, чтобы Save работал и для новых путей.
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}

	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("failed to write config: %w", err)
	}

	configFile = path
	return nil
}

// CreateDefault создает конфигурацию по умолчанию
func CreateDefault(rootPath string) *Config {
	// Эта функция формирует стартовый шаблон для первичного запуска init,
	// когда у пользователя ещё нет собственного файла конфигурации.
	cfg = &Config{
		RootPath: rootPath,
		DB: DBConfig{
			Host:            "localhost",
			Port:            5435,
			Database:        "codebase",
			User:            "postgres",
			Password:        "",
			SSLMode:         "disable",
			ConnectTimeout:  10,
			MaxOpenConns:    25,
			MaxIdleConns:    25,
			ConnMaxLifetime: "5m",
		},
		Indexer: IndexerConfig{
			Parallel:           4,
			BatchSize:          100,
			BatchInsertSize:    50000,
			ProgressIntervalMs: 250,
			IncludePatterns: []string{
				"*.sql", "*.h", "*.pas", "*.inc", "*.js", "*.smf", "*.dfm", "*.tpr", "*.rpt",
			},
			ExcludePatterns: []string{
				"*/.*", "*~", "*.bak", "*.old",
			},
		},
		Query: QueryConfig{
			DefaultLimit: 100,
			MaxLimit:     1000,
		},
		RTI: RTIConfig{
			SlowThresholdMs: 100,
			TopSlowCount:    10,
			ParseTimeoutSec: 300,
		},
		TRC: TRCConfig{
			SlowThresholdMs:           100,
			MaxEnrichWorkers:          16,
			MinProcsForParallelEnrich: 16,
			ParseTimeoutSec:           300,
		},
		Logging: LoggingConfig{
			CommandEnabled: boolPtr(true),
		},
		MCP: MCPConfig{
			PaginationChunkSize:   8_000,
			PaginationTTL:         "15m",
			QueryTimeoutSec:       30,
			ReviewTimeoutSec:      120,
			RegexpCacheMaxEntries: 2048,
		},
	}
	return cfg
}

func boolPtr(v bool) *bool {
	return &v
}
