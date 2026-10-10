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
	Spec     SpecConfig    `toml:"spec"`
	DescLSA  DescLSAConfig `toml:"desc_lsa"`
}

// IndexerConfig конфигурация индексатора
type IndexerConfig struct {
	Parallel           int      `toml:"parallel"`
	BatchSize          int      `toml:"batch_size"`
	BatchInsertSize    int      `toml:"batch_insert_size"`
	ProgressIntervalMs int      `toml:"progress_interval_ms"`
	IncludePatterns    []string `toml:"include_patterns"`
	ExcludePatterns    []string `toml:"exclude_patterns"`
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
	ParseTimeoutSec *int `toml:"parse_timeout_sec"` // таймаут MCP codebase_rti_parse; nil = 300, явный 0 = без таймаута
}

// TRCConfig настройки TRC-анализатора
type TRCConfig struct {
	SlowThresholdMs           int `toml:"slow_threshold_ms"`
	MaxEnrichWorkers          int `toml:"max_enrich_workers"`
	MinProcsForParallelEnrich int `toml:"min_procs_for_parallel_enrich"`
	ParseTimeoutSec           *int `toml:"parse_timeout_sec"` // таймаут MCP codebase_trc_parse; nil = 300, явный 0 = без таймаута
}

type LoggingConfig struct {
	CommandEnabled *bool `toml:"command_enabled"`
}

// MCPConfig конфигурация MCP-сервера
type MCPConfig struct {
	PaginationChunkSize   int    `toml:"pagination_chunk_size"`
	PaginationTTL         string `toml:"pagination_ttl"`        // Go duration: "15m", "30m"
	QueryTimeoutSec       *int   `toml:"query_timeout_sec"`     // nil = 30, явный 0 = без таймаута
	ReviewTimeoutSec      *int   `toml:"review_timeout_sec"`    // nil = 120, явный 0 = без таймаута
	RegexpCacheMaxEntries int    `toml:"regexp_cache_max_entries"`
}

// SpecConfig конфигурация полнотекстового слоя OpenSpec (LSA)
type SpecConfig struct {
	LSAEnabled          bool     `toml:"lsa_enabled"`           // включить LSA-постпроцессинг (default: true)
	LSAK                int      `toml:"lsa_k"`                 // размерность LSA (default: 512 — по данным lsaktune поднимает редкие термины в топ-5)
	LSAMinDF            int      `toml:"lsa_min_df"`            // минимальная document frequency (default: 3)
	LSAMaxDF            float64  `toml:"lsa_max_df"`            // максимальная доля документов (default: 0.3)
	LSAMinCorpus        int      `toml:"lsa_min_corpus"`        // минимальный размер корпуса для обучения (default: 100)
	LSARetrainThreshold *int     `toml:"lsa_retrain_threshold"` // порог накопленных изменений для амортизации переобучения; nil = 100, явный 0 = переобучать при любом изменении fingerprint
	LSAModelPath        string   `toml:"lsa_model_path"`        // путь к файлу модели (default: рядом с БД)
	LSAMinCosine        *float64 `toml:"lsa_min_cosine"`        // минимальный cosine для semantic-хита; nil = 0.15, 0 = без абсолютного фильтра
	LSARelativeCutoff   *float64 `toml:"lsa_relative_cutoff"`   // относительный cutoff: доля от maxRank; nil = 0.5, 0 = без relative cutoff
}

// DescLSAConfig конфигурация LSA-модели корпуса описаний (процедуры + контракты).
// Корпус независим от спекового (D8 change add-description-search): собственные
// sidecar-файлы, публикации поколений и параметры.
type DescLSAConfig struct {
	LSAEnabled          *bool    `toml:"lsa_enabled"`           // включить desc-LSA постпроцессинг; nil = true (включено по умолчанию)
	LSAK                int      `toml:"lsa_k"`                 // размерность LSA (default: 512)
	LSAMinDF            int      `toml:"lsa_min_df"`            // минимальная document frequency (default: 10)
	LSAMaxDF            float64  `toml:"lsa_max_df"`            // максимальная доля документов (default: 0.3)
	LSAMinCorpus        int      `toml:"lsa_min_corpus"`        // минимальный размер корпуса (default: 100)
	LSARetrainThreshold *int     `toml:"lsa_retrain_threshold"` // порог амортизации переобучения; nil = 100
	LSAModelPath        string   `toml:"lsa_model_path"`        // путь к файлу модели (default: desc_lsa_model.bin рядом с конфигом)
	LSAMinCosine        *float64 `toml:"lsa_min_cosine"`        // минимальный cosine semantic-хита; nil = 0.15
	LSARelativeCutoff   *float64 `toml:"lsa_relative_cutoff"`   // относительный cutoff; nil = 0.5
}

// Enabled возвращает эффективное состояние desc-LSA: nil = включено.
func (c DescLSAConfig) Enabled() bool {
	if c.LSAEnabled == nil {
		return true
	}
	return *c.LSAEnabled
}

// RetrainThreshold возвращает эффективный порог накопленных изменений.
func (c DescLSAConfig) RetrainThreshold() int {
	if c.LSARetrainThreshold == nil {
		return 100
	}
	return *c.LSARetrainThreshold
}

// MinCosine возвращает эффективное значение абсолютного порога cosine.
func (c DescLSAConfig) MinCosine() float64 {
	if c.LSAMinCosine == nil {
		return 0.15
	}
	return *c.LSAMinCosine
}

// RelativeCutoff возвращает эффективное значение относительного cutoff.
func (c DescLSAConfig) RelativeCutoff() float64 {
	if c.LSARelativeCutoff == nil {
		return 0.5
	}
	return *c.LSARelativeCutoff
}

// RetrainThreshold возвращает эффективный порог накопленных изменений.
// Дефолт 100 — амортизация дорогого SVD (замер: ~97 c на корпусе 2322 capability);
// явный 0 отключает амортизацию («переобучать при любом изменении fingerprint»).
func (s SpecConfig) RetrainThreshold() int {
	if s.LSARetrainThreshold == nil {
		return 100
	}
	return *s.LSARetrainThreshold
}

// MinCosine возвращает эффективное значение абсолютного порога cosine.
func (s SpecConfig) MinCosine() float64 {
	if s.LSAMinCosine == nil {
		return 0.15
	}
	return *s.LSAMinCosine
}

// RelativeCutoff возвращает эффективное значение относительного cutoff.
func (s SpecConfig) RelativeCutoff() float64 {
	if s.LSARelativeCutoff == nil {
		return 0.5
	}
	return *s.LSARelativeCutoff
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

func SpecLSAModelPath() string {
	modelPath := ""
	if cfg != nil {
		modelPath = cfg.Spec.LSAModelPath
	}
	if modelPath == "" {
		modelPath = "spec_lsa_model.bin"
	}
	if filepath.IsAbs(modelPath) {
		return filepath.Clean(modelPath)
	}

	base := "."
	if configFile != "" {
		base = filepath.Dir(configFile)
	}
	if absBase, err := filepath.Abs(base); err == nil {
		base = absBase
	} else {
		base = filepath.Clean(base)
	}
	return filepath.Clean(filepath.Join(base, modelPath))
}

func SpecLSAStatePath() string {
	return filepath.Join(filepath.Dir(SpecLSAModelPath()), "spec_lsa_state.json")
}

// SpecLSAEmbeddingsPath возвращает путь к бинарному кэшу эмбеддингов spec-LSA
// (sidecar-артефакт, инвалидация по поколению в заголовке файла).
func SpecLSAEmbeddingsPath() string {
	return filepath.Join(filepath.Dir(SpecLSAModelPath()), "spec_lsa_embeddings.bin")
}

// DescLSAModelPath возвращает путь к файлу desc-LSA-модели (корпус описаний).
func DescLSAModelPath() string {
	modelPath := ""
	if cfg != nil {
		modelPath = cfg.DescLSA.LSAModelPath
	}
	if modelPath == "" {
		modelPath = "desc_lsa_model.bin"
	}
	if filepath.IsAbs(modelPath) {
		return filepath.Clean(modelPath)
	}

	base := "."
	if configFile != "" {
		base = filepath.Dir(configFile)
	}
	if absBase, err := filepath.Abs(base); err == nil {
		base = absBase
	} else {
		base = filepath.Clean(base)
	}
	return filepath.Clean(filepath.Join(base, modelPath))
}

// DescLSAStatePath возвращает путь к state-файлу desc-LSA-модели.
func DescLSAStatePath() string {
	return filepath.Join(filepath.Dir(DescLSAModelPath()), "desc_lsa_state.json")
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
	// ParseTimeoutSec: nil → дефолт 300 применяется потребителем (timeoutForTool);
	// явный 0 легален («без таймаута»), отрицательные значения — ошибка.
	if cfg.RTI.ParseTimeoutSec != nil && *cfg.RTI.ParseTimeoutSec < 0 {
		return fmt.Errorf("rti.parse_timeout_sec must be >= 0, got %d", *cfg.RTI.ParseTimeoutSec)
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
	if cfg.TRC.ParseTimeoutSec != nil && *cfg.TRC.ParseTimeoutSec < 0 {
		return fmt.Errorf("trc.parse_timeout_sec must be >= 0, got %d", *cfg.TRC.ParseTimeoutSec)
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
	// Таймауты MCP: nil → дефолты (30/120) применяются потребителем (timeoutForTool);
	// явный 0 легален («без таймаута»), отрицательные значения — ошибка.
	if cfg.MCP.QueryTimeoutSec != nil && *cfg.MCP.QueryTimeoutSec < 0 {
		return fmt.Errorf("mcp.query_timeout_sec must be >= 0, got %d", *cfg.MCP.QueryTimeoutSec)
	}
	if cfg.MCP.ReviewTimeoutSec != nil && *cfg.MCP.ReviewTimeoutSec < 0 {
		return fmt.Errorf("mcp.review_timeout_sec must be >= 0, got %d", *cfg.MCP.ReviewTimeoutSec)
	}

	// Spec defaults
	if cfg.Spec.LSAK <= 0 {
		cfg.Spec.LSAK = 512
	}
	if cfg.Spec.LSAMinDF <= 0 {
		cfg.Spec.LSAMinDF = 3
	}
	if cfg.Spec.LSAMaxDF <= 0 {
		cfg.Spec.LSAMaxDF = 0.3
	}
	if cfg.Spec.LSAMinCorpus <= 0 {
		cfg.Spec.LSAMinCorpus = 100
	}
	// LSARetrainThreshold: дефолт применяется через геттер RetrainThreshold();
	// явный 0 легален («переобучать при любом изменении»), отрицательные значения — ошибка.
	if cfg.Spec.LSARetrainThreshold != nil && *cfg.Spec.LSARetrainThreshold < 0 {
		return fmt.Errorf("spec.lsa_retrain_threshold must be >= 0, got %d", *cfg.Spec.LSARetrainThreshold)
	}
	// Пороги semantic-фильтрации: nil → дефолты применяются через методы-геттеры;
	// явные значения обязаны лежать в [0, 1].
	if cfg.Spec.LSAMinCosine != nil && (*cfg.Spec.LSAMinCosine < 0 || *cfg.Spec.LSAMinCosine > 1) {
		return fmt.Errorf("spec.lsa_min_cosine must be in [0, 1], got %v", *cfg.Spec.LSAMinCosine)
	}
	if cfg.Spec.LSARelativeCutoff != nil && (*cfg.Spec.LSARelativeCutoff < 0 || *cfg.Spec.LSARelativeCutoff > 1) {
		return fmt.Errorf("spec.lsa_relative_cutoff must be in [0, 1], got %v", *cfg.Spec.LSARelativeCutoff)
	}

	// DescLSA defaults: nil-поля получают включённое состояние (Enabled() = true),
	// числовые — те же дефолты, что и у спек-слоя. LSAEnabled — *bool (nil = true):
	// отсутствие секции [desc_lsa] в toml не должно отключать desc-поиск.
	if cfg.DescLSA.LSAK <= 0 {
		cfg.DescLSA.LSAK = 512
	}
	if cfg.DescLSA.LSAMinDF <= 0 {
		cfg.DescLSA.LSAMinDF = 10
	}
	if cfg.DescLSA.LSAMaxDF <= 0 {
		cfg.DescLSA.LSAMaxDF = 0.3
	}
	if cfg.DescLSA.LSAMinCorpus <= 0 {
		cfg.DescLSA.LSAMinCorpus = 100
	}
	if cfg.DescLSA.LSARetrainThreshold != nil && *cfg.DescLSA.LSARetrainThreshold < 0 {
		return fmt.Errorf("desc_lsa.lsa_retrain_threshold must be >= 0, got %d", *cfg.DescLSA.LSARetrainThreshold)
	}
	if cfg.DescLSA.LSAMinCosine != nil && (*cfg.DescLSA.LSAMinCosine < 0 || *cfg.DescLSA.LSAMinCosine > 1) {
		return fmt.Errorf("desc_lsa.lsa_min_cosine must be in [0, 1], got %v", *cfg.DescLSA.LSAMinCosine)
	}
	if cfg.DescLSA.LSARelativeCutoff != nil && (*cfg.DescLSA.LSARelativeCutoff < 0 || *cfg.DescLSA.LSARelativeCutoff > 1) {
		return fmt.Errorf("desc_lsa.lsa_relative_cutoff must be in [0, 1], got %v", *cfg.DescLSA.LSARelativeCutoff)
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
			ParseTimeoutSec: intPtr(300),
		},
		TRC: TRCConfig{
			SlowThresholdMs:           100,
			MaxEnrichWorkers:          16,
			MinProcsForParallelEnrich: 16,
			ParseTimeoutSec:           intPtr(300),
		},
		Logging: LoggingConfig{
			CommandEnabled: boolPtr(true),
		},
		MCP: MCPConfig{
			PaginationChunkSize:   8_000,
			PaginationTTL:         "15m",
			QueryTimeoutSec:       intPtr(30),
			ReviewTimeoutSec:      intPtr(120),
			RegexpCacheMaxEntries: 2048,
		},
		Spec: SpecConfig{
			LSAEnabled:          true,
			LSAK:                512,
			LSAMinDF:            3,
			LSAMaxDF:            0.3,
			LSAMinCorpus:        100,
			LSARetrainThreshold: intPtr(100),
			LSAMinCosine:        float64Ptr(0.15),
			LSARelativeCutoff:   float64Ptr(0.5),
		},
		DescLSA: DescLSAConfig{
			LSAEnabled:          boolPtr(true),
			LSAK:                512,
			LSAMinDF:            3,
			LSAMaxDF:            0.3,
			LSAMinCorpus:        100,
			LSARetrainThreshold: intPtr(100),
			LSAMinCosine:        float64Ptr(0.15),
			LSARelativeCutoff:   float64Ptr(0.5),
		},
	}
	return cfg
}

func boolPtr(v bool) *bool {
	return &v
}

func float64Ptr(v float64) *float64 {
	return &v
}

func intPtr(v int) *int {
	return &v
}
