package config

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/pelletier/go-toml/v2"
)

// DBConfig конфигурация подключения к PostgreSQL
type DBConfig struct {
	Host     string `toml:"host"`
	Port     int    `toml:"port"`
	Database string `toml:"database"`
	User     string `toml:"user"`
	Password string `toml:"password"`
	SSLMode  string `toml:"sslmode"`
}

// Config полная конфигурация приложения
type Config struct {
	RootPath string        `toml:"root_path"`
	DB       DBConfig      `toml:"database"`
	Indexer  IndexerConfig `toml:"indexer"`
	Logging  LoggingConfig `toml:"logging"`
}

// IndexerConfig конфигурация индексатора
type IndexerConfig struct {
	Parallel        int      `toml:"parallel"`
	BatchSize       int      `toml:"batch_size"`
	IncludePatterns []string `toml:"include_patterns"`
	ExcludePatterns []string `toml:"exclude_patterns"`
}

type LoggingConfig struct {
	CommandEnabled *bool `toml:"command_enabled"`
}

var (
	cfg       *Config
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
	if cfg.Indexer.Parallel == 0 {
		cfg.Indexer.Parallel = 4
	}
	if cfg.Indexer.BatchSize == 0 {
		cfg.Indexer.BatchSize = 100
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
			Host:     "localhost",
			Port:     5435,
			Database: "codebase",
			User:     "postgres",
			Password: "",
			SSLMode:  "disable",
		},
		Indexer: IndexerConfig{
			Parallel:  4,
			BatchSize: 100,
			IncludePatterns: []string{
				"*.sql", "*.h", "*.pas", "*.inc", "*.js", "*.smf", "*.dfm", "*.tpr", "*.rpt",
			},
			ExcludePatterns: []string{
				"*/.*", "*~", "*.bak", "*.old",
			},
		},
		Logging: LoggingConfig{
			CommandEnabled: boolPtr(true),
		},
	}
	return cfg
}

func boolPtr(v bool) *bool {
	return &v
}
