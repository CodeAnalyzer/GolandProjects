package config

import (
	"fmt"
	"os"
	"time"

	"github.com/pelletier/go-toml/v2"
)

type Config struct {
	App       AppConfig       `toml:"app"`
	Server    ServerConfig    `toml:"server"`
	Database  DatabaseConfig  `toml:"database"`
	Paths     PathsConfig     `toml:"paths"`
	Import    ImportConfig    `toml:"import"`
	Cache     CacheConfig     `toml:"cache"`
	Web       WebConfig       `toml:"web"`
	Stockfish StockfishConfig `toml:"stockfish"`
}

type AppConfig struct {
	Name string `toml:"name"`
	Mode string `toml:"mode"`
}

type ServerConfig struct {
	Host            string        `toml:"host"`
	Port            int           `toml:"port"`
	ReadTimeout     time.Duration `toml:"read_timeout"`
	WriteTimeout    time.Duration `toml:"write_timeout"`
	IdleTimeout     time.Duration `toml:"idle_timeout"`
	ShutdownTimeout time.Duration `toml:"shutdown_timeout"`
}

type DatabaseConfig struct {
	Postgres PostgresConfig `toml:"postgres"`
	Redis    RedisConfig    `toml:"redis"`
}

type PostgresConfig struct {
	Host            string        `toml:"host"`
	Port            int           `toml:"port"`
	User            string        `toml:"user"`
	Password        string        `toml:"password"`
	DBName          string        `toml:"dbname"`
	SSLMode         string        `toml:"sslmode"`
	MaxOpenConns    int           `toml:"max_open_conns"`
	MaxIdleConns    int           `toml:"max_idle_conns"`
	ConnMaxLifetime time.Duration `toml:"conn_max_lifetime"`
}

type RedisConfig struct {
	Enabled  bool   `toml:"enabled"`
	Host     string `toml:"host"`
	Port     int    `toml:"port"`
	Password string `toml:"password"`
	DB       int    `toml:"db"`
}

type PathsConfig struct {
	PGNDir        string `toml:"pgn_dir"`
	StockfishPath string `toml:"stockfish_path"`
	ImportLogFile string `toml:"import_log_file"`
}

type ImportConfig struct {
	MaxWorkers        int    `toml:"max_workers"`
	BatchSize         int    `toml:"batch_size"`
	SkipProcessedFiles bool  `toml:"skip_processed_files"`
	EncodingFallback  string `toml:"encoding_fallback"`
}

type CacheConfig struct {
	Enabled           bool                `toml:"enabled"`
	KeyPrefix         string              `toml:"key_prefix"`
	CleanupOnWebStart bool                `toml:"cleanup_on_web_start"`
	WarmupOnWebStart  bool                `toml:"warmup_on_web_start"`
	TTL               CacheTTLConfig      `toml:"ttl"`
	Patterns          CachePatternsConfig `toml:"patterns"`
	Warmup            CacheWarmupConfig   `toml:"warmup"`
}

type CacheTTLConfig struct {
	Search           time.Duration `toml:"search"`
	FEN              time.Duration `toml:"fen"`
	Moves            time.Duration `toml:"moves"`
	PopularPositions time.Duration `toml:"popular_positions"`
}

type CachePatternsConfig struct {
	Position         string `toml:"position"`
	Search           string `toml:"search"`
	Moves            string `toml:"moves"`
	FEN              string `toml:"fen"`
	PopularPositions string `toml:"popular_positions"`
}

type CacheWarmupConfig struct {
	PopularECOCodes []string `toml:"popular_eco_codes"`
}

type WebConfig struct {
	Defaults WebDefaultsConfig `toml:"defaults"`
}

type WebDefaultsConfig struct {
	SearchCount int    `toml:"search_count"`
	SearchDepth int    `toml:"search_depth"`
	Color       string `toml:"color"`
}

type StockfishConfig struct {
	Enabled      bool `toml:"enabled"`
	DefaultDepth int  `toml:"default_depth"`
	MultiPV      int  `toml:"multi_pv"`
}

type rawConfig struct {
	App       AppConfig              `toml:"app"`
	Server    rawServerConfig        `toml:"server"`
	Database  rawDatabaseConfig      `toml:"database"`
	Paths     PathsConfig            `toml:"paths"`
	Import    ImportConfig           `toml:"import"`
	Cache     rawCacheConfig         `toml:"cache"`
	Web       WebConfig              `toml:"web"`
	Stockfish StockfishConfig        `toml:"stockfish"`
}

type rawServerConfig struct {
	Host            string `toml:"host"`
	Port            int    `toml:"port"`
	ReadTimeout     string `toml:"read_timeout"`
	WriteTimeout    string `toml:"write_timeout"`
	IdleTimeout     string `toml:"idle_timeout"`
	ShutdownTimeout string `toml:"shutdown_timeout"`
}

type rawDatabaseConfig struct {
	Postgres rawPostgresConfig `toml:"postgres"`
	Redis    RedisConfig       `toml:"redis"`
}

type rawPostgresConfig struct {
	Host            string `toml:"host"`
	Port            int    `toml:"port"`
	User            string `toml:"user"`
	Password        string `toml:"password"`
	DBName          string `toml:"dbname"`
	SSLMode         string `toml:"sslmode"`
	MaxOpenConns    int    `toml:"max_open_conns"`
	MaxIdleConns    int    `toml:"max_idle_conns"`
	ConnMaxLifetime string `toml:"conn_max_lifetime"`
}

type rawCacheConfig struct {
	Enabled           bool                 `toml:"enabled"`
	KeyPrefix         string               `toml:"key_prefix"`
	CleanupOnWebStart bool                 `toml:"cleanup_on_web_start"`
	WarmupOnWebStart  bool                 `toml:"warmup_on_web_start"`
	TTL               rawCacheTTLConfig    `toml:"ttl"`
	Patterns          CachePatternsConfig  `toml:"patterns"`
	Warmup            CacheWarmupConfig    `toml:"warmup"`
}

type rawCacheTTLConfig struct {
	Search           string `toml:"search"`
	FEN              string `toml:"fen"`
	Moves            string `toml:"moves"`
	PopularPositions string `toml:"popular_positions"`
}

func Default() *Config {
	return &Config{
		App: AppConfig{
			Name: "ChessDB",
			Mode: "web",
		},
		Server: ServerConfig{
			Host:            "0.0.0.0",
			Port:            9000,
			ReadTimeout:     15 * time.Second,
			WriteTimeout:    15 * time.Second,
			IdleTimeout:     60 * time.Second,
			ShutdownTimeout: 30 * time.Second,
		},
		Database: DatabaseConfig{
			Postgres: PostgresConfig{
				Host:            "localhost",
				Port:            5435,
				User:            "postgres",
				Password:        "",
				DBName:          "chessdb",
				SSLMode:         "disable",
				MaxOpenConns:    20,
				MaxIdleConns:    10,
				ConnMaxLifetime: 30 * time.Minute,
			},
			Redis: RedisConfig{
				Enabled:  true,
				Host:     "localhost",
				Port:     6379,
				Password: "",
				DB:       0,
			},
		},
		Paths: PathsConfig{
			PGNDir:        "base",
			StockfishPath: "D:\\GITHUB\\stockfish-bmi2\\stockfish-windows-x86-64-bmi2.exe",
			ImportLogFile: "import.log",
		},
		Import: ImportConfig{
			MaxWorkers:        12,
			BatchSize:         500,
			SkipProcessedFiles: true,
			EncodingFallback:  "windows-1251",
		},
		Cache: CacheConfig{
			Enabled:           true,
			KeyPrefix:         "chess",
			CleanupOnWebStart: true,
			WarmupOnWebStart:  true,
			TTL: CacheTTLConfig{
				Search:           time.Hour,
				FEN:              24 * time.Hour,
				Moves:            6 * time.Hour,
				PopularPositions: 12 * time.Hour,
			},
			Patterns: CachePatternsConfig{
				Position:         "pos",
				Search:           "search",
				Moves:            "moves",
				FEN:              "fen",
				PopularPositions: "popular:positions",
			},
			Warmup: CacheWarmupConfig{
				PopularECOCodes: []string{"C20", "C50", "D20", "B20", "A00"},
			},
		},
		Web: WebConfig{
			Defaults: WebDefaultsConfig{
				SearchCount: 5,
				SearchDepth: 3,
				Color:       "white",
			},
		},
		Stockfish: StockfishConfig{
			Enabled:      true,
			DefaultDepth: 15,
			MultiPV:      3,
		},
	}
}

func Load(path string) (*Config, error) {
	cfg := Default()
	if path == "" {
		path = "config.toml"
	}

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			applyEnvOverrides(cfg)
			return cfg, nil
		}
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	raw := rawConfig{}
	if err := toml.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	if err := mergeRawConfig(cfg, raw); err != nil {
		return nil, fmt.Errorf("failed to apply config file: %w", err)
	}

	applyEnvOverrides(cfg)
	return cfg, nil
}

func mergeRawConfig(cfg *Config, raw rawConfig) error {
	cfg.App = raw.App
	cfg.Paths = raw.Paths
	cfg.Import = raw.Import
	cfg.Web = raw.Web
	cfg.Stockfish = raw.Stockfish

	serverReadTimeout, err := parseDurationOrDefault(raw.Server.ReadTimeout, cfg.Server.ReadTimeout)
	if err != nil {
		return fmt.Errorf("server.read_timeout: %w", err)
	}
	serverWriteTimeout, err := parseDurationOrDefault(raw.Server.WriteTimeout, cfg.Server.WriteTimeout)
	if err != nil {
		return fmt.Errorf("server.write_timeout: %w", err)
	}
	serverIdleTimeout, err := parseDurationOrDefault(raw.Server.IdleTimeout, cfg.Server.IdleTimeout)
	if err != nil {
		return fmt.Errorf("server.idle_timeout: %w", err)
	}
	serverShutdownTimeout, err := parseDurationOrDefault(raw.Server.ShutdownTimeout, cfg.Server.ShutdownTimeout)
	if err != nil {
		return fmt.Errorf("server.shutdown_timeout: %w", err)
	}

	cfg.Server = ServerConfig{
		Host:            raw.Server.Host,
		Port:            raw.Server.Port,
		ReadTimeout:     serverReadTimeout,
		WriteTimeout:    serverWriteTimeout,
		IdleTimeout:     serverIdleTimeout,
		ShutdownTimeout: serverShutdownTimeout,
	}

	connMaxLifetime, err := parseDurationOrDefault(raw.Database.Postgres.ConnMaxLifetime, cfg.Database.Postgres.ConnMaxLifetime)
	if err != nil {
		return fmt.Errorf("database.postgres.conn_max_lifetime: %w", err)
	}

	cfg.Database = DatabaseConfig{
		Postgres: PostgresConfig{
			Host:            raw.Database.Postgres.Host,
			Port:            raw.Database.Postgres.Port,
			User:            raw.Database.Postgres.User,
			Password:        raw.Database.Postgres.Password,
			DBName:          raw.Database.Postgres.DBName,
			SSLMode:         raw.Database.Postgres.SSLMode,
			MaxOpenConns:    raw.Database.Postgres.MaxOpenConns,
			MaxIdleConns:    raw.Database.Postgres.MaxIdleConns,
			ConnMaxLifetime: connMaxLifetime,
		},
		Redis: raw.Database.Redis,
	}

	searchTTL, err := parseDurationOrDefault(raw.Cache.TTL.Search, cfg.Cache.TTL.Search)
	if err != nil {
		return fmt.Errorf("cache.ttl.search: %w", err)
	}
	fenTTL, err := parseDurationOrDefault(raw.Cache.TTL.FEN, cfg.Cache.TTL.FEN)
	if err != nil {
		return fmt.Errorf("cache.ttl.fen: %w", err)
	}
	movesTTL, err := parseDurationOrDefault(raw.Cache.TTL.Moves, cfg.Cache.TTL.Moves)
	if err != nil {
		return fmt.Errorf("cache.ttl.moves: %w", err)
	}
	popularPositionsTTL, err := parseDurationOrDefault(raw.Cache.TTL.PopularPositions, cfg.Cache.TTL.PopularPositions)
	if err != nil {
		return fmt.Errorf("cache.ttl.popular_positions: %w", err)
	}

	cfg.Cache = CacheConfig{
		Enabled:           raw.Cache.Enabled,
		KeyPrefix:         raw.Cache.KeyPrefix,
		CleanupOnWebStart: raw.Cache.CleanupOnWebStart,
		WarmupOnWebStart:  raw.Cache.WarmupOnWebStart,
		TTL: CacheTTLConfig{
			Search:           searchTTL,
			FEN:              fenTTL,
			Moves:            movesTTL,
			PopularPositions: popularPositionsTTL,
		},
		Patterns: raw.Cache.Patterns,
		Warmup:   raw.Cache.Warmup,
	}

	return nil
}

func parseDurationOrDefault(value string, defaultValue time.Duration) (time.Duration, error) {
	if value == "" {
		return defaultValue, nil
	}

	parsed, err := time.ParseDuration(value)
	if err != nil {
		return 0, err
	}

	return parsed, nil
}

func applyEnvOverrides(cfg *Config) {
	if value := os.Getenv("CHESSDB_PG_PASSWORD"); value != "" {
		cfg.Database.Postgres.Password = value
	}
	if value := os.Getenv("CHESSDB_REDIS_PASSWORD"); value != "" {
		cfg.Database.Redis.Password = value
	}
	if value := os.Getenv("CHESSDB_STOCKFISH_PATH"); value != "" {
		cfg.Paths.StockfishPath = value
	}
}
