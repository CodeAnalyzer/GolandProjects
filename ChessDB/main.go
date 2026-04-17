package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"ChessDB/internal/config"
	"ChessDB/internal/db"
	"ChessDB/internal/web"
)

func main() {
	fmt.Println("ChessDB - поиск шахматных партий в PostgreSQL")

	// Флаги командной строки
	configPath := flag.String("config", "config.toml", "Путь к TOML конфигу")
	mode := flag.String("mode", "", "Режим работы: init, import, web, stats, update-ratings, cache-warmup, cache-clear")
	pgnDir := flag.String("pgn", "", "Каталог с PGN файлами")
	port := flag.String("port", "", "Порт для веб-сервера")
	flag.Parse()

	appConfig, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("Ошибка загрузки конфигурации: %v", err)
	}

	effectiveMode := appConfig.App.Mode
	if *mode != "" {
		effectiveMode = *mode
	}

	effectivePGNDir := appConfig.Paths.PGNDir
	if *pgnDir != "" {
		effectivePGNDir = *pgnDir
	}

	serverPort := appConfig.Server.Port
	if *port != "" {
		parsedPort, err := strconv.Atoi(*port)
		if err != nil {
			log.Fatalf("Некорректный порт: %v", err)
		}
		serverPort = parsedPort
	}

	// Конфигурация баз данных
	dbConfig := db.Config{
		PostgresHost:                 appConfig.Database.Postgres.Host,
		PostgresPort:                 appConfig.Database.Postgres.Port,
		PostgresUser:                 appConfig.Database.Postgres.User,
		PostgresPassword:             appConfig.Database.Postgres.Password,
		PostgresDBName:               appConfig.Database.Postgres.DBName,
		PostgresSSLMode:              appConfig.Database.Postgres.SSLMode,
		PostgresMaxOpenConns:         appConfig.Database.Postgres.MaxOpenConns,
		PostgresMaxIdleConns:         appConfig.Database.Postgres.MaxIdleConns,
		PostgresConnMaxLifetime:      appConfig.Database.Postgres.ConnMaxLifetime,
		RedisEnabled:                 appConfig.Database.Redis.Enabled,
		RedisHost:                    appConfig.Database.Redis.Host,
		RedisPort:                    appConfig.Database.Redis.Port,
		RedisPassword:                appConfig.Database.Redis.Password,
		RedisDB:                      appConfig.Database.Redis.DB,
		CacheEnabled:                 appConfig.Cache.Enabled,
		CacheKeyPrefix:               appConfig.Cache.KeyPrefix,
		CacheSearchTTL:               appConfig.Cache.TTL.Search,
		CacheFENTTL:                  appConfig.Cache.TTL.FEN,
		CacheMovesTTL:                appConfig.Cache.TTL.Moves,
		CachePopularPositionsTTL:     appConfig.Cache.TTL.PopularPositions,
		CachePositionPattern:         appConfig.Cache.Patterns.Position,
		CacheSearchPattern:           appConfig.Cache.Patterns.Search,
		CacheMovesPattern:            appConfig.Cache.Patterns.Moves,
		CacheFENPattern:              appConfig.Cache.Patterns.FEN,
		CachePopularPositionsPattern: appConfig.Cache.Patterns.PopularPositions,
		WarmupPopularECOCodes:        appConfig.Cache.Warmup.PopularECOCodes,
		ImportLogFile:                appConfig.Paths.ImportLogFile,
		ImportSkipProcessedFiles:     appConfig.Import.SkipProcessedFiles,
		ImportEncodingFallback:       appConfig.Import.EncodingFallback,
	}

	// Подключение к базам данных
	chessDB, err := db.NewChessDB(dbConfig)
	if err != nil {
		log.Fatalf("Ошибка подключения к базам данных: %v", err)
	}
	defer chessDB.Close()

	switch effectiveMode {
	case "init":
		initializeDatabase(chessDB, effectivePGNDir)
	case "import":
		importPGNFiles(chessDB, effectivePGNDir, appConfig)
	case "web":
		// Запускаем warmup кэша перед стартом веб-сервера
		redisClient := chessDB.GetRedis()
		if redisClient != nil && appConfig.Cache.WarmupOnWebStart {
			if err := chessDB.WarmupCache(); err != nil {
				log.Printf("Cache warmup failed: %v", err)
			}
		}

		// Очищаем кэш от некорректных данных с NaN
		if redisClient != nil && appConfig.Cache.CleanupOnWebStart {
			ctx := context.Background()
			pattern := appConfig.Cache.KeyPrefix + ":" + appConfig.Cache.Patterns.Position + ":*"
			keys, err := redisClient.Keys(ctx, pattern).Result()
			if err == nil && len(keys) > 0 {
				err = redisClient.Del(ctx, keys...).Err()
				if err != nil {
					log.Printf("Ошибка очистки кэша: %v", err)
				} else {
					log.Printf("Очищено %d ключей из кэша", len(keys))
				}
			}
		}

		// Запускаем warmup для загрузки LLM
		go func() {
			analyzer := web.NewChessAnalyzer()
			if err := analyzer.Warmup(); err != nil {
				log.Printf("LLM warmup failed: %v", err)
			}
		}()

		startWebServer(chessDB, appConfig, serverPort)
	case "stats":
		showStatistics(chessDB)
	case "update-ratings":
		updateAllRatings(chessDB)
	case "cache-warmup":
		warmupCache(chessDB)
	case "cache-clear":
		clearCache(chessDB)
	default:
		fmt.Printf("Неизвестный режим: %s\n", effectiveMode)
		fmt.Println("Доступные режимы: init, import, web, stats, update-ratings, cache-warmup, cache-clear")
	}
}

func initializeDatabase(chessDB *db.ChessDB, pgnDir string) {
	fmt.Println("Инициализация базы данных...")

	if err := chessDB.InitSchema(pgnDir); err != nil {
		log.Fatalf("Ошибка инициализации схемы: %v", err)
	}

	fmt.Println("База данных успешно инициализирована")
}

func importPGNFiles(chessDB *db.ChessDB, pgnDir string, appConfig *config.Config) {
	fmt.Printf("Импорт PGN файлов из каталога: %s\n", pgnDir)

	// Проверяем существование каталога
	if _, err := os.Stat(pgnDir); os.IsNotExist(err) {
		log.Fatalf("Каталог %s не найден", pgnDir)
	}

	// Используем многопоточный импорт
	config := db.ParallelImportConfig{
		MaxWorkers: appConfig.Import.MaxWorkers,
		BatchSize:  appConfig.Import.BatchSize,
	}

	parallelImporter := db.NewParallelImporter(chessDB, config)

	// Выполняем многопоточный импорт
	if err := parallelImporter.ImportFiles(pgnDir); err != nil {
		log.Fatalf("Ошибка импорта: %v", err)
	}

	fmt.Println("Импорт завершен")
}

func startWebServer(chessDB *db.ChessDB, appConfig *config.Config, port int) {
	fmt.Printf("Запуск веб-сервера на порту %d\n", port)

	server := web.NewServer(chessDB, appConfig)

	// Создаем HTTP сервер
	httpServer := &http.Server{
		Addr:         fmt.Sprintf("%s:%d", appConfig.Server.Host, port),
		Handler:      server.GetRouter(),
		ReadTimeout:  appConfig.Server.ReadTimeout,
		WriteTimeout: appConfig.Server.WriteTimeout,
		IdleTimeout:  appConfig.Server.IdleTimeout,
	}

	// Канал для получения сигналов завершения
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	// Запуск сервера в горутине
	go func() {
		log.Printf("Сервер запущен на http://localhost:%d", port)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Ошибка запуска веб-сервера: %v", err)
		}
	}()

	// Ожидание сигнала завершения
	<-quit
	log.Println("Получен сигнал завершения, начинаю graceful shutdown...")

	// Создаем контекст с таймаутом для graceful shutdown
	ctx, cancel := context.WithTimeout(context.Background(), appConfig.Server.ShutdownTimeout)
	defer cancel()

	// Останавливаем HTTP сервер
	if err := httpServer.Shutdown(ctx); err != nil {
		log.Printf("Ошибка при остановке сервера: %v", err)
	} else {
		log.Println("HTTP сервер остановлен gracefully")
	}

	// Закрываем соединения с базами данных
	if err := chessDB.Close(); err != nil {
		log.Printf("Ошибка при закрытии соединений с БД: %v", err)
	} else {
		log.Println("Соединения с базами данных закрыты")
	}

	log.Println("Приложение завершено корректно")
}

func updateAllRatings(chessDB *db.ChessDB) {
	fmt.Println("Обновление рейтингов всех ходов...")

	if err := chessDB.UpdateAllRatings(); err != nil {
		log.Fatalf("Ошибка обновления рейтингов: %v", err)
	}

	fmt.Println("Рейтинги успешно обновлены")
}

func warmupCache(chessDB *db.ChessDB) {
	fmt.Println("Прогрев кэша...")

	if err := chessDB.WarmupCache(); err != nil {
		log.Fatalf("Ошибка прогрева кэша: %v", err)
	}

	fmt.Println("Кэш успешно прогрет")
}

func clearCache(chessDB *db.ChessDB) {
	fmt.Println("Очистка кэша...")

	if err := chessDB.InvalidateSearchCache(); err != nil {
		log.Fatalf("Ошибка очистки кэша: %v", err)
	}

	fmt.Println("Кэш успешно очищен")
}

func showStatistics(chessDB *db.ChessDB) {
	fmt.Println("Статистика базы данных:")

	// Общая статистика
	stats, err := chessDB.GetStats()
	if err != nil {
		log.Printf("Ошибка получения общей статистики: %v", err)
	} else {
		fmt.Printf("Позиций: %v\n", stats["positions_count"])
		fmt.Printf("Ходов: %v\n", stats["moves_count"])
		fmt.Printf("Игр: %v\n", stats["games_count"])
		fmt.Printf("Redis: %v\n", stats["redis_info"])
	}

	// Статистика по ECO кодам
	ecoStats, err := chessDB.GetECOStats()
	if err != nil {
		log.Printf("Ошибка получения статистики ECO: %v", err)
		return
	}

	fmt.Println("\nТоп-10 дебютов по количеству игр:")
	for i, stat := range ecoStats {
		if i >= 10 {
			break
		}
		fmt.Printf("%d. %s (%s): %d игр, рейтинг: %.1f\n",
			i+1, stat["name"], stat["code"],
			stat["total_games"], stat["avg_rating"])
	}
}
