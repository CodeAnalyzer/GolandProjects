package db

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	_ "github.com/lib/pq"
	"golang.org/x/net/context"
)

// Config holds database configuration
type Config struct {
	PostgresHost     string
	PostgresPort     int
	PostgresUser     string
	PostgresPassword string
	PostgresDBName   string
	PostgresSSLMode  string
	PostgresMaxOpenConns int
	PostgresMaxIdleConns int
	PostgresConnMaxLifetime time.Duration
	RedisEnabled     bool
	RedisHost        string
	RedisPort        int
	RedisPassword    string
	RedisDB          int
	CacheEnabled     bool
	CacheKeyPrefix   string
	CacheSearchTTL   time.Duration
	CacheFENTTL      time.Duration
	CacheMovesTTL    time.Duration
	CachePopularPositionsTTL time.Duration
	CachePositionPattern string
	CacheSearchPattern string
	CacheMovesPattern string
	CacheFENPattern string
	CachePopularPositionsPattern string
	WarmupPopularECOCodes []string
	ImportLogFile string
	ImportSkipProcessedFiles bool
	ImportEncodingFallback string
}

// ChessDB manages both PostgreSQL and Redis connections
type ChessDB struct {
	pgDB   *sql.DB
	redis  *redis.Client
	config Config
}

// NewChessDB creates a new ChessDB instance
func NewChessDB(config Config) (*ChessDB, error) {
	// Connect to PostgreSQL
	sslMode := config.PostgresSSLMode
	if sslMode == "" {
		sslMode = "disable"
	}

	pgConnStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		config.PostgresHost, config.PostgresPort, config.PostgresUser, config.PostgresPassword, config.PostgresDBName, sslMode)

	pgDB, err := sql.Open("postgres", pgConnStr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to PostgreSQL: %v", err)
	}

	if config.PostgresMaxOpenConns > 0 {
		pgDB.SetMaxOpenConns(config.PostgresMaxOpenConns)
	}
	if config.PostgresMaxIdleConns > 0 {
		pgDB.SetMaxIdleConns(config.PostgresMaxIdleConns)
	}
	if config.PostgresConnMaxLifetime > 0 {
		pgDB.SetConnMaxLifetime(config.PostgresConnMaxLifetime)
	}

	if err := pgDB.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping PostgreSQL: %v", err)
	}

	// Connect to Redis
	var rdb *redis.Client
	if config.RedisEnabled {
		rdb = redis.NewClient(&redis.Options{
			Addr:     fmt.Sprintf("%s:%d", config.RedisHost, config.RedisPort),
			Password: config.RedisPassword,
			DB:       config.RedisDB,
		})

		if err := rdb.Ping(context.Background()).Err(); err != nil {
			log.Printf("Warning: Redis connection failed: %v", err)
			// Continue without Redis
			rdb = nil
		}
	}

	return &ChessDB{
		pgDB:   pgDB,
		redis:  rdb,
		config: config,
	}, nil
}

// Close closes all database connections
func (db *ChessDB) Close() error {
	var errors []string

	if db.pgDB != nil {
		if err := db.pgDB.Close(); err != nil {
			errors = append(errors, fmt.Sprintf("PostgreSQL close error: %v", err))
		}
	}

	if db.redis != nil {
		if err := db.redis.Close(); err != nil {
			errors = append(errors, fmt.Sprintf("Redis close error: %v", err))
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("errors closing connections: %s", strings.Join(errors, "; "))
	}
	return nil
}

func (db *ChessDB) cacheKey(parts ...string) string {
	segments := []string{db.config.CacheKeyPrefix}
	segments = append(segments, parts...)
	return strings.Join(segments, ":")
}

func (db *ChessDB) cachePattern(pattern string) string {
	return db.cacheKey(pattern) + ":*"
}

// SavePosition saves a chess position to the database
func (db *ChessDB) SavePosition(position *Position) error {
	_, err := db.pgDB.Exec(`
		INSERT INTO positions (position_hash, fen)
		VALUES ($1, $2)
		ON CONFLICT (position_hash) DO NOTHING`,
		position.PositionHash, position.FEN)
	if err != nil {
		return fmt.Errorf("failed to save position: %v", err)
	}
	return nil
}

// SaveMove saves a move (transition between positions) to the database
func (db *ChessDB) SaveMove(move *Move) error {
	_, err := db.pgDB.Exec(`
		INSERT INTO moves (parent_hash, child_hash, move_number, move_from, move_to, piece_type, color, annotation, rating, games_count, wins, draws, losses, eco_code)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
		ON CONFLICT (parent_hash, child_hash, move_number) DO UPDATE SET
			games_count = moves.games_count + EXCLUDED.games_count,
			wins = moves.wins + EXCLUDED.wins,
			draws = moves.draws + EXCLUDED.draws,
			losses = moves.losses + EXCLUDED.losses,
			rating = (moves.rating * moves.games_count + EXCLUDED.rating * EXCLUDED.games_count) / (moves.games_count + EXCLUDED.games_count)`,
		move.ParentHash, move.ChildHash, move.MoveNumber, move.MoveFrom, move.MoveTo, move.PieceType, move.Color, move.Annotation, move.Rating, move.GamesCount, move.Wins, move.Draws, move.Losses, move.ECOCode)
	if err != nil {
		return fmt.Errorf("failed to save move: %v", err)
	}
	return nil
}

// SaveTransition saves a transition between positions (legacy method)
func (db *ChessDB) SaveTransition(parentHash, childHash int64, moveNumber int16) error {
	_, err := db.pgDB.Exec(`
		INSERT INTO position_transitions (parent_hash, child_hash, move_number, games_count)
		VALUES ($1, $2, $3, 1)
		ON CONFLICT (parent_hash, child_hash, move_number) DO UPDATE SET
			games_count = position_transitions.games_count + 1`,
		parentHash, childHash, moveNumber)
	if err != nil {
		return fmt.Errorf("failed to save transition: %v", err)
	}
	return nil
}

// InitSchema creates the database schema
func (db *ChessDB) InitSchema(pgnDir string) error {
	schema := `
	-- Create extensions
	CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

	-- Games table
	CREATE TABLE IF NOT EXISTS games (
		id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
		white_player VARCHAR(255) NOT NULL,
		black_player VARCHAR(255) NOT NULL,
		result VARCHAR(10) NOT NULL,
		eco_code VARCHAR(10),
		pgn_content TEXT,
		processed BOOLEAN DEFAULT FALSE,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);

	-- Positions table (состояния доски после хода)
	CREATE TABLE IF NOT EXISTS positions (
		id BIGSERIAL PRIMARY KEY,
		position_hash BIGINT UNIQUE NOT NULL,
		fen TEXT NOT NULL,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);

	-- Moves table (переходы между позициями)
	CREATE TABLE IF NOT EXISTS moves (
		id BIGSERIAL PRIMARY KEY,
		parent_hash BIGINT REFERENCES positions(position_hash),
		child_hash BIGINT REFERENCES positions(position_hash),
		move_number INTEGER NOT NULL,
		move_from SMALLINT,
		move_to SMALLINT,
		piece_type SMALLINT,
		color SMALLINT,        -- цвет фигуры, которая сделала ход (0=white, 1=black)
		annotation SMALLINT DEFAULT 0,     -- аннотация хода
		rating FLOAT DEFAULT 1.0,          -- рейтинг хода
		games_count INTEGER DEFAULT 1,
		wins INTEGER DEFAULT 0,
		draws INTEGER DEFAULT 0,
		losses INTEGER DEFAULT 0,
		eco_code VARCHAR(10),
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		UNIQUE(parent_hash, child_hash, move_number)
	);

	-- ECO codes reference table
	CREATE TABLE IF NOT EXISTS eco_codes (
		code VARCHAR(10) PRIMARY KEY,
		name VARCHAR(255) NOT NULL,
		moves TEXT NOT NULL,
		family VARCHAR(50)
	);

	-- Processed files table for tracking imported PGN files
	CREATE TABLE IF NOT EXISTS processed_files (
		id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
		filename VARCHAR(500) UNIQUE NOT NULL,
		file_path TEXT NOT NULL,
		processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		games_count INTEGER DEFAULT 0,
		positions_count INTEGER DEFAULT 0,
		file_size BIGINT DEFAULT 0,
		status VARCHAR(50) DEFAULT 'completed'
	);

	-- Create indexes for better performance
	CREATE INDEX IF NOT EXISTS idx_positions_hash ON positions(position_hash);
	CREATE INDEX IF NOT EXISTS idx_moves_parent ON moves(parent_hash);
	CREATE INDEX IF NOT EXISTS idx_moves_child ON moves(child_hash);
	CREATE INDEX IF NOT EXISTS idx_moves_rating ON moves(rating DESC);
	CREATE INDEX IF NOT EXISTS idx_games_result ON games(result);
	CREATE INDEX IF NOT EXISTS idx_games_eco ON games(eco_code);
	CREATE INDEX IF NOT EXISTS idx_processed_files_filename ON processed_files(filename);
	CREATE INDEX IF NOT EXISTS idx_eco_moves_prefix ON eco_codes(moves text_pattern_ops);

	-- Create trigger for updated_at
	CREATE OR REPLACE FUNCTION update_updated_at_column()
	RETURNS TRIGGER AS $$
	BEGIN
		NEW.updated_at = CURRENT_TIMESTAMP;
		RETURN NEW;
	END;
	$$ language 'plpgsql';

	CREATE TRIGGER update_games_updated_at BEFORE UPDATE ON games
		FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
	`

	_, err := db.pgDB.Exec(schema)
	if err != nil {
		return fmt.Errorf("failed to create schema: %v", err)
	}

	// Insert some basic ECO codes
	if err := db.insertBasicECOCodes(pgnDir); err != nil {
		log.Printf("Warning: Failed to insert basic ECO codes: %v", err)
	}

	return nil
}

func (db *ChessDB) insertBasicECOCodes(pgnDir string) error {
	// Читаем ECO коды из файла
	filename := filepath.Join(pgnDir, "eco.def")

	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("failed to open ECO file %s: %v", filename, err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	var currentCode string
	var currentName string
	moveLines := []string{}

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		// Пропускаем пустые строки
		if line == "" {
			continue
		}

		// Если нашли новый ECO код, сохраняем предыдущий
		if strings.HasPrefix(line, "[Site \"") {
			// Сохраняем предыдущий код если он есть
			if currentCode != "" && currentName != "" && len(moveLines) > 0 {
				moves := strings.Join(moveLines, " ")
				err := db.insertECOCode(currentCode, currentName, moves)
				if err != nil {
					log.Printf("Warning: Failed to insert ECO code %s: %v", currentCode, err)
				}
			}

			// Начинаем новый код
			currentCode = strings.Trim(line, "[Site \"]")
			currentName = ""
			moveLines = []string{}
		} else if strings.HasPrefix(line, "[White \"") {
			// Извлекаем название
			currentName = strings.Trim(line, "[White \"]")
		} else if !strings.HasPrefix(line, "[") && !strings.HasPrefix(line, "[Event") {
			// Это строка с ходами - очищаем номера ходов сразу
			moveLine := strings.TrimSpace(line)
			if moveLine != "" {
				cleanedMove := db.cleanMoveNumbers(moveLine)
				if cleanedMove != "" {
					moveLines = append(moveLines, cleanedMove)
				}
			}
		}
	}

	// Сохраняем последний код
	if currentCode != "" && currentName != "" && len(moveLines) > 0 {
		moves := strings.Join(moveLines, " ")
		err := db.insertECOCode(currentCode, currentName, moves)
		if err != nil {
			log.Printf("Warning: Failed to insert ECO code %s: %v", currentCode, err)
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading ECO file: %v", err)
	}

	log.Printf("Successfully processed ECO codes from %s", filename)
	return nil
}

// cleanMoveNumbers удаляет номера ходов из строки ходов (например, "1. e4 Nf6 2. e5" -> "e4 Nf6 e5")
func (db *ChessDB) cleanMoveNumbers(moves string) string {
	// Разделяем строку на токены по пробелам
	tokens := strings.Fields(moves)
	var cleaned []string

	for i := 0; i < len(tokens); i++ {
		token := tokens[i]
		// Пропускаем токены, которые являются номерами ходов (заканчиваются на точку)
		if strings.HasSuffix(token, ".") {
			// Проверяем, является ли токен числом с точкой (например, "1.", "2.", "3.")
			if _, err := strconv.Atoi(strings.TrimSuffix(token, ".")); err == nil {
				continue // Пропускаем номер хода
			}
		}
		// Пропускаем многоточия (...)
		if token == "..." {
			continue
		}
		// Добавляем токен, если это не номер хода
		cleaned = append(cleaned, token)
	}

	return strings.Join(cleaned, " ")
}

func (db *ChessDB) insertECOCode(code, name, moves string) error {
	// Определяем семейство по первой букве кода
	family := "Other"
	if len(code) > 0 {
		switch code[0] {
		case 'A':
			family = "Flank Openings"
		case 'B':
			family = "Semi-Open Games"
		case 'C':
			family = "Open Games"
		case 'D':
			family = "Closed Games"
		case 'E':
			family = "Indian Defenses"
		}
	}

	_, err := db.pgDB.Exec(`
		INSERT INTO eco_codes (code, name, moves, family) 
		VALUES ($1, $2, $3, $4) 
		ON CONFLICT (code) DO NOTHING`,
		code, name, moves, family)

	return err
}

// GetRedis returns the Redis client (can be nil if Redis is not available)
func (db *ChessDB) GetRedis() *redis.Client {
	return db.redis
}

// GetStats returns database statistics
func (db *ChessDB) GetStats() (map[string]interface{}, error) {
	stats := make(map[string]interface{})

	// Get positions count
	var positionsCount int64
	err := db.pgDB.QueryRow("SELECT COUNT(*) FROM positions").Scan(&positionsCount)
	if err != nil {
		return nil, fmt.Errorf("failed to get positions count: %v", err)
	}
	stats["positions_count"] = positionsCount

	// Get moves count
	var movesCount int64
	err = db.pgDB.QueryRow("SELECT COUNT(*) FROM moves").Scan(&movesCount)
	if err != nil {
		return nil, fmt.Errorf("failed to get moves count: %v", err)
	}
	stats["moves_count"] = movesCount

	// Get games count
	var gamesCount int64
	err = db.pgDB.QueryRow("SELECT COUNT(*) FROM games").Scan(&gamesCount)
	if err != nil {
		return nil, fmt.Errorf("failed to get games count: %v", err)
	}
	stats["games_count"] = gamesCount

	// Get ECO codes count
	var ecoCodesCount int64
	err = db.pgDB.QueryRow("SELECT COUNT(DISTINCT eco_code) FROM moves WHERE eco_code IS NOT NULL AND eco_code != ''").Scan(&ecoCodesCount)
	if err != nil {
		return nil, fmt.Errorf("failed to get ECO codes count: %v", err)
	}
	stats["eco_codes_count"] = ecoCodesCount

	// Get Redis info
	if db.redis != nil {
		redisInfo := "Connected"
		if err := db.redis.Ping(context.Background()).Err(); err != nil {
			redisInfo = fmt.Sprintf("Error: %v", err)
		}
		stats["redis_info"] = redisInfo
	} else {
		stats["redis_info"] = "Not connected"
	}

	return stats, nil
}

// GetECOStats returns statistics by ECO codes
func (db *ChessDB) GetECOStats() ([]map[string]interface{}, error) {
	query := `
		SELECT
			ec.code,
			ec.name,
			COALESCE(COUNT(m.id), 0) as total_games,
			COALESCE(AVG(m.rating), 0) as avg_rating,
			COALESCE(SUM(m.games_count), 0) as total_positions
		FROM eco_codes ec
		LEFT JOIN moves m ON ec.code = m.eco_code
		GROUP BY ec.code, ec.name
		ORDER BY ec.code
	`

	rows, err := db.pgDB.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to get ECO stats: %v", err)
	}
	defer rows.Close()

	var stats []map[string]interface{}
	for rows.Next() {
		var code, name string
		var totalGames, totalPositions int64
		var avgRating float64

		err := rows.Scan(&code, &name, &totalGames, &avgRating, &totalPositions)
		if err != nil {
			return nil, fmt.Errorf("failed to scan ECO stats row: %v", err)
		}

		stat := map[string]interface{}{
			"code":            code,
			"name":            name,
			"total_games":     totalGames,
			"avg_rating":      avgRating,
			"total_positions": totalPositions,
		}
		stats = append(stats, stat)
	}

	// Проверяем ошибки после итерации
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating ECO stats: %v", err)
	}

	return stats, nil
}

// GetECOCodesByMovesPrefix ищет ECO коды, которые начинаются с указанной последовательности ходов
func (db *ChessDB) GetECOCodesByMovesPrefix(movesPrefix string) ([]ECOCode, error) {
	query := `
		SELECT code, name, moves, family 
		FROM eco_codes 
		WHERE moves LIKE $1 
		ORDER BY code
	`

	// Добавляем % в конец для поиска по префиксу
	pattern := movesPrefix + "%"

	rows, err := db.pgDB.Query(query, pattern)
	if err != nil {
		return nil, fmt.Errorf("failed to query ECO codes by moves prefix: %v", err)
	}
	defer rows.Close()

	var codes []ECOCode
	for rows.Next() {
		var eco ECOCode
		err := rows.Scan(&eco.Code, &eco.Name, &eco.Moves, &eco.Family)
		if err != nil {
			return nil, fmt.Errorf("failed to scan ECO code: %v", err)
		}
		codes = append(codes, eco)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating ECO codes: %v", err)
	}

	return codes, nil
}

// IsFileProcessed checks if a file has already been processed
func (db *ChessDB) IsFileProcessed(filename string) (bool, error) {
	var exists bool
	err := db.pgDB.QueryRow("SELECT EXISTS(SELECT 1 FROM processed_files WHERE filename = $1)", filename).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed to check if file is processed: %v", err)
	}
	return exists, nil
}

// SaveProcessedFile saves information about a processed file
func (db *ChessDB) SaveProcessedFile(filename, filePath string, gamesCount, positionsCount int, fileSize int64) error {
	_, err := db.pgDB.Exec(`
		INSERT INTO processed_files (filename, file_path, games_count, positions_count, file_size, status)
		VALUES ($1, $2, $3, $4, $5, 'completed')
		ON CONFLICT (filename) DO UPDATE SET
			games_count = EXCLUDED.games_count,
			positions_count = EXCLUDED.positions_count,
			file_size = EXCLUDED.file_size,
			processed_at = CURRENT_TIMESTAMP,
			status = 'completed'`,
		filename, filePath, gamesCount, positionsCount, fileSize)

	if err != nil {
		return fmt.Errorf("failed to save processed file: %v", err)
	}

	return nil
}

// GetProcessedFiles returns list of processed files
func (db *ChessDB) GetProcessedFiles(limit int) ([]ProcessedFile, error) {
	query := `
		SELECT id, filename, file_path, processed_at, games_count, positions_count, file_size, status
		FROM processed_files
		ORDER BY processed_at DESC`

	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}

	rows, err := db.pgDB.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to get processed files: %v", err)
	}
	defer rows.Close()

	var files []ProcessedFile
	for rows.Next() {
		var file ProcessedFile
		err := rows.Scan(
			&file.ID, &file.Filename, &file.FilePath, &file.ProcessedAt,
			&file.GamesCount, &file.PositionsCount, &file.FileSize, &file.Status)
		if err != nil {
			return nil, fmt.Errorf("failed to scan processed file: %v", err)
		}
		files = append(files, file)
	}

	// Проверяем ошибки после итерации
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating processed files: %v", err)
	}

	return files, nil
}

// GetProcessedFilesStats returns statistics about processed files
func (db *ChessDB) GetProcessedFilesStats() (map[string]interface{}, error) {
	stats := make(map[string]interface{})

	// Get total processed files
	var totalFiles int64
	err := db.pgDB.QueryRow("SELECT COUNT(*) FROM processed_files").Scan(&totalFiles)
	if err != nil {
		return nil, fmt.Errorf("failed to get processed files count: %v", err)
	}
	stats["processed_files_count"] = totalFiles

	// Get total games from processed files
	var totalGames int64
	err = db.pgDB.QueryRow("SELECT COALESCE(SUM(games_count), 0) FROM processed_files").Scan(&totalGames)
	if err != nil {
		return nil, fmt.Errorf("failed to get total games from processed files: %v", err)
	}
	stats["processed_files_games"] = totalGames

	// Get total positions from processed files
	var totalPositions int64
	err = db.pgDB.QueryRow("SELECT COALESCE(SUM(positions_count), 0) FROM processed_files").Scan(&totalPositions)
	if err != nil {
		return nil, fmt.Errorf("failed to get total positions from processed files: %v", err)
	}
	stats["processed_files_positions"] = totalPositions

	return stats, nil
}

// UpdateAllRatings пересчитывает рейтинги всех ходов на основе актуальной статистики
func (db *ChessDB) UpdateAllRatings() error {
	log.Println("Starting to update all move ratings...")

	// Получаем все ходы из базы данных
	rows, err := db.pgDB.Query(`
		SELECT parent_hash, child_hash, move_number, annotation, 
		       games_count, wins, draws, losses, rating
		FROM moves
		ORDER BY parent_hash
	`)
	if err != nil {
		return fmt.Errorf("failed to query moves for rating update: %v", err)
	}
	defer rows.Close()

	// Группируем ходы по родительским позициям
	positionMoves := make(map[int64][]struct {
		ChildHash  int64
		MoveNumber int
		Annotation int16
		GamesCount int
		Wins       int
		Draws      int
		Losses     int
		Rating     float64
	})

	for rows.Next() {
		var parentHash, childHash int64
		var moveNumber int
		var annotation int16
		var gamesCount, wins, draws, losses int
		var rating float64

		err := rows.Scan(&parentHash, &childHash, &moveNumber, &annotation,
			&gamesCount, &wins, &draws, &losses, &rating)
		if err != nil {
			log.Printf("Failed to scan move for rating update: %v", err)
			continue
		}

		positionMoves[parentHash] = append(positionMoves[parentHash], struct {
			ChildHash  int64
			MoveNumber int
			Annotation int16
			GamesCount int
			Wins       int
			Draws      int
			Losses     int
			Rating     float64
		}{
			ChildHash:  childHash,
			MoveNumber: moveNumber,
			Annotation: annotation,
			GamesCount: gamesCount,
			Wins:       wins,
			Draws:      draws,
			Losses:     losses,
			Rating:     rating,
		})
	}

	// Проверяем ошибки после итерации
	if err = rows.Err(); err != nil {
		return fmt.Errorf("error iterating moves for rating update: %v", err)
	}

	// Обновляем рейтинги для каждой позиции
	updatedCount := 0
	for parentHash, moves := range positionMoves {
		// Вычисляем общее количество игр для позиции
		totalGamesForPosition := 0
		for _, move := range moves {
			totalGamesForPosition += move.GamesCount
		}

		// Обновляем рейтинг для каждого хода
		for _, move := range moves {
			newRating := calculateMoveRating(
				move.Annotation,
				move.Wins, move.Draws, move.Losses,
				move.GamesCount,
				totalGamesForPosition,
			)

			// Если рейтинг изменился, обновляем в базе данных
			if math.Abs(newRating-move.Rating) > 0.01 {
				_, err := db.pgDB.Exec(`
					UPDATE moves 
					SET rating = $1 
					WHERE parent_hash = $2 AND child_hash = $3 AND move_number = $4
				`, newRating, parentHash, move.ChildHash, move.MoveNumber)

				if err != nil {
					log.Printf("Failed to update rating for move %d->%d: %v",
						parentHash, move.ChildHash, err)
					continue
				}
				updatedCount++
			}
		}
	}

	log.Printf("Rating update completed. Updated %d moves", updatedCount)

	// Очищаем кэш после обновления рейтингов
	if err := db.InvalidateSearchCache(); err != nil {
		log.Printf("Failed to invalidate cache: %v", err)
	}

	return nil
}

// InvalidateSearchCache очищает кэш поиска после обновления данных
func (db *ChessDB) InvalidateSearchCache() error {
	if db.redis == nil {
		return nil
	}

	ctx := context.Background()
	pattern := db.cachePattern(db.config.CacheSearchPattern)
	keys, err := db.redis.Keys(ctx, pattern).Result()
	if err != nil {
		return fmt.Errorf("failed to get cache keys: %v", err)
	}

	if len(keys) > 0 {
		err = db.redis.Del(ctx, keys...).Err()
		if err != nil {
			return fmt.Errorf("failed to delete cache keys: %v", err)
		}
		log.Printf("Invalidated %d cache entries after rating update", len(keys))
	}

	// Также очищаем кэш ходов
	movesPattern := db.cachePattern(db.config.CacheMovesPattern)
	movesKeys, err := db.redis.Keys(ctx, movesPattern).Result()
	if err != nil {
		return fmt.Errorf("failed to get moves cache keys: %v", err)
	}

	if len(movesKeys) > 0 {
		err = db.redis.Del(ctx, movesKeys...).Err()
		if err != nil {
			return fmt.Errorf("failed to delete moves cache keys: %v", err)
		}
		log.Printf("Invalidated %d moves cache entries after rating update", len(movesKeys))
	}

	return nil
}

// getCachedSearchResult получает кэшированный результат поиска
func (db *ChessDB) getCachedSearchResult(positionHash int64, color string, count, depth int) (*SearchResult, error) {
	if db.redis == nil {
		return nil, nil
	}

	key := db.cacheKey(db.config.CacheSearchPattern, strconv.FormatInt(positionHash, 10), color, strconv.Itoa(count), strconv.Itoa(depth))
	data, err := db.redis.Get(context.Background(), key).Result()
	if err != nil {
		return nil, nil // Кэш miss - это нормально
	}

	var result SearchResult
	if err := json.Unmarshal([]byte(data), &result); err != nil {
		log.Printf("Failed to unmarshal cached search result: %v", err)
		return nil, err
	}

	log.Printf("Cache hit for search result: %s", key)
	return &result, nil
}

// setCachedSearchResult сохраняет результат поиска в кэш
func (db *ChessDB) setCachedSearchResult(positionHash int64, color string, count, depth int, result *SearchResult) error {
	if db.redis == nil {
		return nil
	}

	key := db.cacheKey(db.config.CacheSearchPattern, strconv.FormatInt(positionHash, 10), color, strconv.Itoa(count), strconv.Itoa(depth))
	data, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("failed to marshal search result: %v", err)
	}

	err = db.redis.Set(context.Background(), key, data, db.config.CacheSearchTTL).Err()
	if err != nil {
		log.Printf("Failed to cache search result: %v", err)
		return err
	}

	return nil
}

// getCachedFEN получает кэшированную FEN для последовательности ходов
func (db *ChessDB) getCachedFEN(moves []string) (string, error) {
	if db.redis == nil {
		return "", nil
	}

	movesHash := calculateMovesHash(moves)
	key := db.cacheKey(db.config.CacheFENPattern, strconv.FormatInt(movesHash, 10))

	data, err := db.redis.Get(context.Background(), key).Result()
	if err != nil {
		return "", nil
	}

	log.Printf("Cache hit for FEN: %s", key)
	return data, nil
}

// setCachedFEN сохраняет FEN в кэш
func (db *ChessDB) setCachedFEN(moves []string, fen string) error {
	if db.redis == nil {
		return nil
	}

	movesHash := calculateMovesHash(moves)
	key := db.cacheKey(db.config.CacheFENPattern, strconv.FormatInt(movesHash, 10))

	err := db.redis.Set(context.Background(), key, fen, db.config.CacheFENTTL).Err()
	if err != nil {
		log.Printf("Failed to cache FEN: %v", err)
		return err
	}

	log.Printf("Cached FEN: %s", key)
	return nil
}

// getCachedMoves получает кэшированные ходы для позиции
func (db *ChessDB) getCachedMoves(positionHash int64, color string) ([]Move, error) {
	if db.redis == nil {
		return nil, nil
	}

	key := db.cacheKey(db.config.CacheMovesPattern, strconv.FormatInt(positionHash, 10), color)
	data, err := db.redis.Get(context.Background(), key).Result()
	if err != nil {
		return nil, nil
	}

	var moves []Move
	if err := json.Unmarshal([]byte(data), &moves); err != nil {
		log.Printf("Failed to unmarshal cached moves: %v", err)
		return nil, err
	}

	log.Printf("Cache hit for moves: %s", key)
	return moves, nil
}

// setCachedMoves сохраняет ходы для позиции в кэш
func (db *ChessDB) setCachedMoves(positionHash int64, color string, moves []Move) error {
	if db.redis == nil {
		return nil
	}

	key := db.cacheKey(db.config.CacheMovesPattern, strconv.FormatInt(positionHash, 10), color)
	data, err := json.Marshal(moves)
	if err != nil {
		return fmt.Errorf("failed to marshal moves: %v", err)
	}

	err = db.redis.Set(context.Background(), key, data, db.config.CacheMovesTTL).Err()
	if err != nil {
		log.Printf("Failed to cache moves: %v", err)
		return err
	}

	log.Printf("Cached moves: %s (%d moves)", key, len(moves))
	return nil
}

// GetCachedPopularPositions получает кэшированный список популярных позиций
func (db *ChessDB) GetCachedPopularPositions() ([]int64, error) {
	if db.redis == nil {
		return nil, nil
	}

	data, err := db.redis.Get(context.Background(), db.cacheKey(db.config.CachePopularPositionsPattern)).Result()
	if err != nil {
		return nil, nil
	}

	var positions []int64
	if err := json.Unmarshal([]byte(data), &positions); err != nil {
		log.Printf("Failed to unmarshal popular positions: %v", err)
		return nil, err
	}

	log.Printf("Cache hit for popular positions: %d positions", len(positions))
	return positions, nil
}

// SetCachedPopularPositions сохраняет список популярных позиций в кэш
func (db *ChessDB) SetCachedPopularPositions(positions []int64) error {
	if db.redis == nil {
		return nil
	}

	data, err := json.Marshal(positions)
	if err != nil {
		return fmt.Errorf("failed to marshal popular positions: %v", err)
	}

	err = db.redis.Set(context.Background(), db.cacheKey(db.config.CachePopularPositionsPattern), data, db.config.CachePopularPositionsTTL).Err()
	if err != nil {
		log.Printf("Failed to cache popular positions: %v", err)
		return err
	}

	log.Printf("Cached popular positions: %d positions", len(positions))
	return nil
}

// GetECOTypicalMoves получает типичные последовательности ходов для ECO кода
func (db *ChessDB) GetECOTypicalMoves(ecoCode string) ([]string, error) {
	var moves []string

	// Получаем базовые ходы для ECO кода
	var baseMoves string
	err := db.pgDB.QueryRow("SELECT moves FROM eco_codes WHERE code = $1", ecoCode).Scan(&baseMoves)
	if err != nil {
		return nil, err
	}

	// Добавляем базовую последовательность
	moves = append(moves, baseMoves)

	// Добавляем несколько вариаций (упрощенно)
	if len(baseMoves) > 0 {
		parts := strings.Fields(baseMoves)
		if len(parts) >= 2 {
			// Добавляем укороченную версию
			moves = append(moves, strings.Join(parts[:len(parts)-1], " "))
		}
		if len(parts) >= 4 {
			// Добавляем еще более короткую версию
			moves = append(moves, strings.Join(parts[:len(parts)-2], " "))
		}
	}

	return moves, nil
}

// calculateMovesHash вычисляет хеш для последовательности ходов
func calculateMovesHash(moves []string) int64 {
	var hash uint64 = 0
	for _, move := range moves {
		for _, char := range move {
			hash = hash*31 + uint64(char)
		}
		hash = hash*31 + 1 // разделитель
	}
	return int64(hash)
}

// WarmupCache предварительно загружает в кэш популярные дебюты
func (db *ChessDB) WarmupCache() error {
	log.Println("Starting cache warmup...")

	// Сначала пробуем получить кэшированный список популярных позиций
	popularPositions, err := db.GetCachedPopularPositions()
	if err == nil && len(popularPositions) > 0 {
		log.Printf("Using %d cached popular positions", len(popularPositions))
		// Используем кэшированные позиции
		for _, positionHash := range popularPositions {
			db.warmupPosition(positionHash)
		}
	} else {
		// Получаем популярные ECO коды
		popularECOs := db.config.WarmupPopularECOCodes
		if len(popularECOs) == 0 {
			popularECOs = []string{"C20", "C50", "D20", "B20", "A00"}
		}

		var allPositions []int64

		for _, eco := range popularECOs {
			// Получаем типичные ходы для ECO кода
			moves, err := db.GetECOTypicalMoves(eco)
			if err != nil {
				log.Printf("Failed to get typical moves for %s: %v", eco, err)
				continue
			}

			// Для каждой типичной позиции предварительно кэшируем результаты поиска
			for _, moveSeq := range moves {
				cleanMoves := extractCleanMoves(moveSeq)
				if len(cleanMoves) == 0 {
					continue
				}

				// Генерируем FEN и кэшируем его
				fen, err := GenerateFENFromMoves(cleanMoves)
				if err != nil {
					continue
				}
				hash := CalculatePositionHash(fen)

				// Добавляем в список популярных позиций
				allPositions = append(allPositions, hash)

				// Предварительно кэшируем результаты поиска для разных параметров
				db.warmupPosition(hash)
			}
		}

		// Сохраняем список популярных позиций в кэш
		if len(allPositions) > 0 {
			if err := db.SetCachedPopularPositions(allPositions); err != nil {
				log.Printf("Failed to cache popular positions: %v", err)
			}
		}
	}

	log.Println("Cache warmup completed")
	return nil
}

// warmupPosition прогревает кэш для конкретной позиции
func (db *ChessDB) warmupPosition(positionHash int64) {
	for _, color := range []string{"white", "black"} {
		for _, count := range []int{3, 5} {
			for _, depth := range []int{1, 2, 3} {
				// Создаем пустой результат для предварительного кэширования
				result := &SearchResult{
					Continuations: []Continuation{},
					SearchTime:    0,
					TotalFound:    0,
				}
				if err := db.setCachedSearchResult(positionHash, color, count, depth, result); err != nil {
					log.Printf("    Failed to cache warmup result: %v", err)
				}
			}
		}
	}
}
