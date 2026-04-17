package db

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"ChessDB/internal/pgn"

	"golang.org/x/text/encoding/charmap"
)

// ImportError представляет ошибку импорта
type ImportError struct {
	Filename  string
	GameIndex int
	Error     string
	Timestamp time.Time
}

// ParallelImporter handles parallel import of PGN files
type ParallelImporter struct {
	chessDB *ChessDB
	config  ParallelImportConfig
	stats   ImportStats
	timing  *TimingStats
	mutex   sync.Mutex
	errors  []ImportError // накопление ошибок
}

// NewParallelImporter creates a new parallel importer
func NewParallelImporter(chessDB *ChessDB, config ParallelImportConfig) *ParallelImporter {
	return &ParallelImporter{
		chessDB: chessDB,
		config:  config,
		stats: ImportStats{
			StartTime: time.Now(),
		},
		timing: NewTimingStats(),
		errors: make([]ImportError, 0),
	}
}

// addError безопасно добавляет ошибку в список (thread-safe)
func (pi *ParallelImporter) addError(filename string, gameIndex int, err error) {
	pi.mutex.Lock()
	defer pi.mutex.Unlock()

	pi.errors = append(pi.errors, ImportError{
		Filename:  filename,
		GameIndex: gameIndex,
		Error:     err.Error(),
		Timestamp: time.Now(),
	})
}

// saveErrorsToFile сохраняет накопленные ошибки в файл import.log
func (pi *ParallelImporter) saveErrorsToFile(directory string) error {
	if len(pi.errors) == 0 {
		return nil // нет ошибок, файл не создаем
	}

	logFileName := pi.chessDB.config.ImportLogFile
	if logFileName == "" {
		logFileName = "import.log"
	}
	logPath := filepath.Join(directory, logFileName)
	file, err := os.Create(logPath)
	if err != nil {
		return fmt.Errorf("failed to create import.log: %v", err)
	}
	defer file.Close()

	// Записываем заголовок
	if _, err := file.WriteString("=== IMPORT ERRORS LOG ===\n"); err != nil {
		log.Printf("Failed to write log header: %v", err)
	}
	if _, err := file.WriteString(fmt.Sprintf("Generated: %s\n", time.Now().Format("2006-01-02 15:04:05"))); err != nil {
		log.Printf("Failed to write timestamp: %v", err)
	}
	if _, err := file.WriteString(fmt.Sprintf("Total errors: %d\n\n", len(pi.errors))); err != nil {
		log.Printf("Failed to write error count: %v", err)
	}

	// Записываем ошибки
	for _, impErr := range pi.errors {
		if _, err := file.WriteString(fmt.Sprintf("File: %s\n", impErr.Filename)); err != nil {
			log.Printf("Failed to write filename: %v", err)
		}
		if _, err := file.WriteString(fmt.Sprintf("Game: %d\n", impErr.GameIndex)); err != nil {
			log.Printf("Failed to write game index: %v", err)
		}
		if _, err := file.WriteString(fmt.Sprintf("Time: %s\n", impErr.Timestamp.Format("2006-01-02 15:04:05"))); err != nil {
			log.Printf("Failed to write timestamp: %v", err)
		}
		if _, err := file.WriteString(fmt.Sprintf("Error: %s\n", impErr.Error)); err != nil {
			log.Printf("Failed to write error: %v", err)
		}
		if _, err := file.WriteString("---\n"); err != nil {
			log.Printf("Failed to write separator: %v", err)
		}
	}

	log.Printf("Saved %d import errors to %s", len(pi.errors), logPath)
	return nil
}

// ImportFiles imports all PGN files from a directory
func (pi *ParallelImporter) ImportFiles(directory string) error {
	log.Printf("Starting parallel import from directory: %s", directory)

	// Find all PGN files
	files, err := pi.findPGNFiles(directory)
	if err != nil {
		return fmt.Errorf("failed to find PGN files: %v", err)
	}

	if len(files) == 0 {
		return fmt.Errorf("no PGN files found in directory: %s", directory)
	}

	pi.stats.TotalFiles = len(files)
	log.Printf("Found %d PGN files", len(files))

	// Create worker pool
	jobs := make(chan string, len(files))
	results := make(chan error, len(files))

	// Start workers
	for i := 0; i < pi.config.MaxWorkers; i++ {
		go pi.worker(jobs, results, i)
	}

	// Send jobs
	for _, file := range files {
		jobs <- file
	}
	close(jobs)

	// Collect results
	for i := 0; i < len(files); i++ {
		if err := <-results; err != nil {
			pi.mutex.Lock()
			pi.stats.Errors = append(pi.stats.Errors, err.Error())
			pi.mutex.Unlock()
		}
	}

	// Finalize stats
	pi.stats.EndTime = time.Now()
	pi.stats.Duration = pi.stats.EndTime.Sub(pi.stats.StartTime)

	log.Printf("Import completed. Files: %d/%d, Games: %d, Positions: %d, Duration: %v",
		pi.stats.ProcessedFiles, pi.stats.TotalFiles,
		pi.stats.ProcessedGames, pi.stats.TotalPositions, pi.stats.Duration)

	// Сохраняем ошибки в файл
	if err := pi.saveErrorsToFile(directory); err != nil {
		log.Printf("Failed to save import errors: %v", err)
	}

	// Вывод детальной статистики по таймингам
	pi.printTimingStats()

	return nil
}

func (pi *ParallelImporter) worker(jobs <-chan string, results chan<- error, workerID int) {
	for file := range jobs {
		err := pi.importFile(file, workerID)
		results <- err

		pi.mutex.Lock()
		pi.stats.ProcessedFiles++
		pi.mutex.Unlock()
	}
}

func (pi *ParallelImporter) importFile(filename string, workerID int) error {
	log.Printf("Processing file: %s", filename)

	// Check if file was already processed
	filenameOnly := filepath.Base(filename)
	if pi.chessDB.config.ImportSkipProcessedFiles {
		isProcessed, err := pi.chessDB.IsFileProcessed(filenameOnly)
		if err != nil {
			log.Printf("Warning: Failed to check if file %s is processed: %v", filename, err)
			// Continue processing anyway
		} else if isProcessed {
			log.Printf("Skipping already processed file: %s", filename)
			return nil
		}
	}

	// Read file with encoding conversion
	start := time.Now()
	content, err := pi.readFileWithEncoding(filename)
	pi.timing.RecordTiming("FileRead", time.Since(start), workerID)
	if err != nil {
		return fmt.Errorf("failed to read file %s: %v", filename, err)
	}

	// Parse PGN from entire file content
	start = time.Now()
	games := pgn.ParsePGN(content)
	pi.timing.RecordTiming("PGNParsing", time.Since(start), workerID)
	log.Printf("Found %d games in file %s", len(games), filename)

	gameCount := 0
	positionCount := 0

	// Process each game
	for i, game := range games {
		// Save game to database
		start = time.Now()
		dbGame := &Game{
			WhitePlayer: game.WhitePlayer,
			BlackPlayer: game.BlackPlayer,
			Result:      game.Result,
			ECOCode:     determineECOFromMoves(convertMovesToStrings(game.Moves), pi.chessDB),
			PGNContent:  convertMovesToPGNString(game.Moves),
			Processed:   false,
		}

		gameID, err := pi.chessDB.SaveGame(dbGame)
		pi.timing.RecordTiming("GameSaving", time.Since(start), workerID)
		if err != nil {
			pi.addError(filename, i, fmt.Errorf("Error saving game %d: %v", i, err))
			continue
		}

		// Process positions
		start = time.Now()
		positions, err := pi.processGamePositions(game, gameID, workerID)
		pi.timing.RecordTiming("PositionProcessing", time.Since(start), workerID)
		if err != nil {
			pi.addError(filename, i, fmt.Errorf("Error processing positions: %v", err))
			// Удаляем игру из базы, так как позиции не удалось обработать
			if deleteErr := pi.chessDB.DeleteGame(gameID); deleteErr != nil {
				pi.addError(filename, i, fmt.Errorf("Error deleting game %s after position processing failure: %v", gameID, deleteErr))
			}
			continue
		}

		gameCount++
		positionCount += positions
	}

	// Save processed file info
	fileInfo, err := os.Stat(filename)
	fileSize := int64(0)
	if err == nil {
		fileSize = fileInfo.Size()
	}

	err = pi.chessDB.SaveProcessedFile(filenameOnly, filename, gameCount, positionCount, fileSize)
	if err != nil {
		log.Printf("Warning: Failed to save processed file info for %s: %v", filename, err)
	}

	// Update stats
	pi.mutex.Lock()
	pi.stats.ProcessedGames += gameCount
	pi.stats.TotalPositions += positionCount
	pi.mutex.Unlock()

	log.Printf("Completed file %s: %d games, %d positions", filename, gameCount, positionCount)

	return nil
}

// readFileWithEncoding reads a file and converts encoding from Windows-1251 to UTF-8 if needed
func (pi *ParallelImporter) readFileWithEncoding(filename string) (string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return "", err
	}
	defer file.Close()

	// Read entire file as bytes first
	content, err := io.ReadAll(file)
	if err != nil {
		return "", err
	}

	// Check if content is valid UTF-8
	if utf8.Valid(content) {
		log.Printf("File %s is valid UTF-8", filename)
		return string(content), nil
	}

	// Try to decode as Windows-1251
	encodingFallback := strings.ToLower(pi.chessDB.config.ImportEncodingFallback)
	if encodingFallback == "" {
		encodingFallback = "windows-1251"
	}
	if encodingFallback != "windows-1251" {
		return string(content), nil
	}
	log.Printf("File %s is not valid UTF-8, trying Windows-1251", filename)
	decoder := charmap.Windows1251.NewDecoder()
	utf8Content, err := decoder.Bytes(content)
	if err != nil {
		log.Printf("Failed to decode %s as Windows-1251: %v", filename, err)
		// Fallback: try to replace invalid UTF-8 sequences
		return string(content), nil
	}

	log.Printf("Successfully converted %s from Windows-1251 to UTF-8", filename)
	return string(utf8Content), nil
}

func (pi *ParallelImporter) processGamePositions(game pgn.Game, _ string, workerID int) (int, error) {
	// Используем ходы с оценками из game.Moves
	moves := game.Moves

	if len(moves) == 0 {
		return 0, nil
	}

	// Конвертируем ходы в строки для ECO определения
	var moveStrings []string
	for _, move := range moves {
		moveStrings = append(moveStrings, move.SAN)
	}

	positionCount := 0

	// Определение ECO кода
	start := time.Now()
	ecoCode := determineECOFromMoves(moveStrings, pi.chessDB)
	pi.timing.RecordTiming("ECODetermination", time.Since(start), workerID)

	// Начальная позиция FEN
	currentFEN := "rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1"

	// Сохранить начальную позицию
	start = time.Now()
	initialHash := CalculatePositionHash(currentFEN)
	pi.timing.RecordTiming("HashCalculation", time.Since(start), workerID)

	start = time.Now()
	if err := pi.chessDB.SavePosition(&Position{
		PositionHash: initialHash,
		FEN:          currentFEN,
	}); err != nil {
		// Не добавляем в ошибки импорта, т.к. это не ошибка конкретной игры
		log.Printf("Error saving initial position: %v", err)
	}
	pi.timing.RecordTiming("PositionSaving", time.Since(start), workerID)

	// Process each move
	var fens []string
	fens = append(fens, "rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1") // начальная позиция

	// Генерируем все FEN за один проход
	start = time.Now()
	for i := range moves {
		fen, err := GenerateFENFromMoves(moveStrings[:i+1])
		if err != nil {
			// Ошибка уже добавлена в importFile, просто возвращаем ошибку наверх
			return 0, fmt.Errorf("неисправимая ошибка в партии, пропускаем: %v", err)
		}
		fens = append(fens, fen)
	}
	pi.timing.RecordTiming("FENGeneration", time.Since(start), workerID)

	for i, move := range moves {
		// Используем заранее сгенерированные FEN
		fenBefore := fens[i]
		currentFEN = fens[i+1]

		// Parse move
		moveFrom, moveTo, pieceType := pgn.ParseMove(move.SAN, fenBefore)
		if moveFrom == 0 && moveTo == 0 {
			// Это отладочная информация, оставляем как log.Printf
			log.Printf("Zero: move.SAN = %v, moveFrom = %v, moveTo = %v", move.SAN, moveFrom, moveTo)
			log.Printf("      fenBefore  = %v", fenBefore)
		}

		// Calculate position hash after this move
		start = time.Now()
		positionHash := CalculatePositionHash(currentFEN)
		pi.timing.RecordTiming("HashCalculation", time.Since(start), workerID)

		// Save the new position
		start = time.Now()
		if err := pi.chessDB.SavePosition(&Position{
			PositionHash: positionHash,
			FEN:          currentFEN,
		}); err != nil {
			// Ошибка сохранения позиции - добавляем в ошибки импорта
			pi.addError(fmt.Sprintf("game_%d", i), i, fmt.Errorf("Error saving position %d: %v", i+1, err))
			continue
		}
		pi.timing.RecordTiming("PositionSaving", time.Since(start), workerID)

		// Determine color - who made THIS move (not who should move next)
		color := int16(0) // white made this move
		if i%2 == 1 {
			color = 1 // black made this move
		}

		// Create move record
		parentHash := initialHash
		if i > 0 {
			parentHash = CalculatePositionHash(fenBefore)
		}

		// Получаем общее количество ходов из родительской позиции для вычисления популярности
		totalGamesForPosition := pi.getTotalGamesForPosition(parentHash)

		// Вычисляем рейтинг на основе аннотаций и результатов
		// Пока используем базовые значения, результаты обновятся позже
		rating := calculateMoveRating(int16(move.Annotation), 0, 0, 0, 1, totalGamesForPosition)

		moveRecord := &Move{
			ParentHash: parentHash,
			ChildHash:  positionHash,
			MoveNumber: i + 1,
			MoveFrom:   moveFrom,
			MoveTo:     moveTo,
			PieceType:  pieceType,
			Color:      color,
			Annotation: int16(move.Annotation),
			Rating:     rating,
			GamesCount: 1,
			Wins:       0,
			Draws:      0,
			Losses:     0,
			ECOCode:    ecoCode,
		}

		// Update result statistics ONLY for final position
		if i == len(moves)-1 {
			pi.updateResultStatsForFinalMove(moveRecord, game.Result)
		}

		// Save move
		start = time.Now()
		if err := pi.chessDB.SaveMove(moveRecord); err != nil {
			log.Printf("Error saving move %d: %v", i+1, err)
			continue
		}
		pi.timing.RecordTiming("MoveSaving", time.Since(start), workerID)

		positionCount++
	}

	return positionCount, nil
}

// getTotalGamesForPosition получает общее количество ходов из указанной позиции
func (pi *ParallelImporter) getTotalGamesForPosition(positionHash int64) int {
	var totalGames int
	err := pi.chessDB.pgDB.QueryRow("SELECT COALESCE(SUM(games_count), 0) FROM moves WHERE parent_hash = $1", positionHash).Scan(&totalGames)
	if err != nil {
		log.Printf("Error getting total games for position %d: %v", positionHash, err)
		return 0
	}
	return totalGames
}

func (pi *ParallelImporter) updateResultStatsForFinalMove(move *Move, result string) {
	switch result {
	case "1-0":
		if move.Color == 0 {
			move.Wins = 1
		} else {
			move.Losses = 1
		}
	case "0-1":
		if move.Color == 0 {
			move.Losses = 1
		} else {
			move.Wins = 1
		}
	case "1/2-1/2":
		move.Draws = 1
	}
}

func (pi *ParallelImporter) findPGNFiles(directory string) ([]string, error) {
	var files []string

	err := filepath.Walk(directory, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !info.IsDir() && strings.ToLower(filepath.Ext(path)) == ".pgn" {
			files = append(files, path)
		}

		return nil
	})

	return files, err
}

// determineECOFromMoves определяет ECO код на основе последовательности ходов
// Использует базу данных для поиска наиболее точного совпадения
func determineECOFromMoves(moves []string, db *ChessDB) string {
	// Если ходов мало, возвращаем A00 (нераспознанный дебют)
	if len(moves) == 0 {
		return "A00"
	}

	var bestMatch *ECOCode
	var lastValidMatch *ECOCode

	// Итеративно ищем по количеству ходов
	for moveCount := 1; moveCount <= len(moves); moveCount++ {
		currentMoves := moves[:moveCount]
		movePattern := strings.Join(currentMoves, " ")

		// Ищем ECO коды, которые начинаются с текущей последовательности ходов
		matchingCodes, err := db.GetECOCodesByMovesPrefix(movePattern)
		if err != nil {
			continue
		}

		if len(matchingCodes) == 1 {
			// Нашли единственное совпадение - это наш ответ
			bestMatch = &matchingCodes[0]
			break
		} else if len(matchingCodes) == 0 {
			// Нет совпадений - используем последний найденный
			break
		} else {
			// Несколько совпадений - запоминаем и продолжаем поиск
			lastValidMatch = &matchingCodes[0]
		}
	}

	// Возвращаем результат
	if bestMatch != nil {
		return bestMatch.Code
	} else if lastValidMatch != nil {
		return lastValidMatch.Code
	} else {
		return "A00"
	}
}

func convertMovesToStrings(moves []pgn.Move) []string {
	var result []string
	for _, move := range moves {
		result = append(result, move.SAN)
	}
	return result
}

func convertMovesToPGNString(moves []pgn.Move) string {
	var parts []string
	for i, move := range moves {
		if i%2 == 0 {
			parts = append(parts, fmt.Sprintf("%d.%s", i/2+1, move.SAN))
		} else {
			parts = append(parts, move.SAN)
		}
	}
	return strings.Join(parts, " ")
}

// printTimingStats выводит детальную статистику по времени выполнения процедур
func (pi *ParallelImporter) printTimingStats() {
	log.Printf("\n=== DETAILED TIMING STATISTICS ===")

	summary := pi.timing.GetSummary()

	// Сортируем процедуры по общему времени выполнения
	type ProcedureSort struct {
		name   string
		timing *ProcedureTiming
	}

	var sortedProcedures []ProcedureSort
	for name, timing := range summary {
		sortedProcedures = append(sortedProcedures, ProcedureSort{name, timing})
	}

	// Сортировка по общему времени (убывание)
	sort.Slice(sortedProcedures, func(i, j int) bool {
		return sortedProcedures[i].timing.TotalTime > sortedProcedures[j].timing.TotalTime
	})

	log.Printf("Procedures sorted by total time:")
	log.Printf("%-20s %-12s %-12s %-12s %-12s %-12s %-15s",
		"Procedure", "Total(s)", "Calls", "Avg(ms)", "Min(ms)", "Max(ms)", "PerWorker(s)")
	log.Printf("%-20s %-12s %-12s %-12s %-12s %-12s %-15s",
		"--------", "-------", "-----", "-------", "-------", "-------", "-------------")

	for _, proc := range sortedProcedures {
		timing := proc.timing
		perWorkerStr := ""
		for i, workerTime := range timing.PerWorker {
			if i > 0 {
				perWorkerStr += ","
			}
			perWorkerStr += fmt.Sprintf("W%d:%.1f", i, float64(workerTime.Nanoseconds())/1000000000)
		}

		log.Printf("%-20s %-12.3f %-12d %-12.1f %-12.1f %-12.1f %-15s",
			proc.name,
			float64(timing.TotalTime.Nanoseconds())/1000000000,
			timing.CallCount,
			float64(timing.AverageTime.Nanoseconds())/1000000,
			float64(timing.MinTime.Nanoseconds())/1000000,
			float64(timing.MaxTime.Nanoseconds())/1000000,
			perWorkerStr)
	}

	// Дополнительная аналитика
	log.Printf("\n=== PERFORMANCE ANALYSIS ===")

	totalTime := time.Duration(0)
	for _, timing := range summary {
		totalTime += timing.TotalTime
	}

	log.Printf("Total time spent in all procedures: %.3f seconds",
		float64(totalTime.Nanoseconds())/1000000000)

	// Находим самые медленные процедуры
	if len(sortedProcedures) > 0 {
		slowestProc := sortedProcedures[0]
		slowestPct := float64(slowestProc.timing.TotalTime.Nanoseconds()) / float64(totalTime.Nanoseconds()) * 100
		log.Printf("Slowest procedure: %s (%.1f%% of total time, avg: %.1fms)",
			slowestProc.name, slowestPct,
			float64(slowestProc.timing.AverageTime.Nanoseconds())/1000000)
	}

	// Анализ баланса нагрузки между worker'ами
	log.Printf("\n=== WORKER LOAD BALANCE ===")
	for _, proc := range sortedProcedures {
		if len(proc.timing.PerWorker) > 1 {
			maxWorkerTime := time.Duration(0)
			minWorkerTime := proc.timing.PerWorker[0]
			for _, workerTime := range proc.timing.PerWorker {
				if workerTime > maxWorkerTime {
					maxWorkerTime = workerTime
				}
				if workerTime < minWorkerTime || minWorkerTime == 0 {
					minWorkerTime = workerTime
				}
			}

			if minWorkerTime > 0 {
				imbalance := float64(maxWorkerTime-minWorkerTime) / float64(minWorkerTime) * 100
				log.Printf("%s: load imbalance %.1f%% (max: %.1fs, min: %.1fs)",
					proc.name, imbalance,
					float64(maxWorkerTime.Nanoseconds())/1000000000,
					float64(minWorkerTime.Nanoseconds())/1000000000)
			}
		}
	}

	log.Printf("=== END TIMING STATISTICS ===\n")
}
