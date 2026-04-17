package db

import (
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"time"
)

// extractCleanMoves извлекает чистые ходы из PGN фрагмента
func extractCleanMoves(pgnFragment string) []string {
	var cleanMoves []string

	// Разбиваем на токены по пробелам
	tokens := strings.Fields(pgnFragment)

	for _, token := range tokens {
		// Пропускаем пустые токены
		if token == "" {
			continue
		}

		// Пропускаем номера ходов (например, "1.", "2.", "3...")
		// Но НЕ пропускаем ходы типа "1.e4" или "Bxe4"
		if strings.HasSuffix(token, ".") && len(token) <= 3 {
			// Проверяем, что это именно номер хода, а не часть хода
			if _, err := strconv.Atoi(strings.TrimSuffix(token, ".")); err == nil {
				continue
			}
		}

		// Пропускаем результаты игр
		if token == "1-0" || token == "0-1" || token == "1/2-1/2" || token == "*" {
			continue
		}

		// Если токен содержит номер хода в начале (например, "1.e4", "2.Nf3"),
		// извлекаем только часть после точки
		if strings.Contains(token, ".") {
			parts := strings.SplitN(token, ".", 2)
			if len(parts) == 2 {
				// Проверяем, что первая часть - это число
				if _, err := strconv.Atoi(parts[0]); err == nil {
					cleanMoves = append(cleanMoves, parts[1])
					continue
				}
			}
		}

		// Добавляем остальные токены как есть
		cleanMoves = append(cleanMoves, token)
	}

	return cleanMoves
}

// SearchContinuations ищет лучшие продолжения из заданной позиции
func (db *ChessDB) SearchContinuations(req SearchRequest) (*SearchResult, error) {
	startTime := time.Now()

	// 1. Получить из введенного фрагмента PGN чистые SAN ходы без номеров
	cleanMoves := extractCleanMoves(req.Moves)

	if len(cleanMoves) == 0 {
		return nil, fmt.Errorf("no valid moves found in PGN fragment")
	}

	// 2. Проверяем кэш FEN для последовательности ходов
	var currentFEN string
	var err error

	if cachedFEN, err := db.getCachedFEN(cleanMoves); err == nil && cachedFEN != "" {
		currentFEN = cachedFEN
		log.Printf("Cache hit for FEN generation")
	} else {
		// Генерируем FEN из ходов
		currentFEN, err = GenerateFENFromMoves(cleanMoves)
		if err != nil {
			return nil, fmt.Errorf("failed to generate FEN from moves: %v", err)
		}
		// Кэшируем FEN для будущих запросов
		if err := db.setCachedFEN(cleanMoves, currentFEN); err != nil {
			log.Printf("Failed to cache FEN: %v", err)
		}
	}

	currentHash := CalculatePositionHash(currentFEN)

	// 3. Проверяем кэш полного результата поиска
	if cachedResult, err := db.getCachedSearchResult(currentHash, req.Color, req.Count, req.Depth); err == nil && cachedResult != nil {
		log.Printf("Cache hit for search result: hash=%d, color=%s, count=%d, depth=%d", currentHash, req.Color, req.Count, req.Depth)
		cachedResult.SearchTime = 0 // Instant access from cache
		return cachedResult, nil
	}

	// 4. Найти позицию в таблице positions
	var positionExists bool
	err = db.pgDB.QueryRow("SELECT EXISTS(SELECT 1 FROM positions WHERE position_hash = $1)", currentHash).Scan(&positionExists)
	if err != nil {
		return nil, fmt.Errorf("failed to check position existence: %v", err)
	}

	if !positionExists {
		log.Printf("Position with hash %d not found in database", currentHash)

		// Попробуем найти похожие позиции
		log.Printf("Trying to find similar positions...")
		similarPositions := db.findSimilarPositions(currentHash)
		if len(similarPositions) > 0 {
			log.Printf("Found %d similar positions: %v", len(similarPositions), similarPositions)
		} else {
			log.Printf("No similar positions found")
		}

		result := &SearchResult{
			Continuations: []Continuation{},
			SearchTime:    time.Since(startTime).Milliseconds(),
			TotalFound:    0,
		}

		// Кэшируем пустой результат
		if err := db.setCachedSearchResult(currentHash, req.Color, req.Count, req.Depth, result); err != nil {
			log.Printf("Failed to cache empty result: %v", err)
		}
		return result, nil
	}

	// 5. Проверяем кэш ходов для позиции
	if cachedMoves, err := db.getCachedMoves(currentHash, req.Color); err == nil && cachedMoves != nil {
		log.Printf("Cache hit for moves: hash=%d, color=%s, %d moves", currentHash, req.Color, len(cachedMoves))

		// Обрабатываем кэшированные ходы
		continuations := db.processCachedMoves(cachedMoves, req)

		result := &SearchResult{
			Continuations: continuations,
			SearchTime:    time.Since(startTime).Milliseconds(),
			TotalFound:    len(continuations),
		}

		// Сохраняем результат в кэш
		if err := db.setCachedSearchResult(currentHash, req.Color, req.Count, req.Depth, result); err != nil {
			log.Printf("Failed to cache search result: %v", err)
		}
		return result, nil
	}

	// 6. Выполняем обычный поиск с кэшированием ходов
	continuations, err := db.searchWithDepthCached(currentHash, req.Color, req.Count, req.Depth)
	if err != nil {
		return nil, fmt.Errorf("search with depth failed: %v", err)
	}

	// Рассчитываем время поиска
	searchTime := time.Since(startTime).Milliseconds()

	result := &SearchResult{
		Continuations: continuations,
		SearchTime:    searchTime,
		TotalFound:    len(continuations),
	}

	// Сохраняем результат в кэш
	if err := db.setCachedSearchResult(currentHash, req.Color, req.Count, req.Depth, result); err != nil {
		log.Printf("Failed to cache search result: %v", err)
	}

	return result, nil
}

// searchWithDepthCached выполняет поиск продолжений с кэшированием
func (db *ChessDB) searchWithDepthCached(positionHash int64, color string, count, depth int) ([]Continuation, error) {
	// Сначала выполняем обычный поиск
	continuations, err := db.searchWithDepth(positionHash, color, count, depth)
	if err != nil {
		return nil, err
	}

	// Кэшируем ходы для базовой позиции (без глубины)
	if depth == 1 {
		// Получаем ходы из базы данных для кэширования
		moves, err := db.getMovesFromDB(positionHash, color)
		if err == nil && len(moves) > 0 {
			if err := db.setCachedMoves(positionHash, color, moves); err != nil {
				log.Printf("Failed to cache moves: %v", err)
			}
		}
	}

	return continuations, nil
}

// getMovesFromDB получает ходы из базы данных для кэширования
func (db *ChessDB) getMovesFromDB(positionHash int64, color string) ([]Move, error) {
	colorFilter := int16(0) // white
	if color == "black" {
		colorFilter = 1 // black
	}

	query := `SELECT m.move_from, m.move_to, m.piece_type, m.annotation, m.rating, 
                     m.games_count, m.wins, m.draws, m.losses, m.eco_code, m.move_number, m.child_hash
              FROM moves m 
              WHERE m.parent_hash = $1 AND m.color = $2 
              ORDER BY m.rating DESC, m.games_count DESC`

	rows, err := db.pgDB.Query(query, positionHash, colorFilter)
	if err != nil {
		return nil, fmt.Errorf("failed to query moves: %v", err)
	}
	defer rows.Close()

	var moves []Move
	for rows.Next() {
		var move Move
		err := rows.Scan(&move.MoveFrom, &move.MoveTo, &move.PieceType, &move.Annotation,
			&move.Rating, &move.GamesCount, &move.Wins, &move.Draws, &move.Losses,
			&move.ECOCode, &move.MoveNumber, &move.ChildHash)
		if err != nil {
			log.Printf("Failed to scan move row: %v", err)
			continue
		}
		moves = append(moves, move)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating moves: %v", err)
	}

	return moves, nil
}

// processCachedMoves обрабатывает кэшированные ходы в продолжения
func (db *ChessDB) processCachedMoves(moves []Move, req SearchRequest) []Continuation {
	var continuations []Continuation

	for _, move := range moves {
		// Преобразуем ход в алгебраическую нотацию
		moveStr := moveToAlgebraic(move.MoveFrom, move.MoveTo, move.PieceType)

		// Рассчитываем проценты
		totalGames := move.Wins + move.Draws + move.Losses
		var winRate, drawRate, lossRate float64
		if totalGames > 0 {
			winRate = float64(move.Wins) / float64(totalGames)
			drawRate = float64(move.Draws) / float64(totalGames)
			lossRate = float64(move.Losses) / float64(totalGames)
		}

		// Рассчитываем уверенность
		confidence := calculateConfidence(move.GamesCount, move.Rating)

		continuation := Continuation{
			Moves:       []string{moveStr},
			TotalRating: move.Rating,
			Confidence:  confidence,
			ECOCode:     move.ECOCode,
			WinRate:     winRate,
			DrawRate:    drawRate,
			LossRate:    lossRate,
			GamesCount:  move.GamesCount,
		}

		// Если глубина > 1, ищем продолжения рекурсивно
		if req.Depth > 1 {
			// Меняем цвет для следующего хода
			nextColor := "black"
			if req.Color == "black" {
				nextColor = "white"
			}

			// Рекурсивный поиск для дочерней позиции
			childContinuations, err := db.searchWithDepthCached(move.ChildHash, nextColor, req.Count, req.Depth-1)
			if err != nil {
				log.Printf("Failed to search deeper for child hash %d: %v", move.ChildHash, err)
				// Если не удалось найти продолжения, оставляем текущий ход
				continuations = append(continuations, continuation)
				continue
			}

			// Если найдены продолжения, добавляем рейтинг лучшего продолжения
			if len(childContinuations) > 0 {
				bestChild := childContinuations[0]
				// Суммируем рейтинги
				continuation.TotalRating += bestChild.TotalRating
				// Добавляем ходы дочернего продолжения
				continuation.Moves = append(continuation.Moves, bestChild.Moves...)
			}
		}

		continuations = append(continuations, continuation)
	}

	// Сортируем по убыванию суммарного рейтинга
	sort.Slice(continuations, func(i, j int) bool {
		return continuations[i].TotalRating > continuations[j].TotalRating
	})

	return continuations
}
func (db *ChessDB) searchWithDepth(positionHash int64, color string, count, depth int) ([]Continuation, error) {
	log.Printf("searchWithDepth called: hash=%d, color=%s, count=%d, depth=%d", positionHash, color, count, depth)

	// Определяем цвет для поиска
	colorFilter := int16(0) // white
	if color == "black" {
		colorFilter = 1 // black
	}

	// 4. Найти ходы в moves для которых эта позиция является родительской и выстроить их по убыванию рейтинга
	query := `SELECT m.move_from, m.move_to, m.piece_type, m.annotation, m.rating, 
                     m.games_count, m.wins, m.draws, m.losses, m.eco_code, m.move_number, m.child_hash
              FROM moves m 
              WHERE m.parent_hash = $1 AND m.color = $2 
              ORDER BY m.rating DESC, m.games_count DESC`

	rows, err := db.pgDB.Query(query, positionHash, colorFilter)
	if err != nil {
		return nil, fmt.Errorf("failed to query moves: %v", err)
	}
	defer rows.Close()

	var baseContinuations []struct {
		MoveFrom   int16
		MoveTo     int16
		PieceType  int16
		Annotation int16
		Rating     float64
		GamesCount int
		Wins       int
		Draws      int
		Losses     int
		ECOCode    string
		MoveNumber int16
		ChildHash  int64
	}

	for rows.Next() {
		var cont struct {
			MoveFrom   int16
			MoveTo     int16
			PieceType  int16
			Annotation int16
			Rating     float64
			GamesCount int
			Wins       int
			Draws      int
			Losses     int
			ECOCode    string
			MoveNumber int16
			ChildHash  int64
		}

		err := rows.Scan(&cont.MoveFrom, &cont.MoveTo, &cont.PieceType, &cont.Annotation,
			&cont.Rating, &cont.GamesCount, &cont.Wins, &cont.Draws, &cont.Losses,
			&cont.ECOCode, &cont.MoveNumber, &cont.ChildHash)
		if err != nil {
			log.Printf("Failed to scan move row: %v", err)
			continue
		}

		baseContinuations = append(baseContinuations, cont)
	}

	// Проверяем ошибки после итерации
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating moves: %v", err)
	}

	if len(baseContinuations) == 0 {
		log.Printf("No moves found for position hash %d with color %s", positionHash, color)
		return []Continuation{}, nil
	}

	log.Printf("Found %d base moves for position hash %d", len(baseContinuations), positionHash)

	// Если найдено меньше ходов, чем запрошено, попробуем найти дополнительные ходы
	// из позиций с похожим FEN (fallback логика)
	var additionalMoves []struct {
		MoveFrom   int16
		MoveTo     int16
		PieceType  int16
		Annotation int16
		Rating     float64
		GamesCount int
		Wins       int
		Draws      int
		Losses     int
		ECOCode    string
		MoveNumber int16
		ChildHash  int64
	}

	originalCount := len(baseContinuations)

	if len(baseContinuations) < count {
		additionalMoves = db.findSimilarPositionMoves(positionHash, colorFilter, count*3) // Ищем с запасом
		if len(additionalMoves) > 0 {
			log.Printf("Found %d additional moves from similar positions", len(additionalMoves))
			baseContinuations = append(baseContinuations, additionalMoves...)
		}
	}

	log.Printf("Total moves found: %d (original: %d, additional: %d)",
		len(baseContinuations),
		originalCount,
		len(additionalMoves))

	// 5. Далее на указанную глубину повторить поиск ходов
	// 6. Сложить рейтинг найденных ходов и выбрать заданное количество ходов с наибольшим рейтингом
	var finalContinuations []Continuation

	for _, baseCont := range baseContinuations {
		// Преобразуем ход в алгебраическую нотацию
		moveStr := moveToAlgebraic(baseCont.MoveFrom, baseCont.MoveTo, baseCont.PieceType)

		// Рассчитываем проценты
		totalGames := baseCont.Wins + baseCont.Draws + baseCont.Losses
		var winRate, drawRate, lossRate float64
		if totalGames > 0 {
			winRate = float64(baseCont.Wins) / float64(totalGames)
			drawRate = float64(baseCont.Draws) / float64(totalGames)
			lossRate = float64(baseCont.Losses) / float64(totalGames)
		}

		// Рассчитываем уверенность
		confidence := calculateConfidence(baseCont.GamesCount, baseCont.Rating)

		continuation := Continuation{
			Moves:       []string{moveStr},
			TotalRating: baseCont.Rating,
			Confidence:  confidence,
			ECOCode:     baseCont.ECOCode,
			WinRate:     winRate,
			DrawRate:    drawRate,
			LossRate:    lossRate,
			GamesCount:  baseCont.GamesCount,
		}

		// Если глубина > 1, ищем продолжения рекурсивно
		if depth > 1 {
			// Меняем цвет для следующего хода
			nextColor := "black"
			if color == "black" {
				nextColor = "white"
			}

			// Рекурсивный поиск для дочерней позиции
			childContinuations, err := db.searchWithDepth(baseCont.ChildHash, nextColor, count, depth-1)
			if err != nil {
				log.Printf("Failed to search deeper for child hash %d: %v", baseCont.ChildHash, err)
				// Если не удалось найти продолжения, оставляем текущий ход
				finalContinuations = append(finalContinuations, continuation)
				continue
			}

			// Если найдены продолжения, добавляем рейтинг лучшего продолжения
			if len(childContinuations) > 0 {
				bestChild := childContinuations[0]
				// Суммируем рейтинги
				continuation.TotalRating += bestChild.TotalRating
				// Добавляем ходы дочернего продолжения
				continuation.Moves = append(continuation.Moves, bestChild.Moves...)
			}
		}

		finalContinuations = append(finalContinuations, continuation)
	}

	// Сортируем по убыванию суммарного рейтинга
	sort.Slice(finalContinuations, func(i, j int) bool {
		return finalContinuations[i].TotalRating > finalContinuations[j].TotalRating
	})

	// НЕ ограничиваем количество результатов - возвращаем все найденные варианты
	// Это позволит пользователю видеть все доступные ходы, даже если их меньше запрошенного

	log.Printf("Returning %d final continuations (requested: %d) - showing all available", len(finalContinuations), count)

	return finalContinuations, nil
}

// findSimilarPositionMoves ищет ходы из позиций с похожим FEN
func (db *ChessDB) findSimilarPositionMoves(positionHash int64, colorFilter int16, limit int) []struct {
	MoveFrom   int16
	MoveTo     int16
	PieceType  int16
	Annotation int16
	Rating     float64
	GamesCount int
	Wins       int
	Draws      int
	Losses     int
	ECOCode    string
	MoveNumber int16
	ChildHash  int64
} {
	// Получаем FEN текущей позиции
	var currentFEN string
	err := db.pgDB.QueryRow("SELECT fen FROM positions WHERE position_hash = $1", positionHash).Scan(&currentFEN)
	if err != nil {
		log.Printf("Failed to get FEN for position %d: %v", positionHash, err)
		return nil
	}

	// Ищем позиции с похожим FEN (игнорируя ходы и права на рокировку)
	// Берем только основную часть FEN до информации о ходах
	fenParts := strings.Fields(currentFEN)
	if len(fenParts) < 4 {
		return nil
	}

	baseFEN := strings.Join(fenParts[:4], " ") // только позиция на доске и очередь хода

	// Ищем все позиции с такой же расстановкой фигур
	query := `
		SELECT m.move_from, m.move_to, m.piece_type, m.annotation, m.rating, 
		       m.games_count, m.wins, m.draws, m.losses, m.eco_code, m.move_number, m.child_hash
		FROM moves m
		INNER JOIN positions p ON m.parent_hash = p.position_hash
		WHERE p.fen LIKE $1 || '%' AND m.color = $2 AND m.parent_hash != $3
		ORDER BY m.rating DESC, m.games_count DESC
		LIMIT $4
	`

	rows, err := db.pgDB.Query(query, baseFEN, colorFilter, positionHash, limit*2) // берем с запасом
	if err != nil {
		log.Printf("Failed to query similar positions: %v", err)
		return nil
	}
	defer rows.Close()

	var similarMoves []struct {
		MoveFrom   int16
		MoveTo     int16
		PieceType  int16
		Annotation int16
		Rating     float64
		GamesCount int
		Wins       int
		Draws      int
		Losses     int
		ECOCode    string
		MoveNumber int16
		ChildHash  int64
	}

	for rows.Next() {
		var move struct {
			MoveFrom   int16
			MoveTo     int16
			PieceType  int16
			Annotation int16
			Rating     float64
			GamesCount int
			Wins       int
			Draws      int
			Losses     int
			ECOCode    string
			MoveNumber int16
			ChildHash  int64
		}

		err := rows.Scan(&move.MoveFrom, &move.MoveTo, &move.PieceType, &move.Annotation,
			&move.Rating, &move.GamesCount, &move.Wins, &move.Draws, &move.Losses,
			&move.ECOCode, &move.MoveNumber, &move.ChildHash)
		if err != nil {
			log.Printf("Failed to scan similar move: %v", err)
			continue
		}

		similarMoves = append(similarMoves, move)
	}

	// Проверяем ошибки после итерации
	if err = rows.Err(); err != nil {
		log.Printf("Error iterating similar moves: %v", err)
		return nil
	}

	// Ограничиваем количество
	if len(similarMoves) > limit {
		similarMoves = similarMoves[:limit]
	}

	return similarMoves
}

// findSimilarPositions ищет похожие позиции в базе данных
func (db *ChessDB) findSimilarPositions(_ int64) []int64 {
	// Ищем случайные позиции для тестирования
	var similarHashes []int64

	query := `
		SELECT position_hash 
		FROM positions 
		ORDER BY RANDOM()
		LIMIT 10
	`

	rows, err := db.pgDB.Query(query)
	if err != nil {
		log.Printf("Failed to query similar positions: %v", err)
		return nil
	}
	defer rows.Close()

	for rows.Next() {
		var hash int64
		if err := rows.Scan(&hash); err != nil {
			log.Printf("Failed to scan similar position hash: %v", err)
			continue
		}
		similarHashes = append(similarHashes, hash)
	}

	// Проверяем ошибки после итерации
	if err = rows.Err(); err != nil {
		log.Printf("Error iterating similar positions: %v", err)
		return nil
	}

	return similarHashes
}

// moveToAlgebraic преобразует ход в алгебраическую нотацию
func moveToAlgebraic(from, to, pieceType int16) string {
	// Преобразуем позиции в алгебраическую нотацию с устранением неоднозначности при необходимости
	pieceChar := ""
	switch pieceType {
	case 2:
		pieceChar = "N"
	case 3:
		pieceChar = "B"
	case 4:
		pieceChar = "R"
	case 5:
		pieceChar = "Q"
	case 6:
		pieceChar = "K"
	}

	toSquare := indexToSquare(to)

	// Для фигур, кроме пешек и королей, может потребоваться устранение неоднозначности
	// В полной реализации это проверяло бы, могут ли несколько фигур двигаться на ту же клетку
	// Пока что мы добавим исходную клетку для ладей, слонов и ферзей как базовое устранение неоднозначности
	if pieceType >= 3 && pieceType <= 5 { // Bishop, Rook, Queen
		fromSquare := indexToSquare(from)
		if fromSquare != "" {
			// Добавляем вертикаль (a-h) для устранения неоднозначности, когда несколько фигур могут достичь той же клетки
			pieceChar += string(fromSquare[0]) // Add file letter
		}
	}

	return pieceChar + toSquare
}

func indexToSquare(index int16) string {
	if index < 0 || index > 63 {
		return ""
	}

	file := rune('a' + (index % 8))
	rank := rune('1' + (index / 8))

	return string([]rune{file, rank})
}

func calculateConfidence(gamesCount int, _ float64) float64 {
	// Простой расчет уверенности на основе количества игр
	if gamesCount < 3 {
		return 0.3
	} else if gamesCount < 5 {
		return 0.6
	} else if gamesCount < 10 {
		return 0.8
	}
	return 1.0
}
