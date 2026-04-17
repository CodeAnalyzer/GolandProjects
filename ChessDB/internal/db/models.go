package db

import (
	"fmt"
	"math"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/notnil/chess"
)

// Глобальная Zobrist таблица для хеширования позиций
// Инициализируется один раз при старте программы
var (
	zobristTable     [12][64]uint64
	zobristSideToPlay uint64 = 0x123456789ABCDEF0
	zobristCastling   [4]uint64 = [4]uint64{0x123456789ABCDEF1, 0x123456789ABCDEF2, 0x123456789ABCDEF3, 0x123456789ABCDEF4}
	zobristOnce      sync.Once
)

// initZobristTable инициализирует Zobrist таблицу один раз
func initZobristTable() {
	zobristOnce.Do(func() {
		// Инициализация таблицы с использованием псевдослучайных чисел
		for piece := 0; piece < 12; piece++ {
			for square := 0; square < 64; square++ {
				zobristTable[piece][square] = uint64(piece*64+square+1) * 0x9E3779B97F4A7C15
			}
		}
	})
}

// Piece types (matching pgn package)
const (
	PiecePawn   = 1
	PieceKnight = 2
	PieceBishop = 3
	PieceRook   = 4
	PieceQueen  = 5
	PieceKing   = 6
)

// Game представляет шахматную партию
type Game struct {
	ID          string    `json:"id"`
	WhitePlayer string    `json:"white_player"`
	BlackPlayer string    `json:"black_player"`
	Result      string    `json:"result"`
	ECOCode     string    `json:"eco_code"`
	PGNContent  string    `json:"pgn_content"`
	Processed   bool      `json:"processed"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

// Position представляет состояние шахматной доски (позицию после хода)
type Position struct {
	ID           int64     `json:"id"`
	PositionHash int64     `json:"position_hash"`
	FEN          string    `json:"fen"`
	CreatedAt    time.Time `json:"created_at"`
}

// Move представляет переход между двумя позициями
type Move struct {
	ID         int64     `json:"id"`
	ParentHash int64     `json:"parent_hash"`
	ChildHash  int64     `json:"child_hash"`
	MoveNumber int       `json:"move_number"`
	MoveFrom   int16     `json:"move_from"`
	MoveTo     int16     `json:"move_to"`
	PieceType  int16     `json:"piece_type"`
	Color      int16     `json:"color"`      // 0=white, 1=black
	Annotation int16     `json:"annotation"` // оценка хода: 0=обычный, 1=!, 2=!!, 3=!!!, -1=?, -2=??
	Rating     float64   `json:"rating"`
	GamesCount int       `json:"games_count"`
	Wins       int       `json:"wins"`
	Draws      int       `json:"draws"`
	Losses     int       `json:"losses"`
	ECOCode    string    `json:"eco_code"`
	CreatedAt  time.Time `json:"created_at"`
}

// ECOCode представляет код Энциклопедии шахматных дебютов
type ECOCode struct {
	Code   string `json:"code"`
	Name   string `json:"name"`
	Moves  string `json:"moves"`
	Family string `json:"family"`
}

// SearchRequest представляет поисковый запрос для продолжений шахматных партий
type SearchRequest struct {
	Moves string `json:"moves"`
	Color string `json:"color"` // "white" or "black"
	Count int    `json:"count"`
	Depth int    `json:"depth"`
}

// SearchResult представляет результат поиска шахматной позиции
type SearchResult struct {
	Continuations []Continuation `json:"continuations"`
	SearchTime    int64          `json:"search_time"` // in milliseconds
	TotalFound    int            `json:"total_found"`
}

// Continuation представляет возможное продолжение хода
type Continuation struct {
	Moves       []string `json:"moves"`
	TotalRating float64  `json:"total_rating"`
	Confidence  float64  `json:"confidence"`
	ECOCode     string   `json:"eco_code"`
	WinRate     float64  `json:"win_rate"`
	DrawRate    float64  `json:"draw_rate"`
	LossRate    float64  `json:"loss_rate"`
	GamesCount  int      `json:"games_count"`
}

// ParallelImportConfig содержит конфигурацию для параллельного импорта
type ParallelImportConfig struct {
	MaxWorkers int
	BatchSize  int
}

// ImportStats содержит статистику для операций импорта
type ImportStats struct {
	TotalFiles     int           `json:"total_files"`
	ProcessedFiles int           `json:"processed_files"`
	TotalGames     int           `json:"total_games"`
	ProcessedGames int           `json:"processed_games"`
	TotalPositions int           `json:"total_positions"`
	Errors         []string      `json:"errors"`
	StartTime      time.Time     `json:"start_time"`
	EndTime        time.Time     `json:"end_time"`
	Duration       time.Duration `json:"duration"`
}

// TimingStats содержит детальную статистику по времени выполнения процедур
type TimingStats struct {
	mutex      sync.RWMutex
	Procedures map[string]*ProcedureTiming `json:"procedures"`
}

// ProcedureTiming содержит тайминги для конкретной процедуры
type ProcedureTiming struct {
	Name        string          `json:"name"`
	TotalTime   time.Duration   `json:"total_time"`
	CallCount   int             `json:"call_count"`
	AverageTime time.Duration   `json:"average_time"`
	MinTime     time.Duration   `json:"min_time"`
	MaxTime     time.Duration   `json:"max_time"`
	PerWorker   []time.Duration `json:"per_worker"` // время по каждому worker'у
}

// NewTimingStats создает новую структуру для сбора таймингов
func NewTimingStats() *TimingStats {
	return &TimingStats{
		Procedures: make(map[string]*ProcedureTiming),
	}
}

// RecordTiming записывает время выполнения процедуры
func (ts *TimingStats) RecordTiming(procedureName string, duration time.Duration, workerID int) {
	ts.mutex.Lock()
	defer ts.mutex.Unlock()

	proc, exists := ts.Procedures[procedureName]
	if !exists {
		proc = &ProcedureTiming{
			Name:      procedureName,
			MinTime:   duration,
			MaxTime:   duration,
			PerWorker: make([]time.Duration, 0),
		}
		ts.Procedures[procedureName] = proc
	}

	proc.TotalTime += duration
	proc.CallCount++
	proc.AverageTime = proc.TotalTime / time.Duration(proc.CallCount)

	if duration < proc.MinTime {
		proc.MinTime = duration
	}
	if duration > proc.MaxTime {
		proc.MaxTime = duration
	}

	// Обеспечиваем достаточный размер слайса для worker'ов
	for len(proc.PerWorker) <= workerID {
		proc.PerWorker = append(proc.PerWorker, 0)
	}
	proc.PerWorker[workerID] += duration
}

// GetSummary возвращает суммарную статистику
func (ts *TimingStats) GetSummary() map[string]*ProcedureTiming {
	ts.mutex.RLock()
	defer ts.mutex.RUnlock()

	result := make(map[string]*ProcedureTiming)
	for k, v := range ts.Procedures {
		result[k] = &ProcedureTiming{
			Name:        v.Name,
			TotalTime:   v.TotalTime,
			CallCount:   v.CallCount,
			AverageTime: v.AverageTime,
			MinTime:     v.MinTime,
			MaxTime:     v.MaxTime,
			PerWorker:   make([]time.Duration, len(v.PerWorker)),
		}
		copy(result[k].PerWorker, v.PerWorker)
	}
	return result
}

// ProcessedFile представляет обработанный PGN файл
type ProcessedFile struct {
	ID             string    `json:"id"`
	Filename       string    `json:"filename"`
	FilePath       string    `json:"file_path"`
	ProcessedAt    time.Time `json:"processed_at"`
	GamesCount     int       `json:"games_count"`
	PositionsCount int       `json:"positions_count"`
	FileSize       int64     `json:"file_size"`
	Status         string    `json:"status"`
}

// CalculatePositionHash вычисляет Zobrist хеш для шахматной позиции на основе FEN
// Использует глобальную Zobrist таблицу для максимальной производительности
func CalculatePositionHash(fen string) int64 {
	// Убеждаемся что таблица инициализирована
	initZobristTable()

	// Парсинг FEN
	parts := strings.Fields(fen)
	if len(parts) < 1 {
		return 0
	}

	board := parts[0] // Расстановка фигур
	var hash uint64 = 0

	// Анализ расстановки фигур
	rank := 7
	file := 0
	for _, char := range board {
		if char == '/' {
			rank--
			file = 0
			continue
		}

		if char >= '1' && char <= '8' {
			// Пустые клетки
			file += int(char - '0')
		} else {
			// Фигура
			pieceType := getPieceType(char)
			square := rank*8 + file

			if pieceType >= 0 {
				hash ^= zobristTable[pieceType][square]
			}
			file++
		}
	}

	// Учет права хода
	if len(parts) > 1 && parts[1] == "b" {
		hash ^= zobristSideToPlay
	}

	// Учет прав рокировки
	if len(parts) > 2 {
		castling := parts[2]
		if strings.Contains(castling, "K") {
			hash ^= zobristCastling[0] // Королевская рокировка белых
		}
		if strings.Contains(castling, "Q") {
			hash ^= zobristCastling[1] // Ферзевая рокировка белых
		}
		if strings.Contains(castling, "k") {
			hash ^= zobristCastling[2] // Королевская рокировка черных
		}
		if strings.Contains(castling, "q") {
			hash ^= zobristCastling[3] // Ферзевая рокировка черных
		}
	}

	return int64(hash)
}

// getPieceType преобразует символ фигуры в индекс для Zobrist таблицы
func getPieceType(piece rune) int {
	switch piece {
	case 'P':
		return 0 // Белая пешка
	case 'N':
		return 1 // Белый конь
	case 'B':
		return 2 // Белый слон
	case 'R':
		return 3 // Белая ладья
	case 'Q':
		return 4 // Белый ферзь
	case 'K':
		return 5 // Белый король
	case 'p':
		return 6 // Черная пешка
	case 'n':
		return 7 // Черный конь
	case 'b':
		return 8 // Черный слон
	case 'r':
		return 9 // Черная ладья
	case 'q':
		return 10 // Черный ферзь
	case 'k':
		return 11 // Черный король
	default:
		return -1
	}
}

// Board представляет шахматную доску с дополнительным состоянием игры
type Board struct {
	squares   [8][8]rune
	toMove    rune // 'w' or 'b'
	halfMoves int  // halfmoves since last capture or pawn advance
	fullMoves int  // full move number
}

// NewBoard создает новую доску в начальной позиции
func NewBoard() *Board {
	board := &Board{toMove: 'w', halfMoves: 0, fullMoves: 1}

	// Инициализируем фигуры в правильном порядке
	// Ранг 8 (индекс 0) - черные фигуры в нижнем регистре
	blackPieces := []rune{'r', 'n', 'b', 'q', 'k', 'b', 'n', 'r'}
	for i, piece := range blackPieces {
		board.squares[0][i] = piece // черные фигуры
		board.squares[1][i] = 'p'   // черные пешки
	}

	// Ранг 1 (индекс 7) - белые фигуры в верхнем регистре
	whitePieces := []rune{'R', 'N', 'B', 'Q', 'K', 'B', 'N', 'R'}
	for i, piece := range whitePieces {
		board.squares[6][i] = 'P'   // белые пешки
		board.squares[7][i] = piece // белые фигуры
	}

	return board
}

// MakeMove применяет ход к доске (упрощенная реализация)
func (b *Board) MakeMove(moveFrom, moveTo int16) {
	fromRow := int(moveFrom / 8)
	fromCol := int(moveFrom % 8)
	toRow := int(moveTo / 8)
	toCol := int(moveTo % 8)

	piece := b.squares[fromRow][fromCol]
	captured := b.squares[toRow][toCol] != 0

	// Перемещаем фигуру
	b.squares[toRow][toCol] = piece
	b.squares[fromRow][fromCol] = 0

	// Сбрасываем счетчик полуходов при взятии или ходе пешки
	if captured || piece == 'p' || piece == 'P' {
		b.halfMoves = 0
	} else {
		b.halfMoves++
	}

	// Меняем очередь хода и обновляем номер полного хода
	if b.toMove == 'w' {
		b.toMove = 'b'
	} else {
		b.toMove = 'w'
		b.fullMoves++
	}
}

// ToFEN преобразует доску в FEN строку
func (b *Board) ToFEN() string {
	var fen strings.Builder

	// Позиция на доске
	for row := 0; row < 8; row++ {
		empty := 0
		for col := 0; col < 8; col++ {
			if b.squares[row][col] == 0 {
				empty++
			} else {
				if empty > 0 {
					fen.WriteByte(byte('0' + empty))
					empty = 0
				}
				fen.WriteRune(b.squares[row][col])
			}
		}
		if empty > 0 {
			fen.WriteByte(byte('0' + empty))
		}
		if row < 7 {
			fen.WriteByte('/')
		}
	}

	// Очередь хода
	fen.WriteByte(' ')
	fen.WriteRune(b.toMove)

	// Права рокировки (упрощено - пока без отслеживания рокировок)
	fen.WriteString(" KQkq")

	// Взятие на проходе (упрощено)
	fen.WriteString(" -")

	// Счетчик полуходов и номер полного хода
	fen.WriteString(fmt.Sprintf(" %d %d", b.halfMoves, b.fullMoves))

	return fen.String()
}

// tryAmbiguousMoves пытается разные варианты дизамбигуации для неоднозначных ходов
func tryAmbiguousMoves(game *chess.Game, move string) error {
	var piece, target string
	var hasCapture bool

	// Определяем тип хода (взятие или обычный ход)
	if strings.Contains(move, "x") {
		// Взятие: Rxe8, Nxf3, Bxb5 и т.д.
		re := regexp.MustCompile(`^([KQRBN])x([a-h][1-8])`)
		matches := re.FindStringSubmatch(move)
		if len(matches) == 3 {
			piece = matches[1]
			target = matches[2]
			hasCapture = true
		}
	} else {
		// Обычный ход: Rf1, Ne4, Bc5 и т.д.
		re := regexp.MustCompile(`^([KQRBN])([a-h][1-8])$`)
		matches := re.FindStringSubmatch(move)
		if len(matches) == 3 {
			piece = matches[1]
			target = matches[2]
			hasCapture = false
		}
	}

	if piece == "" || target == "" {
		return fmt.Errorf("could not parse move: %s", move)
	}

	// Пробуем дизамбигуацию по файлу: a, b, c, d, e, f, g, h
	for _, file := range []string{"a", "b", "c", "d", "e", "f", "g", "h"} {
		var ambigMove string
		if hasCapture {
			ambigMove = piece + file + "x" + target
		} else {
			ambigMove = piece + file + target
		}
		if err := game.MoveStr(ambigMove); err == nil {
			return nil // успех
		}
	}

	// Пробуем дизамбигуацию по рангу: 1, 2, 3, 4, 5, 6, 7, 8
	for _, rank := range []string{"1", "2", "3", "4", "5", "6", "7", "8"} {
		var ambigMove string
		if hasCapture {
			ambigMove = piece + rank + "x" + target
		} else {
			ambigMove = piece + rank + target
		}
		if err := game.MoveStr(ambigMove); err == nil {
			return nil // успех
		}
	}

	return fmt.Errorf("could not find valid disambiguation for %s", move)
}

// GenerateFENFromMoves генерирует FEN для позиции после последовательности ходов
func GenerateFENFromMoves(moves []string) (string, error) {
	game := chess.NewGame()

	// Применяем каждый ход
	for _, moveStr := range moves {
		// Сначала нормализуем формат превращения
		normalizedMove := normalizePromotionFormat(moveStr)

		if err := game.MoveStr(normalizedMove); err != nil {
			// Пробуем убрать дизамбигуацию и повторить попытку
			cleanMove := removeDisambiguation(normalizedMove)
			if err2 := game.MoveStr(cleanMove); err2 != nil {
				// Пробуем убрать шах/мат annotations (+ и #) и повторить попытку
				cleanMove2 := removeCheckAnnotations(cleanMove)
				if err3 := game.MoveStr(cleanMove2); err3 != nil {
					// Пробуем разные варианты дизамбигуации для неоднозначных ходов
					if err4 := tryAmbiguousMoves(game, cleanMove2); err4 != nil {
						return "", fmt.Errorf("could not apply move %s (tried: %s, %s, %s): %v", moveStr, normalizedMove, cleanMove, cleanMove2, err3)
					}
				}
			}
		}
	}

	finalFEN := game.FEN()
	return finalFEN, nil
}

// normalizePromotionFormat конвертирует альтернативный формат превращения в стандартный
// Например: d1Q -> d1=Q, bxa8N -> bxa8=N, d1Q++ -> d1=Q++
func normalizePromotionFormat(move string) string {
	// Проверяем на альтернативный формат превращения: клетка + фигура
	re := regexp.MustCompile(`^([a-h]?x?)([a-h][1-8])([KQRBN])(\+\+?|\+#?|#?)$`)
	matches := re.FindStringSubmatch(move)

	if len(matches) == 5 {
		disambiguation := matches[1]
		targetSquare := matches[2]
		promoPiece := matches[3]
		suffix := matches[4]

		return disambiguation + targetSquare + "=" + promoPiece + suffix
	}

	return move
}

// removeDisambiguation убирает дизамбигуацию из шахматного хода
// Например: Ndxb5 -> Nxb5, Rfe1 -> Re1, B2d5 -> Bd5, R7xe8+ -> Rxe8+
func removeDisambiguation(move string) string {
	// Сначала обрабатываем числовую дизамбигуацию (горизонталь): R7xe8+ -> Rxe8+
	reNum := regexp.MustCompile(`^([KQRBN])([1-8])x([a-h][1-8].*)$`)
	matchesNum := reNum.FindStringSubmatch(move)
	if len(matchesNum) == 4 {
		piece := matchesNum[1]
		target := matchesNum[3]
		return piece + "x" + target
	}

	// Регулярное выражение для удаления файловой/ранговой дизамбигуации
	// Ищем паттерн: фигура + (файл/ранг) + взятие/целевая клетка
	re := regexp.MustCompile(`^([KQRBN])([a-h]?[1-8]?)(x?[a-h][1-8].*)$`)
	matches := re.FindStringSubmatch(move)

	if len(matches) == 4 {
		piece := matches[1]
		rest := matches[3]
		return piece + rest
	}

	// Для ходов пешек с дизамбигуацией взятия: exd5 -> exd5 (без изменений)
	rePawn := regexp.MustCompile(`^([a-h])x([a-h][1-8].*)$`)
	matchesPawn := rePawn.FindStringSubmatch(move)
	if len(matchesPawn) == 3 {
		return move // для пешек дизамбигуация обязательна при взятии
	}

	return move
}

// removeCheckAnnotations убирает аннотации шаха и мата из шахматного хода
// Например: Kf6+ -> Kf6, Qh8# -> Qh8, Rxf7++ -> Rxf7+
func removeCheckAnnotations(move string) string {
	// Заменяем двойной шах на одинарный (безусловная замена)
	move = strings.Replace(move, "++", "+", -1)

	// Убираем + (шах) и # (мат) в конце хода
	if strings.HasSuffix(move, "+") {
		return move[:len(move)-1]
	}
	if strings.HasSuffix(move, "#") {
		return move[:len(move)-1]
	}
	return move
}

// calculateMoveRating вычисляет рейтинг хода на основе аннотаций, результатов и частоты
func calculateMoveRating(annotation int16, wins, draws, losses, gamesCount int, totalGamesForPosition int) float64 {
	var baseRating float64

	// 1. Рейтинг на основе аннотаций PGN
	switch annotation {
	case 1: // !
		baseRating = 1.2
	case 2: // !!
		baseRating = 1.5
	case 3: // !!!
		baseRating = 2.0
	case -1: // ?
		baseRating = 0.8
	case -2: // ??
		baseRating = 0.5
	default: // без аннотации
		baseRating = 1.0
	}

	// 2. Рейтинг на основе результатов игры
	if gamesCount > 0 {
		totalGames := wins + draws + losses
		if totalGames > 0 {
			winRate := float64(wins) / float64(totalGames)
			drawRate := float64(draws) / float64(totalGames)

			// Вычисляем эффективность хода
			// Победа = 1.0, ничья = 0.5, поражение = 0.0
			effectiveness := winRate*1.0 + drawRate*0.5

			// Корректируем базовый рейтинг на основе эффективности
			// Эффективность > 0.5 увеличивает рейтинг, < 0.5 уменьшает
			if effectiveness > 0.5 {
				efficiencyBonus := (effectiveness - 0.5) * 0.8 // максимум +0.4
				baseRating += efficiencyBonus
			} else {
				efficiencyPenalty := (0.5 - effectiveness) * 0.6 // максимум -0.3
				baseRating -= efficiencyPenalty
			}
		}
	}

	// 3. Рейтинг на основе частоты использования (популярности)
	if totalGamesForPosition > 0 && gamesCount > 0 {
		popularity := float64(gamesCount) / float64(totalGamesForPosition)

		// Популярные ходы (более 10% всех ходов из позиции) получают бонус
		if popularity > 0.1 {
			popularityBonus := (popularity - 0.1) * 0.5 // максимум +0.45
			baseRating += popularityBonus
		}

		// Очень редкие ходы (менее 1%) получают штраф
		if popularity < 0.01 {
			rarityPenalty := 0.1
			baseRating -= rarityPenalty
		}
	}

	// Ограничиваем рейтинг в разумных пределах
	if baseRating > 3.0 {
		baseRating = 3.0
	}
	if baseRating < 0.1 {
		baseRating = 0.1
	}

	// Округляем до 2 знаков после запятой
	return math.Round(baseRating*100) / 100
}
