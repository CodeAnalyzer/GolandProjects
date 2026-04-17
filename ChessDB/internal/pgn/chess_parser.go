package pgn

import (
	"regexp"
	"strings"
)

// Типы фигур
const (
	PiecePawn   = 1
	PieceKnight = 2
	PieceBishop = 3
	PieceRook   = 4
	PieceQueen  = 5
	PieceKing   = 6
)

// ChessMove представляет разобранный шахматный ход
type ChessMove struct {
	From      int16  // 0-63 (a1=0, h8=63)
	To        int16  // 0-63 (a1=0, h8=63)
	PieceType int16  // 1-6 (пешка, конь, слон, ладья, ферзь, король)
	Capture   bool   // ход с взятием
	Check     bool   // шах
	Checkmate bool   // мат
	Promotion int16  // тип фигуры при превращении (0 если нет)
	Castle    string // "O-O" или "O-O-O" или ""
	EnPassant bool   // взятие на проходе
}

// Сопоставление клеток: a1=0, b1=1, ..., h1=7, a2=8, ..., h8=63
func squareToIndex(square string) int16 {
	if len(square) != 2 {
		return -1
	}

	file := int(square[0] - 'a')
	rank := int(square[1] - '1')

	if file < 0 || file > 7 || rank < 0 || rank > 7 {
		return -1
	}

	return int16(rank*8 + file)
}

func parseFENToBoard(fen string) [8][8]rune {
	var board [8][8]rune
	parts := strings.Fields(fen)
	if len(parts) == 0 {
		return board
	}
	boardStr := parts[0]
	rank := 7
	file := 0
	for _, char := range boardStr {
		if char == '/' {
			rank--
			file = 0
			continue
		}
		if char >= '1' && char <= '8' {
			empty := int(char - '0')
			file += empty
		} else {
			board[rank][file] = char
			file++
		}
	}
	return board
}

func pieceTypeFromChar(piece rune) int16 {
	switch strings.ToUpper(string(piece)) {
	case "P":
		return PiecePawn
	case "N":
		return PieceKnight
	case "B":
		return PieceBishop
	case "R":
		return PieceRook
	case "Q":
		return PieceQueen
	case "K":
		return PieceKing
	default:
		return PiecePawn // default to pawn
	}
}

// ParseMove разбирает шахматный ход в алгебраической нотации с текущим FEN
func ParseMove(move string, fen string) (int16, int16, int16) {
	board := parseFENToBoard(fen)

	// Извлекаем информацию о ходе из FEN
	parts := strings.Fields(fen)
	isWhiteTurn := true // по умолчанию
	if len(parts) > 1 && parts[1] == "b" {
		isWhiteTurn = false
	}

	move = strings.TrimSpace(move)

	// Обрабатываем рокировку
	if move == "O-O" || move == "O-O+" || move == "O-O#" {
		// Короткая рокировка: e1-g1 (белые) или e8-g8 (черные)
		return 4, 6, PieceKing // e1 to g1
	}
	if move == "O-O-O" || move == "O-O-O+" || move == "O-O-O#" {
		// Длинная рокировка: e1-c1 (белые) или e8-c8 (черные)
		return 4, 2, PieceKing // e1 to c1
	}

	// Обрабатываем превращения (оба формата: со знаком = и без)
	if strings.Contains(move, "=") || regexp.MustCompile(`[a-h]?x?[a-h][1-8][NBRQK][+#]?$`).MatchString(move) {
		return parsePromotionMove(move, board, isWhiteTurn)
	}

	// Обрабатываем взятия
	if strings.Contains(move, "x") {
		return parseCaptureMove(move, board, isWhiteTurn)
	}

	// Обрабатываем обычные ходы
	return parseNormalMove(move, board, isWhiteTurn)
}

func parseNormalMove(move string, board [8][8]rune, isWhiteTurn bool) (int16, int16, int16) {
	// Удаляем символы шаха и мата для разбора
	cleanMove := strings.TrimSuffix(strings.TrimSuffix(move, "#"), "+")

	// Шаблон: Nf3, Bb5, Rfe1, e4 и т.д.
	re := regexp.MustCompile(`^([NBRQK])?([a-h]?[1-8]?)([a-h][1-8])$`)
	matches := re.FindStringSubmatch(cleanMove)

	if len(matches) != 4 {
		// Попробовать шаблон хода пешки: e4, d5 и т.д.
		re2 := regexp.MustCompile(`^([a-h][1-8])$`)
		matches2 := re2.FindStringSubmatch(cleanMove)
		if len(matches2) == 2 {
			targetSquare := matches2[1]
			toIndex := squareToIndex(targetSquare)
			if toIndex == -1 {
				return 0, 0, PiecePawn
			}
			fromIndex := findPieceForTarget(board, toIndex, PiecePawn, "", isWhiteTurn)
			return fromIndex, toIndex, PiecePawn
		}
		return 0, 0, PiecePawn // fallback
	}

	pieceChar := matches[1]
	disambiguation := matches[2]
	targetSquare := matches[3]

	pieceType := int16(PiecePawn)
	if pieceChar != "" {
		pieceType = pieceTypeFromChar(rune(pieceChar[0]))
	}

	toIndex := squareToIndex(targetSquare)
	if toIndex == -1 {
		return 0, 0, PiecePawn
	}

	fromIndex := findPieceForTarget(board, toIndex, pieceType, disambiguation, isWhiteTurn)

	return fromIndex, toIndex, pieceType
}

func parseCaptureMove(move string, board [8][8]rune, isWhiteTurn bool) (int16, int16, int16) {
	// Удаляем символы шаха и мата для разбора
	cleanMove := strings.TrimSuffix(strings.TrimSuffix(move, "#"), "+")

	// Шаблон: Nxf3, Bxb5, exd5 и т.д.
	re := regexp.MustCompile(`^([NBRQK])?([a-h]?[1-8]?)x([a-h][1-8])$`)
	matches := re.FindStringSubmatch(cleanMove)

	if len(matches) != 4 {
		return 0, 0, PiecePawn // fallback
	}

	pieceChar := matches[1]
	disambiguation := matches[2]
	targetSquare := matches[3]

	pieceType := int16(PiecePawn)
	if pieceChar != "" {
		pieceType = pieceTypeFromChar(rune(pieceChar[0]))
	}

	toIndex := squareToIndex(targetSquare)
	if toIndex == -1 {
		return 0, 0, PiecePawn
	}

	fromIndex := findPieceForTarget(board, toIndex, pieceType, disambiguation, isWhiteTurn)

	return fromIndex, toIndex, pieceType
}

func parsePromotionMove(move string, board [8][8]rune, isWhiteTurn bool) (int16, int16, int16) {
	// Сначала пробуем стандартный формат: a8=Q, bxa8=N+ и т.д.
	re := regexp.MustCompile(`^([a-h]?x?)([a-h][1-8])=([NBRQK])`)
	matches := re.FindStringSubmatch(move)

	if len(matches) == 4 {
		disambiguation := matches[1]
		targetSquare := matches[2]
		promoChar := matches[3]

		toIndex := squareToIndex(targetSquare)
		if toIndex == -1 {
			return 0, 0, PiecePawn
		}

		pieceType := pieceTypeFromChar(rune(promoChar[0]))
		fromIndex := findPieceForTarget(board, toIndex, PiecePawn, disambiguation, isWhiteTurn)

		return fromIndex, toIndex, pieceType
	}

	// Пробуем альтернативный формат: a8Q, bxa8N+ и т.д.
	re2 := regexp.MustCompile(`^([a-h]?x?)([a-h][1-8])([NBRQK])`)
	matches2 := re2.FindStringSubmatch(move)

	if len(matches2) == 4 {
		disambiguation := matches2[1]
		targetSquare := matches2[2]
		promoChar := matches2[3]

		toIndex := squareToIndex(targetSquare)
		if toIndex == -1 {
			return 0, 0, PiecePawn
		}

		pieceType := pieceTypeFromChar(rune(promoChar[0]))
		fromIndex := findPawnForPromotion(board, toIndex, disambiguation, isWhiteTurn)

		return fromIndex, toIndex, pieceType
	}

	return 0, 0, PiecePawn // fallback
}

func findPawnForPromotion(board [8][8]rune, toIndex int16, disambiguation string, isWhiteTurn bool) int16 {
	toRow := int(toIndex / 8)

	// Для превращения пешка должна быть на соседней горизонтали
	var pawnRow int
	if isWhiteTurn {
		pawnRow = toRow - 1 // белая пешка подходит с 7-й на 8-ю
	} else {
		pawnRow = toRow + 1 // черная пешка подходит с 2-й на 1-ю
	}

	// Если есть дизамбигуация по файлу, используем её
	if len(disambiguation) > 0 && disambiguation[len(disambiguation)-1] != 'x' {
		pawnCol := int(disambiguation[0] - 'a')
		if pawnCol >= 0 && pawnCol < 8 && pawnRow >= 0 && pawnRow < 8 {
			piece := board[pawnRow][pawnCol]
			if piece != 0 {
				isWhitePiece := piece >= 'A' && piece <= 'Z'
				if isWhitePiece == isWhiteTurn {
					pieceType := pieceTypeFromChar(piece)
					if pieceType == PiecePawn {
						return int16(pawnRow*8 + pawnCol)
					}
				}
			}
		}
	}

	// Ищем все пешки на правильной горизонтали
	var candidates []int16
	for col := 0; col < 8; col++ {
		if pawnRow < 0 || pawnRow >= 8 {
			continue
		}

		piece := board[pawnRow][col]
		if piece == 0 {
			continue
		}

		// Проверяем цвет фигуры
		isWhitePiece := piece >= 'A' && piece <= 'Z'
		if isWhitePiece != isWhiteTurn {
			continue
		}

		// Проверяем, что это пешка
		pieceType := pieceTypeFromChar(piece)
		if pieceType == PiecePawn {
			// Проверяем, может ли пешка достичь целевого поля
			from := int16(pawnRow*8 + col)
			if canPawnReach(from, toIndex, board) {
				candidates = append(candidates, from)
			}
		}
	}

	if len(candidates) > 0 {
		return candidates[0] // возвращаем первого кандидата
	}

	return -1
}

func canPawnReach(from, to int16, board [8][8]rune) bool {
	fromRow := int(from / 8)
	fromCol := int(from % 8)
	toRow := int(to / 8)
	toCol := int(to % 8)

	piece := board[fromRow][fromCol]
	if piece == 0 {
		return false
	}

	// Определяем цвет пешки
	isWhitePawn := piece == 'P'

	// Прямой ход (без взятия)
	if fromCol == toCol {
		if isWhitePawn {
			// Белая пешка движется вверх
			return toRow == fromRow+1 || (fromRow == 1 && toRow == 3) // начальный ход на 2 клетки
		} else {
			// Черная пешка движется вниз
			return toRow == fromRow-1 || (fromRow == 6 && toRow == 4) // начальный ход на 2 клетки
		}
	}

	// Взятие по диагонали
	if abs(toCol-fromCol) == 1 {
		if isWhitePawn {
			return toRow == fromRow+1 // белая пешка берет по диагонали вверх
		} else {
			return toRow == fromRow-1 // черная пешка берет по диагонали вниз
		}
	}

	return false
}

func findPieceForTarget(board [8][8]rune, to int16, pieceType int16, disambiguation string, isWhiteTurn bool) int16 {
	var candidates []int16
	for row := 0; row < 8; row++ {
		for col := 0; col < 8; col++ {
			piece := board[row][col]
			if piece == 0 {
				continue
			}
			// Проверяем цвет фигуры
			isWhitePiece := piece >= 'A' && piece <= 'Z'
			if isWhitePiece != isWhiteTurn {
				continue // Пропускаем фигуры другого цвета
			}
			currentPieceType := pieceTypeFromChar(piece)
			if currentPieceType == pieceType {
				from := int16(row*8 + col)
				if canPieceMoveTo(board, from, to, piece) {
					// Check disambiguation
					if disambiguation == "" || matchesDisambiguation(row, col, disambiguation) {
						candidates = append(candidates, from)
					}
				}
			}
		}
	}

	if len(candidates) > 0 {
		return candidates[0] // return first
	}

	return -1
}

func matchesDisambiguation(row, col int, disambiguation string) bool {
	if disambiguation == "" {
		return true
	}

	// disambiguation by file: a-h
	if len(disambiguation) == 1 && disambiguation[0] >= 'a' && disambiguation[0] <= 'h' {
		expectedCol := int(disambiguation[0] - 'a')
		return col == expectedCol
	}

	// disambiguation by rank: 1-8
	if len(disambiguation) == 1 && disambiguation[0] >= '1' && disambiguation[0] <= '8' {
		expectedRow := int(disambiguation[0] - '1')
		return row == expectedRow
	}

	// full: a1, b2 etc
	if len(disambiguation) == 2 {
		expectedCol := int(disambiguation[0] - 'a')
		expectedRow := int(disambiguation[1] - '1')
		return row == expectedRow && col == expectedCol
	}

	return false
}

func canPieceMoveTo(board [8][8]rune, from, to int16, piece rune) bool {
	fromRow := int(from / 8)
	fromCol := int(from % 8)
	toRow := int(to / 8)
	toCol := int(to % 8)

	// Проверить, есть ли на клетке назначения фигура того же цвета
	destPiece := board[toRow][toCol]
	if destPiece != 0 {
		if (piece >= 'A' && piece <= 'Z' && destPiece >= 'A' && destPiece <= 'Z') ||
			(piece >= 'a' && piece <= 'z' && destPiece >= 'a' && destPiece <= 'z') {
			return false
		}
	}

	// Упрощенная проверка перемещений фигур
	switch piece {
	case 'P': // Белая пешка
		if fromCol == toCol && destPiece == 0 {
			if toRow == fromRow+1 {
				return true
			}
			if fromRow == 1 && toRow == fromRow+2 && destPiece == 0 {
				return true
			}
		}
		// Взятие (по диагонали)
		if abs(toCol-fromCol) == 1 && toRow == fromRow+1 && destPiece != 0 {
			return true
		}
	case 'p': // Черная пешка
		if fromCol == toCol && destPiece == 0 {
			if toRow == fromRow-1 {
				return true
			}
			if fromRow == 6 && toRow == fromRow-2 && destPiece == 0 {
				return true
			}
		}
		// Взятие (по диагонали)
		if abs(toCol-fromCol) == 1 && toRow == fromRow-1 && destPiece != 0 {
			return true
		}
	case 'N', 'n': // Конь
		if (abs(toRow-fromRow) == 2 && abs(toCol-fromCol) == 1) ||
			(abs(toRow-fromRow) == 1 && abs(toCol-fromCol) == 2) {
			return true
		}
	case 'B', 'b': // Слон
		if abs(toRow-fromRow) == abs(toCol-fromCol) {
			return isPathClear(board, from, to)
		}
	case 'R', 'r': // Ладья
		if toRow == fromRow || toCol == fromCol {
			return isPathClear(board, from, to)
		}
	case 'Q', 'q': // Ферзь
		if toRow == fromRow || toCol == fromCol || abs(toRow-fromRow) == abs(toCol-fromCol) {
			return isPathClear(board, from, to)
		}
	case 'K', 'k': // Король
		if abs(toRow-fromRow) <= 1 && abs(toCol-fromCol) <= 1 {
			return true
		}
	}

	return false
}

func isPathClear(board [8][8]rune, from, to int16) bool {
	fromRow := int(from / 8)
	fromCol := int(from % 8)
	toRow := int(to / 8)
	toCol := int(to % 8)

	// Векторы направлений
	rowDir := 0
	colDir := 0
	if toRow > fromRow {
		rowDir = 1
	} else if toRow < fromRow {
		rowDir = -1
	}
	if toCol > fromCol {
		colDir = 1
	} else if toCol < fromCol {
		colDir = -1
	}

	// Проверяем каждую клетку вдоль пути
	currentRow := fromRow + rowDir
	currentCol := fromCol + colDir
	for currentRow != toRow || currentCol != toCol {
		if board[currentRow][currentCol] != 0 {
			return false
		}
		currentRow += rowDir
		currentCol += colDir
	}

	return true
}

func abs(x int) int {
	if x < 0 {
		return -x
	}
	return x
}

// Расширенный разбор с большим контекстом
func ParseMoveDetailed(move string) ChessMove {
	move = strings.TrimSpace(move)

	chessMove := ChessMove{
		From:      -1,
		To:        -1,
		PieceType: PiecePawn,
		Capture:   false,
		Check:     strings.HasSuffix(move, "+"),
		Checkmate: strings.HasSuffix(move, "#"),
		EnPassant: false,
		Castle:    "",
		Promotion: 0,
	}

	// Удалить символы шаха/мата
	cleanMove := strings.TrimSuffix(strings.TrimSuffix(move, "#"), "+")

	// Обрабатываем рокировку
	if cleanMove == "O-O" || cleanMove == "O-O-O" {
		chessMove.Castle = cleanMove
		chessMove.PieceType = PieceKing
		if cleanMove == "O-O" {
			chessMove.From = 4 // e1/e8
			chessMove.To = 6   // g1/g8
		} else {
			chessMove.From = 4 // e1/e8
			chessMove.To = 2   // c1/c8
		}
		return chessMove
	}

	// Обрабатываем превращения (оба формата)
	if strings.Contains(cleanMove, "=") {
		parts := strings.Split(cleanMove, "=")
		if len(parts) == 2 {
			chessMove.Promotion = pieceTypeFromChar(rune(parts[1][0]))
			cleanMove = parts[0]
		}
	} else if regexp.MustCompile(`[a-h][1-8][NBRQK]$`).MatchString(cleanMove) {
		// Альтернативный формат: d1Q, a8N и т.д.
		if len(cleanMove) >= 3 {
			chessMove.Promotion = pieceTypeFromChar(rune(cleanMove[len(cleanMove)-1]))
			cleanMove = cleanMove[:len(cleanMove)-1]
		}
	}

	// Обрабатываем взятия
	if strings.Contains(cleanMove, "x") {
		chessMove.Capture = true
		if strings.HasSuffix(cleanMove, "e.p.") {
			chessMove.EnPassant = true
			cleanMove = strings.TrimSuffix(cleanMove, "e.p.")
		}
	}

	// Извлечь целевую клетку
	re := regexp.MustCompile(`([a-h][1-8])`)
	targetMatches := re.FindStringSubmatch(cleanMove)
	if len(targetMatches) > 1 {
		chessMove.To = squareToIndex(targetMatches[1])
	}

	// Извлечь тип фигуры
	if strings.HasPrefix(cleanMove, "N") {
		chessMove.PieceType = PieceKnight
	} else if strings.HasPrefix(cleanMove, "B") {
		chessMove.PieceType = PieceBishop
	} else if strings.HasPrefix(cleanMove, "R") {
		chessMove.PieceType = PieceRook
	} else if strings.HasPrefix(cleanMove, "Q") {
		chessMove.PieceType = PieceQueen
	} else if strings.HasPrefix(cleanMove, "K") {
		chessMove.PieceType = PieceKing
	} else {
		chessMove.PieceType = PiecePawn
	}

	// Пока From является placeholder - потребуется состояние доски
	chessMove.From = 0

	return chessMove
}

// Конвертирует ход в алгебраическую запись
func MoveToAlgebraic(from, to int16, pieceType int16) string {
	if from < 0 || from > 63 || to < 0 || to > 63 {
		return ""
	}

	toSquare := indexToSquare(to)

	var pieceChar string
	switch pieceType {
	case PieceKnight:
		pieceChar = "N"
	case PieceBishop:
		pieceChar = "B"
	case PieceRook:
		pieceChar = "R"
	case PieceQueen:
		pieceChar = "Q"
	case PieceKing:
		pieceChar = "K"
	default:
		pieceChar = ""
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
