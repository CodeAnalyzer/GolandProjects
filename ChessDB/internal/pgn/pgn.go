package pgn

import (
	"fmt"
	"regexp"
	"strings"
)

type Move struct {
	SAN        string // алгебраическая нотация хода
	Annotation int    // оценка: 0=обычный, 1=!, 2=!!, 3=!!!, -1=?, -2=??
}

type Game struct {
	Moves       []Move // ходы с оценками вместо строки
	Result      string
	WhitePlayer string
	BlackPlayer string
}

func ParsePGN(content string) []Game {
	// Ищем только заголовки [Event " с кавычками, чтобы исключить [EventDate и другие метаданные
	re := regexp.MustCompile(`\[Event "[^\]]*\]`)

	events := re.FindAllStringIndex(content, -1)

	var games []Game
	for i, loc := range events {
		start := loc[1] // после [Event...]
		end := len(content)
		for j := i + 1; j < len(events); j++ {
			if events[j][0] > start {
				end = events[j][0]
				break
			}
		}
		gameText := content[start:end]

		// Извлечь игроков
		whitePlayer := extractPlayerFromGame(gameText, "White")
		blackPlayer := extractPlayerFromGame(gameText, "Black")

		// Извлечь результат
		reResult := regexp.MustCompile(`(1-0|0-1|1/2-1/2)`)
		result := reResult.FindString(gameText)

		// Извлекаем основную линию ходов (без вариантов)
		mainLine := extractMainLine(gameText)

		// Парсим ходы с оценками
		moves := parseMovesWithAnnotations(mainLine)

		// Пропускать блоки без ходов
		if len(moves) == 0 {
			continue
		}

		games = append(games, Game{
			Moves:       moves,
			Result:      result,
			WhitePlayer: whitePlayer,
			BlackPlayer: blackPlayer,
		})
	}

	return games
}

// extractMainLine извлекает только основную линию ходов, игнорируя варианты
func extractMainLine(gameText string) string {
	// Убираем метаданные
	cleaned := regexp.MustCompile(`\[.*?\]`).ReplaceAllString(gameText, "")

	// Убираем варианты в скобках (включая вложенные)
	// Используем более сложный regex для обработки вложенных скобок
	cleaned = removeVariations(cleaned)

	// Убираем комментарии в фигурных скобках (включая вложенные)
	cleaned = removeNestedComments(cleaned)

	// Убираем engine annotations в квадратных скобках
	cleaned = regexp.MustCompile(`\[%[^\]]*\]`).ReplaceAllString(cleaned, "")

	// Убираем результат в конце
	cleaned = regexp.MustCompile(`\s*(1-0|0-1|1/2-1/2|\*)\s*$`).ReplaceAllString(cleaned, "")

	cleaned = strings.TrimSpace(cleaned)

	// Разделяем на строки и обрабатываем построчно
	lines := strings.Split(cleaned, "\n")
	var mainLineMoves []string

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		// Убираем нумерацию ходов (в любом месте строки)
		// Ищем паттерн: "1. e4 e5 2. Nf3 Nc6" и полуходы "1... e6"
		reMoveNum := regexp.MustCompile(`\b\d+\.{1,3}\s*`)
		line = reMoveNum.ReplaceAllString(line, "")

		// Убираем числовые аннотации PGN ($1, $2, $3, etc.)
		reAnnotations := regexp.MustCompile(`\$\d+`)
		line = reAnnotations.ReplaceAllString(line, "")

		// Разделяем ходы в строке
		movesInLine := strings.Fields(line)

		// Добавляем только валидные ходы
		for _, move := range movesInLine {
			if isValidMove(move) {
				mainLineMoves = append(mainLineMoves, move)
			}
		}
	}

	return strings.Join(mainLineMoves, " ")
}

// removeNestedComments рекурсивно удаляет комментарии в фигурных скобках
func removeNestedComments(text string) string {
	for {
		openIndex := strings.Index(text, "{")
		if openIndex == -1 {
			break
		}

		closeIndex := findMatchingBrace(text, openIndex)
		if closeIndex == -1 {
			break
		}

		text = text[:openIndex] + text[closeIndex+1:]
	}
	return text
}

// findMatchingBrace находит соответствующую закрывающую фигурную скобку
func findMatchingBrace(text string, openIndex int) int {
	depth := 1
	for i := openIndex + 1; i < len(text); i++ {
		switch text[i] {
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

// removeVariations рекурсивно удаляет варианты в круглых скобках
func removeVariations(text string) string {
	// Ищем открывающую скобку
	for {
		openIndex := strings.Index(text, "(")
		if openIndex == -1 {
			break // нет скобок
		}

		// Ищем соответствующую закрывающую скобку
		closeIndex := findMatchingParenthesis(text, openIndex)
		if closeIndex == -1 {
			break // нет закрывающей скобки
		}

		// Удаляем вариант
		text = text[:openIndex] + text[closeIndex+1:]
	}

	return text
}

// findMatchingParenthesis находит соответствующую закрывающую скобку
func findMatchingParenthesis(text string, openIndex int) int {
	depth := 1
	for i := openIndex + 1; i < len(text); i++ {
		switch text[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

// isValidMove проверяет, является ли токен валидным ходом
func isValidMove(token string) bool {
	// Пропускаем пустые токены
	if token == "" {
		return false
	}

	// Пропускаем результаты игры
	if regexp.MustCompile(`^(1-0|0-1|1/2-1/2|\*|1/2-1/2)$`).MatchString(token) {
		return false
	}

	// Пропускаем числовые токены (номера ходов)
	if regexp.MustCompile(`^\d+\.$`).MatchString(token) {
		return false
	}

	// Проверяем на наличие шахматных фигур или клеток
	// Валидные ходы содержат: a-h, 1-8, KQRBN, x, =, +, ++, #, O-O
	// Поддерживаем оба формата превращения: d1=Q и d1Q
	return regexp.MustCompile(`^[KQRBN]?[a-h]?[1-8]?x?[a-h][1-8](=[KQRBN]|[KQRBN])?(\+\+)?[+#]?$`).MatchString(token) ||
		regexp.MustCompile(`^O-O(-O)?(\+\+)?[+#]?$`).MatchString(token)
}

func CleanGame(game string) string {
	// убрать метаданные в []
	re := regexp.MustCompile(`\[.*?\]`)
	cleaned := re.ReplaceAllString(game, "")
	// убрать {}
	re = regexp.MustCompile(`\{.*?\}`)
	cleaned = re.ReplaceAllString(cleaned, "")
	// убрать нумерацию ходов
	re = regexp.MustCompile(`\d+\.\s*`)
	cleaned = re.ReplaceAllString(cleaned, "")
	// убрать результат в конце
	re = regexp.MustCompile(`\s*(1-0|0-1|1/2-1/2)\s*$`)
	cleaned = re.ReplaceAllString(cleaned, "")
	return strings.TrimSpace(cleaned)
}

func parseMovesWithAnnotations(input string) []Move {
	// Убираем нумерацию ходов для чистоты
	cleaned := regexp.MustCompile(`\d+\.`).ReplaceAllString(input, "")
	// Убираем оценки для обратной совместимости
	cleaned = regexp.MustCompile(`[!\?]{1,3}`).ReplaceAllString(cleaned, "")
	// Разделяем на токены
	tokens := strings.Fields(cleaned)

	var moves []Move
	for _, token := range tokens {
		// Пропускаем невалидные токены
		if !isValidMove(token) {
			continue
		}

		// Проверяем на наличие оценки в конце
		annotation := 0 // обычный ход
		san := token

		if strings.HasSuffix(token, "???") {
			annotation = -2 // ??
			san = strings.TrimSuffix(token, "???")
		} else if strings.HasSuffix(token, "??") {
			annotation = -2 // ??
			san = strings.TrimSuffix(token, "??")
		} else if strings.HasSuffix(token, "?!") {
			annotation = -1 // ?
			san = strings.TrimSuffix(token, "?!")
		} else if strings.HasSuffix(token, "?") {
			annotation = -1 // ?
			san = strings.TrimSuffix(token, "?")
		} else if strings.HasSuffix(token, "!!!") {
			annotation = 3 // !!!
			san = strings.TrimSuffix(token, "!!!")
		} else if strings.HasSuffix(token, "!!") {
			annotation = 2 // !!
			san = strings.TrimSuffix(token, "!!")
		} else if strings.HasSuffix(token, "!") {
			annotation = 1 // !
			san = strings.TrimSuffix(token, "!")
		}

		// Проверяем что это действительно ход (содержит буквы шахматных фигур или клетки, либо рокировка)
		if regexp.MustCompile(`[KQRBNa-h1-8]`).MatchString(san) || regexp.MustCompile(`^O-O(-O)?[+#]?$`).MatchString(san) {
			moves = append(moves, Move{
				SAN:        san,
				Annotation: annotation,
			})
		}
	}

	return moves
}

func ParseMoves(input string) []string {
	cleaned := regexp.MustCompile(`\d+\.`).ReplaceAllString(input, "")
	// Убрать оценки для обратной совместимости
	cleaned = regexp.MustCompile(`[!\?]{1,3}`).ReplaceAllString(cleaned, "")
	return strings.Fields(cleaned)
}

func extractPlayerFromGame(gameText, color string) string {
	lines := strings.Split(gameText, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, fmt.Sprintf(`[%s "`, color)) {
			start := strings.Index(line, `"`) + 1
			end := strings.LastIndex(line, `"`)
			if start > 0 && end > start {
				return line[start:end]
			}
		}
	}
	return "Unknown"
}
