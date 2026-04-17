package web

import (
	"ChessDB/internal/db"
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// ChessAnalyzer - структура для анализа шахматных позиций с помощью LLM
type ChessAnalyzer struct {
	model      string
	httpClient *http.Client
}

// NewChessAnalyzer создает новый анализатор шахматных позиций
func NewChessAnalyzer() *ChessAnalyzer {
	return &ChessAnalyzer{
		model: "gemma3:4b",
		httpClient: &http.Client{
			Timeout: 45 * time.Second,
		},
	}
}

func (ca *ChessAnalyzer) sendChatRequest(messages []map[string]string) (map[string]interface{}, error) {
	payload := map[string]interface{}{
		"model":    ca.model,
		"messages": messages,
		"stream":   false,
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("ошибка маршалинга JSON: %w", err)
	}

	req, err := http.NewRequest(http.MethodPost, "http://localhost:11434/api/chat", bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("ошибка создания запроса к LLM: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := ca.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("ошибка запроса к LLM: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("ошибка запроса к LLM: status code: %d", resp.StatusCode)
	}

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("ошибка декодирования ответа: %w", err)
	}

	return result, nil
	}

// Warmup выполняет короткий запрос для фоновой загрузки модели
func (ca *ChessAnalyzer) Warmup() error {
	_, err := ca.sendChatRequest([]map[string]string{
		{
			"role":    "system",
			"content": "Ответь только OK.",
		},
		{
			"role":    "user",
			"content": "ping",
		},
	})

	if err != nil {
		return fmt.Errorf("ошибка warmup LLM: %w", err)
	}

	return nil
	}

// AnalyzeChessPosition анализирует шахматную позицию с помощью LLM
func (ca *ChessAnalyzer) AnalyzeChessPosition(fen string, pgn string, color string, depth int) (string, error) {
	result, err := ca.sendChatRequest([]map[string]string{
			{
				"role":    "system",
				"content": `Ты шахматный гроссмейстер. Проанализируй PGN и выведи только одно предложение с анализом стратегии. Формат вывода: [Анализ]. Без рассуждений, thinking-тэгов или дополнительных текстов.`,
			},
			{
				"role":    "user",
				"content": fmt.Sprintf(`Проанализируй PGN: %s и предложи для %s игрока самый лучший ход и наиболее вероятный ответный ход противника. Без рассуждений, thinking-тэгов или дополнительных текстов.`, pgn, strings.ToUpper(color[:1])+color[1:]),
			},
		})
	if err != nil {
		return "", err
	}

	if message, ok := result["message"].(map[string]interface{}); ok {
		if content, ok := message["content"].(string); ok {
			// Убираем квадратные скобки, если они есть
			content = strings.TrimSpace(content)
			if strings.HasPrefix(content, "[") && strings.HasSuffix(content, "]") {
				content = strings.TrimPrefix(content, "[")
				content = strings.TrimSuffix(content, "]")
				content = strings.TrimSpace(content)
			}
			return content, nil
		}
	}

	return "", fmt.Errorf("неверный формат ответа от Ollama")
}

// AnalyzePositionFromPGN анализирует позицию напрямую из PGN
func (ca *ChessAnalyzer) AnalyzePositionFromPGN(pgn string, color string, depth int) (string, error) {
	// Разбираем PGN на отдельные ходы
	moves := strings.Fields(pgn)
	var cleanMoves []string

	for _, moveStr := range moves {
		move := moveStr
		if idx := strings.Index(moveStr, "."); idx >= 0 {
			move = moveStr[idx+1:]
		}
		move = strings.TrimSpace(move)
		if move == "" {
			continue
		}
		cleanMoves = append(cleanMoves, move)
	}

	// Используем существующую функцию из проекта
	fen, err := db.GenerateFENFromMoves(cleanMoves)
	if err != nil {
		return "", fmt.Errorf("ошибка конвертации PGN в FEN: %w", err)
	}

	return ca.AnalyzeChessPosition(fen, pgn, color, depth)
}
