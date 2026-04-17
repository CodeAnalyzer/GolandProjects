package engine

import (
	"bufio"
	"fmt"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"time"

	"ChessDB/internal/db"

	"github.com/notnil/chess"
)

// StockfishEngine представляет UCI шахматный движок Stockfish
type StockfishEngine struct {
	cmd    *exec.Cmd
	stdin  *bufio.Writer
	stdout *bufio.Scanner
	path   string
}

// EngineMove представляет один вариант хода от движка
type EngineMove struct {
	Move      string `json:"move"`
	MultiPV   int    `json:"multipv"`
	Score     int    `json:"score"`
	Depth     int    `json:"depth"`
	Nodes     int64  `json:"nodes"`
	PV        string `json:"pv"`
	ScoreType string `json:"score_type"`
}

// EngineAnalysis представляет результат анализа позиции
type EngineAnalysis struct {
	BestMoves  []EngineMove `json:"best_moves"`
	SearchTime int64        `json:"search_time"`
	Position   string       `json:"position"`
}

// NewStockfishEngine создает новый экземпляр Stockfish движка
func NewStockfishEngine(enginePath string, multiPV int) (*StockfishEngine, error) {
	if multiPV < 1 {
		multiPV = 1
	}

	cmd := exec.Command(enginePath)

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("failed to get stdin pipe: %v", err)
	}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("failed to get stdout pipe: %v", err)
	}

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to start engine: %v", err)
	}

	engine := &StockfishEngine{
		cmd:    cmd,
		stdin:  bufio.NewWriter(stdin),
		stdout: bufio.NewScanner(stdout),
		path:   enginePath,
	}

	if err := engine.initialize(multiPV); err != nil {
		engine.Close()
		return nil, fmt.Errorf("failed to initialize engine: %v", err)
	}

	return engine, nil
}

// initialize инициализирует UCI протокол и настраивает MultiPV для получения 5 лучших вариантов
func (e *StockfishEngine) initialize(multiPV int) error {
	if err := e.sendCommand("uci"); err != nil {
		return err
	}

	for e.stdout.Scan() {
		line := e.stdout.Text()
		if line == "uciok" {
			break
		}
	}

	if err := e.sendCommand(fmt.Sprintf("setoption name MultiPV value %d", multiPV)); err != nil {
		return err
	}

	if err := e.sendCommand("isready"); err != nil {
		return err
	}

	for e.stdout.Scan() {
		line := e.stdout.Text()
		if line == "readyok" {
			break
		}
	}

	return nil
}

// sendCommand отправляет команду движку
func (e *StockfishEngine) sendCommand(cmd string) error {
	_, err := e.stdin.WriteString(cmd + "\n")
	if err != nil {
		return err
	}
	return e.stdin.Flush()
}

// AnalyzePosition анализирует позицию и возвращает 5 лучших вариантов продолжения
func (e *StockfishEngine) AnalyzePosition(moves []string, depth int) (*EngineAnalysis, error) {
	startTime := time.Now()

	fen, err := db.GenerateFENFromMoves(moves)
	if err != nil {
		return nil, fmt.Errorf("failed to generate FEN: %v", err)
	}

	// Конвертируем SAN ходы в UCI для Stockfish
	uciMoves, err := convertSANtoUCI(moves)
	if err != nil {
		return nil, fmt.Errorf("failed to convert moves to UCI: %v", err)
	}

	positionCmd := "position startpos"
	if len(uciMoves) > 0 {
		positionCmd += " moves " + strings.Join(uciMoves, " ")
	}

	if err := e.sendCommand(positionCmd); err != nil {
		return nil, fmt.Errorf("failed to send position: %v", err)
	}

	goCmd := fmt.Sprintf("go depth %d", depth)
	if err := e.sendCommand(goCmd); err != nil {
		return nil, fmt.Errorf("failed to send go command: %v", err)
	}

	var bestMoves []EngineMove
	pvMap := make(map[int]*EngineMove)

	for e.stdout.Scan() {
		line := e.stdout.Text()

		if strings.HasPrefix(line, "info") {
			move := parseInfoLine(line, moves)
			if move != nil && move.Move != "" && move.MultiPV > 0 {
				pvMap[move.MultiPV] = move
			}
		}

		if strings.HasPrefix(line, "bestmove") {
			break
		}
	}

	keys := make([]int, 0, len(pvMap))
	for multiPV := range pvMap {
		keys = append(keys, multiPV)
	}
	sort.Ints(keys)

	for _, multiPV := range keys {
		move := pvMap[multiPV]
		if move == nil {
			continue
		}

		bestMoves = append(bestMoves, *move)
		if len(bestMoves) >= 5 {
			break
		}
	}

	if len(bestMoves) == 0 && len(pvMap) > 0 {
		for _, move := range pvMap {
			bestMoves = append(bestMoves, *move)
			if len(bestMoves) >= 5 {
				break
			}
		}
	}

	searchTime := time.Since(startTime).Milliseconds()

	return &EngineAnalysis{
		BestMoves:  bestMoves,
		SearchTime: searchTime,
		Position:   fen,
	}, nil
}

// convertSANtoUCI конвертирует SAN ходы в UCI нотацию для Stockfish
func convertSANtoUCI(sanMoves []string) ([]string, error) {
	game := chess.NewGame()
	var uciMoves []string

	sanEncoder := chess.AlgebraicNotation{}
	uciEncoder := chess.UCINotation{}

	for _, sanMove := range sanMoves {
		// Получаем текущую позицию
		pos := game.Position()

		// Ищем среди всех валидных ходов тот, который соответствует SAN
		var foundMove *chess.Move
		for _, validMove := range pos.ValidMoves() {
			if sanEncoder.Encode(pos, validMove) == sanMove {
				foundMove = validMove
				break
			}
		}

		if foundMove == nil {
			return nil, fmt.Errorf("move %s is not valid in current position", sanMove)
		}

		// Конвертируем найденный ход в UCI
		uciStr := uciEncoder.Encode(pos, foundMove)
		uciMoves = append(uciMoves, uciStr)

		// Применяем ход для следующей итерации
		if err := game.Move(foundMove); err != nil {
			return nil, fmt.Errorf("failed to apply move %s: %v", sanMove, err)
		}
	}

	return uciMoves, nil
}

// convertUCItoSAN конвертирует UCI ходы в стандартную алгебраическую нотацию (SAN)
func convertUCItoSAN(uciMoves []string, baseMoves []string) ([]string, error) {
	game := chess.NewGame()

	uciDecoder := chess.UCINotation{}
	sanEncoder := chess.AlgebraicNotation{}

	// Конвертируем базовые SAN ходы в UCI и применяем их
	baseUCIMoves, err := convertSANtoUCI(baseMoves)
	if err != nil {
		return nil, fmt.Errorf("failed to convert base moves: %v", err)
	}

	// Применяем базовые ходы для установки текущей позиции
	for _, uciMove := range baseUCIMoves {
		move, err := uciDecoder.Decode(game.Position(), uciMove)
		if err != nil {
			return nil, fmt.Errorf("failed to decode base UCI move %s: %v", uciMove, err)
		}
		if err := game.Move(move); err != nil {
			return nil, fmt.Errorf("failed to apply base move %s: %v", uciMove, err)
		}
	}

	var sanMoves []string
	for _, uciMove := range uciMoves {
		// Парсим UCI ход
		move, err := uciDecoder.Decode(game.Position(), uciMove)
		if err != nil {
			// Если не удалось распарсить UCI, пропускаем этот ход
			continue
		}

		// Конвертируем в SAN перед применением хода
		sanMove := sanEncoder.Encode(game.Position(), move)
		sanMoves = append(sanMoves, sanMove)

		// Применяем move объект для следующей итерации
		if err := game.Move(move); err != nil {
			// Если ход не применился, прерываем обработку остальных ходов
			break
		}
	}

	return sanMoves, nil
}

// parseInfoLine разбирает строку info от UCI движка и конвертирует UCI в SAN
func parseInfoLine(line string, baseMoves []string) *EngineMove {
	parts := strings.Fields(line)
	move := &EngineMove{}

	for i := 0; i < len(parts); i++ {
		switch parts[i] {
		case "multipv":
			if i+1 < len(parts) {
				multiPV, _ := strconv.Atoi(parts[i+1])
				move.MultiPV = multiPV
				i++
			}
		case "depth":
			if i+1 < len(parts) {
				depth, _ := strconv.Atoi(parts[i+1])
				move.Depth = depth
				i++
			}
		case "nodes":
			if i+1 < len(parts) {
				nodes, _ := strconv.ParseInt(parts[i+1], 10, 64)
				move.Nodes = nodes
				i++
			}
		case "score":
			if i+2 < len(parts) {
				move.ScoreType = parts[i+1]
				score, _ := strconv.Atoi(parts[i+2])
				move.Score = score
				i += 2
			}
		case "pv":
			if i+1 < len(parts) {
				uciMoves := parts[i+1:]
				if len(uciMoves) > 0 {
					// Конвертируем UCI ходы в SAN
					sanMoves, err := convertUCItoSAN(uciMoves, baseMoves)
					if err == nil && len(sanMoves) > 0 {
						move.Move = sanMoves[0]
						move.PV = strings.Join(sanMoves, " ")
					} else {
						// Если конвертация не удалась, используем UCI как есть
						move.Move = uciMoves[0]
						move.PV = strings.Join(uciMoves, " ")
					}
				}
			}
			return move
		}
	}

	return move
}

// Close закрывает движок
func (e *StockfishEngine) Close() error {
	if e.stdin != nil {
		if err := e.sendCommand("quit"); err != nil {
			return fmt.Errorf("failed to send quit command: %v", err)
		}
	}

	if e.cmd != nil && e.cmd.Process != nil {
		return e.cmd.Process.Kill()
	}

	return nil
}
