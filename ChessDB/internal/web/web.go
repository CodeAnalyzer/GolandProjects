package web

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"

	"ChessDB/internal/config"
	"ChessDB/internal/db"
	"ChessDB/internal/engine"
)

type Server struct {
	db     *db.ChessDB
	config *config.Config
}

func NewServer(database *db.ChessDB, cfg *config.Config) *Server {
	return &Server{db: database, config: cfg}
}

// GetRouter возвращает HTTP mux с зарегистрированными обработчиками
func (s *Server) GetRouter() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.homeHandler)
	mux.HandleFunc("/search", s.searchHandler)
	mux.HandleFunc("/api/search", s.apiSearchHandler)
	mux.HandleFunc("/api/stats", s.apiStatsHandler)
	mux.HandleFunc("/api/eco-stats", s.apiECOStatsHandler)
	mux.HandleFunc("/api/stockfish", s.apiStockfishHandler)
	return mux
}

func (s *Server) Start(port string) error {
	http.HandleFunc("/", s.homeHandler)
	http.HandleFunc("/search", s.searchHandler)
	http.HandleFunc("/api/search", s.apiSearchHandler)
	http.HandleFunc("/api/stats", s.apiStatsHandler)
	http.HandleFunc("/api/eco-stats", s.apiECOStatsHandler)

	fmt.Printf("ChessDB сервер запущен на http://localhost:%s\n", port)
	return http.ListenAndServe(":"+port, nil)
}

func (s *Server) homeHandler(w http.ResponseWriter, r *http.Request) {
	html := `<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>ChessDB - поиск шахматных партий в PostgreSQL</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
            background-color: #f5f5f5;
        }
        .container {
            background-color: white;
            padding: 30px;
            border-radius: 10px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }
        h1 {
            color: #333;
            text-align: center;
            margin-bottom: 30px;
        }
        .stats {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
            gap: 15px;
        }
        .stat-item {
            background-color: #f8f9fa;
            padding: 20px;
            border-radius: 8px;
            text-align: center;
            border: 1px solid #e9ecef;
        }
        .stat-value {
            font-size: 24px;
            font-weight: bold;
            color: #007bff;
        }
        .stat-label {
            font-size: 14px;
            color: #666;
            margin-top: 5px;
        }
        .search-form {
            display: grid;
            grid-template-columns: 1fr auto;
            gap: 20px;
            align-items: start;
            margin-bottom: 20px;
        }
        .moves-section {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 20px;
            align-items: start;
        }
        .db-results {
            display: flex;
            flex-direction: column;
        }
        .db-results label {
            font-weight: bold;
            margin-bottom: 8px;
            color: #333;
        }
        .db-results-content {
            background-color: #f8f9fa;
            padding: 15px;
            border-radius: 6px;
            border: 2px solid #ddd;
            min-height: 80px;
            flex-grow: 1;
        }
        .move-item {
            background-color: white;
            padding: 8px 12px;
            margin-bottom: 5px;
            border-radius: 4px;
            border-left: 3px solid #007bff;
            font-size: 13px;
        }
        .search-controls {
            display: flex;
            gap: 15px;
            align-items: end;
        }
        .form-group {
            display: flex;
            flex-direction: column;
        }
        .form-group label {
            font-weight: bold;
            margin-bottom: 8px;
            color: #333;
        }
        .form-group textarea,
        .form-group select {
            padding: 12px;
            border: 2px solid #ddd;
            border-radius: 6px;
            font-size: 14px;
            transition: border-color 0.3s;
        }
        .form-group textarea:focus,
        .form-group select:focus {
            outline: none;
            border-color: #007bff;
        }
        .form-group textarea {
            resize: vertical;
            min-height: 80px;
        }
        .top-input {
            min-height: 180px;
            height: 180px;
        }
        button {
            grid-column: 1 / -1;
            background-color: #007bff;
            color: white;
            padding: 15px 30px;
            border: none;
            border-radius: 6px;
            font-size: 16px;
            cursor: pointer;
            transition: background-color 0.3s;
        }
        button:hover {
            background-color: #0056b3;
        }
        button:disabled {
            background-color: #6c757d;
            cursor: not-allowed;
        }
        .results {
            margin-top: 30px;
        }
        .result-summary {
            background-color: #e7f3ff;
            padding: 15px;
            border-radius: 6px;
            margin-bottom: 20px;
            font-weight: bold;
            color: #0066cc;
            border-left: 4px solid #007bff;
        }
        .result-item {
            background-color: #f8f9fa;
            padding: 20px;
            margin-bottom: 15px;
            border-radius: 8px;
            border-left: 4px solid #007bff;
        }
        .result-moves {
            font-size: 18px;
            font-weight: bold;
            margin-bottom: 10px;
            color: #333;
        }
        .result-meta {
            font-size: 14px;
            color: #666;
        }
        .eco-badge {
            background-color: #007bff;
            color: white;
            padding: 4px 10px;
            border-radius: 12px;
            font-size: 12px;
            margin-left: 10px;
        }
        .loading {
            text-align: center;
            color: #666;
            font-style: italic;
            padding: 20px;
        }
        @media (max-width: 768px) {
            .search-form {
                grid-template-columns: 1fr;
            }
            button {
                grid-column: 1;
            }
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>ChessDB - поиск шахматных партий</h1>
        
        <div class="stats" id="stats">
            <div class="stats-grid">
                <div class="stat-item">
                    <div class="stat-value" id="positions-count">-</div>
                    <div class="stat-label">Позиций</div>
                </div>
                <div class="stat-item">
                    <div class="stat-value" id="games-count">-</div>
                    <div class="stat-label">Игр</div>
                </div>
                <div class="stat-item">
                    <div class="stat-value" id="eco-count">-</div>
                    <div class="stat-label">ECO кодов</div>
                </div>
                <div class="stat-item">
                    <div class="stat-value" id="search-time">-</div>
                    <div class="stat-label">Время поиска</div>
                </div>
            </div>
        </div>
        
        <form id="searchForm">
            <div class="moves-section">
                <div class="form-group">
                    <label for="moves">Ходы (PGN):</label>
                    <textarea class="top-input" id="moves" name="moves" rows="6" placeholder="1. e4 e5 2. Nf3 Nc6" required></textarea>
                </div>
                <div class="form-group">
                    <label for="llmAnalysis">Анализ LLM:</label>
                    <textarea class="top-input" id="llmAnalysis" name="llmAnalysis" rows="6" readonly placeholder="Анализ от LLM появится здесь..."></textarea>
                </div>
            </div>

            <div class="search-controls" style="margin-top: 20px; margin-bottom: 30px;">
                <div class="form-group">
                    <label for="color">Цвет:</label>
                    <select id="color" name="color">
                        <option value="white">Белые</option>
                        <option value="black">Черные</option>
                    </select>
                </div>
                <div class="form-group">
                    <label for="depth">Глубина поиска:</label>
                    <select id="depth" name="depth">
                        <option value="1">1 ход</option>
                        <option value="2">2 хода</option>
                        <option value="3" selected>3 хода</option>
                        <option value="4">4 хода</option>
                        <option value="5">5 ходов</option>
                    </select>
                </div>
                <button type="button" id="dbSearchBtn">Найти лучшие продолжения</button>
                <button type="button" id="llmBtn">Запросить анализ LLM</button>
                <button type="button" id="stockfishBtn">Спросить у Stockfish</button>
            </div>

            <div class="moves-section" style="margin-top: 30px;">
                <div class="db-results">
                    <label>Найденные в БД ходы:</label>
                    <div class="db-results-content" id="dbMovesList">Появятся после поиска...</div>
                </div>
                <div class="db-results">
                    <label>Анализ Stockfish:</label>
                    <div class="db-results-content" id="stockfishResults">Появятся после запроса...</div>
                </div>
            </div>
        </form>
        
        <div class="results" id="resultsContainer"></div>
    </div>

    <script>
        const dbSearchBtn = document.getElementById('dbSearchBtn');
        const llmBtn = document.getElementById('llmBtn');
        const stockfishBtn = document.getElementById('stockfishBtn');
        const searchForm = document.getElementById('searchForm');
        const llmAnalysisTextarea = document.getElementById('llmAnalysis');
        const dbMovesList = document.getElementById('dbMovesList');
        const stockfishResults = document.getElementById('stockfishResults');

        dbSearchBtn.addEventListener('click', async () => {
            const formData = new FormData(searchForm);
            const params = new URLSearchParams(formData);
            params.set('count', '5');
            params.set('mode', 'db');

            dbSearchBtn.textContent = 'Ищу...';
            dbSearchBtn.disabled = true;
            dbMovesList.innerHTML = 'Загрузка...';

            try {
                const response = await fetch('/api/search?' + params, { cache: 'no-cache' });
                const data = await response.json();
                
                if (data.error) {
                    dbMovesList.innerHTML = 'Ошибка: ' + data.error;
                } else {
                    // Обновляем время поиска в любом случае
                    if (data.search_time !== undefined) {
                        console.log('Search time received:', data.search_time);
                        updateSearchTime(data.search_time);
                    }
                    
                    if (data.continuations && data.continuations.length > 0) {
                        let dbMovesHtml = '';
                        const totalFound = data.total_found || 0;
                        const displayCount = Math.min(5, data.continuations.length);
                        dbMovesHtml += '<div class="result-summary">Найдено вариантов: ' + totalFound + ' (показано ' + displayCount + ' лучших)</div>';
                        data.continuations.slice(0, 5).forEach((cont, index) => {
                            const movesText = cont.moves.join(' ');
                            const ecoBadge = cont.eco_code ? '<span class="eco-badge">' + cont.eco_code + '</span>' : '';
                            dbMovesHtml += '<div class="move-item">' + (index + 1) + '. ' + movesText + ecoBadge + ' (Рейтинг: ' + cont.total_rating.toFixed(1) + ', Уверенность: ' + (cont.confidence * 100).toFixed(1) + '%)</div>';
                        });
                        dbMovesList.innerHTML = dbMovesHtml;
                    } else {
                        dbMovesList.innerHTML = '<div class="result-summary">Ходы не найдены</div>';
                    }
                }
            } catch (error) {
                dbMovesList.innerHTML = 'Ошибка соединения: ' + error.message;
            } finally {
                dbSearchBtn.textContent = 'Найти лучшие продолжения';
                dbSearchBtn.disabled = false;
            }
        });

        llmBtn.addEventListener('click', () => {
            const formData = new FormData(searchForm);
            const params = new URLSearchParams(formData);
            params.set('mode', 'llm');

            llmBtn.textContent = 'Отправлено...';
            llmBtn.disabled = true;
            llmAnalysisTextarea.value = 'Запрос отправлен, ждем ответ...';

            fetch('/api/search?' + params, { cache: 'no-cache' })
                .then(response => response.json())
                .then(data => {
                    if (data.error) {
                        llmAnalysisTextarea.value = 'Ошибка: ' + data.error;
                    } else if (data.llm_analysis) {
                        // Обрабатываем как строку или массив
                        let analysis = data.llm_analysis;
                        if (Array.isArray(analysis)) {
                            analysis = analysis.join(' ');
                        }
                        llmAnalysisTextarea.value = analysis;
                    } else {
                        llmAnalysisTextarea.value = 'Анализ LLM недоступен';
                    }
                })
                .catch(error => {
                    llmAnalysisTextarea.value = 'Ошибка соединения: ' + error.message;
                })
                .finally(() => {
                    llmBtn.textContent = 'Запросить анализ LLM';
                    llmBtn.disabled = false;
                });
        });

        stockfishBtn.addEventListener('click', async () => {
            const formData = new FormData(searchForm);
            const params = new URLSearchParams(formData);
            params.set('depth', document.getElementById('depth').value);

            stockfishBtn.textContent = 'Анализирую...';
            stockfishBtn.disabled = true;
            stockfishResults.innerHTML = 'Загрузка...';

            try {
                const response = await fetch('/api/stockfish?' + params, { cache: 'no-cache' });
                const data = await response.json();
                
                if (data.error) {
                    stockfishResults.innerHTML = 'Ошибка: ' + data.error;
                } else if (data.best_moves && data.best_moves.length > 0) {
                    let html = '<div class="result-summary">Найдено вариантов: ' + data.best_moves.length + ' (время: ' + data.search_time + 'мс)</div>';
                    data.best_moves.forEach((move, index) => {
                        const scoreText = move.score_type === 'cp' ? (move.score / 100).toFixed(2) : 'M' + move.score;
                        html += '<div class="move-item">' + (index + 1) + '. ' + move.pv + ' <span class="eco-badge">Оценка: ' + scoreText + '</span> (Глубина: ' + move.depth + ')</div>';
                    });
                    stockfishResults.innerHTML = html;
                } else {
                    stockfishResults.innerHTML = 'Варианты не найдены';
                }
            } catch (error) {
                stockfishResults.innerHTML = 'Ошибка соединения: ' + error.message;
            } finally {
                stockfishBtn.textContent = 'Спросить у Stockfish';
                stockfishBtn.disabled = false;
            }
        });

        function updateSearchTime(searchTime) {
            document.getElementById('search-time').textContent = searchTime + 'мс';
        }
        
        // Загрузка статистики при загрузке страницы
        async function loadStats() {
            try {
                const response = await fetch('/api/stats');
                const stats = await response.json();
                
                document.getElementById('positions-count').textContent = stats.positions_count || 0;
                document.getElementById('games-count').textContent = stats.games_count || 0;
                document.getElementById('eco-count').textContent = stats.eco_codes_count || 0;
            } catch (error) {
                console.error('Ошибка загрузки статистики:', error);
            }
        }
        
        // Загрузка статистики при загрузке страницы
        loadStats();
    </script>
</body>
</html>`

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, err := w.Write([]byte(html))
	if err != nil {
		log.Println("Ошибка записи ответа:", err)
	}
}

func (s *Server) searchHandler(w http.ResponseWriter, r *http.Request) {
	// Обратная совместимость со старым интерфейсом
	s.apiSearchHandler(w, r)
}

func (s *Server) apiSearchHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Парсинг параметров
	moves := r.URL.Query().Get("moves")
	color := r.URL.Query().Get("color")
	countStr := r.URL.Query().Get("count")
	depthStr := r.URL.Query().Get("depth")
	mode := r.URL.Query().Get("mode")

	// Значения по умолчанию
	if color == "" {
		color = s.config.Web.Defaults.Color
	}
	if countStr == "" {
		countStr = strconv.Itoa(s.config.Web.Defaults.SearchCount)
	}
	if depthStr == "" {
		depthStr = strconv.Itoa(s.config.Web.Defaults.SearchDepth)
	}

	count, err := strconv.Atoi(countStr)
	if err != nil || count < 1 {
		count = s.config.Web.Defaults.SearchCount
	}

	depth, err := strconv.Atoi(depthStr)
	if err != nil || depth < 1 {
		depth = s.config.Web.Defaults.SearchDepth
	}

	// Создаем запрос поиска
	searchReq := db.SearchRequest{
		Moves: moves,
		Color: color,
		Count: count,
		Depth: depth,
	}

	// Создаем анализатор LLM
	analyzer := NewChessAnalyzer()

	// Каналы для выполнения
	dbResultChan := make(chan *db.SearchResult, 1)
	llmResultChan := make(chan string, 1)
	dbErrorChan := make(chan error, 1)
	llmErrorChan := make(chan error, 1)

	// Ожидаем результаты
	var dbResult *db.SearchResult
	var llmAnalysis string
	var errors []string

	if mode == "llm" {
		// Запускаем только анализ LLM
		go func() {
			llmResult, err := analyzer.AnalyzePositionFromPGN(moves, color, depth)
			if err != nil {
				llmErrorChan <- err
				return
			}
			llmResultChan <- llmResult
		}()

		// Ожидаем результат LLM
		select {
		case llmResult := <-llmResultChan:
			llmAnalysis = llmResult
		case err := <-llmErrorChan:
			errors = append(errors, fmt.Sprintf("Ошибка LLM: %v", err))
		}
	} else {
		// Запускаем только поиск в БД
		go func() {
			result, err := s.db.SearchContinuations(searchReq)
			if err != nil {
				dbErrorChan <- err
				return
			}
			dbResultChan <- result
		}()

		// Ожидаем результат БД
		select {
		case result := <-dbResultChan:
			dbResult = result
		case err := <-dbErrorChan:
			errors = append(errors, fmt.Sprintf("Ошибка БД: %v", err))
		}
	}

	// ... (rest of the code remains the same)
	response := make(map[string]interface{})

	if dbResult != nil {
		log.Printf("DB search completed, time: %d ms", dbResult.SearchTime)
		response["continuations"] = dbResult.Continuations
		response["total_found"] = dbResult.TotalFound
		response["search_time"] = dbResult.SearchTime
	}

	if llmAnalysis != "" {
		response["llm_analysis"] = llmAnalysis
	}

	if len(errors) > 0 {
		response["error"] = strings.Join(errors, "; ")
	}

	// Отправляем результат
	w.Header().Set("Content-Type", "application/json")
	log.Printf("Sending JSON response: %+v", response)
	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Printf("Ошибка кодирования JSON: %v", err)
	}
}

func (s *Server) apiStatsHandler(w http.ResponseWriter, r *http.Request) {
	stats, err := s.db.GetStats()
	if err != nil {
		log.Printf("Ошибка получения статистики: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(stats); err != nil {
		log.Printf("Ошибка кодирования JSON: %v", err)
	}
}

func (s *Server) apiECOStatsHandler(w http.ResponseWriter, r *http.Request) {
	stats, err := s.db.GetECOStats()
	if err != nil {
		log.Printf("Ошибка получения статистики ECO: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]interface{}{
		"eco_stats": stats,
	}); err != nil {
		log.Printf("Ошибка кодирования JSON ECO stats: %v", err)
	}
}

func (s *Server) apiStockfishHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	moves := r.URL.Query().Get("moves")
	depthStr := r.URL.Query().Get("depth")

	if depthStr == "" {
		depthStr = strconv.Itoa(s.config.Stockfish.DefaultDepth)
	}

	depth, err := strconv.Atoi(depthStr)
	if err != nil || depth < 1 {
		depth = s.config.Stockfish.DefaultDepth
	}

	cleanMoves := extractCleanMoves(moves)

	if !s.config.Stockfish.Enabled {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"error": "Stockfish disabled in config",
		})
		return
	}

	enginePath := s.config.Paths.StockfishPath

	stockfish, err := engine.NewStockfishEngine(enginePath, s.config.Stockfish.MultiPV)
	if err != nil {
		log.Printf("Failed to start Stockfish: %v", err)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"error": fmt.Sprintf("Failed to start Stockfish: %v", err),
		})
		return
	}
	defer func() {
		if err := stockfish.Close(); err != nil {
			log.Printf("Failed to close Stockfish: %v", err)
		}
	}()

	analysis, err := stockfish.AnalyzePosition(cleanMoves, depth)
	if err != nil {
		log.Printf("Failed to analyze position: %v", err)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"error": fmt.Sprintf("Failed to analyze: %v", err),
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	log.Printf("Sending Stockfish JSON response: %+v", analysis)
	if err := json.NewEncoder(w).Encode(analysis); err != nil {
		log.Printf("Failed to encode response: %v", err)
	}
}

func extractCleanMoves(pgnFragment string) []string {
	var cleanMoves []string
	tokens := strings.Fields(pgnFragment)

	for _, token := range tokens {
		if token == "" {
			continue
		}

		if strings.HasSuffix(token, ".") && len(token) <= 3 {
			if _, err := strconv.Atoi(strings.TrimSuffix(token, ".")); err == nil {
				continue
			}
		}

		if token == "1-0" || token == "0-1" || token == "1/2-1/2" || token == "*" {
			continue
		}

		if strings.Contains(token, ".") {
			parts := strings.SplitN(token, ".", 2)
			if len(parts) == 2 {
				if _, err := strconv.Atoi(parts[0]); err == nil {
					cleanMoves = append(cleanMoves, parts[1])
					continue
				}
			}
		}

		cleanMoves = append(cleanMoves, token)
	}

	return cleanMoves
}
