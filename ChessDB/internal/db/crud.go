package db

import (
	"database/sql"
	"fmt"
)

// SaveGame saves a game to the database
func (db *ChessDB) SaveGame(game *Game) (string, error) {
	var id string

	err := db.pgDB.QueryRow(`
		INSERT INTO games (white_player, black_player, result, eco_code, pgn_content, processed)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING id`,
		game.WhitePlayer, game.BlackPlayer, game.Result, game.ECOCode, game.PGNContent, game.Processed).Scan(&id)

	if err != nil {
		return "", fmt.Errorf("failed to save game: %v", err)
	}

	return id, nil
}

// GetGame retrieves a game by ID
func (db *ChessDB) GetGame(id string) (*Game, error) {
	var game Game

	err := db.pgDB.QueryRow(`
		SELECT id, white_player, black_player, result, eco_code, pgn_content, processed, created_at, updated_at
		FROM games WHERE id = $1`, id).Scan(
		&game.ID, &game.WhitePlayer, &game.BlackPlayer, &game.Result,
		&game.ECOCode, &game.PGNContent, &game.Processed,
		&game.CreatedAt, &game.UpdatedAt)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("game not found")
		}
		return nil, fmt.Errorf("failed to get game: %v", err)
	}

	return &game, nil
}

// GetTransitionsFrom retrieves all transitions from a given position
func (db *ChessDB) GetTransitionsFrom(parentHash int64, limit int) ([]Move, error) {
	query := `
		SELECT id, parent_hash, child_hash, move_number, move_from, move_to, piece_type, color, annotation, rating, games_count, wins, draws, losses, eco_code, created_at
		FROM moves
		WHERE parent_hash = $1
		ORDER BY games_count DESC`

	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}

	rows, err := db.pgDB.Query(query, parentHash)
	if err != nil {
		return nil, fmt.Errorf("failed to get transitions: %v", err)
	}
	defer rows.Close()

	var moves []Move
	for rows.Next() {
		var move Move
		err := rows.Scan(
			&move.ID, &move.ParentHash, &move.ChildHash,
			&move.MoveNumber, &move.MoveFrom, &move.MoveTo, &move.PieceType, &move.Color, &move.Annotation, &move.Rating,
			&move.GamesCount, &move.Wins, &move.Draws, &move.Losses, &move.ECOCode, &move.CreatedAt)
		if err != nil {
			return nil, fmt.Errorf("failed to scan move: %v", err)
		}
		moves = append(moves, move)
	}

	// Проверяем ошибки после итерации
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating transitions: %v", err)
	}

	return moves, nil
}

// GetECOCode retrieves an ECO code by its code
func (db *ChessDB) GetECOCode(code string) (*ECOCode, error) {
	var ecoCode ECOCode

	err := db.pgDB.QueryRow(`
		SELECT code, name, moves, family
		FROM eco_codes WHERE code = $1`, code).Scan(
		&ecoCode.Code, &ecoCode.Name, &ecoCode.Moves, &ecoCode.Family)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("ECO code not found")
		}
		return nil, fmt.Errorf("failed to get ECO code: %v", err)
	}

	return &ecoCode, nil
}

// GetAllECOCodes retrieves all ECO codes
func (db *ChessDB) GetAllECOCodes() ([]ECOCode, error) {
	rows, err := db.pgDB.Query(`
		SELECT code, name, moves, family
		FROM eco_codes 
		ORDER BY code`)
	if err != nil {
		return nil, fmt.Errorf("failed to get all ECO codes: %v", err)
	}
	defer rows.Close()

	var ecoCodes []ECOCode
	for rows.Next() {
		var ecoCode ECOCode
		err := rows.Scan(&ecoCode.Code, &ecoCode.Name, &ecoCode.Moves, &ecoCode.Family)
		if err != nil {
			return nil, fmt.Errorf("failed to scan ECO code: %v", err)
		}
		ecoCodes = append(ecoCodes, ecoCode)
	}

	// Проверяем ошибки после итерации
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating ECO codes: %v", err)
	}

	return ecoCodes, nil
}

// UpdateGame updates a game in the database
func (db *ChessDB) UpdateGame(game *Game) error {
	_, err := db.pgDB.Exec(`
		UPDATE games 
		SET white_player = $1, black_player = $2, result = $3, eco_code = $4, 
			pgn_content = $5, processed = $6, updated_at = CURRENT_TIMESTAMP
		WHERE id = $7`,
		game.WhitePlayer, game.BlackPlayer, game.Result, game.ECOCode,
		game.PGNContent, game.Processed, game.ID)

	if err != nil {
		return fmt.Errorf("failed to update game: %v", err)
	}

	return nil
}

// DeleteGame deletes a game from the database
func (db *ChessDB) DeleteGame(id string) error {
	_, err := db.pgDB.Exec("DELETE FROM games WHERE id = $1", id)
	if err != nil {
		return fmt.Errorf("failed to delete game: %v", err)
	}

	return nil
}

// SearchGames searches for games by criteria
func (db *ChessDB) SearchGames(whitePlayer, blackPlayer, result, ecoCode string, limit int) ([]Game, error) {
	query := `
		SELECT id, white_player, black_player, result, eco_code, pgn_content, processed, created_at, updated_at
		FROM games WHERE 1=1`

	args := []interface{}{}
	argIndex := 1

	if whitePlayer != "" {
		query += fmt.Sprintf(" AND white_player ILIKE $%d", argIndex)
		args = append(args, "%"+whitePlayer+"%")
		argIndex++
	}

	if blackPlayer != "" {
		query += fmt.Sprintf(" AND black_player ILIKE $%d", argIndex)
		args = append(args, "%"+blackPlayer+"%")
		argIndex++
	}

	if result != "" {
		query += fmt.Sprintf(" AND result = $%d", argIndex)
		args = append(args, result)
		argIndex++
	}

	if ecoCode != "" {
		query += fmt.Sprintf(" AND eco_code = $%d", argIndex)
		args = append(args, ecoCode)
		// argIndex больше не используется после этого
	}

	query += " ORDER BY created_at DESC"

	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}

	rows, err := db.pgDB.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to search games: %v", err)
	}
	defer rows.Close()

	var games []Game
	for rows.Next() {
		var game Game
		err := rows.Scan(
			&game.ID, &game.WhitePlayer, &game.BlackPlayer, &game.Result,
			&game.ECOCode, &game.PGNContent, &game.Processed,
			&game.CreatedAt, &game.UpdatedAt)
		if err != nil {
			return nil, fmt.Errorf("failed to scan game: %v", err)
		}
		games = append(games, game)
	}

	// Проверяем ошибки после итерации
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating games: %v", err)
	}

	return games, nil
}

// GetImportStats returns import statistics
func (db *ChessDB) GetImportStats() (*ImportStats, error) {
	stats := &ImportStats{}

	// Get total games
	var totalGames int64
	err := db.pgDB.QueryRow("SELECT COUNT(*) FROM games").Scan(&totalGames)
	if err != nil {
		return nil, fmt.Errorf("failed to get total games: %v", err)
	}
	stats.ProcessedGames = int(totalGames)

	// Get total positions
	var totalPositions int64
	err = db.pgDB.QueryRow("SELECT COUNT(*) FROM chess_positions").Scan(&totalPositions)
	if err != nil {
		return nil, fmt.Errorf("failed to get total positions: %v", err)
	}
	stats.TotalPositions = int(totalPositions)

	// Get processed games
	var processedGames int64
	err = db.pgDB.QueryRow("SELECT COUNT(*) FROM games WHERE processed = true").Scan(&processedGames)
	if err != nil {
		return nil, fmt.Errorf("failed to get processed games: %v", err)
	}

	return stats, nil
}
