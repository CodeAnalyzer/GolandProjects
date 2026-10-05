package store

import (
	"context"
	"fmt"
	"strings"

	"golang.org/x/text/encoding/charmap"
)

const (
	// sqlProcedureSearchVectorExpr — вектор поиска процедуры: имя = вес A,
	// header-описание = вес B (конфигурация 'russian', по образцу spec-векторов).
	sqlProcedureSearchVectorExpr = `setweight(to_tsvector('russian', coalesce(proc_name,'')), 'A') ||
	setweight(to_tsvector('russian', coalesce(description,'')), 'B')`

	// apiContractSearchVectorExpr — вектор поиска контракта: имя = вес A,
	// краткое и полное описание = вес B. События и callback-контракты лежат
	// в той же таблице и покрываются тем же вектором.
	apiContractSearchVectorExpr = `setweight(to_tsvector('russian', coalesce(contract_name,'')), 'A') ||
	setweight(to_tsvector('russian', coalesce(short_description,'') || ' ' || coalesce(full_description,'')), 'B')`
)

// EnsureDescriptionSearchVectors заполняет search_vector процедур и API-контрактов
// файла, вставленных без полнотекстового вектора. Вызывается после batch insert файла.
func (db *DB) EnsureDescriptionSearchVectors(ctx context.Context, fileID int64) error {
	stmts := []string{
		fmt.Sprintf(`UPDATE sql_procedures SET search_vector = %s WHERE file_id = $1 AND search_vector IS NULL`, sqlProcedureSearchVectorExpr),
		fmt.Sprintf(`UPDATE api_contracts SET search_vector = %s WHERE file_id = $1 AND search_vector IS NULL`, apiContractSearchVectorExpr),
	}
	for _, stmt := range stmts {
		if _, err := db.ExecContext(ctx, stmt, fileID); err != nil {
			return fmt.Errorf("ensure description search_vector: %w", err)
		}
	}
	return nil
}

// BackfillAPIContractSearchVectors идемпотентно заполняет search_vector всех
// API-контрактов без вектора: описания уже в БД, переиндексация не требуется.
// Для процедур глобального бэкфилла нет сознательно: до перепарсинга файла
// описание неизвестно, вектор от имени-only «застыл» бы после вставки описания.
func (db *DB) BackfillAPIContractSearchVectors(ctx context.Context) error {
	stmt := fmt.Sprintf(`UPDATE api_contracts SET search_vector = %s WHERE search_vector IS NULL`, apiContractSearchVectorExpr)
	if _, err := db.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("backfill api_contracts search_vector: %w", err)
	}
	return nil
}

// DescLSADocRow — документ корпуса desc-LSA: имя + описание сущности.
type DescLSADocRow struct {
	ID          int64
	EntityType  string // procedure | contract
	Name        string
	Description string
}

// cp866ArtifactRunes — руны, которые CP866-декодер выдаёт для байтов
// 0xC0-0xDF и 0xF2-0xF7/0xF9-0xFD/0xFE: рамки/блоки, украинские буквы,
// математика. В легитимном русском тексте описаний они не встречаются, но
// всегда появляются, если CP1251-файл декодирован как CP866 (файлы
// fa-administrator *.SQL). Диагностика: выгрузка desc_vocab change
// add-description-search.
func hasArtifactRunes(s string) bool {
	for _, r := range s {
		if (r >= 0x2500 && r <= 0x259F) || // ─ │ ┌ ┘ █ ▀ ...
				r == 0x0404 || r == 0x0454 || // Є є
				r == 0x0407 || r == 0x0457 || // Ї ї
				r == 0x040E || r == 0x045E || // Ў ў
				r == 0x00A4 || r == 0x25A0 { // ¤ ■
			return true
		}
	}
	return false
}

// cp866RuneToWin1251 — обратная карта «руна CP866-декода → руна CP1251»:
// собирается из таблиц обеих кодировок для байтов 0x80-0xFF.
var cp866RuneToWin1251 = buildCP866ToWin1251()

func buildCP866ToWin1251() map[rune]rune {
	m := make(map[rune]rune, 128)
	dec866 := charmap.CodePage866.NewDecoder()
	dec1251 := charmap.Windows1251.NewDecoder()
	for b := 0x80; b <= 0xFF; b++ {
		r866, err1 := dec866.Bytes([]byte{byte(b)})
		r1251, err2 := dec1251.Bytes([]byte{byte(b)})
		if err1 == nil && err2 == nil && len(r866) > 0 && len(r1251) > 0 {
			m[[]rune(string(r866))[0]] = []rune(string(r1251))[0]
		}
	}
	return m
}

// deMojibakeText переводит руны CP866-декода обратно в CP1251-руны.
// Для легитимного текста часть рун изменится (у кодировок разное распределение),
// для mojibake — текст «восстановится» в русский.
func deMojibakeText(s string) string {
	return strings.Map(func(r rune) rune {
		if repl, ok := cp866RuneToWin1251[r]; ok {
			return repl
		}
		return r
	}, s)
}

// commonRussianMarkers — маркеры живого русского текста описаний процедур
// (проверяются подстрокой в lower-case тексте с паддингом).
var commonRussianMarkers = []string{
	" и ", " не ", " для ", " по ", " если", "возврат", "возвращ", "значени",
	"документ", "договор", "идентификатор", "метод", "процедур", "сумм",
	"проверк", "обновлени", "поиск", "ошибк", "список", "установк", "получени",
}

func russianMarkerScore(s string) int {
	padded := " " + strings.ToLower(s) + " "
	score := 0
	for _, m := range commonRussianMarkers {
		if strings.Contains(padded, m) {
			score++
		}
	}
	return score
}

// isMojibakeText определяет CP1251-текст, декодированный как CP866 (файлы
// fa-administrator *.SQL). Два сигнала:
//  1. артефакт-руны (рамки/украинские/математика) — от заглавных и букв р-я;
//  2. все-кириллический mojibake: после обратной транслитерации текст
//     «оживает» (маркеры русского в де-транслите > маркеров в оригинале).
//
// Корневой фикс кодировок: change fix-sql-encoding-detection.
func isMojibakeText(s string) bool {
	if hasArtifactRunes(s) {
		return true
	}
	de := deMojibakeText(s)
	if de == s {
		return false
	}
	deScore := russianMarkerScore(de)
	return deScore >= 3 && deScore > russianMarkerScore(s)
}

// LoadDescriptionsForLSA загружает весь корпус описаний для LSA-обучения:
// процедуры (имя + header-описание) и API-контракты (имя + short + full).
// Документы с mojibake-описаниями (CP1251-файлы, декодированные как CP866 —
// корневой фикс кодировок: change fix-sql-encoding-detection) исключаются,
// чтобы словарь LSA не учился на артефактах декодирования. Порядок
// детерминирован — fingerprint корпуса стабилен между прогонами.
func (db *DB) LoadDescriptionsForLSA(ctx context.Context) ([]DescLSADocRow, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT id, entity_type, name, description FROM (
			SELECT id, 'procedure' AS entity_type, proc_name AS name, coalesce(description, '') AS description
			FROM sql_procedures
			UNION ALL
			SELECT id, 'contract' AS entity_type, contract_name AS name,
			       coalesce(short_description, '') || ' ' || coalesce(full_description, '') AS description
			FROM api_contracts
		) docs
		ORDER BY entity_type, id`)
	if err != nil {
		return nil, fmt.Errorf("load descriptions for LSA: %w", err)
	}
	defer rows.Close()

	docs := make([]DescLSADocRow, 0)
	for rows.Next() {
		var doc DescLSADocRow
		if err := rows.Scan(&doc.ID, &doc.EntityType, &doc.Name, &doc.Description); err != nil {
			return nil, fmt.Errorf("scan descriptions for LSA: %w", err)
		}
		if isMojibakeText(doc.Name) || isMojibakeText(doc.Description) {
			continue
		}
		docs = append(docs, doc)
	}
	return docs, rows.Err()
}
