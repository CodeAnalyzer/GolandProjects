package rti

import (
	"strings"
)

// HRTI (Hashed RTI) — формат анонимизированных RTI-логов Diasoft 5NT.
// Все строковые поля кодируются алгоритмом TDsHash:
//
//	NewIndex = ((Ord(char) XOR M1) + M2) mod 128
//	CipherChar = cLetters[NewIndex]
//
// Закодированные значения обёрнуты в маркер "6D6" (декодируется как "#$#").
// Найденные ключи: M1=3, M2=102 (и эквивалентная пара M1=67, M2=38).

const (
	hrtiM1     = 3
	hrtiM2     = 102
	hrtiMarker = "6D6"
)

// cLetters — 128 символов алфавита TDsHash (по индексу 0..127).
// Порядок: 0-9, A-Z, a-z, Ё, А-Я, ё, а-я.
var cLetters [128]rune

// ordToIndex — обратное отображение: Unicode code point → индекс в cLetters.
var ordToIndex map[rune]int

// mod128ToRussian — для значения v (0..127) список русских букв с ord%128 == v.
// Используется для разрешения коллизии ASCII↔Русский при декодировании.
var mod128ToRussian map[int][]rune

func init() {
	idx := 0
	// 0-9
	for c := '0'; c <= '9'; c++ {
		cLetters[idx] = c
		idx++
	}
	// A-Z
	for c := 'A'; c <= 'Z'; c++ {
		cLetters[idx] = c
		idx++
	}
	// a-z
	for c := 'a'; c <= 'z'; c++ {
		cLetters[idx] = c
		idx++
	}
	// Ё
	cLetters[idx] = 0x0401
	idx++
	// А-Я (0x0410-0x042F)
	for c := rune(0x0410); c <= 0x042F; c++ {
		cLetters[idx] = c
		idx++
	}
	// ё
	cLetters[idx] = 0x0451
	idx++
	// а-я (0x0430-0x044F)
	for c := rune(0x0430); c <= 0x044F; c++ {
		cLetters[idx] = c
		idx++
	}
	if idx != 128 {
		panic("cLetters must have exactly 128 entries")
	}

	ordToIndex = make(map[rune]int, 128)
	for i, r := range cLetters {
		ordToIndex[r] = i
	}

	// Построение mod128ToRussian: для каждой русской буквы вычисляем ord%128.
	mod128ToRussian = make(map[int][]rune)
	russianRanges := []rune{0x0401, 0x0451}
	for c := rune(0x0410); c <= 0x044F; c++ {
		russianRanges = append(russianRanges, c)
	}
	for _, c := range russianRanges {
		v := int(c) % 128
		mod128ToRussian[v] = append(mod128ToRussian[v], c)
	}
}

// decodeHRTIString декодирует строку, закодированную TDsHash.
// Ожидаемый формат: "6D6<encoded_content>6D6".
// Если маркер не найден или строка слишком короткая — возвращается оригинал.
// isLatinFieldType возвращает true для типов полей, которые по конвенции Diasoft
// всегда содержат латинский текст (имена полей, параметров, типов).
func isLatinFieldType(t string) bool {
	switch t {
	case "DSFIELDNAMEVAR", "DSFIELDNAME", "DSPARAMNAME", "DSTYPENAME":
		return true
	default:
		return false
	}
}

// isUnambiguousLatin — v-значение, которое однозначно латинское (нет коллизии с русскими).
// a-z (97-122), P (80), R-Z (82-90), [\]^_`{|}~ (91-96, 123-126), цифры (48-57).
func isUnambiguousLatin(v int) bool {
	_, hasRussian := mod128ToRussian[v]
	return !hasRussian && v >= 32 && v <= 126
}

// isUnambiguousRussian — v-значение, которое однозначно русское (коллизирует только с control chars <32).
// А-Я кроме Р (ord%128 = 16-31, 33-47, 48-63, 80), Ё (ord%128 = 1).
func isUnambiguousRussian(v int) bool {
	_, hasRussian := mod128ToRussian[v]
	return hasRussian && (v < 32 || v == 127)
}

// detectPreferLatin эвристика: подсчитывает однозначно-латинские и однозначно-русские
// v-значения в закодированной строке. Возвращает true если латинских больше.
func detectPreferLatin(inner string) bool {
	latinScore, russianScore := 0, 0
	for _, r := range inner {
		cIdx, ok := ordToIndex[r]
		if !ok {
			continue
		}
		v := ((cIdx - hrtiM2%128 + 128) % 128) ^ hrtiM1
		if isUnambiguousLatin(v) {
			latinScore++
		} else if isUnambiguousRussian(v) {
			russianScore++
		}
	}
	return latinScore >= russianScore
}

func decodeHRTIString(s string, fieldType string) string {
	s = strings.TrimSpace(s)
	if len(s) < 6 {
		return s
	}
	if !strings.HasPrefix(s, hrtiMarker) || !strings.HasSuffix(s, hrtiMarker) {
		return s
	}
	inner := s[3 : len(s)-3]
	if inner == "" {
		return ""
	}

	preferLatin := isLatinFieldType(fieldType)
	if !preferLatin {
		preferLatin = detectPreferLatin(inner)
	}

	var b strings.Builder
	b.Grow(len(inner))

	for _, r := range inner {
		cIdx, ok := ordToIndex[r]
		if !ok {
			b.WriteRune(r)
			continue
		}
		v := ((cIdx - hrtiM2%128 + 128) % 128) ^ hrtiM1

		// Однозначные символы (без коллизии) — выводим напрямую.
		if isUnambiguousLatin(v) {
			b.WriteByte(byte(v))
			continue
		}
		if isUnambiguousRussian(v) {
			if russians, ok := mod128ToRussian[v]; ok && len(russians) > 0 {
				b.WriteRune(russians[0])
			} else {
				b.WriteRune(r)
			}
			continue
		}

		// Неоднозначные символы (коллизия ASCII↔Русский) — разрешаем по контексту.
		switch v {
		case 32: // space vs Р — всегда пробел (padding использует v=32, пробелы встречаются чаще 'Р')
			b.WriteByte(' ')
		case 47: // / vs Я — слэш не бывает в русском тексте
			b.WriteByte('/')
		case 37, 40, 41, 44, 46: // %, (, ), comma, dot — ASCII punctuation
			b.WriteByte(byte(v))
		default:
			if preferLatin && v >= 32 && v <= 126 {
				b.WriteByte(byte(v))
			} else if russians, ok := mod128ToRussian[v]; ok && len(russians) > 0 {
				b.WriteRune(russians[0])
			} else if v >= 32 && v <= 126 {
				b.WriteByte(byte(v))
			} else {
				b.WriteRune(r)
			}
		}
	}

	return b.String()
}

// decodeHRTIIfNeeded проверяет наличие маркера "6D6" и декодирует при необходимости.
// Если маркер не найден — возвращает оригинальную строку без изменений.
func decodeHRTIIfNeeded(value string, fieldType string) string {
	value = strings.TrimSpace(value)
	if len(value) < 6 {
		return value
	}
	if !strings.HasPrefix(value, hrtiMarker) || !strings.HasSuffix(value, hrtiMarker) {
		return value
	}
	return decodeHRTIString(value, fieldType)
}

// isHRTIContent проверяет, содержит ли контент HRTI-кодированные строки.
// Проверяет наличие маркера "6D6" в начале и конце строковых значений.
func isHRTIContent(calls []*RTICall) bool {
	checkCount := 0
	for _, call := range calls {
		for _, p := range call.Params {
			val := strings.TrimSpace(p.Value)
			if len(val) >= 6 && strings.HasPrefix(val, hrtiMarker) && strings.HasSuffix(val, hrtiMarker) {
				checkCount++
				if checkCount >= 3 {
					return true
				}
			}
		}
		if call.RetValContext != "" {
			ctx := strings.TrimSpace(call.RetValContext)
			if len(ctx) >= 6 && strings.HasPrefix(ctx, hrtiMarker) && strings.HasSuffix(ctx, hrtiMarker) {
				checkCount++
				if checkCount >= 3 {
					return true
				}
			}
		}
	}
	return false
}

// DecodeHRTIResult декодирует все HRTI-закодированные строковые поля в результате парсинга.
// Вызывается после полного парсинга, если isHRTIContent вернул true.
func DecodeHRTIResult(result *RTIParseResult) {
	for _, call := range result.Calls {
		// Параметры
		for i := range call.Params {
			call.Params[i].Value = decodeHRTIIfNeeded(call.Params[i].Value, call.Params[i].Type)
		}
		// Контекст возврата
		if call.RetValContext != "" {
			call.RetValContext = decodeHRTIIfNeeded(call.RetValContext, "")
		}
		// BLog blocks
		for i := range call.BLogBlocks {
			call.BLogBlocks[i].BlockName = decodeHRTIIfNeeded(call.BLogBlocks[i].BlockName, "")
		}
		// BLog tables
		for i := range call.BLogTables {
			tbl := &call.BLogTables[i]
			tbl.TableName = decodeHRTIIfNeeded(tbl.TableName, "")
			for j := range tbl.Columns {
				tbl.Columns[j] = decodeHRTIIfNeeded(tbl.Columns[j], "")
			}
			for j := range tbl.Rows {
				tbl.Rows[j] = decodeHRTIRow(tbl.Rows[j])
			}
		}
	}

	// Клиентские события
	for _, ev := range result.ClientEvents {
		ev.ErrorText = decodeHRTIIfNeeded(ev.ErrorText, "")
		ev.RawBody = decodeHRTIIfNeeded(ev.RawBody, "")
		if ev.SQL != nil {
			ev.SQL.Text = decodeHRTIIfNeeded(ev.SQL.Text, "")
			ev.SQL.ExecProcedure = decodeHRTIIfNeeded(ev.SQL.ExecProcedure, "")
			for i := range ev.SQL.ExecParams {
				ev.SQL.ExecParams[i].Value = decodeHRTIIfNeeded(ev.SQL.ExecParams[i].Value, ev.SQL.ExecParams[i].Type)
			}
		}
		if ev.Connection != nil {
			ev.Connection.Server = decodeHRTIIfNeeded(ev.Connection.Server, "")
			ev.Connection.Database = decodeHRTIIfNeeded(ev.Connection.Database, "")
			ev.Connection.User = decodeHRTIIfNeeded(ev.Connection.User, "")
			ev.Connection.AppName = decodeHRTIIfNeeded(ev.Connection.AppName, "")
		}
		for i := range ev.BPL {
			ev.BPL[i].File = decodeHRTIIfNeeded(ev.BPL[i].File, "")
			ev.BPL[i].Title = decodeHRTIIfNeeded(ev.BPL[i].Title, "")
			ev.BPL[i].Comment = decodeHRTIIfNeeded(ev.BPL[i].Comment, "")
		}
	}
}

// decodeHRTIRow декодирует строку таблицы BLog.
// Строки таблиц используют разделитель "_|_" между ячейками.
// Каждая ячейка может быть закодирована в 6D6...6D6 или быть обычным значением.
func decodeHRTIRow(row string) string {
	cells := strings.Split(row, "_|_")
	for i, cell := range cells {
		cells[i] = decodeHRTIIfNeeded(cell, "")
	}
	return strings.Join(cells, "_|_")
}

