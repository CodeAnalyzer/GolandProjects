package trc

import (
	"regexp"
	"strings"
)

// execRe распознаёт вызовы вида:
//
//	exec ProcName @p1=v1, @p2=v2
//	exec @RetVal = ProcName @p1=v1, @p2=v2
//	exec @RetVal =   ProcName @p1=v1
//	exec @RetVal =   /*0*/ ProcName @p1=v1
//
// Именно этот паттерн используется во всех exec-вызовах Diasoft 5NT,
// встреченных в DIAPR-391.trc (см. Modifications/DIAPR-391.utf8full.txt).
// sp_executesql здесь не встречается в тестовых данных, поэтому не
// покрывается — best-effort ограничение, отмеченное в плане.
//
// Группа (?:/\*.*?\*/\s*)* пропускает inline-комментарии /*0*/ /*1*/,
// используемые в Diasoft для разметки секций exec-вызова, чтобы
// захватить реальное имя процедуры, а не комментарий.
var execRe = regexp.MustCompile(`(?is)^\s*exec(?:ute)?\s+(?:@\w+\s*=\s*)?(?:/\*.*?\*/\s*)*(\S+)`)

// paramHeaderRe находит начало каждого параметра `@Name =` в списке
// аргументов exec-вызова. Значение параметра — всё, что лежит между концом
// текущего совпадения и началом следующего (либо концом строки); такой
// подход (вместо lookahead, не поддерживаемого RE2/regexp) корректно
// обрабатывает многострочные и содержащие запятые значения.
var paramHeaderRe = regexp.MustCompile(`@(\w+)\s*=\s*`)

// ExtractProcedureAndParams разбирает TextData события и извлекает имя
// вызванной процедуры и список именованных параметров, если TextData
// является exec-вызовом. Для остальных событий (SELECT/DECLARE/и т.д.)
// возвращает ("", nil).
func ExtractProcedureAndParams(textData string) (string, []TRCParam) {
	m := execRe.FindStringSubmatch(textData)
	if m == nil {
		return "", nil
	}
	procedure := m[1]
	rest := textData[len(m[0]):]

	headers := paramHeaderRe.FindAllStringSubmatchIndex(rest, -1)
	if len(headers) == 0 {
		return procedure, nil
	}
	var params []TRCParam
	for i, h := range headers {
		name := rest[h[2]:h[3]]
		valueStart := h[1]
		valueEnd := len(rest)
		if i+1 < len(headers) {
			valueEnd = headers[i+1][0]
		}
		value := trimParamValue(rest[valueStart:valueEnd])
		value = strings.TrimSuffix(value, ",")
		value = trimParamValue(value)
		params = append(params, TRCParam{Name: name, Value: value})
	}
	return procedure, params
}

// trimParamValue убирает завершающие пробелы/переводы строк вокруг значения
// параметра, которые остаются после разбиения по запятым в многострочном
// exec-вызове.
func trimParamValue(v string) string {
	start, end := 0, len(v)
	for start < end && isTrimSpace(v[start]) {
		start++
	}
	for end > start && isTrimSpace(v[end-1]) {
		end--
	}
	return v[start:end]
}

func isTrimSpace(b byte) bool {
	return b == ' ' || b == '\t' || b == '\r' || b == '\n'
}
