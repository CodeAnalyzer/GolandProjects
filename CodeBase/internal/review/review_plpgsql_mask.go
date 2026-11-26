package review

import (
	"bytes"
	"regexp"
	"sort"
	"strings"
)

// Маскирование dollar-quoted plpgsql-областей (change
// mask-plpgsql-dollar-quoted-blocks). Сканер — однопроходный автомат состояний
// (normal / single-quote / line-comment / block-comment / dollar-quote):
// открывающий dollar-тег распознаётся только вне строковых литералов и
// комментариев, закрывающий — парный тег с тем же именем. Маскируемая область
// заменяется пробелами с сохранением \n и \r — номера строк не смещаются,
// байтовая длина не меняется. Серии из трёх и более подряд $ (маркеры
// клиентских макросов вида $$$NAME) не открывают и не закрывают область.
// Незакрытый тег — fail-open: область не маскируется.

// createHeaderRe — строка-заголовок PG-объекта, начинающая объёмлющий оператор
// CREATE [OR REPLACE] FUNCTION|PROCEDURE (для обратного расширения маски).
var createHeaderRe = regexp.MustCompile(`(?i)^\s*create\s+(or\s+replace\s+)?(function|procedure)\b`)

// dollarRegion — маскируемая область [start, end) в байтовых offset-ах.
type dollarRegion struct {
	start int
	end   int
}

// maskDollarQuotedBlocks возвращает контент с маскированными dollar-quoted
// plpgsql-областями. Область охватывает тело $$…$$ / $tag$…$tag$, а при
// обратном расширении — весь объёмлющий оператор от строки create-заголовка
// (когда между заголовком и открывающим тегом нет ';'). Анонимные do-блоки
// маскируются без расширения назад.
func maskDollarQuotedBlocks(content string) string {
	if !strings.Contains(content, "$") {
		return content
	}
	b := []byte(content)
	n := len(b)

	lineStarts := make([]int, 1, 64)
	for i := 0; i < n; i++ {
		if b[i] == '\n' {
			lineStarts = append(lineStarts, i+1)
		}
	}

	var regions []dollarRegion

	i := 0
	for i < n {
		c := b[i]
		switch {
		case c == '\'':
			// Строковый литерал: удвоение '' не закрывает.
			i++
			for i < n {
				if b[i] == '\'' {
					if i+1 < n && b[i+1] == '\'' {
						i += 2
						continue
					}
					i++
					break
				}
				i++
			}
		case c == '-' && i+1 < n && b[i+1] == '-':
			// Комментарий до конца строки.
			for i < n && b[i] != '\n' {
				i++
			}
		case c == '/' && i+1 < n && b[i+1] == '*':
			// Блочный комментарий; в PG комментарии вкладываются — глубина
			// считается. Незакрытый — до EOF.
			i += 2
			depth := 1
			for i < n {
				if b[i] == '/' && i+1 < n && b[i+1] == '*' {
					depth++
					i += 2
					continue
				}
				if b[i] == '*' && i+1 < n && b[i+1] == '/' {
					depth--
					i += 2
					if depth == 0 {
						break
					}
					continue
				}
				i++
			}
		case c == '$':
			// Серия 3+ подряд $ — маркер клиентских макросов: не открывает область.
			run := dollarRunLen(b, i, n)
			if run >= 3 {
				i += run
				continue
			}
			tag, ok := matchDollarTag(b, i, n)
			if !ok {
				i++
				continue
			}
			closePos, found := findDollarClose(b, i+len(tag), tag, n)
			if !found {
				// fail-open: незакрытый тег не маскируется, скан продолжается.
				i += len(tag)
				continue
			}
			start := i
			if hdr, ok := extendToCreateHeader(b, lineStarts, i); ok {
				start = hdr
			}
			regions = append(regions, dollarRegion{start: start, end: closePos + len(tag)})
			i = closePos + len(tag)
		default:
			i++
		}
	}

	if len(regions) == 0 {
		return content
	}
	return applyDollarMask(b, regions)
}

// dollarRunLen считает подряд идущие $ начиная с i.
func dollarRunLen(b []byte, i, n int) int {
	run := 0
	for i+run < n && b[i+run] == '$' {
		run++
	}
	return run
}

// matchDollarTag распознаёт тег доллар-кавычки в позиции i (первый $). Имя
// тега подчиняется правилам unquoted-идентификатора PG: пустое ($$) либо
// начинается с буквы или подчёркивания — последовательности вида $1$
// (позиционные параметры) тегом не являются.
func matchDollarTag(b []byte, i, n int) ([]byte, bool) {
	j := i + 1
	if j < n && b[j] != '$' && !isDollarTagStartChar(b[j]) {
		return nil, false
	}
	for j < n && isDollarTagWordChar(b[j]) {
		j++
	}
	if j < n && b[j] == '$' {
		return b[i : j+1], true
	}
	return nil, false
}

// isDollarTagStartChar — первый символ имени тега доллар-кавычки.
func isDollarTagStartChar(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

// isDollarTagWordChar — допустимые символы имени тега доллар-кавычки.
func isDollarTagWordChar(c byte) bool {
	return c == '_' || (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

// findDollarClose ищет парный закрывающий тег от позиции from. Кандидат не
// может быть частью серии 3+ подряд $ (guard симметричен открывающему).
func findDollarClose(b []byte, from int, tag []byte, n int) (int, bool) {
	p := from
	for p+len(tag) <= n {
		if b[p] != '$' {
			p++
			continue
		}
		rs := p
		for rs > from && b[rs-1] == '$' {
			rs--
		}
		re := rs
		for re < n && b[re] == '$' {
			re++
		}
		if re-rs >= 3 {
			p = re
			continue
		}
		if bytes.Equal(b[rs:rs+len(tag)], tag) {
			return rs, true
		}
		p = re
	}
	return 0, false
}

// extendToCreateHeader выполняет обратное расширение маски до строки
// create-заголовка. Сканирует строки вверх от строки открывающего тега:
// строка с ';' — барьер (расширения нет), строка create-заголовка без ';'
// между ней и тегом — начало области.
func extendToCreateHeader(b []byte, lineStarts []int, tagPos int) (int, bool) {
	li := lineIndexForOffset(lineStarts, tagPos)
	for k := li; k >= 0; k-- {
		ls := lineStarts[k]
		le := lineEndAt(b, lineStarts, k)
		var prefix []byte
		if k == li {
			prefix = b[ls:tagPos]
		} else {
			prefix = b[ls:le]
		}
		if bytes.ContainsRune(prefix, ';') {
			return 0, false
		}
		if createHeaderRe.Match(prefix) {
			return ls, true
		}
	}
	return 0, false
}

// lineIndexForOffset — индекс строки, содержащей offset (lineStarts отсортированы).
func lineIndexForOffset(lineStarts []int, offset int) int {
	li := sort.Search(len(lineStarts), func(j int) bool { return lineStarts[j] > offset }) - 1
	if li < 0 {
		li = 0
	}
	return li
}

// lineEndAt — offset конца строки k (без завершающего \n).
func lineEndAt(b []byte, lineStarts []int, k int) int {
	if k+1 < len(lineStarts) {
		return lineStarts[k+1] - 1
	}
	return len(b)
}

// applyDollarMask заменяет байты областей пробелами, сохраняя \n и \r.
// Перед маскированием области сортируются и объединяются (перекрытия
// невозможны в нормальном потоке, объединение — страховка).
func applyDollarMask(b []byte, regions []dollarRegion) string {
	sort.Slice(regions, func(i, j int) bool { return regions[i].start < regions[j].start })
	merged := make([]dollarRegion, 0, len(regions))
	for _, r := range regions {
		if len(merged) > 0 && r.start <= merged[len(merged)-1].end {
			if r.end > merged[len(merged)-1].end {
				merged[len(merged)-1].end = r.end
			}
			continue
		}
		merged = append(merged, r)
	}
	out := make([]byte, len(b))
	copy(out, b)
	for _, r := range merged {
		for i := r.start; i < r.end && i < len(out); i++ {
			if out[i] != '\n' && out[i] != '\r' {
				out[i] = ' '
			}
		}
	}
	return string(out)
}
