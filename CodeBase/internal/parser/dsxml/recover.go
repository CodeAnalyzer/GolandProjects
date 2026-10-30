package dsxml

import "strings"

// maxRecoverFixes ограничивает число автозакрытий на файл: защита от
// глубоко битой разметки, где «лечение» порождало бы бессмысленные
// вставки. При превышении лимита контент возвращается неизменным.
const maxRecoverFixes = 32

// recoverUnclosedElements чинит некорректную вложенность вида
// «element <A> closed by </B>»: закрывающий тег встречается, когда
// поверх соответствующего ему открытого элемента остались незакрытые.
// Для каждого такого места перед закрывающим тегом вставляются
// закрывающие теги всех элементов, лежащих поверх него. Остальной
// контент не меняется; комментарии, CDATA и инструкции обработки
// пропускаются.
//
// Возвращает исправленный контент, число вставленных закрывающих
// тегов и признак успеха (false — вставок не потребовалось либо
// превышен лимит; контент остаётся исходным).
func recoverUnclosedElements(content string) (string, int, bool) {
	var out strings.Builder
	var stack []string
	last := 0 // префикс content[last:...] ещё не скопирован в out
	fixes := 0
	i := 0
	n := len(content)
	copyUpTo := func(upto int) {
		out.WriteString(content[last:upto])
		last = upto
	}
	for i < n {
		if content[i] != '<' {
			i++
			continue
		}
		rest := content[i:]
		switch {
		case strings.HasPrefix(rest, "<!--"):
			j := strings.Index(rest[4:], "-->")
			if j < 0 {
				i = n
			} else {
				i += 4 + j + 3
			}
		case strings.HasPrefix(rest, "<![CDATA["):
			j := strings.Index(rest[9:], "]]>")
			if j < 0 {
				i = n
			} else {
				i += 9 + j + 3
			}
		case strings.HasPrefix(rest, "<?"):
			j := strings.Index(rest[2:], "?>")
			if j < 0 {
				i = n
			} else {
				i += 2 + j + 2
			}
		case strings.HasPrefix(rest, "<!"):
			// Декларации (DOCTYPE и пр.) — пропускаем, стек не трогаем.
			j := strings.IndexByte(rest, '>')
			if j < 0 {
				i = n
			} else {
				i += j + 1
			}
		case strings.HasPrefix(rest, "</"):
			j := strings.IndexByte(rest, '>')
			if j < 0 {
				i = n
				break
			}
			name := strings.TrimSpace(rest[2:j])
			closeStart := i
			i += j + 1
			depth := lastIndexOfString(stack, name)
			if depth < 0 {
				// Закрывающий без пары — не чиним: окончательный вердикт
				// вынесет encoding/xml.
				continue
			}
			if depth == len(stack)-1 {
				stack = stack[:depth]
				continue
			}
			// Тег есть глубже в стеке: автозакрыть всё, что поверх него.
			closing := len(stack) - 1 - depth
			if fixes+closing > maxRecoverFixes {
				return content, 0, false
			}
			copyUpTo(closeStart)
			for k := len(stack) - 1; k > depth; k-- {
				out.WriteString("</" + stack[k] + ">")
			}
			fixes += closing
			stack = stack[:depth+1]
		default:
			// Открывающий тег: имя до первого разделителя; self-closing
			// («<name/>») в стек не попадает.
			j := strings.IndexByte(rest, '>')
			if j < 0 {
				i = n
				break
			}
			tag := rest[1:j]
			if name, ok := openTagName(tag); ok {
				stack = append(stack, name)
			}
			i += j + 1
		}
	}
	if fixes == 0 {
		return content, 0, false
	}
	out.WriteString(content[last:])
	return out.String(), fixes, true
}

// openTagName извлекает имя открывающего тега из tag («name attr="v"»),
// без угловых скобок; ok=false для self-closing и пустых имён.
func openTagName(tag string) (string, bool) {
	if strings.HasSuffix(tag, "/") {
		return "", false
	}
	end := strings.IndexAny(tag, " \t\r\n/")
	if end < 0 {
		end = len(tag)
	}
	name := tag[:end]
	if name == "" {
		return "", false
	}
	return name, true
}

func lastIndexOfString(stack []string, name string) int {
	for k := len(stack) - 1; k >= 0; k-- {
		if stack[k] == name {
			return k
		}
	}
	return -1
}
