// Package lst разбирает deployment-списки Diasoft (.lst).
//
// Единственный содержательный формат — ModuleTableInMemory.lst (extension
// «таблицы в памяти»): заголовки секций [<Продукт>] + строки <Таблица>;<Модуль>.
// Прочие .lst (ld_AfterApiReg.lst и т.п. — списки путей без «;») парсер не
// разбирает: диспетчеризация indexer'а выполняется по каноническому basename.
//
// Контракт lenient-разбора: Parse никогда не возвращает ошибку на мусорной
// строке — некорректные строки собираются в MalformedLines (номера строк),
// корректные записи возвращаются в Records; вызовитель пишет диагностику в
// errorLogger и инкрементирует счётчик ошибок, не откатывая транзакцию файла.
//
// Вход — UTF-8 строка (indexer декодирует байты по files.encoding; UTF-16/BOM
// детектируется walk-воркером до диспетчеризации), возможный BOM срезается.
package lst

import (
	"strings"

	"github.com/codebase/internal/branchkey"
	"github.com/codebase/internal/model"
)

// Result — контракт lenient-разбора: записи + диагностика мусорных строк.
type Result struct {
	Records        []model.MemoryTableRecord
	MalformedLines []int // 1-based номера строк, не прошедших формат
}

// Parse разбирает содержимое ModuleTableInMemory.lst.
func Parse(content string) Result {
	var res Result
	var currentProduct string
	sawSection := false

	content = strings.TrimPrefix(content, "\ufeff")

	for i, rawLine := range strings.Split(content, "\n") {
		lineNo := i + 1
		line := strings.TrimSuffix(rawLine, "\r")
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		// Заголовок секции: [Продукт]
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			inner := strings.TrimSpace(line[1 : len(line)-1])
			if inner == "" {
				res.MalformedLines = append(res.MalformedLines, lineNo)
				continue
			}
			product, _, _ := branchkey.NormalizeProduct(inner, "")
			currentProduct = product
			sawSection = true
			continue
		}

		// Табличная строка: Таблица;Модуль (разделение по первой «;»).
		table, module, ok := splitEntry(line)
		if !ok {
			res.MalformedLines = append(res.MalformedLines, lineNo)
			continue
		}
		// Строка до первого заголовка секции: продукта нет — запись невозможна.
		if !sawSection {
			res.MalformedLines = append(res.MalformedLines, lineNo)
			continue
		}
		res.Records = append(res.Records, model.MemoryTableRecord{
			ProductName: currentProduct,
			ModuleName:  module,
			TableName:   table,
			LineNumber:  lineNo,
		})
	}
	return res
}

// splitEntry делит строку по первой «;», тримит поля. Корректна запись
// с непустой таблицей; пустой модуль допустим (module_name DEFAULT ”).
func splitEntry(line string) (table, module string, ok bool) {
	idx := strings.IndexByte(line, ';')
	if idx < 0 {
		return "", "", false
	}
	table = strings.TrimSpace(line[:idx])
	module = strings.TrimSpace(line[idx+1:])
	if table == "" {
		return "", "", false
	}
	return table, module, true
}
