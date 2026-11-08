package indexer

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/codebase/internal/fswalk"
	"github.com/codebase/internal/model"
	"github.com/codebase/internal/parser/lst"
)

// parseLSTFile индексирует deployment-списки Diasoft (.lst). Сущности порождает
// только канонический ModuleTableInMemory.lst (extension «таблицы в памяти»:
// секции [<Продукт>] + строки <Таблица>;<Модуль> → признак «объявлена как
// таблица в памяти»); прочие .lst (ld_AfterApiReg.lst и т.п. — списки путей
// другого формата) индексируются как файлы без сущностей — по прецеденту XML
// вне /DSArchitectData/. Разбор lenient: мусорные строки диагностируются в
// errorLogger и счётчик ошибок, транзакция файла не откатывается.
func (idx *Indexer) parseLSTFile(ctx context.Context, file fswalk.FileInfo, fileID int64, stats *model.ScanStats) error {
	if !strings.EqualFold(filepath.Base(file.Path), "ModuleTableInMemory.lst") {
		return nil
	}

	content, err := decodeIndexedFileContent(file)
	if err != nil {
		return fmt.Errorf("failed to decode LST file: %w", err)
	}

	res := lst.Parse(content)
	for _, lineNo := range res.MalformedLines {
		idx.logError(fmt.Sprintf("%s:%d", file.Path, lineNo), "malformed ModuleTableInMemory.lst line")
		stats.Errors++
	}
	if len(res.Records) == 0 {
		return nil
	}

	records := make([]*model.MemoryTableRecord, len(res.Records))
	for i := range res.Records {
		res.Records[i].FileID = fileID
		records[i] = &res.Records[i]
	}
	return idx.db.BatchInsertMemoryTables(ctx, records, idx.config.Indexer.BatchSize)
}
