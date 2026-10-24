package dsinfo

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/codebase/internal/encoding"
	"golang.org/x/text/transform"
)

const (
	// maxLineSize — буфер строки bufio.Scanner (1 МБ): строки «Процедур»
	// короткие, но запас защищает от нестандартных выгрузок.
	maxLineSize = 1 << 20
	// maxHeaderRunes — заголовок секции: строка без ';' короче 60 символов;
	// более длинные строки без ';' считаются мусором и секцию не переключают.
	maxHeaderRunes = 60
)

// Имена секций реального файла. Заголовки — единственные точки привязки
// к формату: различия между версиями системы не должны ломать разбор.
const (
	sectionInfoName       = "Информация"
	sectionManifestName   = "Продукты Digital Q"
	sectionModulesName    = "Модули"
	sectionProceduresName = "Процедуры"
	sectionConfigName     = "Конфигурация"
	statsSubsectionName   = "Статистика"
	modulesHeaderPrefix   = "FileName;"
)

// errNotDiasoftSystemInfo возвращается, когда файл не содержит обоих
// маркеров валидности; ничего не сохраняется.
var errNotDiasoftSystemInfo = errors.New(
	"файл не похож на DiasoftSystemInfo: не найдены маркеры «Продукты Digital Q» и «Модули» (заголовок FileName;)")

// ParseFile читает и разбирает DiasystemInfo-выгрузку в один проход
// (WIN-1251, разделитель ';', хвостовые ';'). Имя файла не важно —
// валидность определяется содержимым (см. errNotDiasoftSystemInfo).
func ParseFile(path string) (*Profile, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open DiasoftSystemInfo file: %w", err)
	}
	defer f.Close()

	st, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat DiasoftSystemInfo file: %w", err)
	}

	profile := &Profile{
		FilePath:      path,
		SessionName:   filepath.Base(path),
		FileSize:      st.Size(),
		ParserVersion: ParserVersion,
	}

	if err := parseStream(transform.NewReader(f, encoding.GetDecoder(encoding.WIN1251)), profile); err != nil {
		return nil, err
	}
	return profile, nil
}

// parseStream — конечный автомат по секциям: заголовок = строка без ';'
// короче maxHeaderRunes; строки данных маршрутизируются по текущей секции.
func parseStream(r io.Reader, profile *Profile) error {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), maxLineSize)

	var (
		section        string
		modulesHeader  bool // заголовок FileName; в текущей секции «Модули»
		sawManifest    bool
		sawModulesHead bool
		lineNo         int
	)
	for scanner.Scan() {
		lineNo++
		trimmed := strings.TrimSpace(scanner.Text())
		if trimmed == "" {
			continue
		}

		// Заголовок секции — строка без ';'.
		if !strings.Contains(trimmed, ";") {
			if len([]rune(trimmed)) >= maxHeaderRunes {
				continue
			}
			// Подсекция «Статистика» внутри «Процедур» не завершает секцию:
			// после блока статистики идут строки объектов БД.
			if section == sectionProceduresName && trimmed == statsSubsectionName {
				continue
			}
			section = trimmed
			modulesHeader = false
			if section == sectionManifestName {
				sawManifest = true
			}
			continue
		}

		cols := strings.Split(trimmed, ";")
		switch section {
		case sectionManifestName:
			parseManifestRow(cols, lineNo, profile)
		case sectionModulesName:
			// Вторая секция «Модули» («Установленные пакеты» и параметры)
			// не имеет заголовка FileName; — не собирается. Строки без
			// заголовка игнорируются.
			if !modulesHeader {
				if strings.HasPrefix(trimmed, modulesHeaderPrefix) {
					modulesHeader = true
					sawModulesHead = true
				}
				continue
			}
			if len(cols) >= 10 {
				parseModuleRow(cols, profile)
			}
		case sectionProceduresName:
			// Строки объектов БД: ≥8 колонок; блоки «Статистика» (≤2 колонок)
			// и заголовки колонок (state не число) отсеиваются в parseProcedureRow.
			if len(cols) >= 8 {
				parseProcedureRow(cols, profile)
			}
		case sectionInfoName:
			if len(cols) >= 2 && cols[0] == "Алиас SQL-сервера" {
				profile.ClientAlias = cols[1]
			}
		case sectionConfigName:
			if len(cols) >= 2 {
				switch cols[0] {
				case "Имя сервера":
					profile.DBServer = cols[1]
				case "Имя базы":
					profile.DBName = cols[1]
				}
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("failed to read DiasoftSystemInfo: %w", err)
	}
	if !sawManifest || !sawModulesHead {
		return errNotDiasoftSystemInfo
	}
	return nil
}

// parseManifestRow разбирает строку манифеста `NAME[\PACKET_REF];`.
func parseManifestRow(cols []string, lineNo int, profile *Profile) {
	raw := cols[0]
	rawName, packetRef := raw, ""
	if idx := strings.Index(raw, `\`); idx >= 0 {
		rawName, packetRef = raw[:idx], raw[idx+1:]
	}
	productName, branchKey, hotfixName := NormalizeProduct(rawName, packetRef)
	profile.Products = append(profile.Products, Product{
		LineNo:      lineNo,
		RawName:     rawName,
		PacketRef:   packetRef,
		ProductName: productName,
		BranchKey:   branchKey,
		HotfixName:  hotfixName,
	})
}

// parseModuleRow разбирает строку секции «Модули» (18 колонок; индексы
// соответствуют заголовку FileName;...;Hash).
func parseModuleRow(cols []string, profile *Profile) {
	profile.Modules = append(profile.Modules, Module{
		FileName:        moduleCol(cols, 0),
		FileDescription: moduleCol(cols, 1),
		ModuleName:      moduleCol(cols, 2),
		ProductVersion:  moduleColInt(cols, 3),
		ProductRelease:  moduleColInt(cols, 4),
		ProductIssue:    moduleColInt(cols, 5),
		ProductBuild:    moduleColInt(cols, 6),
		PacketName:      moduleCol(cols, 7),
		HotfixName:      moduleCol(cols, 15),
		Comments:        moduleCol(cols, 16),
		FileHash:        moduleCol(cols, 17),
	})
}

// parseProcedureRow разбирает строку объектов БД. Строки заголовков колонок
// («[Статус процедуры]», «pi_Status» в 7-й колонке) не проходят парсинг
// state как числа и пропускаются.
func parseProcedureRow(cols []string, profile *Profile) {
	state, err := strconv.Atoi(moduleCol(cols, 6))
	if err != nil {
		return
	}
	profile.Procedures = append(profile.Procedures, Procedure{
		ElemSchema:  moduleCol(cols, 0),
		ObjectName:  moduleCol(cols, 1),
		InstallDate: moduleCol(cols, 2),
		ModuleName:  moduleCol(cols, 3),
		PacketName:  moduleCol(cols, 4),
		SourceFile:  moduleCol(cols, 5),
		State:       state,
		Status:      moduleCol(cols, 7),
		Comment:     moduleCol(cols, 8),
	})
}

func moduleCol(cols []string, i int) string {
	if i < len(cols) {
		return strings.TrimSpace(cols[i])
	}
	return ""
}

func moduleColInt(cols []string, i int) int {
	v, _ := strconv.Atoi(moduleCol(cols, i))
	return v
}
