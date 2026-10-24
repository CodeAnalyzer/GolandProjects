// Package dsinfo разбирает CSV-выгрузки DiasoftSystemInfo (снимок окружения
// Diasoft 5NT: манифест продуктов, клиентские модули, объекты БД) в один проход.
//
// Имя файла не канонично — валидность определяется по содержимому: к концу
// разбора должны встретиться маркеры «Продукты Digital Q» и «Модули» с
// заголовком FileName; (см. ParseFile).
package dsinfo

// ParserVersion — версия логики разбора DiasoftSystemInfo. Входит в ключ
// идемпотентности профилей (dsinfo.file_hash, parser_version).
//
// ВАЖНО: инкрементируйте константу при ЛЮБОМ изменении логики разбора в этом
// пакете, иначе повторный parse того же файла будет возвращать профиль,
// разобранный старой логикой.
const ParserVersion = 1

// Profile — полный снимок одного файла DiasoftSystemInfo.
type Profile struct {
	FilePath      string
	SessionName   string
	FileSize      int64
	ParserVersion int

	// Метаданные окружения: «Конфигурация» (Имя сервера/Имя базы) и
	// «Информация» (Алиас SQL-сервера). Отсутствие секций — не ошибка,
	// поля остаются пустыми.
	DBServer    string
	DBName      string
	ClientAlias string

	Products   []Product
	Modules    []Module
	Procedures []Procedure
}

// ProductsCount / ModulesCount / ProceduresCount — счётчики для метаданных
// профиля (дублируют len слайсов, удобны для вывода).
func (p *Profile) ProductsCount() int   { return len(p.Products) }
func (p *Profile) ModulesCount() int    { return len(p.Modules) }
func (p *Profile) ProceduresCount() int { return len(p.Procedures) }

// Product — строка манифеста «Продукты Digital Q».
type Product struct {
	LineNo      int    `json:"line_no"`      // номер строки в исходном файле (1-based)
	RawName     string `json:"raw_name"`     // имя до '\' (например FA-Contracts-Ext-9-01-553-PKG-IFRS9_TOYOTA)
	PacketRef   string `json:"packet_ref"`   // ссылка на пакет после '\' (может быть пустой)
	ProductName string `json:"product_name"` // нормализованное имя продукта (fa-contracts-ext)
	BranchKey   string `json:"branch_key"`   // product_name + первые три числовых токена
	HotfixName  string `json:"hotfix_name"`  // packet_ref целиком, если матчит -HF<цифры>, иначе ""
}

// Module — строка секции «Модули» (18 колонок CSV; пустые во всех
// просмотренных файлах PacketVersion*/Addition* не хранятся).
type Module struct {
	FileName        string `json:"file_name"`
	FileDescription string `json:"file_description"`
	ModuleName      string `json:"module_name"`
	ProductVersion  int    `json:"product_version"`
	ProductRelease  int    `json:"product_release"`
	ProductIssue    int    `json:"product_issue"`
	ProductBuild    int    `json:"product_build"`
	PacketName      string `json:"packet_name"`
	HotfixName      string `json:"hotfix_name"`
	Comments        string `json:"comments"`
	FileHash        string `json:"file_hash"`
}

// Procedure — строка секции «Процедуры» (объекты БД). InstallDate хранится
// как есть (в файле dd/mm/yyyy hh:mm:ss, dd.mm.yyyy и нулевая 01/01/1900).
type Procedure struct {
	ElemSchema  string `json:"elem_schema"`
	ObjectName  string `json:"object_name"`
	InstallDate string `json:"install_date"`
	ModuleName  string `json:"module_name"`
	PacketName  string `json:"packet_name"`
	SourceFile  string `json:"source_file"`
	State       int    `json:"state"`
	Status      string `json:"status"`
	Comment     string `json:"comment"`
}
