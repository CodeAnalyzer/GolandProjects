package dsinfo

import (
	"errors"
	"strings"
	"testing"
)

// fragment — фрагмент реального DiasoftSystemInfo (уже декодированный из
// WIN-1251): все ключевые конструкции формата.
const fragment = `
Информация
Parameter;Info;
Алиас SQL-сервера;sql2019_testv;
Имя базы данных;testdb2;

Параметры системы
Лицензия;x;y;

Продукты Digital Q
FA-Administrator-11-01-7;
FA-Contracts-Ext-9-01-553-PKG-IFRS9_TOYOTA\FA-Contracts-Ext-9-01-553-PKG-IFRS9_TOYOTA-901553;
FA-AdminToolKit-11-01-7\FA-AdminToolKit-11-01-7-HF001;

Модули
FileName;FileDescription;ModuleName;ProductVersion;ProductRelease;ProductIssue;ProductBuild;PacketName;PacketVersion;PacketRelease;PacketBuild;AdditionName;AdditionVersion;AdditionRelease;AdditionBuild;HotfixName;Comments;Hash;
TYPEDBF.EXE;;;0;0;0;0;;;;;;;;;;;6CBF9680;
D5NTTREASURYCORE.BPL;FA# Treasury Core;TreasuryCore;11;1;3;7836;;;;;;;;;FA-MoneyMarket-11-01-3-HF007-25121001;;ED384A26;

Процедуры

Дата последней проверки процедур;Проверка не проводилась

Статистика
Конфигурационный элемент;Fa_Core
Статус;Количество
ОК;2
Всего;2

[Конфигурационный элемент];[Имя процедуры];[Дата компиляции];[Имя модуля];[Пакет изменений];[Имя файла];[Статус процедуры];[Расшифровка статуса];[Причина недистрибутивности]
pi_ConfName;pi_ProcName;pi_CompilationDate;ri_Product;ri_InstallPackageName;pi_FileName;pi_Status;pi_StatusComment;pi_NpnDistReason
Fa_Core;AddProcVersion;16/08/2025 07:38:56;AdminToolKit;ШТАТНАЯ УСТАНОВКА – ПАКЕТ НЕ ОПРЕДЕЛЕН;AddProcHashHistory_AddProcVersion.sql;1;ОК;
Fa_Core;ChangeRollBackPointFileName;18/06/2022 07:22:44;;ШТАТНАЯ УСТАНОВКА – ПАКЕТ НЕ ОПРЕДЕЛЕН;;1;ОК;
Fa_Core;SomeUnknown;01/01/1900 00:00:00;;;;16;Нет информации об установке;
Fa_Core;SomeMissing;01/01/1900 00:00:00;;;;32;Отсутствует в БД;

Конфигурация
Элемент;Fa_Core;
Имя сервера;RUMOS072VT;
Имя базы;testdb2;
Версия сервера;Microsoft SQL Server 2019;

Продукты
Имя;Версия;Описание;
Core;11.1;Ядро;

Модули
Установленные пакеты
[Время установки];[Имя пользователя];[Имя пакета]
16.03.2024 11:02:11;dca;FA-Core-9-01-1;
`

func parseString(t *testing.T, content string) (*Profile, error) {
	t.Helper()
	profile := &Profile{ParserVersion: ParserVersion}
	err := parseStream(strings.NewReader(content), profile)
	return profile, err
}

func TestParseStream_Fragment(t *testing.T) {
	p, err := parseString(t, fragment)
	if err != nil {
		t.Fatalf("parseStream: %v", err)
	}

	if got := p.ProductsCount(); got != 3 {
		t.Fatalf("products = %d, want 3", got)
	}
	if got := p.ModulesCount(); got != 2 {
		t.Fatalf("modules = %d, want 2 (заголовок и вторая секция «Модули» не считаются)", got)
	}
	if got := p.ProceduresCount(); got != 4 {
		t.Fatalf("procedures = %d, want 4 (статистика и заголовки колонок отсеиваются)", got)
	}

	if p.DBServer != "RUMOS072VT" || p.DBName != "testdb2" || p.ClientAlias != "sql2019_testv" {
		t.Fatalf("metadata = %q/%q/%q, want RUMOS072VT/testdb2/sql2019_testv", p.DBServer, p.DBName, p.ClientAlias)
	}

	// Манифест: hotfix-строка.
	hf := p.Products[2]
	if hf.RawName != "FA-AdminToolKit-11-01-7" || hf.PacketRef != "FA-AdminToolKit-11-01-7-HF001" {
		t.Fatalf("hotfix product = %+v", hf)
	}
	if hf.ProductName != "fa-admintoolkit" || hf.BranchKey != "fa-admintoolkit-11-01-7" ||
		hf.HotfixName != "FA-AdminToolKit-11-01-7-HF001" {
		t.Fatalf("hotfix normalization = %+v", hf)
	}
	// PKG-строка: пакет без HF.
	pkg := p.Products[1]
	if pkg.ProductName != "fa-contracts-ext" || pkg.BranchKey != "fa-contracts-ext-9-01-553" || pkg.HotfixName != "" {
		t.Fatalf("pkg normalization = %+v", pkg)
	}

	// Модуль с пустым PacketName и пакетом в HotfixName (реальный сюрприз файла).
	m := p.Modules[1]
	if m.FileName != "D5NTTREASURYCORE.BPL" || m.ModuleName != "TreasuryCore" {
		t.Fatalf("treasury module = %+v", m)
	}
	if m.PacketName != "" || m.HotfixName != "FA-MoneyMarket-11-01-3-HF007-25121001" {
		t.Fatalf("treasury packet/hotfix = %q/%q, want empty/FA-MoneyMarket-11-01-3-HF007-25121001", m.PacketName, m.HotfixName)
	}
	if m.ProductVersion != 11 || m.ProductRelease != 1 || m.ProductIssue != 3 || m.ProductBuild != 7836 {
		t.Fatalf("treasury version = %d/%d/%d/%d", m.ProductVersion, m.ProductRelease, m.ProductIssue, m.ProductBuild)
	}
	if m.FileHash != "ED384A26" {
		t.Fatalf("treasury hash = %q", m.FileHash)
	}
	// Модуль без модуля/пакета.
	if p.Modules[0].FileName != "TYPEDBF.EXE" || p.Modules[0].FileHash != "6CBF9680" || p.Modules[0].ModuleName != "" {
		t.Fatalf("typedbf module = %+v", p.Modules[0])
	}

	// Процедуры: state 1/16/32, пустые поля хранятся как есть.
	byName := map[string]Procedure{}
	for _, pr := range p.Procedures {
		byName[pr.ObjectName] = pr
	}
	if pr := byName["AddProcVersion"]; pr.State != 1 || pr.ModuleName != "AdminToolKit" ||
		pr.SourceFile != "AddProcHashHistory_AddProcVersion.sql" || pr.Status != "ОК" {
		t.Fatalf("AddProcVersion = %+v", pr)
	}
	if pr := byName["ChangeRollBackPointFileName"]; pr.ModuleName != "" || pr.SourceFile != "" ||
		pr.PacketName != "ШТАТНАЯ УСТАНОВКА – ПАКЕТ НЕ ОПРЕДЕЛЕН" {
		t.Fatalf("empty-fields procedure = %+v", pr)
	}
	if pr := byName["SomeUnknown"]; pr.State != 16 || pr.PacketName != "" || pr.InstallDate != "01/01/1900 00:00:00" {
		t.Fatalf("state-16 procedure = %+v", pr)
	}
	if pr := byName["SomeMissing"]; pr.State != 32 || pr.Status != "Отсутствует в БД" {
		t.Fatalf("state-32 procedure = %+v", pr)
	}
}

func TestParseStream_NoMetadataSections(t *testing.T) {
	// Отсутствие «Информации»/«Конфигурации» — не ошибка: метаданные пусты.
	p, err := parseString(t, "Продукты Digital Q\nFA-Core-9-01-3;\n\nМодули\nFileName;Hash;\nX.BPL;;0;0;0;0;;;;;;;;;;;AB;\n")
	if err != nil {
		t.Fatalf("parseStream: %v", err)
	}
	if p.ProductsCount() != 1 || p.ModulesCount() != 1 {
		t.Fatalf("counts = %d/%d, want 1/1", p.ProductsCount(), p.ModulesCount())
	}
	if p.DBServer != "" || p.DBName != "" || p.ClientAlias != "" {
		t.Fatalf("metadata = %q/%q/%q, want empty", p.DBServer, p.DBName, p.ClientAlias)
	}
}

func TestParseStream_ForeignCSV(t *testing.T) {
	_, err := parseString(t, "Отчёт\nКолонка1;Колонка2;\na;b;\n")
	if !errors.Is(err, errNotDiasoftSystemInfo) {
		t.Fatalf("err = %v, want errNotDiasoftSystemInfo", err)
	}
}

func TestParseStream_MissingMarkers(t *testing.T) {
	// Манифест без секции «Модули» с заголовком FileName; — не валиден.
	_, err := parseString(t, "Продукты Digital Q\nFA-Core-9-01-3;\n")
	if !errors.Is(err, errNotDiasoftSystemInfo) {
		t.Fatalf("manifest only: err = %v, want errNotDiasoftSystemInfo", err)
	}
	// «Модули» без манифеста — не валиден.
	_, err = parseString(t, "Модули\nFileName;Hash;\nX.BPL;;0;0;0;0;;;;;;;;;;;AB;\n")
	if !errors.Is(err, errNotDiasoftSystemInfo) {
		t.Fatalf("modules only: err = %v, want errNotDiasoftSystemInfo", err)
	}
	// «Модули» без заголовка FileName; не считается маркером.
	_, err = parseString(t, "Продукты Digital Q\nFA-Core-9-01-3;\n\nМодули\nDeal32.exe;11.1.7.7836;21.07.2026;Administrator;11.1.7.7836;;T:\\Deal32.exe;ED384A26;\n")
	if !errors.Is(err, errNotDiasoftSystemInfo) {
		t.Fatalf("modules without header: err = %v, want errNotDiasoftSystemInfo", err)
	}
}
