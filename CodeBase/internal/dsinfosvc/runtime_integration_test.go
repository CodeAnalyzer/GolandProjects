//go:build integration

package dsinfosvc

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/codebase/internal/store/testutil"
	"golang.org/x/text/encoding/charmap"
)

// dsinfoFixture — фрагмент DiasoftSystemInfo (тот же набор конструкций, что в
// юнит-тестах internal/dsinfo): 3 продукта, 2 модуля, 4 процедуры.
const dsinfoFixture = `
Информация
Parameter;Info;
Алиас SQL-сервера;sql2019_testv;
Имя базы данных;testdb2;

Продукты Digital Q
FA-Administrator-11-01-7;
FA-Contracts-Ext-9-01-553-PKG-IFRS9_TOYOTA\FA-Contracts-Ext-9-01-553-PKG-IFRS9_TOYOTA-901553;
FA-AdminToolKit-11-01-7\FA-AdminToolKit-11-01-7-HF001;

Модули
FileName;FileDescription;ModuleName;ProductVersion;ProductRelease;ProductIssue;ProductBuild;PacketName;PacketVersion;PacketRelease;PacketBuild;AdditionName;AdditionVersion;AdditionRelease;AdditionBuild;HotfixName;Comments;Hash;
TYPEDBF.EXE;;;0;0;0;0;;;;;;;;;;;6CBF9680;
D5NTTREASURYCORE.BPL;FA# Treasury Core;TreasuryCore;11;1;3;7836;;;;;;;;;FA-MoneyMarket-11-01-3-HF007-25121001;;ED384A26;

Процедуры

Статистика
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
`

// writeDsInfoFile кодирует фрагмент в WIN-1251 (как реальный файл) и пишет
// во временную директорию.
func writeDsInfoFile(t *testing.T, name, content string) string {
	t.Helper()
	encoded, err := charmap.Windows1251.NewEncoder().Bytes([]byte(content))
	if err != nil {
		t.Fatalf("encode cp1251: %v", err)
	}
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, encoded, 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}
	return path
}

// TestExecuteParse_Idempotent: повторный parse того же содержимого возвращает
// тот же profile_id (reused=true) без дублей строк; delete → новый parse
// создаёт новый профиль.
func TestExecuteParse_Idempotent(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()
	path := writeDsInfoFile(t, "DiasoftSystemInfo.csv", dsinfoFixture)

	r1, err := ExecuteParse(ctx, db, ParseParams{FilePath: path})
	if err != nil {
		t.Fatalf("first ExecuteParse: %v", err)
	}
	if r1.ProfileID == 0 || r1.Reused {
		t.Fatalf("first parse: id=%d reused=%v, want new profile", r1.ProfileID, r1.Reused)
	}
	if r1.ProductsCount != 3 || r1.ModulesCount != 2 || r1.ProceduresCount != 4 {
		t.Fatalf("first parse counts = %d/%d/%d, want 3/2/4", r1.ProductsCount, r1.ModulesCount, r1.ProceduresCount)
	}
	if r1.DBServer != "RUMOS072VT" || r1.DBName != "testdb2" || r1.ClientAlias != "sql2019_testv" {
		t.Fatalf("metadata = %q/%q/%q", r1.DBServer, r1.DBName, r1.ClientAlias)
	}
	if r1.SessionName != "DiasoftSystemInfo.csv" {
		t.Fatalf("session_name = %q, want file base name", r1.SessionName)
	}

	r2, err := ExecuteParse(ctx, db, ParseParams{FilePath: path, Name: "explicit"})
	if err != nil {
		t.Fatalf("second ExecuteParse: %v", err)
	}
	if r2.ProfileID != r1.ProfileID || !r2.Reused {
		t.Fatalf("reuse: id=%d reused=%v, want %d reused=true", r2.ProfileID, r2.Reused, r1.ProfileID)
	}

	var profiles int
	if err := db.QueryRow(`SELECT count(*) FROM dsinfo`).Scan(&profiles); err != nil {
		t.Fatalf("count dsinfo: %v", err)
	}
	if profiles != 1 {
		t.Fatalf("dsinfo rows after reuse = %d, want 1", profiles)
	}

	// delete сбрасывает кэш: новый parse → новый профиль
	if _, err := ExecuteDelete(ctx, db, DeleteParams{ProfileID: r1.ProfileID}); err != nil {
		t.Fatalf("ExecuteDelete: %v", err)
	}
	r3, err := ExecuteParse(ctx, db, ParseParams{FilePath: path})
	if err != nil {
		t.Fatalf("ExecuteParse after delete: %v", err)
	}
	if r3.ProfileID == r1.ProfileID || r3.Reused {
		t.Fatalf("parse after delete: id=%d reused=%v, want new profile", r3.ProfileID, r3.Reused)
	}
}

// TestExecuteShow_Filters: без фильтров процедуры не выбираются (только
// счётчик); с фильтрами — выборка с limit; неизвестный id — ошибка.
func TestExecuteShow_Filters(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()
	path := writeDsInfoFile(t, "DiasoftSystemInfo.csv", dsinfoFixture)

	parsed, err := ExecuteParse(ctx, db, ParseParams{FilePath: path})
	if err != nil {
		t.Fatalf("ExecuteParse: %v", err)
	}

	// Без фильтров: метаданные + манифест + модули, процедуры — только счётчик.
	show, err := ExecuteShow(ctx, db, ShowParams{ProfileID: parsed.ProfileID})
	if err != nil {
		t.Fatalf("ExecuteShow: %v", err)
	}
	if show.Profile.ID != parsed.ProfileID || show.Profile.DBServer != "RUMOS072VT" {
		t.Fatalf("show profile = %+v", show.Profile)
	}
	if len(show.Products) != 3 || len(show.Modules) != 2 {
		t.Fatalf("show products/modules = %d/%d, want 3/2", len(show.Products), len(show.Modules))
	}
	if show.Procedures != nil || show.ProceduresCount != 4 || show.ProceduresFiltered {
		t.Fatalf("show without filters: procedures=%v count=%d filtered=%v, want nil/4/false",
			show.Procedures != nil, show.ProceduresCount, show.ProceduresFiltered)
	}

	// Точный фильтр по модулю.
	byModule, err := ExecuteShow(ctx, db, ShowParams{ProfileID: parsed.ProfileID, Module: "AdminToolKit"})
	if err != nil {
		t.Fatalf("ExecuteShow module: %v", err)
	}
	if len(byModule.Procedures) != 1 || byModule.Procedures[0].ObjectName != "AddProcVersion" {
		t.Fatalf("show by module: %+v", byModule.Procedures)
	}
	if byModule.ProceduresCount != 1 || !byModule.ProceduresFiltered {
		t.Fatalf("show by module: count=%d filtered=%v", byModule.ProceduresCount, byModule.ProceduresFiltered)
	}

	// Подстрока в пакете (ILIKE).
	byPacket, err := ExecuteShow(ctx, db, ShowParams{ProfileID: parsed.ProfileID, Packet: "ПАКЕТ"})
	if err != nil {
		t.Fatalf("ExecuteShow packet: %v", err)
	}
	if len(byPacket.Procedures) != 2 || byPacket.ProceduresCount != 2 {
		t.Fatalf("show by packet: n=%d count=%d, want 2/2", len(byPacket.Procedures), byPacket.ProceduresCount)
	}

	// Подстрока в имени объекта + limit.
	bySearch, err := ExecuteShow(ctx, db, ShowParams{ProfileID: parsed.ProfileID, Search: "Some", Limit: 1})
	if err != nil {
		t.Fatalf("ExecuteShow search: %v", err)
	}
	if len(bySearch.Procedures) != 1 || bySearch.ProceduresCount != 2 {
		t.Fatalf("show by search+limit: n=%d count=%d, want 1/2", len(bySearch.Procedures), bySearch.ProceduresCount)
	}

	// Неизвестный id — ошибка.
	if _, err := ExecuteShow(ctx, db, ShowParams{ProfileID: 999999}); err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("show unknown id: err = %v, want not found", err)
	}
}

// TestExecuteUseDeleteActive: use назначает/показывает активный профиль,
// use 999 — ошибка без изменения; delete активного сбрасывает ссылку.
func TestExecuteUseDeleteActive(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()
	path := writeDsInfoFile(t, "DiasoftSystemInfo.csv", dsinfoFixture)

	parsed, err := ExecuteParse(ctx, db, ParseParams{FilePath: path})
	if err != nil {
		t.Fatalf("ExecuteParse: %v", err)
	}

	// Активного пока нет.
	cur, err := ExecuteUse(ctx, db, UseParams{})
	if err != nil {
		t.Fatalf("ExecuteUse empty: %v", err)
	}
	if cur.Active != nil {
		t.Fatalf("initial active = %+v, want nil", cur.Active)
	}

	// Назначение.
	used, err := ExecuteUse(ctx, db, UseParams{ProfileID: parsed.ProfileID})
	if err != nil {
		t.Fatalf("ExecuteUse: %v", err)
	}
	if used.Active == nil || used.Active.ID != parsed.ProfileID || !used.Active.IsActive {
		t.Fatalf("after use: active = %+v", used.Active)
	}

	// use 999 — ошибка, активный не меняется.
	if _, err := ExecuteUse(ctx, db, UseParams{ProfileID: 999}); err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("use 999: err = %v, want not found", err)
	}
	cur, err = ExecuteUse(ctx, db, UseParams{})
	if err != nil {
		t.Fatalf("ExecuteUse after 999: %v", err)
	}
	if cur.Active == nil || cur.Active.ID != parsed.ProfileID {
		t.Fatalf("active after use 999 = %+v, want unchanged", cur.Active)
	}

	// Список помечает активный.
	list, err := ExecuteList(ctx, db, 10)
	if err != nil {
		t.Fatalf("ExecuteList: %v", err)
	}
	if len(list.Profiles) != 1 || !list.Profiles[0].IsActive {
		t.Fatalf("list = %+v, want 1 active profile", list.Profiles)
	}

	// Delete активного: удалён, ссылка сброшена БД (ON DELETE SET NULL).
	del, err := ExecuteDelete(ctx, db, DeleteParams{ProfileID: parsed.ProfileID})
	if err != nil {
		t.Fatalf("ExecuteDelete: %v", err)
	}
	if !del.Deleted || !del.WasActive || !del.ActiveReset {
		t.Fatalf("delete active: %+v", del)
	}
	cur, err = ExecuteUse(ctx, db, UseParams{})
	if err != nil {
		t.Fatalf("ExecuteUse after delete: %v", err)
	}
	if cur.Active != nil {
		t.Fatalf("active after delete = %+v, want nil", cur.Active)
	}

	// Повторный delete — ошибка.
	if _, err := ExecuteDelete(ctx, db, DeleteParams{ProfileID: parsed.ProfileID}); err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("second delete: err = %v, want not found", err)
	}
}

// TestExecuteParse_DifferentFiles: два разных содержимых → два профиля,
// list по убыванию parsed_at.
func TestExecuteParse_DifferentFiles(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()

	p1 := writeDsInfoFile(t, "DiasoftSystemInfo.csv", dsinfoFixture)
	other := strings.Replace(dsinfoFixture, "RUMOS072VT", "OTHERSRV", 1)
	p2 := writeDsInfoFile(t, "DiasoftSystemInfo2.csv", other)

	r1, err := ExecuteParse(ctx, db, ParseParams{FilePath: p1})
	if err != nil {
		t.Fatalf("parse 1: %v", err)
	}
	r2, err := ExecuteParse(ctx, db, ParseParams{FilePath: p2})
	if err != nil {
		t.Fatalf("parse 2: %v", err)
	}
	if r1.ProfileID == r2.ProfileID {
		t.Fatalf("different files share id %d", r1.ProfileID)
	}
	if r2.DBServer != "OTHERSRV" {
		t.Fatalf("parse 2: db_server = %q, want OTHERSRV", r2.DBServer)
	}

	list, err := ExecuteList(ctx, db, 10)
	if err != nil {
		t.Fatalf("ExecuteList: %v", err)
	}
	if len(list.Profiles) != 2 {
		t.Fatalf("list n = %d, want 2", len(list.Profiles))
	}
	if list.Profiles[0].ParsedAt.Before(list.Profiles[1].ParsedAt) {
		t.Fatalf("list order: parsed_at %v before %v", list.Profiles[0].ParsedAt, list.Profiles[1].ParsedAt)
	}
}
