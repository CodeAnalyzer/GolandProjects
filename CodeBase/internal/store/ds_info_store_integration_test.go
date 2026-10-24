//go:build integration

package store_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/codebase/internal/dsinfo"
	"github.com/codebase/internal/store"
	"github.com/codebase/internal/store/testutil"
)

func dsinfoTestProfile() *dsinfo.Profile {
	return &dsinfo.Profile{
		FilePath:      `D:\tmp\DiasoftSystemInfo.csv`,
		SessionName:   "DiasoftSystemInfo.csv",
		FileSize:      100500,
		ParserVersion: dsinfo.ParserVersion,
		DBServer:      "RUMOS072VT",
		DBName:        "testdb2",
		ClientAlias:   "sql2019_testv",
		Products: []dsinfo.Product{
			{LineNo: 388, RawName: "FA-Administrator-11-01-7", ProductName: "fa-administrator", BranchKey: "fa-administrator-11-01-7"},
			{LineNo: 389, RawName: "FA-AdminToolKit-11-01-7", PacketRef: "FA-AdminToolKit-11-01-7-HF001", ProductName: "fa-admintoolkit", BranchKey: "fa-admintoolkit-11-01-7", HotfixName: "FA-AdminToolKit-11-01-7-HF001"},
		},
		Modules: []dsinfo.Module{
			{FileName: "TYPEDBF.EXE", FileHash: "6CBF9680"},
			{FileName: "D5NTTREASURYCORE.BPL", ModuleName: "TreasuryCore", ProductVersion: 11, ProductBuild: 7836, HotfixName: "FA-MoneyMarket-11-01-3-HF007-25121001"},
		},
		Procedures: []dsinfo.Procedure{
			{ElemSchema: "Fa_Core", ObjectName: "AddProcVersion", InstallDate: "16/08/2025 07:38:56", ModuleName: "AdminToolKit", PacketName: "ШТАТНАЯ УСТАНОВКА – ПАКЕТ НЕ ОПРЕДЕЛЕН", SourceFile: "AddProcVersion.sql", State: 1, Status: "ОК"},
			{ElemSchema: "Fa_Core", ObjectName: "Deal_Save", ModuleName: "Payments", PacketName: "FA-Payments-11-01-3-25090101", State: 1, Status: "ОК"},
			{ElemSchema: "Fa_Core", ObjectName: "UnknownProc", InstallDate: "01/01/1900 00:00:00", State: 16, Status: "Нет информации"},
		},
	}
}

func TestDsInfoProfileInsertGetList(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()
	profile := dsinfoTestProfile()

	id, reused, err := db.InsertDsInfoProfile(ctx, profile, "hash-dsinfo-1", "custom-name")
	if err != nil {
		t.Fatalf("InsertDsInfoProfile: %v", err)
	}
	if reused {
		t.Fatal("first insert must not be reused")
	}

	got, err := db.GetDsInfoProfile(ctx, id)
	if err != nil {
		t.Fatalf("GetDsInfoProfile: %v", err)
	}
	if got.FilePath != profile.FilePath || got.SessionName != "custom-name" || got.FileHash != "hash-dsinfo-1" ||
		got.DBServer != "RUMOS072VT" || got.DBName != "testdb2" || got.ClientAlias != "sql2019_testv" {
		t.Fatalf("profile metadata = %+v", got)
	}
	if got.ProductsCount != 2 || got.ModulesCount != 2 || got.ProceduresCount != 3 {
		t.Fatalf("counts = %d/%d/%d, want 2/2/3", got.ProductsCount, got.ModulesCount, got.ProceduresCount)
	}
	if got.IsActive {
		t.Fatal("fresh profile must not be active")
	}

	// Повторная вставка того же hash — reuse без дубля (unique violation → id).
	id2, reused, err := db.InsertDsInfoProfile(ctx, profile, "hash-dsinfo-1", "another")
	if err != nil {
		t.Fatalf("second InsertDsInfoProfile: %v", err)
	}
	if !reused || id2 != id {
		t.Fatalf("second insert: id=%d reused=%v, want id=%d reused=true", id2, reused, id)
	}
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM dsinfo`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("dsinfo rows = %d (err %v), want 1", n, err)
	}

	// Find по hash / промах.
	found, err := db.FindDsInfoProfileByHash(ctx, "hash-dsinfo-1", dsinfo.ParserVersion)
	if err != nil || found == nil || found.ID != id {
		t.Fatalf("FindDsInfoProfileByHash = %+v (err %v)", found, err)
	}
	missed, err := db.FindDsInfoProfileByHash(ctx, "unknown-hash", dsinfo.ParserVersion)
	if err != nil || missed != nil {
		t.Fatalf("FindDsInfoProfileByHash(unknown) = %+v (err %v), want nil", missed, err)
	}
	missed, err = db.FindDsInfoProfileByHash(ctx, "hash-dsinfo-1", 999)
	if err != nil || missed != nil {
		t.Fatalf("FindDsInfoProfileByHash(other version) = %+v (err %v), want nil", missed, err)
	}

	// List: убывание parsed_at, активный помечен.
	if ok, err := db.SetDsInfoActiveProfile(ctx, id); err != nil || !ok {
		t.Fatalf("SetDsInfoActiveProfile: ok=%v err=%v", ok, err)
	}
	list, err := db.ListDsInfoProfiles(ctx, 10)
	if err != nil || len(list) != 1 {
		t.Fatalf("ListDsInfoProfiles = %v (err %v)", list, err)
	}
	if !list[0].IsActive {
		t.Fatal("active profile must be marked in list")
	}
}

func TestDsInfoChildRowsAndFilters(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()

	id, _, err := db.InsertDsInfoProfile(ctx, dsinfoTestProfile(), "hash-dsinfo-2", "")
	if err != nil {
		t.Fatalf("InsertDsInfoProfile: %v", err)
	}

	products, err := db.LoadDsInfoProducts(ctx, id, 10)
	if err != nil || len(products) != 2 {
		t.Fatalf("LoadDsInfoProducts = %v (err %v)", products, err)
	}
	if products[0].RawName != "FA-Administrator-11-01-7" || products[1].HotfixName != "FA-AdminToolKit-11-01-7-HF001" {
		t.Fatalf("products = %+v", products)
	}

	modules, err := db.LoadDsInfoModules(ctx, id, 10)
	if err != nil || len(modules) != 2 {
		t.Fatalf("LoadDsInfoModules = %v (err %v)", modules, err)
	}
	if modules[1].HotfixName != "FA-MoneyMarket-11-01-3-HF007-25121001" || modules[1].ProductBuild != 7836 {
		t.Fatalf("modules[1] = %+v", modules[1])
	}

	// Фильтр по модулю (точное совпадение).
	procs, err := db.LoadDsInfoProcedures(ctx, id, store.DsInfoProcedureFilter{Module: "Payments"}, 100)
	if err != nil || len(procs) != 1 || procs[0].ObjectName != "Deal_Save" {
		t.Fatalf("filter module = %v (err %v)", procs, err)
	}
	// Фильтр по пакету (ILIKE, подстрока).
	procs, err = db.LoadDsInfoProcedures(ctx, id, store.DsInfoProcedureFilter{Packet: "payments-11"}, 100)
	if err != nil || len(procs) != 1 || procs[0].ObjectName != "Deal_Save" {
		t.Fatalf("filter packet = %v (err %v)", procs, err)
	}
	// Поиск по имени объекта (ILIKE).
	procs, err = db.LoadDsInfoProcedures(ctx, id, store.DsInfoProcedureFilter{Search: "proc"}, 100)
	if err != nil || len(procs) != 2 {
		t.Fatalf("filter search = %v (err %v), want AddProcVersion+UnknownProc", procs, err)
	}
	// Комбинированный фильтр + limit.
	procs, err = db.LoadDsInfoProcedures(ctx, id, store.DsInfoProcedureFilter{Search: "proc", Module: "AdminToolKit"}, 1)
	if err != nil || len(procs) != 1 || procs[0].ObjectName != "AddProcVersion" {
		t.Fatalf("combined filter = %v (err %v)", procs, err)
	}

	// Счётчики: пустой фильтр — все, с фильтром — по фильтру.
	if n, err := db.CountDsInfoProcedures(ctx, id, store.DsInfoProcedureFilter{}); err != nil || n != 3 {
		t.Fatalf("count all = %d (err %v), want 3", n, err)
	}
	if n, err := db.CountDsInfoProcedures(ctx, id, store.DsInfoProcedureFilter{Module: "Payments"}); err != nil || n != 1 {
		t.Fatalf("count filtered = %d (err %v), want 1", n, err)
	}
}

func TestDsInfoDeleteCascadeAndActive(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()

	id, _, err := db.InsertDsInfoProfile(ctx, dsinfoTestProfile(), "hash-dsinfo-3", "")
	if err != nil {
		t.Fatalf("InsertDsInfoProfile: %v", err)
	}

	// Активный профиль: назначение, чтение, неизвестный id.
	ok, err := db.SetDsInfoActiveProfile(ctx, id)
	if err != nil || !ok {
		t.Fatalf("SetDsInfoActiveProfile: ok=%v err=%v", ok, err)
	}
	active, err := db.GetDsInfoActiveProfile(ctx)
	if err != nil || active == nil || active.ID != id {
		t.Fatalf("GetDsInfoActiveProfile = %+v (err %v)", active, err)
	}
	if ok, err = db.SetDsInfoActiveProfile(ctx, 999); err != nil || ok {
		t.Fatalf("SetDsInfoActiveProfile(999): ok=%v err=%v, want false", ok, err)
	}
	if active, _ = db.GetDsInfoActiveProfile(ctx); active == nil || active.ID != id {
		t.Fatalf("active changed after failed set: %+v", active)
	}

	// Удаление активного: каскад по дочерним таблицам + SET NULL в dsinfo_state.
	deleted, err := db.DeleteDsInfoProfile(ctx, id)
	if err != nil || !deleted {
		t.Fatalf("DeleteDsInfoProfile: deleted=%v err=%v", deleted, err)
	}
	for _, table := range []string{"dsinfo_products", "dsinfo_modules", "dsinfo_procedures"} {
		var n int
		if err := db.QueryRow(`SELECT count(*) FROM `+table).Scan(&n); err != nil || n != 0 {
			t.Fatalf("%s after delete = %d (err %v), want 0", table, n, err)
		}
	}
	if _, err := db.GetDsInfoProfile(ctx, id); err != sql.ErrNoRows {
		t.Fatalf("GetDsInfoProfile(deleted) err = %v, want sql.ErrNoRows", err)
	}
	active, err = db.GetDsInfoActiveProfile(ctx)
	if err != nil || active != nil {
		t.Fatalf("GetDsInfoActiveProfile after delete = %+v (err %v), want nil", active, err)
	}
	var activeID sql.NullInt64
	if err := db.QueryRow(`SELECT active_dsinfo_id FROM dsinfo_state WHERE id = 1`).Scan(&activeID); err != nil || activeID.Valid {
		t.Fatalf("dsinfo_state.active_dsinfo_id = %v (err %v), want NULL", activeID, err)
	}

	// Повторное удаление — false.
	deleted, err = db.DeleteDsInfoProfile(ctx, id)
	if err != nil || deleted {
		t.Fatalf("DeleteDsInfoProfile(again): deleted=%v err=%v, want false", deleted, err)
	}
}
