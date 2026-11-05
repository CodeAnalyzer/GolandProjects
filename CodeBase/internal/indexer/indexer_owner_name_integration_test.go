//go:build integration

package indexer

import (
	"io"
	"log"
	"testing"

	"github.com/codebase/internal/config"
	"github.com/codebase/internal/store/testutil"
)

// TestIndexerFillsSymbolsOwnerName — индексатор заполняет материализованный
// ключ владельца symbols.owner_name: method — именем класса как записано в
// коде метода (регистр ссылки сохраняется при регистронезависимом резолве
// класса), component — классом родительской формы того же файла; прочие типы
// (class, form, unit) — пустой строкой. Методы с классом, отсутствующим в
// юните, символов не создают (пре-существующее поведение) (change
// symbols-owner-key-materialization).
func TestIndexerFillsSymbolsOwnerName(t *testing.T) {
	db := testutil.Open(t)
	root := t.TempDir()
	if err := osWriteFile(root+"/OwnerName.pas", ownerNamePASContent); err != nil {
		t.Fatalf("write .pas: %v", err)
	}
	if err := osWriteFile(root+"/AimCnt.dfm", ownerNameDFMContent); err != nil {
		t.Fatalf("write .dfm: %v", err)
	}
	idx := &Indexer{
		db:          db,
		config:      &config.Config{Indexer: config.IndexerConfig{Parallel: 1, BatchSize: 100, IncludePatterns: []string{"*.pas", "*.dfm"}}},
		errorLogger: log.New(io.Discard, "", 0),
		shared:      newIndexerSharedState(),
	}
	// Init прогоняет полный пайплайн, включая постобработку pending-резолва:
	// класс TUnknownForm не резолвится — владелец обязан остаться как записан.
	if _, err := idx.Init(root, 1); err != nil {
		t.Fatalf("Init: %v", err)
	}

	// symbolOwners возвращает владельцев ВСЕХ символов с данным именем
	// (парсер создаёт method-строки и для объявления в классе, и для
	// реализации — кардинальность >1 нормальна, все владельцы обязаны
	// совпадать; QueryRow-проверка одной строки пропустила бы регрессию
	// одного из дублей).
	symbolOwners := func(name, symbolType string) []string {
		rows, err := idx.db.Query(`
			SELECT owner_name FROM symbols WHERE symbol_name = $1 AND symbol_type = $2
		`, name, symbolType)
		if err != nil {
			t.Fatalf("symbols %s/%s: %v", symbolType, name, err)
		}
		defer rows.Close()
		owners := make([]string, 0, 2)
		for rows.Next() {
			var owner string
			if err := rows.Scan(&owner); err != nil {
				t.Fatalf("scan %s/%s: %v", symbolType, name, err)
			}
			owners = append(owners, owner)
		}
		if len(owners) == 0 {
			t.Fatalf("symbol %s/%s: no rows", symbolType, name)
		}
		return owners
	}
	allOwners := func(name, symbolType, want string) {
		t.Helper()
		for _, owner := range symbolOwners(name, symbolType) {
			if owner != want {
				t.Fatalf("%s/%s owner = %q, want %q (all rows)", symbolType, name, owner, want)
			}
		}
	}

	// method: класс как записан в декларации метода.
	allOwners("Execute", "method", "TFormA")
	allOwners("Prepare", "method", "TFormB")
	// Регистр ссылки (D4.2): класс объявлен TFormA, реализация пишет tforma —
	// метод резолвится в класс (регистронезависимый ключ), но owner_name
	// хранит написание из кода метода.
	allOwners("CaseRun", "method", "tforma")
	var classID int64
	var className string
	if err := idx.db.QueryRow(`
		SELECT pm.class_id, pc.class_name
		FROM symbols s
		JOIN pas_methods pm ON pm.id = s.entity_id
		JOIN pas_classes pc ON pc.id = pm.class_id
		WHERE s.symbol_name = 'CaseRun' AND s.symbol_type = 'method'
		LIMIT 1
	`).Scan(&classID, &className); err != nil {
		t.Fatalf("CaseRun pas_methods row: %v", err)
	}
	if classID == 0 || className != "TFormA" {
		t.Fatalf("CaseRun class link = {id=%d name=%q}, want resolved TFormA", classID, className)
	}
	// Метод с классом, отсутствующим в юните, символа не создаёт —
	// пре-существующее поведение (FindPASMethodIDsByUnit ключует по
	// резолвнутому имени класса), изменение его не расширяет.
	var unresolvedSymbols int
	if err := idx.db.QueryRow(`
		SELECT count(*) FROM symbols WHERE symbol_name = 'Run' AND symbol_type = 'method'
	`).Scan(&unresolvedSymbols); err != nil {
		t.Fatalf("count Run symbols: %v", err)
	}
	if unresolvedSymbols != 0 {
		t.Fatalf("Run symbols = %d, want 0 (unresolved-class methods get no symbols)", unresolvedSymbols)
	}
	// component: класс родительской формы того же файла.
	allOwners("dlName", "component", "TAimCnt")
	// Прочие типы: пустой владелец.
	allOwners("TFormA", "class", "")
	allOwners("OwnerName", "unit", "")
	allOwners("AimCnt", "form", "")

	// Владелец заполнен только у method/component.
	var unexpected int
	if err := idx.db.QueryRow(`
		SELECT count(*) FROM symbols WHERE owner_name <> ''
		 AND symbol_type NOT IN ('method', 'component')
	`).Scan(&unexpected); err != nil {
		t.Fatalf("count unexpected owners: %v", err)
	}
	if unexpected != 0 {
		t.Fatalf("symbols with owner outside method/component = %d, want 0", unexpected)
	}

	// Переиндексация изменённого файла пересоздаёт его symbols с owner_name
	// от индексатора (каскадное удаление + повторная вставка, reconcile
	// не требуется): после Update владельцы на месте и консистентны.
	if err := osWriteFile(root+"/OwnerName.pas", ownerNamePASContentV2); err != nil {
		t.Fatalf("rewrite .pas: %v", err)
	}
	if _, err := idx.Update(root, true, 1); err != nil {
		t.Fatalf("Update: %v", err)
	}
	allOwners("Execute", "method", "TFormA")
	allOwners("Renamed", "method", "TFormD")
	allOwners("OwnerName", "unit", "")
}

const ownerNamePASContent = `unit OwnerName;

interface

type
  TFormA = class(TObject)
  public
    procedure Execute;
  end;
  TFormB = class(TObject)
  public
    procedure Prepare;
  end;

implementation

procedure TFormA.Execute;
begin
end;

procedure TFormB.Prepare;
begin
end;

// Регистр ссылки отличается от декларации класса (D4.2).
procedure tforma.CaseRun;
begin
end;

procedure TUnknownForm.Run;
begin
end;

end.`

// ownerNamePASContentV2 — изменённая версия файла для фазы инкрементального
// Update: класс TFormB заменён на TFormD (новый метод Renamed).
const ownerNamePASContentV2 = `unit OwnerName;

interface

type
  TFormA = class(TObject)
  public
    procedure Execute;
  end;
  TFormD = class(TObject)
  public
    procedure Renamed;
  end;

implementation

procedure TFormA.Execute;
begin
end;

procedure TFormD.Renamed;
begin
end;

end.`

const ownerNameDFMContent = `object AimCnt: TAimCnt
  Caption = 'Aim'
  object dlName: TLabel
    Caption = 'Name'
  end
end`
