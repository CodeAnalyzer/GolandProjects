package openspecmd

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestClassifyPath(t *testing.T) {
	tests := []struct {
		path   string
		kind   Kind
		root   string
		slug   string
		source string
		change string
		arch   bool
	}{
		{path: "D:/repo/fa-cards/openspec/config.yaml", kind: KindConfig, root: "D:/repo/fa-cards/openspec"},
		{path: "D:/repo/fa-cards/openspec/specs/card-limits/spec.md", kind: KindSpec, slug: "card-limits"},
		{path: "D:/repo/fa-reports/openspec/specs/FORMS/0409120/usage/spec.md", kind: KindSpec, root: "D:/repo/fa-reports/openspec", slug: "FORMS/0409120/usage"},
		{path: "D:/repo/fa-reports/FARPRT_Solution/DSArchitectData/FormOCDD/openspec/config.yaml", kind: KindConfig, root: "D:/repo/fa-reports/FARPRT_Solution/DSArchitectData/FormOCDD/openspec"},
		{path: "D:/repo/FA/fa-cards/OpenSpec/specs/factoring-agreement/spec.md", kind: KindSpec, slug: "factoring-agreement"},
		{path: "D:/repo/fa-cards/openspec/scenarios/scenario-sms-disable.md", kind: KindUsecase, source: "scenarios"},
		{path: "D:/repo/fa-financialasset/openspec/usecases/fot/REQ-001-SC-001 — Расчет.md", kind: KindUsecase, source: "usecases"},
		{path: "D:/repo/fa-custody/openspec/business-processes/INDEX.md", kind: KindUsecaseIndex, source: "business-processes"},
		{path: "D:/repo/fa-custody/openspec/business-processes/Депозитарий/1._БП.md", kind: KindUsecase, source: "business-processes"},
		{path: "D:/repo/fa-custody/openspec/specs/usecases/scheta-i-zhurnaly/BP-01.md", kind: KindUsecase, source: "usecases"},
		{path: "D:/repo/fa-custody/openspec/specs/usecases/CAPABILITY-INDEX.md", kind: KindUsecaseIndex, source: "usecases"},
		{path: "D:/repo/fa-cards/openspec/changes/add-card-limits/proposal.md", kind: KindChangeProposal, change: "add-card-limits"},
		{path: "D:/repo/fa-cards/openspec/changes/add-card-limits/specs/card-limits/spec.md", kind: KindChangeDelta, change: "add-card-limits", slug: "card-limits"},
		{path: "D:/repo/fa-cards/openspec/changes/add-card-limits/tasks.md", kind: KindChangeMeta, change: "add-card-limits"},
		{path: "D:/repo/fa-cards/openspec/changes/add-card-limits/.openspec.yaml", kind: KindChangeMeta, change: "add-card-limits"},
		{path: "D:/repo/fa-cards/openspec/changes/archive/2026-08-27-x/proposal.md", kind: KindChangeProposal, change: "2026-08-27-x", arch: true},
		{path: "D:/repo/fa-cards/openspec/changes/archive/2026-08-27-x/specs/card-limits/spec.md", kind: KindChangeDelta, change: "2026-08-27-x", slug: "card-limits", arch: true},
		{path: "D:/repo/fa-cards/openspec/README.md", kind: KindOther},
		{path: "D:/repo/docs/readme.md", kind: KindNone},
		{path: "D:/repo/Cards/SERVER/proc.sql", kind: KindNone},
	}

	for _, tt := range tests {
		got := ClassifyPath(tt.path)
		if got.Kind != tt.kind {
			t.Errorf("ClassifyPath(%q).Kind = %q, want %q", tt.path, got.Kind, tt.kind)
			continue
		}
		if got.Kind != KindNone && got.RootDir == "" {
			t.Errorf("ClassifyPath(%q): RootDir empty for %q", tt.path, tt.kind)
		}
		if tt.root != "" && got.RootDir != tt.root {
			t.Errorf("ClassifyPath(%q).RootDir = %q, want %q", tt.path, got.RootDir, tt.root)
		}
		if tt.slug != "" && got.Slug != tt.slug {
			t.Errorf("ClassifyPath(%q).Slug = %q, want %q", tt.path, got.Slug, tt.slug)
		}
		if tt.source != "" && !strings.EqualFold(got.SourceDir, tt.source) {
			t.Errorf("ClassifyPath(%q).SourceDir = %q, want %q", tt.path, got.SourceDir, tt.source)
		}
		if tt.change != "" && got.ChangeName != tt.change {
			t.Errorf("ClassifyPath(%q).ChangeName = %q, want %q", tt.path, got.ChangeName, tt.change)
		}
		if got.Archived != tt.arch {
			t.Errorf("ClassifyPath(%q).Archived = %v, want %v", tt.path, got.Archived, tt.arch)
		}
	}
}

func TestCapabilityAncestors(t *testing.T) {
	tests := map[string][]string{
		"leaf":                nil,
		"FORMS/0409120/usage": {"FORMS", "FORMS/0409120"},
		`forms\report\usage`:  {"forms", "forms/report"},
	}
	for slug, want := range tests {
		if got := CapabilityAncestors(slug); !reflect.DeepEqual(got, want) {
			t.Errorf("CapabilityAncestors(%q) = %#v, want %#v", slug, got, want)
		}
	}
}

func TestValidateSpecDrivenRoot(t *testing.T) {
	root := t.TempDir()

	ok, err := ValidateSpecDrivenRoot(root)
	if err != nil || ok {
		t.Fatalf("missing config: ok=%v err=%v", ok, err)
	}

	if err := os.WriteFile(filepath.Join(root, "config.yaml"), []byte("schema: freeform\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	ok, err = ValidateSpecDrivenRoot(root)
	if err != nil || ok {
		t.Fatalf("wrong schema: ok=%v err=%v", ok, err)
	}

	if err := os.WriteFile(filepath.Join(root, "config.yaml"), []byte("schema: spec-driven\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	ok, err = ValidateSpecDrivenRoot(root)
	if err != nil || !ok {
		t.Fatalf("spec-driven root: ok=%v err=%v", ok, err)
	}
}

func TestParseConfigYAML(t *testing.T) {
	// Реальный формат fa-cards: schema + context-блок
	content := "schema: spec-driven\n\ncontext: |\n  CodeBase — локальный индексатор.\n\n  Вторая строка контекста.\n\nother_key: value\n"
	schema, ctx, ok := ParseConfigYAML(content)
	if !ok {
		t.Fatal("expected ok for spec-driven config")
	}
	if schema != "spec-driven" {
		t.Fatalf("schema = %q", schema)
	}
	if !strings.Contains(ctx, "CodeBase — локальный индексатор.") || !strings.Contains(ctx, "Вторая строка контекста.") {
		t.Fatalf("context = %q", ctx)
	}

	// Конфиг без context
	_, ctx2, ok2 := ParseConfigYAML("schema: spec-driven\n")
	if !ok2 || ctx2 != "" {
		t.Fatalf("no-context config: ok=%v ctx=%q", ok2, ctx2)
	}

	// Не spec-driven → ok=false
	if _, _, ok3 := ParseConfigYAML("schema: freeform\n"); ok3 {
		t.Fatal("non spec-driven schema must not be indexed")
	}
	if _, _, ok4 := ParseConfigYAML("\uFEFFschema: spec-driven\n"); !ok4 {
		t.Fatal("UTF-8 BOM before schema should be accepted")
	}
}

const sampleSpec = `# Лимиты по операциям

## Purpose

Лимиты карт контролируют доступные суммы операций.

## Requirements

### Requirement: Схемы карточных лимитов
Система SHALL управлять схемами лимитов через процедуры CardLimitScheme_Insert.

| Таблица | Назначение |
| --- | --- |
| tCardLimitScheme | схемы лимитов |

#### Scenario: Создание схемы лимитов
- **GIVEN** существует тип лимита
- **WHEN** создаётся новая схема карточных лимитов
- **THEN** CardLimitScheme_Insert создаёт запись в tCardLimitScheme
- **AND** заполняет Name, Brief, DateFrom

#### Scenario: Привязка схемы к карте
- WHEN схема лимитов создаётся для конкретной карты
- THEN CardLimitScheme_Insert принимает параметр CardID

### Requirement: Русская нормативная формулировка
Система ДОЛЖНА обеспечивать массовую обработку через CardLimit_proc.

#### Scenario: Массовая обработка
- **WHEN** требуется массово обработать лимиты
- **THEN** CardLimit_proc обрабатывает записи

## Related code

### Серверные процедуры
- ` + "`" + `Cards/SERVER/Card/CardLimit_proc.sql` + "`" + ` — обработка лимитов
- CardLimitScheme_Insert/Update/Delete.sql — CRUD схем

### API-процедуры
- ` + "`" + `API_Depo/Server/DepoAccount/API_DepoAccount_MassInsert.sql` + "`" + ` — массовое добавление

## Notes

Связан с доменами: ` + "`" + `card-service` + "`" + `
`

func TestParseSpecFile(t *testing.T) {
	spec := ParseSpecFile(sampleSpec)

	if spec.Title != "Лимиты по операциям" {
		t.Fatalf("Title = %q", spec.Title)
	}
	if !strings.Contains(spec.Purpose, "доступные суммы операций") {
		t.Fatalf("Purpose = %q", spec.Purpose)
	}
	if !strings.Contains(spec.RelatedCode, "CardLimit_proc.sql") {
		t.Fatalf("RelatedCode = %q", spec.RelatedCode)
	}
	if !strings.Contains(spec.Notes, "card-service") {
		t.Fatalf("Notes = %q", spec.Notes)
	}
	if len(spec.Requirements) != 2 {
		t.Fatalf("requirements = %d, want 2", len(spec.Requirements))
	}

	req := spec.Requirements[0]
	if req.Name != "Схемы карточных лимитов" {
		t.Fatalf("req.Name = %q", req.Name)
	}
	if !strings.Contains(req.BodyText, "tCardLimitScheme | схемы лимитов") {
		t.Fatalf("req body must keep markdown table: %q", req.BodyText)
	}
	if req.Order != 1 || req.LineStart == 0 || req.LineEnd < req.LineStart {
		t.Fatalf("req order/lines: %+v", req)
	}
	if len(req.Scenarios) != 2 {
		t.Fatalf("scenarios = %d, want 2", len(req.Scenarios))
	}

	scn := req.Scenarios[0]
	if scn.Name != "Создание схемы лимитов" {
		t.Fatalf("scn.Name = %q", scn.Name)
	}
	if !strings.Contains(scn.Given, "тип лимита") {
		t.Fatalf("scn.Given = %q", scn.Given)
	}
	if !strings.Contains(scn.When, "новая схема") {
		t.Fatalf("scn.When = %q", scn.When)
	}
	if !strings.Contains(scn.Then, "tCardLimitScheme") {
		t.Fatalf("scn.Then = %q", scn.Then)
	}
	// AND после THEN дописан в then_text
	if !strings.Contains(scn.Then, "заполняет Name, Brief, DateFrom") {
		t.Fatalf("scn.Then must include AND-line: %q", scn.Then)
	}

	// plain (не bold) WHEN/THEN
	scn2 := req.Scenarios[1]
	if !strings.Contains(scn2.When, "для конкретной карты") || !strings.Contains(scn2.Then, "CardID") {
		t.Fatalf("plain scenario parse: %+v", scn2)
	}

	// Русская нормативная формулировка
	req2 := spec.Requirements[1]
	if !strings.Contains(req2.BodyText, "ДОЛЖНА") {
		t.Fatalf("russian normative body: %q", req2.BodyText)
	}
}

func TestParseSpecFile_NoH1NoRequirementsHeader(t *testing.T) {
	// Фикстура fa-factoring: спека начинается с ## Purpose, без ## Requirements
	content := "## Purpose\n\nФакторинговые договоры.\n\n### Requirement: Регистрация договора\nСистема SHALL регистрировать договор.\n\n#### Scenario: Регистрация\n- **WHEN** поступает новый договор\n- **THEN** создаётся запись\n"
	spec := ParseSpecFile(content)
	if spec.Title != "" {
		t.Fatalf("Title = %q, want empty", spec.Title)
	}
	if len(spec.Requirements) != 1 {
		t.Fatalf("requirements = %d", len(spec.Requirements))
	}
	if len(spec.Requirements[0].Scenarios) != 1 {
		t.Fatalf("scenarios = %d", len(spec.Requirements[0].Scenarios))
	}
	if !strings.Contains(spec.Purpose, "Факторинговые") {
		t.Fatalf("Purpose = %q", spec.Purpose)
	}
}

func TestParseUsecaseFile_Scenarios(t *testing.T) {
	content := `# Сценарий: Отключение SMS-уведомлений

## Пользователи
- Клиент
- Оператор

## Основной поток
1. Клиент открывает настройки уведомлений
2. Клиент отключает переключатель SMS

## Альтернативный поток
1. SMS-рассылка недоступна — показывается предупреждение

## Архитектурные компоненты
Компонент X.
`
	cl := Classified{Kind: KindUsecase, SourceDir: "scenarios"}
	uc := ParseUsecaseFile(cl, "scenario-sms-disable.md", content)

	if uc.Name != "scenario-sms-disable" || uc.Kind != "scenario" {
		t.Fatalf("uc = %+v", uc)
	}
	if !strings.Contains(uc.Actors, "Клиент") || !strings.Contains(uc.Actors, "Оператор") {
		t.Fatalf("Actors = %q", uc.Actors)
	}
	if !strings.Contains(uc.Architecture, "Компонент X") {
		t.Fatalf("Architecture = %q", uc.Architecture)
	}
	if len(uc.Steps) != 3 {
		t.Fatalf("steps = %d, want 3", len(uc.Steps))
	}
	if uc.Steps[0].FlowKind != "main" || uc.Steps[0].StepOrder != 1 {
		t.Fatalf("step[0] = %+v", uc.Steps[0])
	}
	if !strings.Contains(uc.Steps[0].StepText, "настройки уведомлений") {
		t.Fatalf("step[0].Text = %q", uc.Steps[0].StepText)
	}
	if uc.Steps[2].FlowKind != "alternative" || uc.Steps[2].StepOrder != 1 {
		t.Fatalf("alt step = %+v", uc.Steps[2])
	}
}

func TestParseUsecaseFile_BusinessProcess(t *testing.T) {
	content := `# 10. Бизнес-процесс "Открытие активного счета депо"

> Confluence pageId 403110443 · раздел: Депозитарий. Счета и журналы

## Описание

| Шаг процесса | Система | Роль | Описание |
| --- | --- | --- | --- |
| Регистрация документов | Диасофт | Оператор | Вручную выполнить добавление документов |
| "Требуется открытие?": "Да" |
| 2. Создание счетов | Диасофт | Оператор | Создать карточки счетов |
| | | | |
`
	cl := Classified{Kind: KindUsecase, SourceDir: "business-processes"}
	uc := ParseUsecaseFile(cl, "10._Бизнес-процесс_Открытие_активного_счета_депо.md", content)

	if uc.Kind != "business-process" || uc.SourceDir != "business-processes" {
		t.Fatalf("uc kind/source = %q/%q", uc.Kind, uc.SourceDir)
	}
	if uc.PageID != 403110443 {
		t.Fatalf("PageID = %d", uc.PageID)
	}
	if len(uc.Steps) != 2 {
		t.Fatalf("steps = %d, want 2 (решение и пустая строка не шаги): %+v", len(uc.Steps), uc.Steps)
	}
	if !strings.Contains(uc.Steps[0].StepText, "Регистрация документов — Оператор") {
		t.Fatalf("step[0] = %q", uc.Steps[0].StepText)
	}
	if !strings.Contains(uc.Steps[1].StepText, "Создание счетов") {
		t.Fatalf("step[1] = %q", uc.Steps[1].StepText)
	}
}

func TestParseUsecaseFile_UsecasesFormat(t *testing.T) {
	content := `# Сценарий REQ-001/SC-001 — Расчет ставки налога

Продукт: FA# «Налоговый агент» (FOT/TaxAgent)

## Требование REQ-001

**Описание**: Ставка налога по клиенту без признака иностранного агента.

### Сценарий REQ-001/SC-001 - Расчет ставки налога

**Пользователи и системы**: Система (автоматически); наблюдает Налоговый специалист.
**Тип сценария**: основной
**Бизнес-ценность**: Автоматически применяет корректную льготную ставку.

#### Предусловия

- Дата выплаты дохода >= 01.01.2026.
- Сделка по ЦБ с валютой номинала RUB.

#### Шаги

**Шаг 1**. Загрузка документа выплаты
**Пользователь/система**: Налоговый специалист / система-источник
**Действие**: Загрузить в систему документ выплаты дохода
**Ожидаемый результат**: Документ выплаты дохода загружен в систему.

**Шаг 2**. Формирование НОВД и расчёт налоговых показателей
**Действие**: Сформировать НОВД по документу выплаты
**Ожидаемый результат**: НОВД сформирована, ставка налога определена.

#### Постусловия

- По НОВД применена льготная ставка 20%.
- Ставка налога зафиксирована в карточке расчёта.

**Источник**: RMS-3803951, https://conf.diasoft.ru/pages/viewpage.action?pageId=467512912.
`
	cl := Classified{Kind: KindUsecase, SourceDir: "usecases"}
	uc := ParseUsecaseFile(cl, "REQ-001-SC-001 — Расчет ставки налога.md", content)

	if uc.Kind != "usecase" || uc.SourceDir != "usecases" {
		t.Fatalf("uc kind/source = %q/%q", uc.Kind, uc.SourceDir)
	}
	if uc.Title != "Сценарий REQ-001/SC-001 — Расчет ставки налога" {
		t.Fatalf("Title = %q", uc.Title)
	}
	if !strings.Contains(uc.Description, "Ставка налога") {
		t.Fatalf("Description = %q", uc.Description)
	}
	if !strings.Contains(uc.Actors, "Система") {
		t.Fatalf("Actors = %q", uc.Actors)
	}
	if !strings.Contains(uc.BusinessValue, "льготную ставку") {
		t.Fatalf("BusinessValue = %q", uc.BusinessValue)
	}
	if !strings.Contains(uc.Preconditions, "Дата выплаты") {
		t.Fatalf("Preconditions = %q", uc.Preconditions)
	}
	if !strings.Contains(uc.Postconditions, "льготная ставка 20%") {
		t.Fatalf("Postconditions = %q", uc.Postconditions)
	}
	if uc.PageID != 467512912 {
		t.Fatalf("PageID = %d, want 467512912", uc.PageID)
	}
	if len(uc.Steps) != 2 {
		t.Fatalf("steps = %d, want 2: %+v", len(uc.Steps), uc.Steps)
	}
	if uc.Steps[0].FlowKind != "main" || uc.Steps[0].StepOrder != 1 {
		t.Fatalf("step[0] = %+v", uc.Steps[0])
	}
	if !strings.Contains(uc.Steps[0].StepText, "Загрузка документа выплаты") {
		t.Fatalf("step[0].Text = %q", uc.Steps[0].StepText)
	}
	if uc.Steps[1].StepOrder != 2 {
		t.Fatalf("step[1].Order = %d", uc.Steps[1].StepOrder)
	}
}

func TestParseUsecaseFile_SpecsUsecasesFormat(t *testing.T) {
	content := `# Приём клиента на обслуживание

**Источник**: бизнес-процесс «Приём клиента», Confluence pageId 403104219 (раздел «Счета и журналы»).
**Роли и системы**: Оператор; Система (Custody).
**Бизнес-ценность**: клиент принимается на обслуживание с автоматическим оформлением.
**Точка старта**: получено поручение на приём на обслуживание.

## Схема процесса

BPMN-схема: [` + "`_bpmn/BP-01.bpmn`" + `](` + "`_bpmn/BP-01.bpmn`" + `).

## Шаги

| Шаг | Пользователь/система | Действие | Ожидаемый результат |
|---|---|---|---|
| 0 | Оператор | При отсутствии модуля БО вручную создать поручение | Поручение создано вручную |
| 1 | Оператор (авто) | От объекта «заявка» создать поручение | Поручение создано автоматически |
| 2 | Оператор (авто) | Сформировать входящий документ | Входящий документ создан |

## Ветвления

- **WHEN** «Есть модуль БО?» = Да → создание поручения (шаг 1)
  **ELSE** → ручное создание поручения (шаг 0)

## Постусловия

- Оформлен депозитарный договор; открыты счета и разделы.

## Спеки-компоненты (запчасти)

- ` + "`specs/depo-contract/spec.md`" + ` — шаг 3 (создание договора). Требования: «Ведение договора»
- ` + "`specs/depo-account/spec.md`" + ` — шаги 4, 8 (открытие счетов). Требования: «Открытие счёта»
`
	cl := Classified{Kind: KindUsecase, SourceDir: "usecases"}
	uc := ParseUsecaseFile(cl, "BP-01 Приём клиента на обслуживание.md", content)

	if uc.Kind != "usecase" {
		t.Fatalf("Kind = %q", uc.Kind)
	}
	if uc.PageID != 403104219 {
		t.Fatalf("PageID = %d, want 403104219", uc.PageID)
	}
	if !strings.Contains(uc.Actors, "Оператор") {
		t.Fatalf("Actors = %q", uc.Actors)
	}
	if !strings.Contains(uc.BusinessValue, "автоматическим оформлением") {
		t.Fatalf("BusinessValue = %q", uc.BusinessValue)
	}
	if !strings.Contains(uc.Architecture, "поручение на приём") {
		t.Fatalf("Architecture (точка старта) = %q", uc.Architecture)
	}
	if !strings.Contains(uc.Postconditions, "депозитарный договор") {
		t.Fatalf("Postconditions = %q", uc.Postconditions)
	}
	// 3 table steps (main) + 1 branching bullet (alternative, **ELSE** is continuation)
	if len(uc.Steps) != 4 {
		t.Fatalf("steps = %d, want 4: %+v", len(uc.Steps), uc.Steps)
	}
	// Table steps should be main
	if uc.Steps[0].FlowKind != "main" {
		t.Fatalf("step[0].FlowKind = %q, want main", uc.Steps[0].FlowKind)
	}
	if !strings.Contains(uc.Steps[0].StepText, "создать поручение") {
		t.Fatalf("step[0].Text = %q", uc.Steps[0].StepText)
	}
	// Branching steps should be alternative
	altSteps := 0
	for _, s := range uc.Steps {
		if s.FlowKind == "alternative" {
			altSteps++
		}
	}
	if altSteps != 1 {
		t.Fatalf("alternative steps = %d, want 1", altSteps)
	}
}

func TestParseUsecaseIndex(t *testing.T) {
	content := "# Индекс\n\n- [10. Бизнес-процесс](Депозитарий/10._БП.md) `403110443`\n  - [11. Открытие раздела](Депозитарий/11._ОР.md) `404856314`\n"
	index := ParseUsecaseIndex(content)
	if len(index) != 2 {
		t.Fatalf("index = %v", index)
	}
	if index["10._БП.md"] != 403110443 || index["11._ОР.md"] != 404856314 {
		t.Fatalf("index = %v", index)
	}
}

func TestParseDeltaSpec(t *testing.T) {
	content := `## ADDED Requirements

### Requirement: Схемы карточных лимитов
Система SHALL управлять схемами.

#### Scenario: Создание
- **WHEN** создаётся схема
- **THEN** создана запись

## REMOVED Requirements

### Requirement: Устаревшее требование
Текст удалённого требования.
`
	deltas := ParseDeltaSpec(content)
	if len(deltas) != 2 {
		t.Fatalf("deltas = %d, want 2", len(deltas))
	}
	if deltas[0].Section != "ADDED" || deltas[0].RequirementName != "Схемы карточных лимитов" {
		t.Fatalf("deltas[0] = %+v", deltas[0])
	}
	if strings.Contains(deltas[0].BodyText, "Scenario") {
		// тело — до первого сценария
		t.Fatalf("delta body must not include scenario: %q", deltas[0].BodyText)
	}
	if !strings.Contains(deltas[0].BodyText, "управлять схемами") {
		t.Fatalf("delta[0].BodyText = %q", deltas[0].BodyText)
	}
	if deltas[1].Section != "REMOVED" || !strings.Contains(deltas[1].BodyText, "удалённого") {
		t.Fatalf("deltas[1] = %+v", deltas[1])
	}
}

func TestExtractSpecReferences(t *testing.T) {
	content := "Требования в `openspec/specs/CORE/data` и markdown-ссылка [x](../specs/card-limits/spec.md), также [y](../project/spec.md).\n"
	refs := ExtractSpecReferences(content)
	if _, ok := refs["CORE/data"]; !ok {
		t.Fatalf("refs = %v, want CORE/data", refs)
	}
	if _, ok := refs["card-limits"]; !ok {
		t.Fatalf("refs = %v, want card-limits", refs)
	}
	if _, ok := refs["project"]; !ok {
		t.Fatalf("refs = %v, want project (link without specs segment)", refs)
	}
}

func TestExtractSpecReferencesFromCapabilitySections(t *testing.T) {
	content := `## Why
Причина изменения.

## Capabilities

### New Capabilities
- ` + "`ocdd-maket99`" + `: новая capability для макета 99

### Modified Capabilities
- ` + "`card-soap-call`" + `: вызов сервиса ФМС переводится на BOnline
- ` + "`inputfile-verify`" + `: цепочка проверки паспортов
- ` + "`api-facade`" + `: новый фасад

## Impact
Прочее.
`
	refs := ExtractSpecReferences(content)
	for _, want := range []string{"ocdd-maket99", "card-soap-call", "inputfile-verify", "api-facade"} {
		if _, ok := refs[want]; !ok {
			t.Fatalf("refs = %v, want %q", refs, want)
		}
	}
}

func TestParseSkipSpecsYAMLAndProposal(t *testing.T) {
	if !ParseSkipSpecsYAML("schema: spec-driven\nskip_specs: true\ncreated: 2026-08-27\n") {
		t.Fatal("skip_specs: true must be detected")
	}
	if ParseSkipSpecsYAML("schema: spec-driven\n") {
		t.Fatal("skip_specs absent must be false")
	}

	cl := Classified{Kind: KindChangeProposal, ChangeName: "qtskt-761477", Archived: false}
	pc := ParseChangeProposal(cl, "# Proposal: Fix\n\n## Описание\nИсправление нумерации F120.\n")
	if pc.Name != "qtskt-761477" || pc.Archived {
		t.Fatalf("pc = %+v", pc)
	}
	if !strings.Contains(pc.Description, "нумерации F120") {
		t.Fatalf("Description = %q", pc.Description)
	}
}

func TestExtractMentions(t *testing.T) {
	related := `### Серверные процедуры
- ` + "`" + `Cards/SERVER/Card/CardLimit_proc.sql` + "`" + ` — обработка
- CardLimitScheme_Insert/Update/Delete.sql — CRUD
- Таблицы: ` + "`" + `tCardLimit` + "`" + `, ` + "`" + `tCardLimitScheme` + "`" + `

### API-процедуры
- ` + "`" + `API_Depo/Server/DepoAccount/API_DepoAccount_MassInsert.sql` + "`" + ` — массовое
- ` + "`" + `FCD_Operation` + "`" + ` — фасад
`
	mentions := ExtractMentionsFromRelatedCode(related)
	byKind := map[string][]string{}
	for _, m := range mentions {
		byKind[m.Kind] = append(byKind[m.Kind], m.Name)
	}
	if !containsName(byKind["procedure"], "CardLimit_proc") {
		t.Fatalf("procedure mentions = %v", byKind["procedure"])
	}
	if !containsName(byKind["procedure"], "CardLimitScheme_Insert") {
		t.Fatalf("bare sql path (CardLimitScheme_Insert/Update/Delete.sql) not captured: %v", byKind["procedure"])
	}
	if !containsName(byKind["table"], "tCardLimit") || !containsName(byKind["table"], "tCardLimitScheme") {
		t.Fatalf("table mentions = %v", byKind["table"])
	}
	if !containsName(byKind["api"], "API_DepoAccount_MassInsert") {
		t.Fatalf("api mentions = %v", byKind["api"])
	}
	if !containsName(byKind["procedure"], "FCD_Operation") {
		t.Fatalf("FCD mention = %v", byKind["procedure"])
	}

	// inline: backtick-имена и однозначные маркеры; стоп-слова не попадают
	inline := "Процедура `API_CCred_BindClassifier` вызывается (см. `calc-flow`), таблица tOperPart, но Confluence и Диасофт — не упоминания."
	inlineMentions := ExtractMentionsInline(inline)
	foundAPI, foundTable, foundSlug := false, false, false
	for _, m := range inlineMentions {
		if m.Name == "API_CCred_BindClassifier" {
			foundAPI = true
		}
		if m.Name == "tOperPart" && m.Kind == "table" {
			foundTable = true
		}
		if m.Name == "calc-flow" {
			foundSlug = true
		}
		if strings.EqualFold(m.Name, "confluence") || strings.EqualFold(m.Name, "диасофт") {
			t.Fatalf("stop word captured: %+v", m)
		}
	}
	if !foundAPI || !foundTable {
		t.Fatalf("inline mentions = %+v", inlineMentions)
	}
	if foundSlug {
		t.Fatalf("slug-like token calc-flow must not be a code mention (no underscore): %+v", inlineMentions)
	}
}

func containsName(names []string, want string) bool {
	for _, n := range names {
		if n == want {
			return true
		}
	}
	return false
}
