package indexer

import (
	"testing"

	"github.com/codebase/internal/model"
)

func TestExtractCapabilityDeps_LinkedDomainsNoColon(t *testing.T) {
	slugToID := map[string]int64{
		"1|consumer-cession":   100,
		"1|consumer-credit":    200,
		"1|api-credit":         300,
		"1|client-ui-consumer": 400,
	}
	cap := &model.SpecCapability{
		ID:             100,
		SpecConfigID:   1,
		CapabilityName: "consumer-cession",
		Notes:          "Связан с доменами `consumer-credit`, `api-credit`, `client-ui-consumer`",
	}
	relations := extractCapabilityDeps(cap, slugToID)
	if len(relations) != 3 {
		t.Fatalf("expected 3 relations, got %d", len(relations))
	}
	expected := map[int64]bool{200: false, 300: false, 400: false}
	for _, r := range relations {
		if r.SourceID != 100 || r.RelationType != "depends_on_capability" {
			t.Fatalf("unexpected relation: %+v", r)
		}
		if _, ok := expected[r.TargetID]; !ok {
			t.Fatalf("unexpected targetID %d", r.TargetID)
		}
		expected[r.TargetID] = true
	}
	for id, found := range expected {
		if !found {
			t.Fatalf("missing relation to targetID %d", id)
		}
	}
}

func TestExtractCapabilityDeps_Subdomains(t *testing.T) {
	slugToID := map[string]int64{
		"1|consumer-cession":                      100,
		"1|consumer-cession/portfolio-management": 101,
		"1|consumer-cession/nominal-calculation":  102,
		"1|consumer-cession/purchase":             103,
	}
	cap := &model.SpecCapability{
		ID:             100,
		SpecConfigID:   1,
		CapabilityName: "consumer-cession",
		Purpose:        "Домен разделён на пять поддоменов, каждый из которых описывает отдельную функциональную область.",
		Notes:          "Подробные спецификации по каждому поддомену: `portfolio-management`, `nominal-calculation`, `purchase`",
	}
	relations := extractCapabilityDeps(cap, slugToID)
	if len(relations) != 3 {
		t.Fatalf("expected 3 relations, got %d", len(relations))
	}
	expected := map[int64]bool{101: false, 102: false, 103: false}
	for _, r := range relations {
		if r.SourceID != 100 || r.RelationType != "depends_on_capability" {
			t.Fatalf("unexpected relation: %+v", r)
		}
		if _, ok := expected[r.TargetID]; !ok {
			t.Fatalf("unexpected targetID %d", r.TargetID)
		}
		expected[r.TargetID] = true
	}
	for id, found := range expected {
		if !found {
			t.Fatalf("missing relation to targetID %d", id)
		}
	}
}

func TestExpandHierarchyDeps_ParentToChildren(t *testing.T) {
	// parent=1 has children 2,3
	childrenOf := map[int64][]int64{1: {2, 3}}
	parentOf := map[int64]int64{2: 1, 3: 1}
	direct := []*model.Relation{
		{SourceType: "spec_capability", SourceID: 10, TargetType: "spec_capability", TargetID: 1, RelationType: "depends_on_capability", Confidence: "explicit"},
	}
	result := expandHierarchyDeps(direct, childrenOf, parentOf)
	// 1 direct + 2 hierarchy (children)
	if len(result) != 3 {
		t.Fatalf("expected 3 relations, got %d: %+v", len(result), result)
	}
	hierarchyCount := 0
	for _, r := range result {
		if r.Confidence == "hierarchy" {
			hierarchyCount++
			if r.TargetID != 2 && r.TargetID != 3 {
				t.Fatalf("unexpected hierarchy target %d", r.TargetID)
			}
		}
	}
	if hierarchyCount != 2 {
		t.Fatalf("expected 2 hierarchy relations, got %d", hierarchyCount)
	}
}

func TestExpandHierarchyDeps_ChildToParent(t *testing.T) {
	// child=2 has parent=1
	childrenOf := map[int64][]int64{1: {2}}
	parentOf := map[int64]int64{2: 1}
	direct := []*model.Relation{
		{SourceType: "spec_capability", SourceID: 10, TargetType: "spec_capability", TargetID: 2, RelationType: "depends_on_capability", Confidence: "explicit"},
	}
	result := expandHierarchyDeps(direct, childrenOf, parentOf)
	// 1 direct + 1 hierarchy (parent)
	if len(result) != 2 {
		t.Fatalf("expected 2 relations, got %d: %+v", len(result), result)
	}
	foundParent := false
	for _, r := range result {
		if r.Confidence == "hierarchy" && r.TargetID == 1 {
			foundParent = true
		}
	}
	if !foundParent {
		t.Fatalf("missing hierarchy relation to parent ID 1")
	}
}

func TestExpandHierarchyDeps_Dedup(t *testing.T) {
	// parent=1 has child=2; direct relation to both 1 and 2
	childrenOf := map[int64][]int64{1: {2}}
	parentOf := map[int64]int64{2: 1}
	direct := []*model.Relation{
		{SourceType: "spec_capability", SourceID: 10, TargetType: "spec_capability", TargetID: 1, RelationType: "depends_on_capability", Confidence: "explicit"},
		{SourceType: "spec_capability", SourceID: 10, TargetType: "spec_capability", TargetID: 2, RelationType: "depends_on_capability", Confidence: "explicit"},
	}
	result := expandHierarchyDeps(direct, childrenOf, parentOf)
	// 2 direct + 0 hierarchy (child 2 already direct, parent 1 already direct)
	if len(result) != 2 {
		t.Fatalf("expected 2 relations (dedup), got %d: %+v", len(result), result)
	}
	for _, r := range result {
		if r.Confidence == "hierarchy" {
			t.Fatalf("unexpected hierarchy relation: %+v", r)
		}
	}
}

func TestExpandHierarchyDeps_SiblingNoParentCycle(t *testing.T) {
	// parent=1 has children 2,3 (siblings). Child 2 depends on child 3.
	// Hierarchy expansion should NOT add child→parent (2→1) because
	// source (2) and target (3) share the same parent (1).
	childrenOf := map[int64][]int64{1: {2, 3}}
	parentOf := map[int64]int64{2: 1, 3: 1}
	direct := []*model.Relation{
		{SourceType: "spec_capability", SourceID: 2, TargetType: "spec_capability", TargetID: 3, RelationType: "depends_on_capability", Confidence: "notes"},
	}
	result := expandHierarchyDeps(direct, childrenOf, parentOf)
	// Only the direct relation; no hierarchy relation to parent 1
	if len(result) != 1 {
		t.Fatalf("expected 1 relation (no parent cycle), got %d: %+v", len(result), result)
	}
	if result[0].Confidence == "hierarchy" {
		t.Fatalf("should not have hierarchy relation for siblings: %+v", result[0])
	}
}

// --- Новые тесты для расширенных паттернов ---

func TestExtractCapabilityDeps_DomainsPlural(t *testing.T) {
	slugToID := map[string]int64{
		"1|card-registers":   100,
		"1|card-transaction": 200,
		"1|card-commission":  300,
		"1|card-loyalty":     400,
	}
	cap := &model.SpecCapability{
		ID:             100,
		SpecConfigID:   1,
		CapabilityName: "card-registers",
		Notes:          "Связи с доменами: card-transaction (обороты транзакций), card-commission (комиссии), card-loyalty (кэшбэк/бонусы)",
	}
	relations := extractCapabilityDeps(cap, slugToID)
	if len(relations) != 3 {
		t.Fatalf("expected 3 relations, got %d: %+v", len(relations), relations)
	}
	expected := map[int64]bool{200: false, 300: false, 400: false}
	for _, r := range relations {
		if r.SourceID != 100 || r.RelationType != "depends_on_capability" {
			t.Fatalf("unexpected relation: %+v", r)
		}
		if r.Confidence != "notes" {
			t.Fatalf("expected confidence 'notes', got %q", r.Confidence)
		}
		if _, ok := expected[r.TargetID]; !ok {
			t.Fatalf("unexpected targetID %d", r.TargetID)
		}
		expected[r.TargetID] = true
	}
	for id, found := range expected {
		if !found {
			t.Fatalf("missing relation to targetID %d", id)
		}
	}
}

func TestExtractCapabilityDeps_OtherDomains(t *testing.T) {
	slugToID := map[string]int64{
		"1|card-transaction": 100,
		"1|card-proccenter":  200,
		"1|card-race-export": 300,
		"1|card-registers":   400,
	}
	cap := &model.SpecCapability{
		ID:             100,
		SpecConfigID:   1,
		CapabilityName: "card-transaction",
		Notes:          "Связи с другими доменами: процессинговый центр (card-proccenter, card-race-export) — загрузка рейсов из ПЦ; финансовые регистры (card-registers) — проводки по транзакциям",
	}
	relations := extractCapabilityDeps(cap, slugToID)
	expected := map[int64]bool{200: false, 300: false, 400: false}
	for _, r := range relations {
		if r.SourceID != 100 || r.RelationType != "depends_on_capability" {
			t.Fatalf("unexpected relation: %+v", r)
		}
		if r.Confidence != "notes" {
			t.Fatalf("expected confidence 'notes', got %q", r.Confidence)
		}
		if _, ok := expected[r.TargetID]; !ok {
			t.Fatalf("unexpected targetID %d", r.TargetID)
		}
		expected[r.TargetID] = true
	}
	for id, found := range expected {
		if !found {
			t.Fatalf("missing relation to targetID %d", id)
		}
	}
}

func TestExtractCapabilityDeps_OtherSpecs(t *testing.T) {
	slugToID := map[string]int64{
		"1|operations":      100,
		"1|chart-accounts":  200,
		"1|memorial-orders": 300,
		"1|balances":        400,
	}
	cap := &model.SpecCapability{
		ID:             100,
		SpecConfigID:   1,
		CapabilityName: "operations",
		Notes:          "Связи с другими spec: chart-accounts (контроль остатка при вводе, красное сальдо), memorial-orders (создание МО по проводкам CreateMOByOper), balances (расчёт остатков из tOperPart)",
	}
	relations := extractCapabilityDeps(cap, slugToID)
	expected := map[int64]bool{200: false, 300: false, 400: false}
	for _, r := range relations {
		if r.SourceID != 100 || r.RelationType != "depends_on_capability" {
			t.Fatalf("unexpected relation: %+v", r)
		}
		if r.Confidence != "notes" {
			t.Fatalf("expected confidence 'notes', got %q", r.Confidence)
		}
		if _, ok := expected[r.TargetID]; !ok {
			t.Fatalf("unexpected targetID %d", r.TargetID)
		}
		expected[r.TargetID] = true
	}
	for id, found := range expected {
		if !found {
			t.Fatalf("missing relation to targetID %d", id)
		}
	}
}

func TestExtractCapabilityDeps_LinkedNoDomains(t *testing.T) {
	slugToID := map[string]int64{
		"1|consumer-pay-schedule": 100,
		"1|consumer-credit":       200,
	}
	cap := &model.SpecCapability{
		ID:             100,
		SpecConfigID:   1,
		CapabilityName: "consumer-pay-schedule",
		Notes:          "Связан с `consumer-credit` — графики привязаны к кредитным договорам",
	}
	relations := extractCapabilityDeps(cap, slugToID)
	if len(relations) != 1 {
		t.Fatalf("expected 1 relation, got %d: %+v", len(relations), relations)
	}
	if relations[0].TargetID != 200 {
		t.Fatalf("expected targetID 200, got %d", relations[0].TargetID)
	}
	if relations[0].Confidence != "notes" {
		t.Fatalf("expected confidence 'notes', got %q", relations[0].Confidence)
	}
}

func TestExtractCapabilityDeps_LinkedNoDomainsMultiple(t *testing.T) {
	slugToID := map[string]int64{
		"1|verification":      100,
		"1|contract-coverage": 200,
		"1|object-coverage":   300,
		"1|assessment":        400,
		"1|elements":          500,
		"1|links":             600,
	}
	cap := &model.SpecCapability{
		ID:             100,
		SpecConfigID:   1,
		CapabilityName: "verification",
		Notes:          "Связан с `contract-coverage`, `object-coverage`, `assessment`, `elements`, `links`",
	}
	relations := extractCapabilityDeps(cap, slugToID)
	expected := map[int64]bool{200: false, 300: false, 400: false, 500: false, 600: false}
	for _, r := range relations {
		if r.SourceID != 100 || r.RelationType != "depends_on_capability" {
			t.Fatalf("unexpected relation: %+v", r)
		}
		if _, ok := expected[r.TargetID]; !ok {
			t.Fatalf("unexpected targetID %d", r.TargetID)
		}
		expected[r.TargetID] = true
	}
	for id, found := range expected {
		if !found {
			t.Fatalf("missing relation to targetID %d", id)
		}
	}
}

func TestExtractCapabilityDeps_DomainAngle(t *testing.T) {
	slugToID := map[string]int64{
		"1|object-coverage":   100,
		"1|contract-coverage": 200,
		"1|dictionaries":      300,
		"1|assessment":        400,
	}
	cap := &model.SpecCapability{
		ID:             100,
		SpecConfigID:   1,
		CapabilityName: "object-coverage",
		Notes:          "Связь с доменом «Договоры обеспечения» (contract-coverage): объект привязывается к договору. Связь с доменом «Справочники» (dictionaries): элементы обеспечения. Связь с доменом «Оценка и стоимость» (assessment): экспертные оценки.",
	}
	relations := extractCapabilityDeps(cap, slugToID)
	expected := map[int64]bool{200: false, 300: false, 400: false}
	for _, r := range relations {
		if r.SourceID != 100 || r.RelationType != "depends_on_capability" {
			t.Fatalf("unexpected relation: %+v", r)
		}
		if r.Confidence != "notes" {
			t.Fatalf("expected confidence 'notes', got %q", r.Confidence)
		}
		if _, ok := expected[r.TargetID]; !ok {
			t.Fatalf("unexpected targetID %d", r.TargetID)
		}
		expected[r.TargetID] = true
	}
	for id, found := range expected {
		if !found {
			t.Fatalf("missing relation to targetID %d", id)
		}
	}
}

func TestExtractCapabilityDeps_LinkSpec(t *testing.T) {
	slugToID := map[string]int64{
		"1|change-interest-baserate": 100,
		"1|consumer-credit":          200,
	}
	cap := &model.SpecCapability{
		ID:             100,
		SpecConfigID:   1,
		CapabilityName: "change-interest-baserate",
		Notes:          "Связь с spec `consumer-credit`: договоры из `tContractCredit` — основная сущность домена consumer-credit.",
	}
	relations := extractCapabilityDeps(cap, slugToID)
	if len(relations) != 1 {
		t.Fatalf("expected 1 relation, got %d: %+v", len(relations), relations)
	}
	if relations[0].TargetID != 200 {
		t.Fatalf("expected targetID 200, got %d", relations[0].TargetID)
	}
	if relations[0].Confidence != "notes" {
		t.Fatalf("expected confidence 'notes', got %q", relations[0].Confidence)
	}
}

func TestExtractCapabilityDeps_SeeAlsoNoParens(t *testing.T) {
	slugToID := map[string]int64{
		"1|operations":     100,
		"1|exchange-rates": 200,
	}
	cap := &model.SpecCapability{
		ID:             100,
		SpecConfigID:   1,
		CapabilityName: "operations",
		Notes:          "См. spec `exchange-rates` (курсы).",
	}
	relations := extractCapabilityDeps(cap, slugToID)
	if len(relations) != 1 {
		t.Fatalf("expected 1 relation, got %d: %+v", len(relations), relations)
	}
	if relations[0].TargetID != 200 {
		t.Fatalf("expected targetID 200, got %d", relations[0].TargetID)
	}
	if relations[0].Confidence != "inline" {
		t.Fatalf("expected confidence 'inline', got %q", relations[0].Confidence)
	}
}

func TestExtractCapabilityDeps_SeeAlsoWithParens(t *testing.T) {
	slugToID := map[string]int64{
		"1|object-coverage/verification":   100,
		"1|contract-coverage/verification": 200,
	}
	cap := &model.SpecCapability{
		ID:             100,
		SpecConfigID:   1,
		CapabilityName: "object-coverage/verification",
		Notes:          "(см. `contract-coverage/verification`). Общий механизм повторного ввода описан в `verification`.",
	}
	relations := extractCapabilityDeps(cap, slugToID)
	if len(relations) != 1 {
		t.Fatalf("expected 1 relation, got %d: %+v", len(relations), relations)
	}
	if relations[0].TargetID != 200 {
		t.Fatalf("expected targetID 200, got %d", relations[0].TargetID)
	}
	if relations[0].Confidence != "inline" {
		t.Fatalf("expected confidence 'inline', got %q", relations[0].Confidence)
	}
}

func TestExtractCapabilityDeps_RealOperationsGL(t *testing.T) {
	// Реальный текст из fa-generalledger/openspec/specs/operations/spec.md
	slugToID := map[string]int64{
		"1|operations":       100,
		"1|chart-accounts":   201,
		"1|memorial-orders":  202,
		"1|balances":         203,
		"1|subconto":         204,
		"1|oper-dates":       205,
		"1|mfbo-integration": 206,
		"1|limits-arrests":   207,
		"1|exchange-rates":   208,
	}
	cap := &model.SpecCapability{
		ID:             100,
		SpecConfigID:   1,
		CapabilityName: "operations",
		Notes: "Связи с другими spec: chart-accounts (контроль остатка при вводе, красное сальдо), " +
			"memorial-orders (создание МО по проводкам CreateMOByOper), " +
			"balances (расчёт остатков из tOperPart), subconto (субконто проводок), " +
			"oper-dates (закрытые дни, фиктивные проводки), " +
			"mfbo-integration (выгрузка/импорт через МФБО и tOperBuffer), " +
			"limits-arrests (аресты и лимиты при вводе).\n" +
			"См. spec `exchange-rates` (курсы).",
	}
	relations := extractCapabilityDeps(cap, slugToID)
	expected := map[int64]bool{
		201: false, 202: false, 203: false, 204: false, 205: false,
		206: false, 207: false, 208: false,
	}
	for _, r := range relations {
		if r.SourceID != 100 || r.RelationType != "depends_on_capability" {
			t.Fatalf("unexpected relation: %+v", r)
		}
		if _, ok := expected[r.TargetID]; !ok {
			t.Fatalf("unexpected targetID %d", r.TargetID)
		}
		expected[r.TargetID] = true
	}
	for id, found := range expected {
		if !found {
			t.Fatalf("missing relation to targetID %d (got %d relations: %+v)", id, len(relations), relations)
		}
	}
}

func TestExtractCapabilityDeps_RealCardTransaction(t *testing.T) {
	// Реальный текст из fa-cards/openspec/specs/card-transaction/spec.md
	slugToID := map[string]int64{
		"1|card-transaction":       100,
		"1|card-proccenter":        201,
		"1|card-race-export":       202,
		"1|card-registers":         203,
		"1|card-financial-message": 204,
		"1|card-commission":        205,
		"1|card-limits":            206,
		"1|card-block":             207,
	}
	cap := &model.SpecCapability{
		ID:             100,
		SpecConfigID:   1,
		CapabilityName: "card-transaction",
		Notes: "Связи с другими доменами: процессинговый центр (card-proccenter, card-race-export) — " +
			"загрузка рейсов из ПЦ; финансовые регистры (card-registers) — проводки по транзакциям; " +
			"финансовые сообщения (card-financial-message) — уведомления по транзакциям; " +
			"комиссии (card-commission) — расчёт комиссий по транзакциям; " +
			"лимиты и блокировки (card-limits, card-block) — проверки при авторизации",
	}
	relations := extractCapabilityDeps(cap, slugToID)
	expected := map[int64]bool{
		201: false, 202: false, 203: false, 204: false, 205: false, 206: false, 207: false,
	}
	for _, r := range relations {
		if r.SourceID != 100 || r.RelationType != "depends_on_capability" {
			t.Fatalf("unexpected relation: %+v", r)
		}
		if r.Confidence != "notes" {
			t.Fatalf("expected confidence 'notes', got %q", r.Confidence)
		}
		if _, ok := expected[r.TargetID]; !ok {
			t.Fatalf("unexpected targetID %d", r.TargetID)
		}
		expected[r.TargetID] = true
	}
	for id, found := range expected {
		if !found {
			t.Fatalf("missing relation to targetID %d (got %d relations: %+v)", id, len(relations), relations)
		}
	}
}
