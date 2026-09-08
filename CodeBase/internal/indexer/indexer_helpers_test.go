package indexer

import (
	"reflect"
	"testing"

	"github.com/codebase/internal/config"
	"github.com/codebase/internal/fswalk"
	"github.com/codebase/internal/model"
	"github.com/codebase/internal/store"
)

func TestNormalizeParallel(t *testing.T) {
	if got := normalizeParallel(0); got != 1 {
		t.Fatalf("normalizeParallel(0) = %d, want 1", got)
	}
	if got := normalizeParallel(-3); got != 1 {
		t.Fatalf("normalizeParallel(-3) = %d, want 1", got)
	}
	if got := normalizeParallel(4); got != 4 {
		t.Fatalf("normalizeParallel(4) = %d, want 4", got)
	}
}

func TestMergeScanStatsAndCollector(t *testing.T) {
	dst := &model.ScanStats{FilesScanned: 1, Errors: 2}
	src := &model.ScanStats{FilesScanned: 3, FilesIndexed: 4, Relations: 5, Errors: 1}
	mergeScanStats(dst, src)
	if dst.FilesScanned != 4 || dst.FilesIndexed != 4 || dst.Relations != 5 || dst.Errors != 3 {
		t.Fatalf("merged stats = %+v", dst)
	}

	// SaveMs/ParseMs должны суммироваться
	dst2 := &model.ScanStats{SaveMs: 100, ParseMs: 200}
	src2 := &model.ScanStats{SaveMs: 50, ParseMs: 300}
	mergeScanStats(dst2, src2)
	if dst2.SaveMs != 150 || dst2.ParseMs != 500 {
		t.Fatalf("SaveMs/ParseMs merge: got SaveMs=%d ParseMs=%d, want 150/500", dst2.SaveMs, dst2.ParseMs)
	}

	// PreFilteredFiles должен суммироваться
	dst3 := &model.ScanStats{PreFilteredFiles: 100}
	src3 := &model.ScanStats{PreFilteredFiles: 200}
	mergeScanStats(dst3, src3)
	if dst3.PreFilteredFiles != 300 {
		t.Fatalf("PreFilteredFiles merge: got %d, want 300", dst3.PreFilteredFiles)
	}

	// Spec-счётчики должны суммироваться (параллельные воркеры спек)
	dst4 := &model.ScanStats{MDFiles: 1, YAMLFiles: 2, SpecCapabilities: 3, SpecRequirements: 4, SpecScenarios: 5, SpecUsecases: 6, SpecChanges: 7, SpecConfigs: 1}
	src4 := &model.ScanStats{MDFiles: 10, YAMLFiles: 20, SpecCapabilities: 30, SpecRequirements: 40, SpecScenarios: 50, SpecUsecases: 60, SpecChanges: 70, SpecConfigs: 2}
	mergeScanStats(dst4, src4)
	if dst4.MDFiles != 11 || dst4.YAMLFiles != 22 || dst4.SpecConfigs != 3 ||
		dst4.SpecCapabilities != 33 || dst4.SpecRequirements != 44 ||
		dst4.SpecScenarios != 55 || dst4.SpecUsecases != 66 || dst4.SpecChanges != 77 {
		t.Fatalf("spec stats merge: got MD=%d YAML=%d configs=%d caps=%d reqs=%d scn=%d uc=%d ch=%d",
			dst4.MDFiles, dst4.YAMLFiles, dst4.SpecConfigs, dst4.SpecCapabilities, dst4.SpecRequirements, dst4.SpecScenarios, dst4.SpecUsecases, dst4.SpecChanges)
	}

	c := &statsCollector{}
	c.Add(func(stats *model.ScanStats) {
		stats.FilesScanned = 7
		stats.PostProcessed = 2
	})
	snap := c.Snapshot()
	if snap.FilesScanned != 7 || snap.PostProcessed != 2 {
		t.Fatalf("snapshot = %+v", snap)
	}
	c.Add(func(stats *model.ScanStats) { stats.FilesScanned++ })
	if c.Snapshot().FilesScanned != 8 {
		t.Fatalf("collector increment failed: %+v", c.Snapshot())
	}

	// SaveMs аккумуляция через collector
	c2 := &statsCollector{}
	c2.Add(func(stats *model.ScanStats) { stats.SaveMs += 10 })
	c2.Add(func(stats *model.ScanStats) { stats.SaveMs += 20 })
	if c2.Snapshot().SaveMs != 30 {
		t.Fatalf("SaveMs accumulation: got %d, want 30", c2.Snapshot().SaveMs)
	}
}

func TestPendingSnapshotsClearBuffers(t *testing.T) {
	idx := &Indexer{shared: newIndexerSharedState()}
	idx.addPendingSQLCalls(1, "a.sql", []*model.SQLProcedure{{ProcName: "P"}}, map[string]int64{"p": 1}, []*model.SQLProcedureCall{{CalleeName: "Q"}})
	idx.addPendingFragmentRefs([]*PendingFragmentRef{{FragmentID: 2, LineNumber: 3}})
	idx.addPendingJSCallRefs([]*PendingJSCallRef{{SourceID: 4, ProcName: "Q", LineNumber: 5}})
	idx.addPendingT01SubscriberRefs([]*PendingT01SubscriberRef{{SourceID: 6, CalleeName: "R", LineNumber: 7}})
	idx.addPendingAPIMacroRefs([]*PendingAPIMacroRef{{SourceID: 8, MacroType: "exec", TargetName: "S", LineNumber: 9}})

	if len(idx.snapshotPendingSQLCalls()) != 1 {
		t.Fatal("expected one pending SQL call file")
	}
	if got := idx.snapshotPendingSQLCalls(); len(got) != 0 {
		t.Fatalf("second SQL snapshot must be empty, got %d", len(got))
	}
	if len(idx.snapshotPendingFragmentRefs()) != 1 {
		t.Fatal("expected one fragment ref")
	}
	if len(idx.snapshotPendingFragmentRefs()) != 0 {
		t.Fatal("second fragment snapshot must be empty")
	}
	if len(idx.snapshotPendingJSCallRefs()) != 1 {
		t.Fatal("expected one JS call ref")
	}
	if len(idx.snapshotPendingJSCallRefs()) != 0 {
		t.Fatal("second JS snapshot must be empty")
	}
	if len(idx.snapshotPendingT01SubscriberRefs()) != 1 {
		t.Fatal("expected one T01 ref")
	}
	if len(idx.snapshotPendingT01SubscriberRefs()) != 0 {
		t.Fatal("second T01 snapshot must be empty")
	}
	if len(idx.snapshotPendingAPIMacroRefs()) != 1 {
		t.Fatal("expected one API macro ref")
	}
	if len(idx.snapshotPendingAPIMacroRefs()) != 0 {
		t.Fatal("second API snapshot must be empty")
	}
}

func TestWalkerPatterns_FiltersUnsupportedAndDedups(t *testing.T) {
	idx := &Indexer{config: &config.Config{
		Indexer: config.IndexerConfig{
			IncludePatterns: []string{"*.sql", "*.SQL", "readme.md", "", "sql", "*.unknown"},
			ExcludePatterns: []string{"*/archive/*"},
		},
	}}
	include, exclude := idx.walkerPatterns()
	if !reflect.DeepEqual(exclude, []string{"*/archive/*"}) {
		t.Fatalf("exclude = %#v", exclude)
	}
	// «readme.md» — точное имя (не *.ext): не проходит фильтр расширений (существующее поведение walkerPatterns)
	if !reflect.DeepEqual(include, []string{"*.sql"}) {
		t.Fatalf("include = %#v, want [*.sql]", include)
	}

	empty := &Indexer{}
	defInclude, defExclude := empty.walkerPatterns()
	if !reflect.DeepEqual(defInclude, fswalk.GetSupportedExtensions()) {
		t.Fatalf("default include = %#v", defInclude)
	}
	if defExclude != nil {
		t.Fatalf("default exclude = %#v, want nil", defExclude)
	}
}

func TestBuildCallbackEventRelations_NameModuleFallbackAndDedup(t *testing.T) {
	lookup := &store.EventContractLookup{
		ByNameAndModule: map[string]int64{"onaftertest|mod_a": 201},
		ByName:          map[string]int64{"onaftertest": 200, "otherevent": 300},
	}
	got := buildCallbackEventRelations([]*model.APIContract{
		nil,
		{ID: 0, UsedObjectName: "OnAfterTest"},
		{ID: 1, UsedObjectName: ""},
		{ID: 2, UsedObjectName: "OnAfterTest", UsedModuleSysName: "MOD_A"},
		{ID: 3, UsedObjectName: "OnAfterTest", UsedModuleSysName: "missing"},
		{ID: 4, UsedObjectName: "MissingEvent"},
		{ID: 2, UsedObjectName: "OnAfterTest", UsedModuleSysName: "MOD_A"},
	}, lookup)
	if len(got) != 2 {
		t.Fatalf("relations = %d, want 2: %+v", len(got), got)
	}
	if got[0].SourceID != 2 || got[0].TargetID != 201 || got[0].RelationType != "subscribes_to_event" {
		t.Fatalf("name+module relation: %+v", got[0])
	}
	if got[1].SourceID != 3 || got[1].TargetID != 200 {
		t.Fatalf("name fallback relation: %+v", got[1])
	}
	if buildCallbackEventRelations(nil, nil) != nil {
		t.Fatal("nil lookup must return nil")
	}
}

func TestBuildFragmentAndJSCallRefRelations(t *testing.T) {
	frags := buildFragmentRefRelations([]*PendingFragmentRef{
		nil,
		{FragmentID: 10, LineNumber: 4, TablesReferenced: []string{" tAccount ", "missing"}, ProcCalls: []string{"CalleeB", "CalleeB"}},
	}, map[string]int64{"taccount": 77}, map[string]int64{"calleeb": 88})
	if len(frags) != 2 {
		t.Fatalf("fragment relations = %d, want 2: %+v", len(frags), frags)
	}
	if frags[0].RelationType != "references_table" || frags[0].TargetID != 77 {
		t.Fatalf("table relation: %+v", frags[0])
	}
	if frags[1].RelationType != "calls_procedure" || frags[1].TargetID != 88 {
		t.Fatalf("proc relation: %+v", frags[1])
	}

	js := buildJSCallRefRelations([]*PendingJSCallRef{
		{SourceID: 1, ProcName: "CalleeB", LineNumber: 8},
		{SourceID: 1, ProcName: "CalleeB", LineNumber: 8},
		{SourceID: 2, ProcName: "Missing", LineNumber: 9},
	}, map[string]int64{"calleeb": 88})
	if len(js) != 1 || js[0].SourceType != "js_function" || js[0].TargetID != 88 {
		t.Fatalf("js relations = %+v", js)
	}
}

func TestBuildSpecRelationsPreservesSourcesAndUsecaseTargets(t *testing.T) {
	mentions := []*model.SpecCodeMention{
		{SourceType: "spec_requirement", SourceID: 41, MentionName: "ProcA", MentionKind: "procedure", LineNumber: 12},
		{SourceType: "spec_scenario", SourceID: 42, MentionName: "API_A", MentionKind: "api", LineNumber: 18},
	}
	lookup := &specMentionLookup{Procedures: map[string]int64{"proca": 101}, APIs: map[string]int64{"api_a": 102}}
	relations := buildSpecMentionRelations(mentions, lookup)
	if len(relations) != 2 || relations[0].SourceID != 41 || relations[1].SourceID != 42 {
		t.Fatalf("spec mention relations = %+v", relations)
	}

	usecaseRelations := buildUsecaseInvolvesRelations([]store.SpecUsecaseRefRow{
		{UsecaseID: 7, SpecConfigID: 3, Slug: "cards/service"},
		{UsecaseID: 7, SpecConfigID: 3, Slug: "cards/service"},
		{UsecaseID: 8, SpecConfigID: 4, Slug: "cards/service"},
	}, map[string]int64{specSlugKey(3, "cards/service"): 55, specSlugKey(4, "cards/service"): 66})
	if len(usecaseRelations) != 2 || usecaseRelations[0].SourceID != 7 || usecaseRelations[0].TargetID != 55 || usecaseRelations[0].RelationType != "usecase_involves" || usecaseRelations[1].TargetID != 66 {
		t.Fatalf("usecase relations = %+v", usecaseRelations)
	}
}
