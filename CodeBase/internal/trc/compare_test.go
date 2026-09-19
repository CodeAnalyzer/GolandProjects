package trc

import (
	"math"
	"testing"
)

func cmpEvent(name, proc string, duration int64, spid int32) TRCEvent {
	return TRCEvent{
		EventName:  name,
		Procedure:  proc,
		DurationMs: duration,
		Columns:    map[int]any{12: spid},
	}
}

// TestCompareProceduresRows_WeightedPeerCombined — 10×100 + 30×200 = 175,
// а не среднее средних (150).
func TestCompareProceduresRows_WeightedPeerCombined(t *testing.T) {
	rows := make([]TRCProcAgg, 0)
	// focus: 1 вызов 350мс
	rows = append(rows, TRCProcAgg{SPID: 728, Procedure: "MyProc", Count: 1, TotalMs: 350, MinMs: 350, MaxMs: 350, AvgMs: 350})
	// peer 700: 10 вызовов avg 100
	rows = append(rows, TRCProcAgg{SPID: 700, Procedure: "MyProc", Count: 10, TotalMs: 1000, MinMs: 50, MaxMs: 150, AvgMs: 100})
	// peer 179: 30 вызовов avg 200
	rows = append(rows, TRCProcAgg{SPID: 179, Procedure: "MyProc", Count: 30, TotalMs: 6000, MinMs: 100, MaxMs: 400, AvgMs: 200})

	res := CompareProceduresRows(rows, CompareOptions{FocusSPID: 728, CompareSPIDs: []int{700, 179}, Top: 20, SortBy: "total_ms"})
	if len(res.Procedures) != 1 {
		t.Fatalf("procedures = %d, want 1", len(res.Procedures))
	}
	p := res.Procedures[0]
	if p.Rank != 1 || p.Procedure != "MyProc" {
		t.Fatalf("rank/procedure = %d/%q", p.Rank, p.Procedure)
	}
	if p.PeerCombined.Count != 40 || p.PeerCombined.TotalMs != 7000 {
		t.Fatalf("combined count/total = %d/%d, want 40/7000", p.PeerCombined.Count, p.PeerCombined.TotalMs)
	}
	if p.PeerCombined.AvgMs == nil || math.Abs(*p.PeerCombined.AvgMs-175) > 1e-9 {
		t.Fatalf("combined avg = %v, want 175 (weighted)", p.PeerCombined.AvgMs)
	}
	if p.PeerCombined.MinMs == nil || *p.PeerCombined.MinMs != 50 || p.PeerCombined.MaxMs == nil || *p.PeerCombined.MaxMs != 400 {
		t.Fatalf("combined min/max = %v/%v, want 50/400", p.PeerCombined.MinMs, p.PeerCombined.MaxMs)
	}
	// ratios: avg 350/175=2, max 350/400=0.875, count 1/40=0.025
	if p.Ratios.AvgVsPeers == nil || math.Abs(*p.Ratios.AvgVsPeers-2) > 1e-9 {
		t.Fatalf("avg_vs_peers = %v, want 2", p.Ratios.AvgVsPeers)
	}
	if p.Ratios.MaxVsPeers == nil || math.Abs(*p.Ratios.MaxVsPeers-0.875) > 1e-9 {
		t.Fatalf("max_vs_peers = %v, want 0.875", p.Ratios.MaxVsPeers)
	}
	if p.Ratios.CountVsPeers == nil || math.Abs(*p.Ratios.CountVsPeers-0.025) > 1e-9 {
		t.Fatalf("count_vs_peers = %v, want 0.025", p.Ratios.CountVsPeers)
	}
	// порядок peers сохранён
	if len(p.Peers) != 2 || p.Peers[0].SPID != 700 || p.Peers[1].SPID != 179 {
		t.Fatalf("peers order = %+v, want [700 179]", p.Peers)
	}
}

// TestCompareProceduresRows_MissingPeerNullable — отсутствующая пара отличима
// от нулевой длительности; ratios null при отсутствии peer-вызовов.
func TestCompareProceduresRows_MissingPeerNullable(t *testing.T) {
	rows := []TRCProcAgg{
		{SPID: 728, Procedure: "Only", Count: 3, TotalMs: 0, MinMs: 0, MaxMs: 0, AvgMs: 0},
	}
	res := CompareProceduresRows(rows, CompareOptions{FocusSPID: 728, CompareSPIDs: []int{700}, Top: 20})
	p := res.Procedures[0]
	peer := p.Peers[0]
	if peer.SPID != 700 || peer.Count != 0 || peer.TotalMs != 0 {
		t.Fatalf("peer = %+v, want zeros", peer)
	}
	if peer.MinMs != nil || peer.MaxMs != nil || peer.AvgMs != nil {
		t.Fatalf("peer min/max/avg = %v/%v/%v, want nil", peer.MinMs, peer.MaxMs, peer.AvgMs)
	}
	if p.PeerCombined.Count != 0 || p.PeerCombined.AvgMs != nil {
		t.Fatalf("combined = %+v, want zero/null", p.PeerCombined)
	}
	if p.Ratios.AvgVsPeers != nil || p.Ratios.MaxVsPeers != nil || p.Ratios.CountVsPeers != nil {
		t.Fatalf("ratios = %+v, want all nil (zero denominator)", p.Ratios)
	}
}

// TestCompareProceduresRows_TopByFocusOnly — тяжёлая peer-процедура не входит
// в ответ, Top-N определяется только focus.
func TestCompareProceduresRows_TopByFocusOnly(t *testing.T) {
	rows := []TRCProcAgg{
		{SPID: 728, Procedure: "FocusProc", Count: 1, TotalMs: 100, MinMs: 100, MaxMs: 100, AvgMs: 100},
		{SPID: 700, Procedure: "HeavyPeerProc", Count: 100, TotalMs: 999999, MinMs: 1, MaxMs: 9999, AvgMs: 9999},
	}
	res := CompareProceduresRows(rows, CompareOptions{FocusSPID: 728, CompareSPIDs: []int{700}, Top: 20})
	if len(res.Procedures) != 1 || res.Procedures[0].Procedure != "FocusProc" {
		t.Fatalf("procedures = %+v, want only FocusProc", res.Procedures)
	}
}

// TestCompareProcedures_FileMode — сборка из событий: event_names фильтрует,
// Statement-события не задваивают вызов, top ограничивает.
func TestCompareProcedures_FileMode(t *testing.T) {
	events := []TRCEvent{
		cmpEvent("SP:Completed", "A", 300, 728),
		cmpEvent("SP:Completed", "A", 100, 728),
		cmpEvent("SP:StmtCompleted", "A", 50, 728), // не входит
		cmpEvent("SP:Completed", "B", 40, 728),
		cmpEvent("SP:Completed", "A", 200, 700),
		cmpEvent("RPC:Completed", "A", 10, 700),
		{EventName: "SP:Completed", Procedure: "NoSPID", DurationMs: 5}, // без SPID — вне compare
	}
	res := CompareProcedures(events, CompareOptions{FocusSPID: 728, CompareSPIDs: []int{700}, Top: 10})
	if len(res.Procedures) != 2 {
		t.Fatalf("procedures = %d, want 2", len(res.Procedures))
	}
	if res.Procedures[0].Procedure != "A" || res.Procedures[0].Focus.Count != 2 {
		t.Fatalf("first = %+v, want A with focus count 2", res.Procedures[0])
	}
	peer := res.Procedures[0].Peers[0]
	if peer.Count != 1 || peer.TotalMs != 200 || peer.AvgMs == nil || *peer.AvgMs != 200 {
		t.Fatalf("peer A = %+v, want count 1 total 200", peer)
	}
	// NoSPID-событие не попало ни в focus, ни в peer
	if res.Procedures[1].Procedure != "B" {
		t.Fatalf("second = %q, want B", res.Procedures[1].Procedure)
	}
}

// TestCompareProceduresRows_SortAndTieBreak — sort_by=count, tie по имени.
func TestCompareProceduresRows_SortAndTieBreak(t *testing.T) {
	rows := []TRCProcAgg{
		{SPID: 728, Procedure: "Beta", Count: 5, TotalMs: 100},
		{SPID: 728, Procedure: "Alpha", Count: 5, TotalMs: 10},
		{SPID: 728, Procedure: "Gamma", Count: 9, TotalMs: 1},
	}
	res := CompareProceduresRows(rows, CompareOptions{FocusSPID: 728, CompareSPIDs: []int{700}, Top: 2, SortBy: "count"})
	if len(res.Procedures) != 2 {
		t.Fatalf("top = %d, want 2", len(res.Procedures))
	}
	if res.Procedures[0].Procedure != "Gamma" || res.Procedures[1].Procedure != "Alpha" {
		t.Fatalf("order = %q %q, want Gamma Alpha (count desc, name asc tie)", res.Procedures[0].Procedure, res.Procedures[1].Procedure)
	}
}
