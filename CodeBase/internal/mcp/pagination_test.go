package mcp

import (
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

// --- splitChunks ---

func TestSplitChunks_Even(t *testing.T) {
	text := strings.Repeat("a", 30)
	chunks := splitChunks(text, 10)
	if len(chunks) != 3 {
		t.Fatalf("expected 3 chunks, got %d", len(chunks))
	}
	for i, c := range chunks {
		if len(c) != 10 {
			t.Errorf("chunk %d: expected len 10, got %d", i, len(c))
		}
	}
}

func TestSplitChunks_WithRemainder(t *testing.T) {
	text := strings.Repeat("b", 25)
	chunks := splitChunks(text, 10)
	if len(chunks) != 3 {
		t.Fatalf("expected 3 chunks, got %d", len(chunks))
	}
	if len(chunks[2]) != 5 {
		t.Errorf("last chunk: expected len 5, got %d", len(chunks[2]))
	}
}

func TestSplitChunks_SmallerThanSize(t *testing.T) {
	text := "hello"
	chunks := splitChunks(text, 100)
	if len(chunks) != 1 || chunks[0] != "hello" {
		t.Fatalf("expected single chunk %q, got %v", "hello", chunks)
	}
}

func TestSplitChunks_Empty(t *testing.T) {
	chunks := splitChunks("", 10)
	if len(chunks) != 0 {
		t.Fatalf("expected 0 chunks for empty string, got %d", len(chunks))
	}
}

func TestSplitChunks_UTF8Boundary(t *testing.T) {
	// "АБВ" = 6 bytes (2 bytes per Cyrillic char); split at size=5 must NOT cut inside А(2)+Б(2)+В(2)
	// Valid split points: 0, 2, 4, 6 — so size=5 should yield split=4
	text := "ААББ" // 4 Cyrillic chars = 8 bytes
	chunks := splitChunks(text, 5)
	for _, c := range chunks {
		if !utf8.ValidString(c) {
			t.Errorf("chunk is not valid UTF-8: %q", c)
		}
	}
	// Concatenation must equal original
	if strings.Join(chunks, "") != text {
		t.Errorf("chunks do not reconstruct original string")
	}
}

// --- maybePaginate ---

func TestMaybePaginate_SmallResponse(t *testing.T) {
	ps := newPageStore(100)
	text := strings.Repeat("x", 99)
	result := ps.maybePaginate(text)
	if result != text {
		t.Fatal("small response should be returned unchanged")
	}
	ps.mu.Lock()
	n := len(ps.entries)
	ps.mu.Unlock()
	if n != 0 {
		t.Fatal("no entries should be created for small response")
	}
}

func TestMaybePaginate_ExactlyAtLimit(t *testing.T) {
	ps := newPageStore(100)
	text := strings.Repeat("x", 100)
	result := ps.maybePaginate(text)
	if result != text {
		t.Fatal("response exactly at limit should be returned unchanged")
	}
}

func TestMaybePaginate_LargeResponse(t *testing.T) {
	ps := newPageStore(100)
	text := strings.Repeat("y", 250)
	result := ps.maybePaginate(text)

	if !strings.Contains(result, "⚠️ PAGINATED RESPONSE") {
		t.Error("expected pagination header in result")
	}
	if !strings.Contains(result, "chunk 1/3") {
		t.Error("expected 'chunk 1/3' in result")
	}
	if !strings.Contains(result, "codebase_read_more") {
		t.Error("expected codebase_read_more hint in result")
	}

	ps.mu.Lock()
	n := len(ps.entries)
	ps.mu.Unlock()
	if n != 1 {
		t.Fatalf("expected 1 entry in store, got %d", n)
	}
}

// --- readChunk ---

func TestReadChunk_Sequential(t *testing.T) {
	ps := newPageStore(100)
	text := strings.Repeat("z", 250)
	first := ps.maybePaginate(text)

	// extract continuation_id from the header
	id := extractContinuationID(t, first)

	// chunk 2
	chunk2, err := ps.readChunk(id, 2)
	if err != nil {
		t.Fatalf("readChunk(2): %v", err)
	}
	if !strings.Contains(string(chunk2), "chunk 2/3") {
		t.Error("expected 'chunk 2/3' header in chunk 2")
	}
	if !strings.Contains(string(chunk2), "codebase_read_more") {
		t.Error("expected next-chunk hint in chunk 2")
	}

	// chunk 3 (final)
	chunk3, err := ps.readChunk(id, 3)
	if err != nil {
		t.Fatalf("readChunk(3): %v", err)
	}
	if !strings.Contains(string(chunk3), "✅ FINAL CHUNK") {
		t.Error("expected FINAL CHUNK header in last chunk")
	}
}

func TestReadChunk_LastChunkDeletesEntry(t *testing.T) {
	ps := newPageStore(100)
	text := strings.Repeat("w", 150)
	first := ps.maybePaginate(text)
	id := extractContinuationID(t, first)

	_, err := ps.readChunk(id, 2)
	if err != nil {
		t.Fatalf("readChunk(2): %v", err)
	}

	ps.mu.Lock()
	_, exists := ps.entries[id]
	ps.mu.Unlock()
	if exists {
		t.Fatal("entry should be deleted after reading last chunk")
	}
}

func TestReadChunk_OutOfRange(t *testing.T) {
	ps := newPageStore(100)
	text := strings.Repeat("q", 250)
	first := ps.maybePaginate(text)
	id := extractContinuationID(t, first)

	_, err := ps.readChunk(id, 99)
	if err == nil {
		t.Fatal("expected error for chunk out of range")
	}
}

func TestReadChunk_ChunkOneTreatedAsTwo(t *testing.T) {
	ps := newPageStore(100)
	text := strings.Repeat("r", 250)
	first := ps.maybePaginate(text)
	id := extractContinuationID(t, first)

	// chunk=1 (invalid) should be clamped to 2 by the caller, but readChunk itself errors
	_, err := ps.readChunk(id, 1)
	if err == nil {
		t.Fatal("readChunk(1) should return error — chunk 1 is already in first response")
	}
}

func TestReadChunk_UnknownID(t *testing.T) {
	ps := newPageStore(100)
	_, err := ps.readChunk("nonexistent", 2)
	if err == nil {
		t.Fatal("expected error for unknown continuation_id")
	}
}

// --- TTL/GC ---

func TestPageStore_GC_RemovesExpiredEntries(t *testing.T) {
	ps := newPageStore(10)
	// Manually inject an expired entry
	ps.mu.Lock()
	ps.entries["old"] = &pageEntry{
		chunks:    []string{"data"},
		createdAt: time.Now().Add(-paginationTTL - time.Second),
	}
	ps.mu.Unlock()

	// Trigger GC by calling maybePaginate with a short string (no pagination, but GC runs)
	// GC is only called on paginate path, so we need a large text
	bigText := strings.Repeat("g", 20)
	ps.maybePaginate(bigText)

	ps.mu.Lock()
	_, exists := ps.entries["old"]
	ps.mu.Unlock()
	if exists {
		t.Fatal("expired entry should have been removed by gc")
	}
}

// --- rawMCPText bypass ---

func TestSdkToolPagedResult_RawMCPText_IsVerbatim(t *testing.T) {
	// Save and restore globalPages to avoid side effects
	saved := globalPages
	globalPages = newPageStore(10) // tiny chunk size
	defer func() { globalPages = saved }()

	raw := rawMCPText("this is raw text that should pass through unchanged")
	result, err := sdkToolPagedResult(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Content) == 0 {
		t.Fatal("expected content in result")
	}
	text := result.Content[0].(*mcpsdk.TextContent).Text
	if text != string(raw) {
		t.Errorf("expected verbatim text, got %q", text)
	}
}

func TestSdkToolPagedResult_LargeJSON_GetsPaginated(t *testing.T) {
	saved := globalPages
	globalPages = newPageStore(50)
	defer func() { globalPages = saved }()

	bigData := map[string]string{"data": strings.Repeat("x", 200)}
	result, err := sdkToolPagedResult(bigData)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Content) == 0 {
		t.Fatal("expected content in result")
	}
	text := result.Content[0].(*mcpsdk.TextContent).Text
	if !strings.Contains(text, "⚠️ PAGINATED RESPONSE") {
		t.Error("expected pagination header for large response")
	}
}

// --- proactive GC on readChunk ---

func TestPageStore_GCOnReadChunk(t *testing.T) {
	ps := newPageStore(100)
	// Inject an expired entry manually
	ps.mu.Lock()
	ps.entries["expired"] = &pageEntry{
		chunks:    []string{"old"},
		createdAt: time.Now().Add(-paginationTTL - time.Second),
	}
	ps.mu.Unlock()

	// Create a valid paginated entry
	text := strings.Repeat("z", 250)
	first := ps.maybePaginate(text)
	id := extractContinuationID(t, first)

	// readChunk should trigger gc() and remove the expired entry
	_, err := ps.readChunk(id, 2)
	if err != nil {
		t.Fatalf("readChunk(2): %v", err)
	}

	ps.mu.Lock()
	_, expiredExists := ps.entries["expired"]
	ps.mu.Unlock()
	if expiredExists {
		t.Fatal("expired entry should have been removed by gc() during readChunk")
	}
}

// --- background GC loop ---

func TestPageStore_GCLoopCleansExpired(t *testing.T) {
	savedTTL := paginationTTL
	paginationTTL = 100 * time.Millisecond
	defer func() { paginationTTL = savedTTL }()

	ps := newPageStore(100)
	ps.startGCLoop()
	defer ps.stopGCLoop()

	// Inject an entry that will expire
	ps.mu.Lock()
	ps.entries["will-expire"] = &pageEntry{
		chunks:    []string{"data"},
		createdAt: time.Now(),
	}
	ps.mu.Unlock()

	// Wait for TTL + gc interval (min 1s) to pass
	time.Sleep(1500 * time.Millisecond)

	ps.mu.Lock()
	_, exists := ps.entries["will-expire"]
	ps.mu.Unlock()
	if exists {
		t.Fatal("expired entry should have been cleaned by background gc loop")
	}
}

func TestPageStore_StopGCLoop(t *testing.T) {
	ps := newPageStore(100)
	ps.startGCLoop()

	ps.gcTimerMu.Lock()
	timerActive := ps.gcTimer != nil
	ps.gcTimerMu.Unlock()
	if !timerActive {
		t.Fatal("gcTimer should be set after startGCLoop")
	}

	ps.stopGCLoop()

	ps.gcTimerMu.Lock()
	timerNil := ps.gcTimer == nil
	ps.gcTimerMu.Unlock()
	if !timerNil {
		t.Fatal("gcTimer should be nil after stopGCLoop")
	}
}

// --- helpers ---

func extractContinuationID(t *testing.T, header string) string {
	t.Helper()
	// Header contains continuation_id="<id>"
	const marker = `continuation_id="`
	idx := strings.Index(header, marker)
	if idx < 0 {
		t.Fatalf("could not find continuation_id in: %q", header)
	}
	rest := header[idx+len(marker):]
	end := strings.Index(rest, `"`)
	if end < 0 {
		t.Fatalf("malformed continuation_id in: %q", header)
	}
	return rest[:end]
}
