package mcp

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"time"
	"unicode/utf8"
)

const (
	defaultChunkSize = 8_000
)

var paginationTTL = 15 * time.Minute

// SetPaginationTTL устанавливает TTL для пагинированных ответов.
func SetPaginationTTL(d time.Duration) {
	if d > 0 {
		paginationTTL = d
	}
}

// rawMCPText возвращается verbatim как TextContent.Text:
// без JSON-маршалинга и без повторной пагинации.
type rawMCPText string

type pageEntry struct {
	chunks    []string
	createdAt time.Time
}

type pageStore struct {
	chunkSize int
	mu        sync.Mutex
	entries   map[string]*pageEntry
}

func newPageStore(chunkSize int) *pageStore {
	if chunkSize <= 0 {
		chunkSize = defaultChunkSize
	}
	return &pageStore{
		chunkSize: chunkSize,
		entries:   make(map[string]*pageEntry),
	}
}

// globalPages — пакетный синглтон; переинициализируется из RunStdio по конфигу.
var globalPages = newPageStore(defaultChunkSize)

// maybePaginate проверяет размер text. Если он превышает chunkSize — сохраняет
// все чанки в store и возвращает первый чанк с заголовком-подсказкой для агента.
// Иначе возвращает text без изменений.
func (ps *pageStore) maybePaginate(text string) string {
	if len(text) <= ps.chunkSize {
		return text
	}
	chunks := splitChunks(text, ps.chunkSize)
	id := newEntryID()
	ps.mu.Lock()
	ps.entries[id] = &pageEntry{chunks: chunks, createdAt: time.Now()}
	ps.mu.Unlock()
	ps.gc()
	return fmt.Sprintf(
		"⚠️ PAGINATED RESPONSE: chunk 1/%d | continuation_id=%q\n"+
			"👉 Call codebase_read_more(continuation_id=%q, chunk=2) for next part.\n\n",
		len(chunks), id, id,
	) + chunks[0]
}

// readChunk возвращает запрошенный чанк (chunkIdx >= 2).
// Возвращает rawMCPText, чтобы sdkToolPagedResult не применял повторную пагинацию.
// Последний чанк удаляет запись из store.
func (ps *pageStore) readChunk(id string, chunkIdx int) (rawMCPText, error) {
	ps.mu.Lock()
	entry, ok := ps.entries[id]
	if !ok {
		ps.mu.Unlock()
		return "", fmt.Errorf("continuation %q not found or expired", id)
	}
	if chunkIdx < 2 || chunkIdx > len(entry.chunks) {
		ps.mu.Unlock()
		return "", fmt.Errorf("chunk %d out of range (2..%d)", chunkIdx, len(entry.chunks))
	}
	isLast := chunkIdx == len(entry.chunks)
	total := len(entry.chunks)
	chunk := entry.chunks[chunkIdx-1]
	if isLast {
		delete(ps.entries, id)
	}
	ps.mu.Unlock()

	var header string
	if isLast {
		header = fmt.Sprintf("✅ FINAL CHUNK: chunk %d/%d\n\n", chunkIdx, total)
	} else {
		header = fmt.Sprintf(
			"⚠️ PAGINATED RESPONSE: chunk %d/%d | continuation_id=%q\n"+
				"👉 Call codebase_read_more(continuation_id=%q, chunk=%d) for next part.\n\n",
			chunkIdx, total, id, id, chunkIdx+1,
		)
	}
	return rawMCPText(header + chunk), nil
}

// splitChunks нарезает text на части по size байт,
// всегда выравнивая по границе руны UTF-8 (не режет посередине многобайтовой последовательности).
func splitChunks(text string, size int) []string {
	var chunks []string
	for len(text) > size {
		split := size
		for split > 0 && !utf8.RuneStart(text[split]) {
			split--
		}
		if split == 0 {
			split = size
		}
		chunks = append(chunks, text[:split])
		text = text[split:]
	}
	if len(text) > 0 {
		chunks = append(chunks, text)
	}
	return chunks
}

// gc удаляет записи старше paginationTTL.
func (ps *pageStore) gc() {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	cutoff := time.Now().Add(-paginationTTL)
	for id, e := range ps.entries {
		if e.createdAt.Before(cutoff) {
			delete(ps.entries, id)
		}
	}
}

func newEntryID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
