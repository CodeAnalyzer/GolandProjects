package mcp

import (
	"bytes"
	"context"
	"errors"
	"log"
	"strings"
	"testing"
	"time"
)

// newWarmupRegistry строит registry из map имя → хендлер (без Definition).
func newWarmupRegistry(handlers map[string]toolHandler) map[string]registeredTool {
	registry := make(map[string]registeredTool, len(handlers))
	for name, handler := range handlers {
		registry[name] = registeredTool{Handler: handler}
	}
	return registry
}

// TestWarmupForProfile_NoProbesForProfile_NoHandlerCalls — профиль без списка
// проб (review): ни одного вызова хендлера, no-op (сценарий «Профиль без
// проб — no-op»).
func TestWarmupForProfile_NoProbesForProfile_NoHandlerCalls(t *testing.T) {
	calls := 0
	registry := newWarmupRegistry(map[string]toolHandler{
		"codebase_review_sql": func(ctx context.Context, args map[string]interface{}) (interface{}, error) {
			calls++
			return nil, nil
		},
	})
	var buf bytes.Buffer
	warmupForProfile(context.Background(), "review", registry, log.New(&buf, "", 0), 60)
	if calls != 0 {
		t.Fatalf("хендлер вызван %d раз, ожидался no-op", calls)
	}
	if buf.Len() != 0 {
		t.Fatalf("no-op профиль не должен писать в лог, получено: %q", buf.String())
	}
}

// TestWarmupForProfile_ReplaysAllProbesInOrder — присутствующие в registry
// пробы выполняются в порядке списка.
func TestWarmupForProfile_ReplaysAllProbesInOrder(t *testing.T) {
	var order []string
	registry := newWarmupRegistry(map[string]toolHandler{
		"codebase_query_desc_search": func(ctx context.Context, args map[string]interface{}) (interface{}, error) {
			order = append(order, "desc")
			return nil, nil
		},
		"codebase_query_callers": func(ctx context.Context, args map[string]interface{}) (interface{}, error) {
			order = append(order, "callers")
			return nil, nil
		},
		"codebase_query_sql_fragment": func(ctx context.Context, args map[string]interface{}) (interface{}, error) {
			order = append(order, "fragment")
			return nil, nil
		},
	})
	var buf bytes.Buffer
	warmupForProfile(context.Background(), "query", registry, log.New(&buf, "", 0), 0)

	want := []string{"desc", "callers", "fragment"}
	if len(order) != len(want) {
		t.Fatalf("выполнено %d проб, ожидалось %d", len(order), len(want))
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("порядок проб %v, ожидался %v", order, want)
		}
	}
}

// TestWarmupForProfile_HandlerErrorsDoNotStopBatch — ошибка пробы не прерывает
// пачку: остальные пробы выполняются, счётчик errors растёт (сценарий «Ошибка
// пробы не ломает сервер»).
func TestWarmupForProfile_HandlerErrorsDoNotStopBatch(t *testing.T) {
	calls := 0
	registry := newWarmupRegistry(map[string]toolHandler{
		"codebase_query_desc_search": func(ctx context.Context, args map[string]interface{}) (interface{}, error) {
			calls++
			return nil, errors.New("boom")
		},
		"codebase_query_callers": func(ctx context.Context, args map[string]interface{}) (interface{}, error) {
			calls++
			return nil, nil
		},
	})
	var buf bytes.Buffer
	warmupForProfile(context.Background(), "query", registry, log.New(&buf, "", 0), 0)
	if calls != 2 {
		t.Fatalf("выполнено %d проб из 2: ошибка не должна прерывать пачку", calls)
	}
	if !strings.Contains(buf.String(), "errors=1") {
		t.Fatalf("в логе ожидался errors=1, получено: %q", buf.String())
	}
}

// TestWarmupForProfile_BatchTimeoutStopsRemainingProbes — общий таймаут пачки:
// после истечения таймаута последующие пробы видят отменённый контекст и
// завершаются ошибкой; итоговая лог-строка фиксирует завершение (сценарий
// «Общий таймаут пачки»).
func TestWarmupForProfile_BatchTimeoutStopsRemainingProbes(t *testing.T) {
	completed := 0
	ctxErrs := 0
	registry := newWarmupRegistry(map[string]toolHandler{
		"codebase_query_desc_search": func(ctx context.Context, args map[string]interface{}) (interface{}, error) {
			select {
			case <-ctx.Done():
				ctxErrs++
				return nil, ctx.Err()
			case <-time.After(1200 * time.Millisecond):
				completed++
				return nil, nil
			}
		},
		"codebase_query_callers": func(ctx context.Context, args map[string]interface{}) (interface{}, error) {
			select {
			case <-ctx.Done():
				ctxErrs++
				return nil, ctx.Err()
			case <-time.After(1200 * time.Millisecond):
				completed++
				return nil, nil
			}
		},
	})
	var buf bytes.Buffer
	warmupForProfile(context.Background(), "query", registry, log.New(&buf, "", 0), 0)
	if completed != 2 || ctxErrs != 0 {
		t.Fatalf("без таймаута ожидались 2 успешные пробы, получено completed=%d ctxErrs=%d", completed, ctxErrs)
	}

	completed, ctxErrs = 0, 0
	buf.Reset()
	// Таймаут 1 с: первая проба спит 1.2 с — контекст истекает во время неё,
	// обе пробы видят отмену.
	warmupForProfile(context.Background(), "query", registry, log.New(&buf, "", 0), 1)
	if ctxErrs == 0 {
		t.Fatalf("ожидалась отмена контекста по таймауту пачки, получено completed=%d ctxErrs=%d", completed, ctxErrs)
	}
	if !strings.Contains(buf.String(), "warmup probes=9") {
		t.Fatalf("в логе ожидается итоговая warmup-строка, получено: %q", buf.String())
	}
}

// TestWarmupForProfile_EmptyProfileUsesQueryProbes — пустой профиль (все
// инструменты, включая полный набор query) прогревается тем же списком проб,
// что и профиль query; в логе обозначается как "all".
func TestWarmupForProfile_EmptyProfileUsesQueryProbes(t *testing.T) {
	calls := 0
	registry := newWarmupRegistry(map[string]toolHandler{
		"codebase_query_desc_search": func(ctx context.Context, args map[string]interface{}) (interface{}, error) {
			calls++
			return nil, nil
		},
		"codebase_query_callers": func(ctx context.Context, args map[string]interface{}) (interface{}, error) {
			calls++
			return nil, nil
		},
	})
	var buf bytes.Buffer
	warmupForProfile(context.Background(), "", registry, log.New(&buf, "", 0), 0)
	if calls != 2 {
		t.Fatalf("пустой профиль: ожидалось 2 вызова (список query), получено %d", calls)
	}
	if !strings.Contains(buf.String(), "profile=all") {
		t.Fatalf("в логе ожидался profile=all, получено: %q", buf.String())
	}
}

// TestWarmupForProfile_NilLoggerIsNoOp — logger == nil: проба не запускается
// (сценарий «Отключённое логирование — probe не запускается»).
func TestWarmupForProfile_NilLoggerIsNoOp(t *testing.T) {
	calls := 0
	registry := newWarmupRegistry(map[string]toolHandler{
		"codebase_query_desc_search": func(ctx context.Context, args map[string]interface{}) (interface{}, error) {
			calls++
			return nil, nil
		},
	})
	warmupForProfile(context.Background(), "query", registry, nil, 60)
	if calls != 0 {
		t.Fatalf("при logger=nil хендлер не должен вызываться, вызвано %d раз", calls)
	}
}

// TestWarmupForProfile_MissingToolSkipped — отсутствующий в registry
// инструмент пропускается без ошибки.
func TestWarmupForProfile_MissingToolSkipped(t *testing.T) {
	calls := 0
	registry := newWarmupRegistry(map[string]toolHandler{
		"codebase_query_callers": func(ctx context.Context, args map[string]interface{}) (interface{}, error) {
			calls++
			return nil, nil
		},
	})
	var buf bytes.Buffer
	warmupForProfile(context.Background(), "query", registry, log.New(&buf, "", 0), 0)
	// callers — единственная проба в registry, остальные 8 пропущены.
	if calls != 1 {
		t.Fatalf("ожидался 1 вызов (только присутствующий инструмент), получено %d", calls)
	}
}
