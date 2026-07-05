package rti

import (
	"strings"
	"time"
)

// linkTimeWindow — допустимое окно времени между клиентским exec-вызовом и
// серверным Enter для эвристического сопоставления.
const linkTimeWindow = 2 * time.Second

// LinkClientServerCalls пытается сопоставить клиентские exec-вызовы
// (RTIClientEvent.Kind == "sql_block" с непустым SQL.ExecProcedure) с
// соответствующими серверными вызовами (RTICall) того же файла.
//
// Сопоставление — эвристика: по имени процедуры (регистронезависимо), с
// приоритетом для совпадения SPID (RTISQLBlock.SPID — реальный серверный SPID,
// в отличие от RTIClientEvent.PID, который является PID клиентского процесса)
// и ближайшим по времени EnterTime (>= event.Timestamp предпочтительно, но
// допускается любое направление в пределах linkTimeWindow, т.к. синхронизация
// часов клиента и сервера не гарантирована). Точное сопоставление не
// гарантируется — при отсутствии совпадений ServerCallID остаётся nil.
func LinkClientServerCalls(calls []*RTICall, events []*RTIClientEvent) {
	if len(calls) == 0 || len(events) == 0 {
		return
	}

	byProc := make(map[string][]*RTICall)
	for _, c := range calls {
		key := strings.ToLower(strings.TrimSpace(c.Procedure))
		if key == "" {
			continue
		}
		byProc[key] = append(byProc[key], c)
	}

	for _, ev := range events {
		if ev.Kind != "sql_block" || ev.SQL == nil || ev.SQL.ExecProcedure == "" {
			continue
		}
		candidates := byProc[strings.ToLower(strings.TrimSpace(ev.SQL.ExecProcedure))]
		if len(candidates) == 0 {
			continue
		}

		var best *RTICall
		var bestDiff time.Duration
		var bestSPIDMatch bool

		for _, c := range candidates {
			if c.EnterTime.IsZero() || ev.Timestamp.IsZero() {
				continue
			}
			diff := c.EnterTime.Sub(ev.Timestamp)
			if diff < 0 {
				diff = -diff
			}
			if diff > linkTimeWindow {
				continue
			}
			spidMatch := ev.SQL.SPID != 0 && c.SPID == ev.SQL.SPID

			if best == nil ||
				(spidMatch && !bestSPIDMatch) ||
				(spidMatch == bestSPIDMatch && diff < bestDiff) {
				best = c
				bestDiff = diff
				bestSPIDMatch = spidMatch
			}
		}

		if best != nil {
			id := best.ID
			ev.ServerCallID = &id
		}
	}
}
