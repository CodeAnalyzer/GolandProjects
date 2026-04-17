package main

type finiteStateMachine struct {
	delta   [][]int
	pattern string
	length  int
}

func newFiniteStateMachine(pattern string) *finiteStateMachine {
	fsm := &finiteStateMachine{
		pattern: pattern,
		length:  len(pattern), // Работаем с байтами
	}
	fsm.delta = fsm.createDelta()
	return fsm
}

func (fsm *finiteStateMachine) createDelta() [][]int {
	// Для простоты работаем с байтами строки
	length := fsm.length
	delta := make([][]int, length+1)
	for i := range delta {
		delta[i] = make([]int, 256)
	}

	// Заполняем таблицу переходов для байтового представления
	for state := 0; state <= length; state++ {
		for char := 0; char < 256; char++ {
			if state < length && char == int(fsm.pattern[state]) {
				delta[state][char] = state + 1
			} else {
				// Находим максимальный префикс, который является суффиксом
				k := state
				for k > 0 {
					if k < length && char == int(fsm.pattern[k]) {
						break
					}
					k--
					if k == 0 {
						break
					}
				}
				delta[state][char] = k
			}
		}
	}
	return delta
}

func (fsm *finiteStateMachine) search(text string) []int {
	var results []int
	var state int
	var deltaLen = len(fsm.delta)

	state = 0
	for pos, char := range []byte(text) {
		if state < deltaLen {
			state = fsm.delta[state][char]
		}

		if state == fsm.length {
			results = append(results, pos-fsm.length+1)
			state = fsm.delta[state-1][char] // Продолжаем поиск
		}
	}

	return results
}
