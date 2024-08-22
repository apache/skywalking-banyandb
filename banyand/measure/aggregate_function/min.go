package aggregate_function

import "fmt"

type Min[T MAFInput] struct {
	value T
}

func (m *Min[T]) Combine(arguments [][]T) error {
	if len(arguments) != 1 {
		return fmt.Errorf("needs one argument")
	}
	for _, arg := range arguments[0] {
		if m.value > arg {
			m.value = arg
		}
	}
	return nil
}

func (m *Min[T]) Result() T {
	return m.value
}
