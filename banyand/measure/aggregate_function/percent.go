package aggregate_function

import "fmt"

type Percent[T MAFInput] struct {
	match T // todo 这里本来应该写 T。这种混写 T 与 int64 的方式不够优雅？但在文档里确实只接收 int64。
	total T
}

func (m *Percent[T]) Combine(arguments [][]T) error {
	if len(arguments) != 2 {
		return fmt.Errorf("needs two argument")
	}
	for _, arg := range arguments[0] {
		m.match += arg
	}
	for _, arg := range arguments[1] {
		m.total += arg
	}
	return nil
}

func (m *Percent[T]) Result() T {
	switch any(m.match).(type) {
	/*	case *int64:
		return T(m.match*100) / T(m.total)
	*/
	}
	return zeroValue[T]()
}
