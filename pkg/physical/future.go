package physical

import (
	"github.com/hashicorp/go-multierror"
)

var _ Future = (Futures)(nil)

type Futures []Future

func (f Futures) Append(futures ...Future) Futures {
	return append(f, futures...)
}

func (f Futures) IsComplete() bool {
	for _, single := range f {
		if !single.IsComplete() {
			return false
		}
	}
	return true
}

func (f Futures) Value() Result {
	var globalErr error
	var dg DataGroup
	for _, single := range f {
		result := single.Value()
		if result.Error() != nil {
			globalErr = multierror.Append(globalErr, result.Error())
		}
		var err error
		dg, err = dg.Append(result.Value())
		if err != nil {
			globalErr = multierror.Append(globalErr, err)
		}
	}
	if globalErr != nil {
		return Failure(globalErr)
	}
	return Success(dg)
}

type Result interface {
	Value() Data
	Error() error
}

var _ Result = (*success)(nil)

type success struct {
	data Data
}

func (s *success) Value() Data {
	return s.data
}

func (s *success) Error() error {
	return nil
}

func Success(data Data) Result {
	return &success{data: data}
}

var _ Result = (*failure)(nil)

type failure struct {
	err error
}

func (f *failure) Value() Data {
	return nil
}

func (f *failure) Error() error {
	return f.err
}

func Failure(err error) Result {
	return &failure{err: err}
}

type Future interface {
	IsComplete() bool
	Value() Result
}

var _ Future = (*future)(nil)

type future struct {
	f func() Result
	r Result
}

func NewFuture(fun func() Result) Future {
	f := &future{
		f: fun,
		r: nil,
	}
	go func() {
		f.r = fun()
	}()
	return f
}

func (f *future) IsComplete() bool {
	return f.r != nil
}

func (f *future) Value() Result {
	return f.r
}
