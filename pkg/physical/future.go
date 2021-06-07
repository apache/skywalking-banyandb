package physical

import (
	"fmt"
	"sync"

	"github.com/hashicorp/go-multierror"
)

var _ Future = (Futures)(nil)

type Futures []Future

func (f Futures) Await() Result {
	for _, single := range f {
		_ = single.Await()
	}
	return f.Value()
}

func (f Futures) Then(cb Callback) Future {
	return NewFuture(func() Result {
		result := f.Await()
		callbackResp, err := cb(result)
		if err != nil {
			return Failure(err)
		}
		return Success(callbackResp)
	})
}

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

type Callback func(Result) (Data, error)

type Awaitable interface {
	Await() Result
}

type Future interface {
	Awaitable
	IsComplete() bool
	Value() Result
	Then(Callback) Future
}

var _ Future = (*future)(nil)

type future struct {
	f     func() Result
	r     Result
	cbs   []Callback
	wg    sync.WaitGroup
	mutex sync.Mutex
}

func (f *future) Await() Result {
	f.wg.Wait()
	return f.r
}

func (f *future) Then(cb Callback) Future {
	return NewFuture(func() Result {
		result := f.Await()
		callbackResp, err := cb(result)
		if err != nil {
			return Failure(err)
		}
		return Success(callbackResp)
	})
}

func NewFuture(fun func() Result) Future {
	f := &future{
		f:  fun,
		r:  nil,
		wg: sync.WaitGroup{},
	}
	f.wg.Add(1)
	go func() {
		defer f.handlePanic()
		f.resolve(fun())
	}()
	return f
}

func (f *future) IsComplete() bool {
	return f.r != nil
}

func (f *future) Value() Result {
	return f.r
}

func (f *future) resolve(r Result) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	if f.r != nil {
		return
	}

	f.r = r
	f.wg.Done()
}

func (f *future) handlePanic() {
	e := recover()
	if e != nil {
		switch err := e.(type) {
		case nil:
			f.resolve(Failure(fmt.Errorf("panic recovery with nil error")))
		case error:
			f.resolve(Failure(fmt.Errorf("panic recovery with error: %s", err.Error())))
		default:
			f.resolve(Failure(fmt.Errorf("panic recovery with unknown error: %s", fmt.Sprint(err))))
		}
	}
}
