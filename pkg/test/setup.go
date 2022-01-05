// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package test

import (
	"context"
	"fmt"

	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
)

type StartFunc func() error
type StopFunc func()

type Flow interface {
	// PushErrorHandler only pushes a stopFunc
	PushErrorHandler(StopFunc) Flow

	// Run calls the startFunc and expects no error returned.
	// If a non-nil error is returned or panic, shutdown must be called at once.
	// The error will be stored and thus could be checked by the caller later.
	Run(context.Context, StartFunc, StopFunc) Flow

	// RunWithoutSideEffect calls the startFunc and does not expect any side effects from it.
	// If a non-nil error is returned or panic, shutdown must be called at once.
	// The error will be stored and thus could be checked by the caller later.
	RunWithoutSideEffect(context.Context, StartFunc) Flow

	// Shutdown does not actually shutdown the flow,
	// but only returns a function containing all stopFunc(s) to be executed.
	// The timing to do the real shutdown can be determined by users.
	Shutdown() StopFunc

	ErrorOrNil() error
}

type testFlow struct {
	err       *multierror.Error
	stopFuncs []StopFunc
}

// NewTestFlow creates a flow ready to prepare services/components to be used for testing.
func NewTestFlow() Flow {
	return &testFlow{
		err:       &multierror.Error{},
		stopFuncs: make([]StopFunc, 0),
	}
}

// Shutdown does not actually call the shutdown functions but only return a composition of all
// stop functions given by the user thus it allows the user to determine the timing.
func (tf *testFlow) Shutdown() StopFunc {
	return func() {
		for idx := len(tf.stopFuncs) - 1; idx >= 0; idx-- {
			tf.stopFuncs[idx]()
		}
	}
}

func (tf *testFlow) RunWithoutSideEffect(ctx context.Context, startFunc StartFunc) Flow {
	return tf.Run(ctx, startFunc, nil)
}

func (tf *testFlow) Run(ctx context.Context, startFunc StartFunc, stopFunc StopFunc) Flow {
	if tf.err.ErrorOrNil() != nil {
		return tf
	}

	if stopFunc != nil {
		tf.stopFuncs = append(tf.stopFuncs, stopFunc)
	}

	errCh := make(chan error)
	defer func() {
		close(errCh)
	}()

	donec := make(chan struct{})
	go func() {
		defer func() {
			if r := recover(); r != nil {
				fmt.Println("====== recover ======")
				errCh <- errors.Errorf("panic found %v", r)
			}
		}()
		fmt.Println("======== start run in loop =======")
		err := startFunc()
		fmt.Println("======== end run in loop =======")

		if err != nil {
			errCh <- err
		}
		close(donec)
	}()
	select {
	case <-donec:
	case err := <-errCh:
		tf.err = multierror.Append(tf.err, err)
		tf.Shutdown()()
		return tf
	}

	return tf
}

func (tf *testFlow) PushErrorHandler(stopFunc StopFunc) Flow {
	tf.stopFuncs = append(tf.stopFuncs, stopFunc)
	return tf
}

func (tf *testFlow) ErrorOrNil() error {
	return tf.err.ErrorOrNil()
}
