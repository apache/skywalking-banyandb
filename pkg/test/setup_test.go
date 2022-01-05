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
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_Setup_Single_Error(t *testing.T) {
	require := require.New(t)
	wg := sync.WaitGroup{}
	flow := NewTestFlow().
		Run(context.TODO(), func() error {
			wg.Add(1)
			return errors.New("normal error")
		}, func() {
			wg.Done()
		})

	wg.Wait()

	require.Error(flow.Error())
}

func Test_Setup_Multiple_ErrorHandlers(t *testing.T) {
	require := require.New(t)
	wg := sync.WaitGroup{}
	flow := NewTestFlow().
		Run(context.TODO(), func() error {
			wg.Add(1)
			return nil
		}, func() {
			wg.Done()
		}).
		Run(context.TODO(), func() error {
			wg.Add(1)
			return errors.New("normal error")
		}, func() {
			wg.Done()
		})

	wg.Wait()

	require.Error(flow.Error())
}

func Test_Setup_Panic(t *testing.T) {
	require := require.New(t)
	wg := sync.WaitGroup{}
	flow := NewTestFlow().
		Run(context.TODO(), func() error {
			wg.Add(1)
			panic("oops...")
		}, func() {
			wg.Done()
		})

	wg.Wait()

	require.Error(flow.Error())
}

func Test_Setup_Shutdown(t *testing.T) {
	require := require.New(t)
	wg := sync.WaitGroup{}
	flow := NewTestFlow().
		Run(context.TODO(), func() error {
			wg.Add(1)
			return nil
		}, func() {
			wg.Done()
		}).
		Run(context.TODO(), func() error {
			wg.Add(1)
			return nil
		}, func() {
			wg.Done()
		})

	go func() {
		flow.Shutdown()()
	}()

	wg.Wait()

	require.NoError(flow.Error())
}
