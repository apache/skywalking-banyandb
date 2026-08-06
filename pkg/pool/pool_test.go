// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain a
// copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package pool

import (
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

var testPoolID atomic.Uint64

func uniqueTestPoolName(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("%s-%d", t.Name(), testPoolID.Add(1))
}

func TestSyncedDiscardBalancesCheckoutWithoutPooling(t *testing.T) {
	testPool := Register[*int](uniqueTestPoolName(t))
	value := testPool.Get()
	require.Nil(t, value)
	require.Equal(t, 1, testPool.RefsCount())
	testPool.Put(new(int))
	require.Zero(t, testPool.RefsCount())

	reused := testPool.Get()
	require.NotNil(t, reused)
	testPool.Discard(reused)

	require.Zero(t, testPool.RefsCount())
	require.Nil(t, testPool.Get(), "discarded objects must not be returned by the pool")
	testPool.Discard(new(int))
	require.Zero(t, testPool.RefsCount())
}

func TestBoundedEnforcesAggregateSizeAndReusesDeterministically(t *testing.T) {
	testPool := RegisterBounded(uniqueTestPoolName(t), 8, func() *[]byte {
		value := make([]byte, 0)
		return &value
	}, func(value *[]byte) int64 {
		return int64(cap(*value))
	})
	first := testPool.Get()
	*first = make([]byte, 0, 6)
	second := testPool.Get()
	*second = make([]byte, 0, 6)

	require.True(t, testPool.Put(first))
	require.False(t, testPool.Put(second))
	require.Equal(t, int64(6), testPool.RetainedSize())
	require.Zero(t, testPool.RefsCount())
	runtime.GC()

	reused := testPool.Get()
	require.Same(t, first, reused)
	require.Zero(t, testPool.RetainedSize())
	testPool.Discard(reused)
	require.Zero(t, testPool.RefsCount())
}
