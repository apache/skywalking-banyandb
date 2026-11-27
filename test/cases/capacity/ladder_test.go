// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package capacity_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/test/cases/capacity"
)

// fakeNode serves any limit up to capacity and refuses the larger ones, as a node with a fixed query memory does.
func fakeNode(capacityLimit, total int) func(limit int) (capacity.Outcome, error) {
	return func(limit int) (capacity.Outcome, error) {
		served := min(limit, total)
		if served > capacityLimit {
			return capacity.Outcome{}, fmt.Errorf("node: %w", capacity.ErrExhausted)
		}
		return capacity.Outcome{Returned: served, Bytes: served * 10}, nil
	}
}

func TestFindLocatesTheLargestServedLimit(t *testing.T) {
	for _, capacityLimit := range []int{1, 2, 3, 7, 8, 9, 100, 777, 999} {
		result, err := capacity.Find(1000, fakeNode(capacityLimit, 1000))
		require.NoError(t, err, "capacity %d", capacityLimit)
		require.False(t, result.Saturated)
		require.Equal(t, capacityLimit, result.MaxLimit, "capacity %d", capacityLimit)
		require.Equal(t, capacityLimit+1, result.FirstFail)
		require.Equal(t, capacityLimit, result.Max.Returned)
		// Doubling and bisecting stay logarithmic in the capacity, however large the data set is.
		require.LessOrEqual(t, len(result.Rungs), 24)
	}
}

func TestFindReportsASaturatedLadder(t *testing.T) {
	result, err := capacity.Find(50, fakeNode(1000, 50))
	require.NoError(t, err)
	require.True(t, result.Saturated, "the whole data set was served, so there is no ceiling to report")
	require.Equal(t, 50, result.MaxLimit)
	require.Zero(t, result.FirstFail)
}

func TestFindStopsOnAnErrorThatIsNotAnExhaustedBudget(t *testing.T) {
	boom := errors.New("connection reset")
	result, err := capacity.Find(1000, func(limit int) (capacity.Outcome, error) {
		if limit > 4 {
			return capacity.Outcome{}, boom
		}
		return capacity.Outcome{Returned: limit}, nil
	})
	require.ErrorIs(t, err, boom)
	require.Equal(t, 4, result.MaxLimit)
}

func TestFindRejectsAnEmptyDataSet(t *testing.T) {
	_, err := capacity.Find(0, fakeNode(1, 0))
	require.Error(t, err)
}

// The meter reports the memory a function holds: a function that keeps 64 MiB shows at least that much, and one that
// allocates nothing shows next to none.
func TestMeasureHeapSeesWhatAFunctionHolds(t *testing.T) {
	var keep [][]byte
	held := capacity.MeasureHeap(func() {
		for i := 0; i < 64; i++ {
			chunk := make([]byte, 1<<20)
			for j := range chunk {
				chunk[j] = byte(j)
			}
			keep = append(keep, chunk)
		}
		time.Sleep(20 * time.Millisecond)
	})
	require.GreaterOrEqual(t, held, uint64(60<<20))
	require.Less(t, held, uint64(120<<20))
	keep = nil
	idle := capacity.MeasureHeap(func() { time.Sleep(20 * time.Millisecond) })
	require.Less(t, idle, uint64(4<<20))
	_ = keep
}
