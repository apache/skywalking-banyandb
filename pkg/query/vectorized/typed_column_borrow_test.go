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

package vectorized

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBorrowedColumnIsAViewOfTheSource(t *testing.T) {
	src := []int64{10, 20, 30, 40}
	c := NewInt64Column(8)
	c.Borrow(src)
	require.True(t, c.Borrowed())
	require.Equal(t, 4, c.Len())
	require.Equal(t, src, c.Data())
	// Zero copy: the column reads the source's array.
	require.Same(t, &src[0], &c.Data()[0])
	require.False(t, c.IsNull(2))
}

func TestBorrowedColumnAppendReallocatesAndNeverWritesTheSource(t *testing.T) {
	// The source has spare capacity behind its length: an append that trusted
	// the capacity would write into it.
	backing := []int64{1, 2, 3, 4, 5, 6, 7, 8}
	src := backing[:4]
	c := NewInt64Column(0)
	c.Borrow(src)
	c.Append(99)
	c.AppendSlice([]int64{98, 97})
	c.AppendNull()
	require.False(t, c.Borrowed(), "a mutated column owns its data")
	require.Equal(t, []int64{1, 2, 3, 4, 99, 98, 97, 0}, c.Data())
	require.True(t, c.IsNull(7))
	require.Equal(t, []int64{1, 2, 3, 4, 5, 6, 7, 8}, backing, "the source must be untouched")
}

func TestBorrowedColumnSetAtCopiesOnWrite(t *testing.T) {
	src := []float64{1.5, 2.5, 3.5}
	c := NewFloat64Column(0)
	c.Borrow(src)
	c.SetAt(1, 9)
	require.Equal(t, []float64{1.5, 9, 3.5}, c.Data())
	require.Equal(t, []float64{1.5, 2.5, 3.5}, src, "SetAt on a borrowed column must not write the source")
	require.False(t, c.Borrowed())
}

func TestBorrowedColumnResetDropsTheSourceAndKeepsOwnCapacity(t *testing.T) {
	backing := make([]int64, 8)
	for i := range backing {
		backing[i] = int64(i + 1)
	}
	c := NewInt64Column(64)
	own := c.Data()[:1]
	ownArray := &own[0]
	c.Borrow(backing[:4])
	c.Reset()
	require.False(t, c.Borrowed())
	require.Equal(t, 0, c.Len())
	require.GreaterOrEqual(t, cap(c.Data()), 64, "the column returns to its own array")
	// The next append must go into the column's own array, not the borrowed one.
	c.Append(-1)
	require.Same(t, ownArray, &c.Data()[0])
	require.Equal(t, []int64{1, 2, 3, 4, 5, 6, 7, 8}, backing, "Reset then Append must not write the source")
}

func TestBorrowedColumnReleasedToThePoolNeverWritesTheSource(t *testing.T) {
	backing := []int64{7, 7, 7, 7, 7, 7}
	for range 3 {
		c := AcquireColumn(ColumnTypeInt64, 4).(*TypedColumn[int64])
		c.Borrow(backing[:3])
		ReleaseColumn(c)
		// Whoever acquires the column next appends into it.
		d := AcquireColumn(ColumnTypeInt64, 4).(*TypedColumn[int64])
		require.False(t, d.Borrowed())
		require.Equal(t, 0, d.Len())
		for i := range 6 {
			d.Append(int64(100 + i))
		}
		ReleaseColumn(d)
	}
	require.Equal(t, []int64{7, 7, 7, 7, 7, 7}, backing)
}

func TestBorrowedColumnMarkNullDoesNotTouchData(t *testing.T) {
	src := []int64{1, 2, 3}
	c := NewInt64Column(0)
	c.Borrow(src)
	c.MarkNullAt(1)
	require.True(t, c.IsNull(1))
	require.True(t, c.Borrowed(), "a validity change does not write element memory")
	require.Equal(t, []int64{1, 2, 3}, src)
}

func TestBorrowTwiceKeepsTheOwnedArray(t *testing.T) {
	c := NewInt64Column(32)
	c.Borrow([]int64{1})
	c.Borrow([]int64{2, 3})
	require.Equal(t, []int64{2, 3}, c.Data())
	c.Reset()
	require.GreaterOrEqual(t, cap(c.Data()), 32)
}
