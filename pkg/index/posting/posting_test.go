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

package posting_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/index/posting"
	postingroaring "github.com/apache/skywalking-banyandb/pkg/index/posting/roaring"
)

func TestUnion(t *testing.T) {
	t.Run("empty left operand does not mutate DummyPostingList", func(t *testing.T) {
		b := postingroaring.NewPostingList()
		b.Insert(10)
		b.Insert(20)

		res, err := posting.Union(postingroaring.DummyPostingList, b)
		require.NoError(t, err)
		assert.Equal(t, 2, res.Len())
		assert.True(t, res.Contains(10))
		assert.True(t, res.Contains(20))

		// Regression assertion: global singleton must remain empty
		assert.True(t, postingroaring.DummyPostingList.IsEmpty())
		assert.Equal(t, 0, postingroaring.DummyPostingList.Len())
	})

	t.Run("empty right operand does not mutate DummyPostingList", func(t *testing.T) {
		a := postingroaring.NewPostingList()
		a.Insert(10)

		res, err := posting.Union(a, postingroaring.DummyPostingList)
		require.NoError(t, err)
		assert.Equal(t, 1, res.Len())
		assert.True(t, res.Contains(10))

		assert.True(t, postingroaring.DummyPostingList.IsEmpty())
		assert.Equal(t, 0, postingroaring.DummyPostingList.Len())
	})

	t.Run("non-empty operands produce correct union", func(t *testing.T) {
		a := postingroaring.NewPostingList()
		a.Insert(1)
		a.Insert(2)

		b := postingroaring.NewPostingList()
		b.Insert(2)
		b.Insert(3)

		res, err := posting.Union(a, b)
		require.NoError(t, err)
		assert.Equal(t, 3, res.Len())
		assert.True(t, res.Contains(1))
		assert.True(t, res.Contains(2))
		assert.True(t, res.Contains(3))
	})
}

func TestIntersect(t *testing.T) {
	t.Run("empty left operand does not mutate DummyPostingList", func(t *testing.T) {
		b := postingroaring.NewPostingList()
		b.Insert(10)

		res, err := posting.Intersect(postingroaring.DummyPostingList, b)
		require.NoError(t, err)
		assert.True(t, res.IsEmpty())

		// Regression assertion: global singleton must remain empty
		assert.True(t, postingroaring.DummyPostingList.IsEmpty())
		assert.Equal(t, 0, postingroaring.DummyPostingList.Len())
	})

	t.Run("empty right operand does not mutate DummyPostingList", func(t *testing.T) {
		a := postingroaring.NewPostingList()
		a.Insert(10)

		res, err := posting.Intersect(a, postingroaring.DummyPostingList)
		require.NoError(t, err)
		assert.True(t, res.IsEmpty())

		assert.True(t, postingroaring.DummyPostingList.IsEmpty())
		assert.Equal(t, 0, postingroaring.DummyPostingList.Len())
	})

	t.Run("non-empty operands produce correct intersection", func(t *testing.T) {
		a := postingroaring.NewPostingList()
		a.Insert(1)
		a.Insert(2)

		b := postingroaring.NewPostingList()
		b.Insert(2)
		b.Insert(3)

		res, err := posting.Intersect(a, b)
		require.NoError(t, err)
		assert.Equal(t, 1, res.Len())
		assert.True(t, res.Contains(2))
		assert.False(t, res.Contains(1))
		assert.False(t, res.Contains(3))
	})
}

func TestDifference(t *testing.T) {
	t.Run("empty left operand does not mutate DummyPostingList", func(t *testing.T) {
		b := postingroaring.NewPostingList()
		b.Insert(10)

		res, err := posting.Difference(postingroaring.DummyPostingList, b)
		require.NoError(t, err)
		assert.True(t, res.IsEmpty())

		// Regression assertion: global singleton must remain empty
		assert.True(t, postingroaring.DummyPostingList.IsEmpty())
		assert.Equal(t, 0, postingroaring.DummyPostingList.Len())
	})

	t.Run("empty right operand does not mutate DummyPostingList", func(t *testing.T) {
		a := postingroaring.NewPostingList()
		a.Insert(10)

		res, err := posting.Difference(a, postingroaring.DummyPostingList)
		require.NoError(t, err)
		assert.Equal(t, 1, res.Len())
		assert.True(t, res.Contains(10))

		assert.True(t, postingroaring.DummyPostingList.IsEmpty())
		assert.Equal(t, 0, postingroaring.DummyPostingList.Len())
	})

	t.Run("non-empty operands produce correct difference", func(t *testing.T) {
		a := postingroaring.NewPostingList()
		a.Insert(1)
		a.Insert(2)
		a.Insert(3)

		b := postingroaring.NewPostingList()
		b.Insert(2)

		res, err := posting.Difference(a, b)
		require.NoError(t, err)
		assert.Equal(t, 2, res.Len())
		assert.True(t, res.Contains(1))
		assert.True(t, res.Contains(3))
		assert.False(t, res.Contains(2))
	})
}
