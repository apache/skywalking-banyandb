// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package dump

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSnapshotIteratorSeekAfterDecodesOnlyCursorBlock(t *testing.T) {
	var decoded []int
	iterator := NewIterator(100, func(index int) ([]int, error) {
		decoded = append(decoded, index)
		return []int{index * 2, index*2 + 1}, nil
	})
	require.NoError(t, iterator.SeekAfter(Position{BlockIdx: 80, RowIdx: 0}))
	require.Equal(t, []int{80}, decoded)
	require.True(t, iterator.Next())
	require.Equal(t, 161, iterator.Row())
	require.True(t, iterator.Next())
	require.Equal(t, 162, iterator.Row())
	require.Equal(t, []int{80, 81}, decoded)
	require.Error(t, iterator.SeekAfter(Position{BlockIdx: 80, RowIdx: 5}))
}
