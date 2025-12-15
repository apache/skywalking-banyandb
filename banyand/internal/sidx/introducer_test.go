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

package sidx

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntroductionPooling(t *testing.T) {
	t.Run("flusher introduction pooling reduces allocations", func(t *testing.T) {
		intro1 := generateFlusherIntroduction()
		releaseFlusherIntroduction(intro1)

		intro2 := generateFlusherIntroduction()
		require.NotNil(t, intro2, "pool should provide flusher introduction instance")
		require.NotNil(t, intro2.flushed, "flushed map should be initialized")
		releaseFlusherIntroduction(intro2)

		// Test multiple allocations
		var intros []*FlusherIntroduction
		for i := 0; i < 10; i++ {
			intro := generateFlusherIntroduction()
			intro.flushed[uint64(i)] = &partWrapper{}
			intros = append(intros, intro)
		}

		for _, intro := range intros {
			releaseFlusherIntroduction(intro)
		}
	})

	t.Run("merger introduction pooling reduces allocations", func(t *testing.T) {
		intro1 := generateMergerIntroduction()
		releaseMergerIntroduction(intro1)

		intro2 := generateMergerIntroduction()
		require.NotNil(t, intro2, "pool should provide merger introduction instance")
		require.NotNil(t, intro2.merged, "merged map should be initialized")
		releaseMergerIntroduction(intro2)

		// Test multiple allocations
		var intros []*MergerIntroduction
		for i := 0; i < 10; i++ {
			intro := generateMergerIntroduction()
			intro.merged[uint64(i)] = struct{}{}
			intro.newPart = &partWrapper{}
			intros = append(intros, intro)
		}

		for _, intro := range intros {
			releaseMergerIntroduction(intro)
		}
	})
}

func TestIntroductionReset(t *testing.T) {
	t.Run("flusher introduction reset for reuse", func(t *testing.T) {
		intro := generateFlusherIntroduction()

		// Set up flusher introduction with data
		intro.flushed[1] = &partWrapper{}
		intro.flushed[2] = &partWrapper{}

		// Reset the flusher introduction
		intro.reset()

		// Verify all fields are cleared
		assert.Len(t, intro.flushed, 0, "flushed map should be empty")

		releaseFlusherIntroduction(intro)
	})

	t.Run("merger introduction reset for reuse", func(t *testing.T) {
		intro := generateMergerIntroduction()

		// Set up merger introduction with data
		intro.merged[1] = struct{}{}
		intro.merged[2] = struct{}{}
		intro.newPart = &partWrapper{}

		// Reset the merger introduction
		intro.reset()

		// Verify all fields are cleared
		assert.Len(t, intro.merged, 0, "merged map should be empty")
		assert.Nil(t, intro.newPart, "newPart should be nil")

		releaseMergerIntroduction(intro)
	})
}

func TestIntroductionMapOperations(t *testing.T) {
	t.Run("flusher introduction map operations", func(t *testing.T) {
		intro := generateFlusherIntroduction()

		// Add parts to flushed map
		part1 := &partWrapper{}
		part2 := &partWrapper{}
		intro.flushed[1] = part1
		intro.flushed[2] = part2

		// Verify map contents
		assert.Equal(t, part1, intro.flushed[1], "part1 should be retrievable")
		assert.Equal(t, part2, intro.flushed[2], "part2 should be retrievable")
		assert.Len(t, intro.flushed, 2, "flushed map should have 2 entries")

		// Reset should clear the map
		intro.reset()
		assert.Len(t, intro.flushed, 0, "flushed map should be empty after reset")

		releaseFlusherIntroduction(intro)
	})

	t.Run("merger introduction map operations", func(t *testing.T) {
		intro := generateMergerIntroduction()

		// Add IDs to merged map
		intro.merged[1] = struct{}{}
		intro.merged[2] = struct{}{}
		intro.merged[3] = struct{}{}

		// Verify map contents
		_, exists1 := intro.merged[1]
		_, exists2 := intro.merged[2]
		_, exists3 := intro.merged[3]
		_, exists4 := intro.merged[4]

		assert.True(t, exists1, "ID 1 should exist in merged map")
		assert.True(t, exists2, "ID 2 should exist in merged map")
		assert.True(t, exists3, "ID 3 should exist in merged map")
		assert.False(t, exists4, "ID 4 should not exist in merged map")
		assert.Len(t, intro.merged, 3, "merged map should have 3 entries")

		// Reset should clear the map
		intro.reset()
		assert.Len(t, intro.merged, 0, "merged map should be empty after reset")

		releaseMergerIntroduction(intro)
	})
}

func TestNilSafetyForIntroductions(t *testing.T) {
	t.Run("release nil flusher introduction", func(_ *testing.T) {
		// Should not panic
		releaseFlusherIntroduction(nil)
	})

	t.Run("release nil merger introduction", func(_ *testing.T) {
		// Should not panic
		releaseMergerIntroduction(nil)
	})
}
