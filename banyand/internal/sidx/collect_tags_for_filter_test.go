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

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
)

// testStrDecoder decodes stored string bytes into model TagValue (same shape as query path).
func testStrDecoder(valueType pbv1.ValueType, value []byte, _ [][]byte) *modelv1.TagValue {
	if valueType != pbv1.ValueTypeStr {
		return nil
	}
	return &modelv1.TagValue{
		Value: &modelv1.TagValue_Str{
			Str: &modelv1.Str{Value: string(value)},
		},
	}
}

func tagKeysFromSlice(tags []*modelv1.Tag) []string {
	keys := make([]string, len(tags))
	for idx, tag := range tags {
		keys[idx] = tag.Key
	}
	return keys
}

func newStrTagData(value string) *tagData {
	return &tagData{
		valueType: pbv1.ValueTypeStr,
		values: []tagRow{
			{value: []byte(value)},
		},
	}
}

// TestCollectTagsForFilter_DeterministicOrder verifies tag ordering for TagFilter.Match:
// logical matchers align tag values with schema by slice index, so iteration order must be stable.
// Map iteration order is undefined; without this contract, filtering can flake (0 vs 1 rows).
func TestCollectTagsForFilter_DeterministicOrder(t *testing.T) {
	t.Run("no projection sorts tag names lexicographically", func(t *testing.T) {
		blk := &block{
			userKeys: []int64{1},
			data:     [][]byte{{}},
			tags:     make(map[string]*tagData),
		}
		// Deliberately insert keys in non-lexicographic order.
		blk.tags["zebra"] = newStrTagData("z")
		blk.tags["middle"] = newStrTagData("m")
		blk.tags["alpha"] = newStrTagData("a")

		b := &blockCursorBuilder{block: blk}
		var buf []*modelv1.Tag
		buf = b.collectTagsForFilter(buf, testStrDecoder, 0, nil)
		require.Len(t, buf, 3)
		assert.Equal(t, []string{"alpha", "middle", "zebra"}, tagKeysFromSlice(buf))
	})

	t.Run("tag projection defines order", func(t *testing.T) {
		blk := &block{
			userKeys: []int64{1},
			data:     [][]byte{{}},
			tags:     make(map[string]*tagData),
		}
		blk.tags["zebra"] = newStrTagData("z")
		blk.tags["middle"] = newStrTagData("m")
		blk.tags["alpha"] = newStrTagData("a")

		b := &blockCursorBuilder{block: blk}
		projections := []model.TagProjection{
			{Family: "span", Names: []string{"zebra", "alpha", "middle"}},
		}
		var buf []*modelv1.Tag
		buf = b.collectTagsForFilter(buf, testStrDecoder, 0, projections)
		require.Len(t, buf, 3)
		assert.Equal(t, []string{"zebra", "alpha", "middle"}, tagKeysFromSlice(buf))
	})

	t.Run("duplicate tag names across projection families are deduplicated", func(t *testing.T) {
		blk := &block{
			userKeys: []int64{1},
			data:     [][]byte{{}},
			tags:     make(map[string]*tagData),
		}
		blk.tags["zebra"] = newStrTagData("z")
		blk.tags["middle"] = newStrTagData("m")
		blk.tags["alpha"] = newStrTagData("a")

		b := &blockCursorBuilder{block: blk}
		projections := []model.TagProjection{
			{Family: "span", Names: []string{"zebra", "alpha"}},
			{Family: "service", Names: []string{"alpha", "middle", "zebra"}},
		}
		var buf []*modelv1.Tag
		buf = b.collectTagsForFilter(buf, testStrDecoder, 0, projections)
		require.Len(t, buf, 3)
		assert.Equal(t, []string{"zebra", "alpha", "middle"}, tagKeysFromSlice(buf))
	})
}
