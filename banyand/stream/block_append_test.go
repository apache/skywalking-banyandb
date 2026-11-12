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

package stream

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

// appendTestTag is a one-row string tag holding its own name.
func appendTestTag(name string) tag {
	return tag{name: name, valueType: pbv1.ValueTypeStr, values: [][]byte{[]byte(name)}}
}

// TestFullTagAppendKeepsExistingTagsWhenTheLayoutGrows pins that appending a block whose layout adds a tag or a
// tag family keeps every tag at one value per row; growing the destination slices used to strand the values
// appended through pointers taken before the growth.
func TestFullTagAppendKeepsExistingTagsWhenTheLayoutGrows(t *testing.T) {
	requireRows := func(t *testing.T, bi *blockPointer, rows int) {
		t.Helper()
		for _, tf := range bi.tagFamilies {
			for _, tg := range tf.tags {
				require.Lenf(t, tg.values, rows, "tag %s.%s", tf.name, tg.name)
			}
		}
	}
	t.Run("a new tag before an existing one", func(t *testing.T) {
		bi := &blockPointer{}
		bi.timestamps = []int64{1}
		// A clipped slice is full, so adding a tag reallocates it.
		bi.tagFamilies = []tagFamily{{name: "default", tags: slices.Clip([]tag{appendTestTag("a"), appendTestTag("c")})}}
		b := &blockPointer{}
		b.timestamps = []int64{2}
		b.tagFamilies = []tagFamily{{name: "default", tags: []tag{appendTestTag("a"), appendTestTag("b"), appendTestTag("c")}}}
		fullTagAppend(bi, b, 1)
		requireRows(t, bi, 2)
	})
	t.Run("a new tag family before an existing family that gains a tag", func(t *testing.T) {
		bi := &blockPointer{}
		bi.timestamps = []int64{1}
		bi.tagFamilies = slices.Clip([]tagFamily{{name: "searchable", tags: []tag{appendTestTag("trace_id")}}})
		b := &blockPointer{}
		b.timestamps = []int64{2}
		b.tagFamilies = []tagFamily{
			{name: "data", tags: []tag{appendTestTag("data_binary")}},
			{name: "searchable", tags: []tag{appendTestTag("trace_id"), appendTestTag("status")}},
		}
		fullTagAppend(bi, b, 1)
		requireRows(t, bi, 2)
		names := make(map[string]int)
		for _, tf := range bi.tagFamilies {
			names[tf.name] = len(tf.tags)
		}
		require.Equal(t, map[string]int{"data": 1, "searchable": 2}, names, "the tag added to the existing family must be kept")
	})
}
