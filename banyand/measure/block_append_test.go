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

package measure

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

// appendTestColumn is a one-row string column holding its own name.
func appendTestColumn(name string) column {
	return column{name: name, valueType: pbv1.ValueTypeStr, values: [][]byte{[]byte(name)}}
}

// requireColumnsHaveRows fails when any tag or field column does not hold one value per row.
func requireColumnsHaveRows(t *testing.T, bi *blockPointer, rows int) {
	t.Helper()
	for _, tf := range bi.tagFamilies {
		for _, c := range tf.columns {
			require.Lenf(t, c.values, rows, "tag %s.%s", tf.name, c.name)
		}
	}
	for _, c := range bi.field.columns {
		require.Lenf(t, c.values, rows, "field %s", c.name)
	}
}

// TestFullAppendKeepsExistingColumnsWhenTheLayoutGrows pins that appending a block whose layout adds a tag
// column, a tag family or a field keeps every column at one value per row. Growing the destination slices
// used to strand the values appended through pointers taken before the growth, leaving a column one block
// short, which made the next merge of that block panic and stopped the shard's merge loop.
func TestFullAppendKeepsExistingColumnsWhenTheLayoutGrows(t *testing.T) {
	t.Run("a new tag column before an existing one", func(t *testing.T) {
		bi := &blockPointer{}
		bi.timestamps = []int64{1}
		// A clipped slice is full, so adding a column reallocates it.
		bi.tagFamilies = []columnFamily{{name: "default", columns: slices.Clip([]column{appendTestColumn("a"), appendTestColumn("c")})}}
		b := &blockPointer{}
		b.timestamps = []int64{2}
		b.tagFamilies = []columnFamily{{name: "default", columns: []column{
			appendTestColumn("a"), appendTestColumn("b"), appendTestColumn("c"),
		}}}
		fullTagAppend(bi, b, 1)
		requireColumnsHaveRows(t, bi, 2)
	})
	t.Run("a new tag family before an existing family that gains a tag", func(t *testing.T) {
		bi := &blockPointer{}
		bi.timestamps = []int64{1}
		bi.tagFamilies = slices.Clip([]columnFamily{{name: "storage-only", columns: []column{appendTestColumn("component_ids")}}})
		b := &blockPointer{}
		b.timestamps = []int64{2}
		b.tagFamilies = []columnFamily{
			{name: "default", columns: []column{appendTestColumn("service_id")}},
			{name: "storage-only", columns: []column{appendTestColumn("component_ids"), appendTestColumn("layer")}},
		}
		fullTagAppend(bi, b, 1)
		requireColumnsHaveRows(t, bi, 2)
		names := make(map[string]int)
		for _, tf := range bi.tagFamilies {
			names[tf.name] = len(tf.columns)
		}
		require.Equal(t, map[string]int{"default": 1, "storage-only": 2}, names, "the tag added to the existing family must be kept")
	})
	t.Run("a new field before an existing one", func(t *testing.T) {
		bi := &blockPointer{}
		bi.timestamps = []int64{1}
		bi.field.columns = slices.Clip([]column{appendTestColumn("value")})
		b := &blockPointer{}
		b.timestamps = []int64{2}
		b.field.columns = []column{appendTestColumn("count"), appendTestColumn("value")}
		fullFieldAppend(bi, b, 1)
		requireColumnsHaveRows(t, bi, 2)
	})
}
