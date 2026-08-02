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

package stream

import (
	"testing"

	"github.com/stretchr/testify/require"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// TestBuildElementsFromBatch_HonorsValidityBitmap pins the egress to the
// validity bitmap rather than to the stored pointer.
//
// A column tracks nullness in a bitmap that is independent of the cell it holds,
// and the two can legitimately disagree: AppendColumnRange (which the sorted
// merge uses to gather rows) appends the source value unconditionally and only
// afterwards marks the destination row null, so a null row keeps whatever
// pointer came across. Reading Data() alone therefore emits a stale value where
// the query should report a null tag — silently wrong output rather than a
// crash, which is the hard kind to notice.
//
// The row below is the exact shape that produces it: a real value written into
// the cell, then the row marked null.
func TestBuildElementsFromBatch_HonorsValidityBitmap(t *testing.T) {
	schema := tsSchema()
	batch := buildBatch(schema, []testRow{{ts: 10, elemID: 1, tag: "real-value"}})

	tagIdx, ok := schema.TagIndex(testTagFamily, testTagName)
	require.True(t, ok)
	tagCol := batch.Columns[tagIdx].(*vectorized.TypedColumn[*modelv1.TagValue])

	// Null the row without disturbing the cell, exactly as AppendColumnRange
	// leaves it after copying a null source row.
	tagCol.MarkNullAt(0)
	require.True(t, tagCol.IsNull(0))
	require.NotNil(t, tagCol.Data()[0], "precondition: the stale pointer must still be present")

	elements, err := BuildElementsFromBatch(batch,
		[]model.TagProjection{{Family: testTagFamily, Names: []string{testTagName}}})
	require.NoError(t, err)
	require.Len(t, elements, 1)
	require.Len(t, elements[0].TagFamilies, 1)
	require.Len(t, elements[0].TagFamilies[0].Tags, 1)

	got := elements[0].TagFamilies[0].Tags[0].Value
	require.NotNil(t, got)
	require.IsType(t, &modelv1.TagValue_Null{}, got.GetValue(),
		"a row marked null must be emitted as a null tag, not as the stale cell value")
}

// TestBuildElementsFromBatch_KeepsNonNullValues is the positive control: the
// bitmap check must not turn ordinary populated cells into nulls.
func TestBuildElementsFromBatch_KeepsNonNullValues(t *testing.T) {
	schema := tsSchema()
	batch := buildBatch(schema, []testRow{{ts: 10, elemID: 1, tag: "kept"}})

	elements, err := BuildElementsFromBatch(batch,
		[]model.TagProjection{{Family: testTagFamily, Names: []string{testTagName}}})
	require.NoError(t, err)
	require.Len(t, elements, 1)
	require.Equal(t, "kept",
		elements[0].TagFamilies[0].Tags[0].Value.GetStr().GetValue())
}
