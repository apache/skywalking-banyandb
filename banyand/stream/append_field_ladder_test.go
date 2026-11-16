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

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/index"
)

// A scalar int field is offered to the sort ladder with the order-preserving key of its stored float; an int array is not.
func TestAppendFieldMarksOnlyScalarIntsNumeric(t *testing.T) {
	key := index.FieldKey{IndexRuleID: 3}
	scalar := &modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: -7}}}
	fields := appendField(nil, key, databasev1.TagType_TAG_TYPE_INT, scalar, false)
	require.Len(t, fields, 1)
	require.True(t, fields[0].Numeric)
	require.Equal(t, encoding.Float64ToSortableInt64(fields[0].GetFloat()), fields[0].NumericKey)
	require.Equal(t, int64(-7), fields[0].NumericKey)

	array := &modelv1.TagValue{Value: &modelv1.TagValue_IntArray{IntArray: &modelv1.IntArray{Value: []int64{1, 2}}}}
	fields = appendField(nil, key, databasev1.TagType_TAG_TYPE_INT_ARRAY, array, false)
	require.Len(t, fields, 2)
	for _, field := range fields {
		require.False(t, field.Numeric)
	}

	fields = appendField(nil, key, databasev1.TagType_TAG_TYPE_INT, scalar, true)
	require.Len(t, fields, 1)
	require.True(t, fields[0].NoSort, "the adapter, not the writer, refuses a ladder for a no_sort field")
}
