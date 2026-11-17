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

package index_test

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/index"
)

func intTag(v int64) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: v}}}
}

// MarkNumericTag is the single rule for who gets the sort ladder: a sort-marked scalar int or timestamp tag, whatever
// catalog writes it.
func TestMarkNumericTag(t *testing.T) {
	key := index.FieldKey{IndexRuleID: 1}
	instant := time.Unix(1_700_000_000, 123)
	for _, tc := range []struct {
		value   *modelv1.TagValue
		name    string
		wantKey int64
		tagType databasev1.TagType
		noSort  bool
		want    bool
	}{
		{name: "scalar int", tagType: databasev1.TagType_TAG_TYPE_INT, value: intTag(-42), want: true, wantKey: -42},
		{name: "int extremes", tagType: databasev1.TagType_TAG_TYPE_INT, value: intTag(math.MinInt64), want: true, wantKey: math.MinInt64},
		{name: "timestamp", tagType: databasev1.TagType_TAG_TYPE_TIMESTAMP, value: &modelv1.TagValue{
			Value: &modelv1.TagValue_Timestamp{Timestamp: timestamppb.New(instant)},
		}, want: true, wantKey: instant.UnixNano()},
		{name: "no sort", tagType: databasev1.TagType_TAG_TYPE_INT, value: intTag(7), noSort: true},
		{name: "null int", tagType: databasev1.TagType_TAG_TYPE_INT, value: &modelv1.TagValue{Value: &modelv1.TagValue_Null{}}},
		{name: "int array", tagType: databasev1.TagType_TAG_TYPE_INT_ARRAY, value: &modelv1.TagValue{
			Value: &modelv1.TagValue_IntArray{IntArray: &modelv1.IntArray{Value: []int64{1, 2}}},
		}},
		{name: "string", tagType: databasev1.TagType_TAG_TYPE_STRING, value: &modelv1.TagValue{
			Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "x"}},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			field := index.NewBytesField(key, []byte("v"))
			field.NoSort = tc.noSort
			field.MarkNumericTag(tc.tagType, tc.value)
			require.Equal(t, tc.want, field.Numeric)
			if tc.want {
				require.Equal(t, tc.wantKey, field.NumericKey)
			}
		})
	}
}
