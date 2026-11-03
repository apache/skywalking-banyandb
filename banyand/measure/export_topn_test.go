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

package measure

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

func topNBinary(t *testing.T, values map[string]int64) []byte {
	v := &TopNValue[int64]{}
	v.setMetadata("value", []string{"service_id"})
	for svc, n := range values {
		v.addValue(n, []*modelv1.TagValue{{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: svc}}}})
	}
	out, err := v.marshal(nil)
	require.NoError(t, err)
	return out
}

func TestMergeTopNResults(t *testing.T) {
	params, err := json.Marshal(TopNParameters{Limit: 2, FieldType: databasev1.FieldType_FIELD_TYPE_INT})
	require.NoError(t, err)
	entity := []*modelv1.TagValue{
		{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "cpm_top"}}},
		{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: int64(modelv1.Sort_SORT_DESC)}}},
		{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "sw_metric"}}},
		{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: string(params)}}},
	}
	left := topNBinary(t, map[string]int64{"svc1": 100, "svc2": 50})
	right := topNBinary(t, map[string]int64{"svc3": 80})
	merged, err := MergeTopNResults(entity, 1000, 2, 1, left, right)
	require.NoError(t, err)

	got := &TopNValue[int64]{}
	decoder := GenerateTopNValuesDecoder()
	defer ReleaseTopNValuesDecoder(decoder)
	require.NoError(t, got.Unmarshal(merged, decoder))
	ranks := map[string]int64{}
	for i, e := range got.entities {
		ranks[e[0].GetStr().GetValue()] = got.values[i]
	}
	require.Equal(t, map[string]int64{"svc1": 100, "svc3": 80}, ranks, "the merge keeps the top two of both versions")

	_, err = MergeTopNResults(entity[:3], 1000, 2, 1, left, right)
	require.Error(t, err)

	same, err := MergeTopNResults(entity, 1000, 1, 1, left, append([]byte(nil), left...))
	require.NoError(t, err)
	require.Equal(t, left, same, "merging a value with an identical copy keeps its bytes")
}
