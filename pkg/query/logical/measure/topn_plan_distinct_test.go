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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

func strTagValue(v string) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: v}}}
}

func intFieldValue(v int64) *modelv1.FieldValue {
	return &modelv1.FieldValue{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: v}}}
}

func floatFieldValue(v float64) *modelv1.FieldValue {
	return &modelv1.FieldValue{Value: &modelv1.FieldValue_Float{Float: &modelv1.Float{Value: v}}}
}

func makeIDP(entityValues []*modelv1.TagValue, fieldVal int64) *measurev1.InternalDataPoint {
	tags := make([]*modelv1.Tag, len(entityValues))
	for i, ev := range entityValues {
		tags[i] = &modelv1.Tag{Key: "entity", Value: ev}
	}
	return &measurev1.InternalDataPoint{
		DataPoint: &measurev1.DataPoint{
			TagFamilies: []*modelv1.TagFamily{
				{Name: "_topN", Tags: tags},
			},
			Fields: []*measurev1.DataPoint_Field{
				{
					Name:  "value",
					Value: intFieldValue(fieldVal),
				},
			},
		},
	}
}

func makeFloatIDP(entityValues []*modelv1.TagValue, fieldVal float64) *measurev1.InternalDataPoint {
	tags := make([]*modelv1.Tag, len(entityValues))
	for i, ev := range entityValues {
		tags[i] = &modelv1.Tag{Key: "entity", Value: ev}
	}
	return &measurev1.InternalDataPoint{
		DataPoint: &measurev1.DataPoint{
			TagFamilies: []*modelv1.TagFamily{
				{Name: "_topN", Tags: tags},
			},
			Fields: []*measurev1.DataPoint_Field{
				{
					Name:  "value",
					Value: floatFieldValue(fieldVal),
				},
			},
		},
	}
}

func TestEntityDedupTopN_DESC(t *testing.T) {
	h := newEntityDedupTopN[int64](3, true)

	h.Put("A", 100, makeIDP([]*modelv1.TagValue{strTagValue("A")}, 100))
	h.Put("B", 90, makeIDP([]*modelv1.TagValue{strTagValue("B")}, 90))
	h.Put("A", 200, makeIDP([]*modelv1.TagValue{strTagValue("A")}, 200))
	h.Put("C", 80, makeIDP([]*modelv1.TagValue{strTagValue("C")}, 80))
	h.Put("D", 150, makeIDP([]*modelv1.TagValue{strTagValue("D")}, 150))

	result := h.Elements()
	require.Len(t, result, 3)
	vals := extractFieldValues(result)
	assert.Equal(t, []int64{200, 150, 90}, vals)
}

func TestEntityDedupTopN_ASC(t *testing.T) {
	h := newEntityDedupTopN[int64](3, false)

	h.Put("A", 100, makeIDP([]*modelv1.TagValue{strTagValue("A")}, 100))
	h.Put("B", 90, makeIDP([]*modelv1.TagValue{strTagValue("B")}, 90))
	h.Put("A", 50, makeIDP([]*modelv1.TagValue{strTagValue("A")}, 50))
	h.Put("C", 80, makeIDP([]*modelv1.TagValue{strTagValue("C")}, 80))
	h.Put("D", 30, makeIDP([]*modelv1.TagValue{strTagValue("D")}, 30))

	result := h.Elements()
	require.Len(t, result, 3)
	vals := extractFieldValues(result)
	assert.Equal(t, []int64{30, 50, 80}, vals)
}

func TestEntityDedupTopN_EvictAndReAdd(t *testing.T) {
	h := newEntityDedupTopN[int64](2, true)

	h.Put("A", 100, makeIDP([]*modelv1.TagValue{strTagValue("A")}, 100))
	h.Put("B", 90, makeIDP([]*modelv1.TagValue{strTagValue("B")}, 90))
	h.Put("C", 80, makeIDP([]*modelv1.TagValue{strTagValue("C")}, 80))
	h.Put("C", 95, makeIDP([]*modelv1.TagValue{strTagValue("C")}, 95))

	result := h.Elements()
	require.Len(t, result, 2)
	vals := extractFieldValues(result)
	assert.Equal(t, []int64{100, 95}, vals)
}

func TestEntityDedupTopN_EmptyInput(t *testing.T) {
	h := newEntityDedupTopN[int64](3, true)
	result := h.Elements()
	assert.Empty(t, result)
}

func TestEntityDedupTopN_TopN1(t *testing.T) {
	h := newEntityDedupTopN[int64](1, true)
	h.Put("A", 100, makeIDP([]*modelv1.TagValue{strTagValue("A")}, 100))
	h.Put("B", 200, makeIDP([]*modelv1.TagValue{strTagValue("B")}, 200))
	h.Put("A", 50, makeIDP([]*modelv1.TagValue{strTagValue("A")}, 50))
	h.Put("B", 300, makeIDP([]*modelv1.TagValue{strTagValue("B")}, 300))

	result := h.Elements()
	require.Len(t, result, 1)
	vals := extractFieldValues(result)
	assert.Equal(t, []int64{300}, vals)
}

func TestEntityDedupTopN_AllSameEntity(t *testing.T) {
	h := newEntityDedupTopN[int64](3, true)
	h.Put("A", 100, makeIDP([]*modelv1.TagValue{strTagValue("A")}, 100))
	h.Put("A", 200, makeIDP([]*modelv1.TagValue{strTagValue("A")}, 200))
	h.Put("A", 150, makeIDP([]*modelv1.TagValue{strTagValue("A")}, 150))

	result := h.Elements()
	require.Len(t, result, 1)
	vals := extractFieldValues(result)
	assert.Equal(t, []int64{200}, vals)
}

func TestEntityDedupTopN_Float_DESC(t *testing.T) {
	h := newEntityDedupTopN[float64](3, true)

	h.Put("A", 100.5, makeFloatIDP([]*modelv1.TagValue{strTagValue("A")}, 100.5))
	h.Put("B", 90.25, makeFloatIDP([]*modelv1.TagValue{strTagValue("B")}, 90.25))
	h.Put("A", 200.75, makeFloatIDP([]*modelv1.TagValue{strTagValue("A")}, 200.75))
	h.Put("C", 80.0, makeFloatIDP([]*modelv1.TagValue{strTagValue("C")}, 80.0))
	h.Put("D", 150.5, makeFloatIDP([]*modelv1.TagValue{strTagValue("D")}, 150.5))

	result := h.Elements()
	require.Len(t, result, 3)
	vals := extractFloatFieldValues(result)
	assert.Equal(t, []float64{200.75, 150.5, 90.25}, vals)
}

func TestEntityDedupTopN_Float_ASC(t *testing.T) {
	h := newEntityDedupTopN[float64](3, false)

	h.Put("A", 100.5, makeFloatIDP([]*modelv1.TagValue{strTagValue("A")}, 100.5))
	h.Put("B", 90.25, makeFloatIDP([]*modelv1.TagValue{strTagValue("B")}, 90.25))
	h.Put("A", 50.0, makeFloatIDP([]*modelv1.TagValue{strTagValue("A")}, 50.0))
	h.Put("C", 80.0, makeFloatIDP([]*modelv1.TagValue{strTagValue("C")}, 80.0))
	h.Put("D", 30.75, makeFloatIDP([]*modelv1.TagValue{strTagValue("D")}, 30.75))

	result := h.Elements()
	require.Len(t, result, 3)
	vals := extractFloatFieldValues(result)
	assert.Equal(t, []float64{30.75, 50.0, 80.0}, vals)
}

func TestEntityDedupTopN_Float_EvictAndReAdd(t *testing.T) {
	h := newEntityDedupTopN[float64](2, true)

	h.Put("A", 100.5, makeFloatIDP([]*modelv1.TagValue{strTagValue("A")}, 100.5))
	h.Put("B", 90.25, makeFloatIDP([]*modelv1.TagValue{strTagValue("B")}, 90.25))
	h.Put("C", 80.0, makeFloatIDP([]*modelv1.TagValue{strTagValue("C")}, 80.0))
	h.Put("C", 95.75, makeFloatIDP([]*modelv1.TagValue{strTagValue("C")}, 95.75))

	result := h.Elements()
	require.Len(t, result, 2)
	vals := extractFloatFieldValues(result)
	assert.Equal(t, []float64{100.5, 95.75}, vals)
}

func extractFieldValues(idps []*measurev1.InternalDataPoint) []int64 {
	vals := make([]int64, len(idps))
	for i, idp := range idps {
		vals[i] = idp.GetDataPoint().GetFields()[0].GetValue().GetInt().GetValue()
	}
	return vals
}

func extractFloatFieldValues(idps []*measurev1.InternalDataPoint) []float64 {
	vals := make([]float64, len(idps))
	for i, idp := range idps {
		vals[i] = idp.GetDataPoint().GetFields()[0].GetValue().GetFloat().GetValue()
	}
	return vals
}

func TestTopNDistinctIterator(t *testing.T) {
	items := []*measurev1.InternalDataPoint{
		makeIDP([]*modelv1.TagValue{strTagValue("svc-A")}, 200),
		makeIDP([]*modelv1.TagValue{strTagValue("svc-D")}, 150),
		makeIDP([]*modelv1.TagValue{strTagValue("svc-B")}, 90),
	}
	iter := &topNDistinctIterator{elements: items, index: -1}

	var result []*measurev1.InternalDataPoint
	for iter.Next() {
		current := iter.Current()
		result = append(result, current[0])
	}
	require.NoError(t, iter.Close())

	require.Len(t, result, 3)
	vals := extractFieldValues(result)
	assert.Equal(t, []int64{200, 150, 90}, vals)
}

func TestTopNDistinctIterator_Empty(t *testing.T) {
	iter := &topNDistinctIterator{elements: nil, index: -1}
	require.False(t, iter.Next())
	require.NoError(t, iter.Close())
}

func TestTopNDistinctOp_String(t *testing.T) {
	op := &topNDistinctOp[int64]{topN: 10, sortDesc: true}
	assert.Equal(t, "TopNDistinct(topN=10,sort=DESC)", op.String())

	opAsc := &topNDistinctOp[int64]{topN: 5, sortDesc: false}
	assert.Equal(t, "TopNDistinct(topN=5,sort=ASC)", opAsc.String())
}
