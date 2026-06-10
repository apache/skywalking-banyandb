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

package lifecycle

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/dump"
	dumpmeasure "github.com/apache/skywalking-banyandb/banyand/internal/dump/measure"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

// TestFillTagValue_NilRaw guards the pooled in-place tag reuse against a null
// column entry (raw==nil): an Int slot must not call BytesToInt64(nil) (panic),
// a Str slot must not become an empty string, and both must yield the same
// NullTagValue the row/non-pooled paths produce via dump.DecodeTagValue.
func TestFillTagValue_NilRaw(t *testing.T) {
	t.Run("int_reused_slot_returns_null_not_panic", func(t *testing.T) {
		tv := &modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: 42}}}
		require.NotPanics(t, func() { fillTagValue(tv, pbv1.ValueTypeInt64, nil) })
		require.True(t, proto.Equal(tv, dump.DecodeTagValue(pbv1.ValueTypeInt64, nil, nil)))
	})
	t.Run("str_reused_slot_returns_null_not_empty", func(t *testing.T) {
		tv := &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "x"}}}
		fillTagValue(tv, pbv1.ValueTypeStr, nil)
		require.True(t, proto.Equal(tv, dump.DecodeTagValue(pbv1.ValueTypeStr, nil, nil)),
			"a null Str tag must decode to NullTagValue, not an empty string")
	})
	t.Run("int_present_value_still_decodes_in_place", func(t *testing.T) {
		raw := convert.Int64ToBytes(42)
		tv := &modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: 1}}}
		fillTagValue(tv, pbv1.ValueTypeInt64, raw)
		require.True(t, proto.Equal(tv, dump.DecodeTagValue(pbv1.ValueTypeInt64, raw, nil)))
	})
}

// TestFillFieldValue_NilRaw guards the pooled in-place field reuse against a
// null/absent scalar column entry: Int/Float slots must not decode nil bytes
// (which panics) and must yield the NullFieldValue the non-pooled path produces.
func TestFillFieldValue_NilRaw(t *testing.T) {
	t.Run("int_reused_slot_returns_null_not_panic", func(t *testing.T) {
		fv := &modelv1.FieldValue{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: 42}}}
		require.NotPanics(t, func() { fillFieldValue(fv, pbv1.ValueTypeInt64, nil) })
		require.True(t, proto.Equal(fv, dumpmeasure.DecodeFieldValue(pbv1.ValueTypeInt64, nil)))
	})
	t.Run("float_reused_slot_returns_null_not_panic", func(t *testing.T) {
		fv := &modelv1.FieldValue{Value: &modelv1.FieldValue_Float{Float: &modelv1.Float{Value: 1.5}}}
		require.NotPanics(t, func() { fillFieldValue(fv, pbv1.ValueTypeFloat64, nil) })
		require.True(t, proto.Equal(fv, dumpmeasure.DecodeFieldValue(pbv1.ValueTypeFloat64, nil)))
	})
}

// TestMeasureProtoBuilder_PrepareRefreshesColumnKeys verifies that when the
// builder is re-pointed at a different measure that happens to share the tree
// shape (same tag/field counts, different names), the cached "family.tag" column
// keys are refreshed — otherwise the per-row fast path would look up the prior
// measure's columns and write null/wrong values.
func TestMeasureProtoBuilder_PrepareRefreshesColumnKeys(t *testing.T) {
	mk := func(fam, tag string) *databasev1.Measure {
		return &databasev1.Measure{
			TagFamilies: []*databasev1.TagFamilySpec{
				{Name: fam, Tags: []*databasev1.TagSpec{{Name: tag, Type: databasev1.TagType_TAG_TYPE_STRING}}},
			},
		}
	}
	var b measureProtoBuilder
	b.prepare("g", "A", mk("fa", "ta"))
	require.Equal(t, "fa.ta", b.fams[0].fullNames[0])
	// Same shape, different names: shapeMatches short-circuits the rebuild, so the
	// keys must be refreshed in place to this measure's names.
	b.prepare("g", "B", mk("fb", "tb"))
	require.Equal(t, "fb.tb", b.fams[0].fullNames[0])
}
