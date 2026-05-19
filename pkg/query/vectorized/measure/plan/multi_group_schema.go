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

package plan

import (
	"fmt"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	vmeasure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"
)

// BuildMultiGroupBatchSchema unions the per-group BatchSchemas produced by
// BuildBatchSchema and returns a single merged schema whose column order is:
//
//	metadata (timestamp, version, sid, shardID), then tags in
//	(TagFamilyIdx, TagIdx) walk order across all groups with duplicates
//	skipped, then fields ordered by request projection (or schema-walk
//	order when no projection is given).
//
// Tag type-divergence (same name, different type across groups) falls back to
// ColumnTypeTagValue so the frame v3 proto-bytes passthrough carries each
// cell without forced type conversion. Field type-divergence falls back to
// ColumnTypeFieldValue, mirroring the row path's FIELD_TYPE_UNSPECIFIED
// fallback (schema.go:165-176).
//
// When len(measureSchemas) == 1 the result is identical to BuildBatchSchema
// applied to that single schema (single-group hot path is unchanged).
func BuildMultiGroupBatchSchema(measureSchemas []*databasev1.Measure, req *measurev1.QueryRequest) (*vectorized.BatchSchema, error) {
	if len(measureSchemas) == 0 {
		return nil, fmt.Errorf("vec multi-group schema: no measure schemas supplied")
	}
	if req == nil {
		return nil, fmt.Errorf("vec multi-group schema: nil request")
	}
	if len(measureSchemas) == 1 {
		opts := model.MeasureQueryOptions{
			TagProjection:   multiGroupTagProjection(req),
			FieldProjection: req.GetFieldProjection().GetNames(),
		}
		return vmeasure.BuildBatchSchema(measureSchemas[0], opts)
	}

	// Fixed metadata columns — identical across every group.
	cols := []vectorized.ColumnDef{
		{Role: vectorized.RoleTimestamp, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleVersion, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleSeriesID, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleShardID, Type: vectorized.ColumnTypeInt64},
	}

	// tagColType maps "family\x00name" → ColumnType resolved from first
	// group that declares the tag. A second group with a diverging type
	// degrades the entry to ColumnTypeTagValue (passthrough).
	tagColType := make(map[string]vectorized.ColumnType)

	// tagOrder preserves insertion order so the merged schema is
	// deterministic: tags appear in (TagFamilyIdx, TagIdx) walk order,
	// first occurrence wins insertion slot.
	type tagKey struct{ family, name string }
	var tagOrder []tagKey

	// fieldColType maps field name → ColumnType; diverging type across
	// groups degrades to ColumnTypeFieldValue (passthrough).
	fieldColType := make(map[string]vectorized.ColumnType)
	var fieldOrder []string

	// Union tag families and fields across all groups.
	for _, ms := range measureSchemas {
		if ms == nil {
			continue
		}
		for _, tf := range ms.GetTagFamilies() {
			for _, ts := range tf.GetTags() {
				key := nativeKeyMulti(tf.GetName(), ts.GetName())
				colType, typeErr := tagTypeToColumnTypeMG(ts.GetType())
				if typeErr != nil {
					// Unsupported tag type — fall back to passthrough.
					colType = vectorized.ColumnTypeTagValue
				}
				if existing, seen := tagColType[key]; !seen {
					tagColType[key] = colType
					tagOrder = append(tagOrder, tagKey{family: tf.GetName(), name: ts.GetName()})
				} else if existing != colType {
					// Type divergence across groups — downgrade to passthrough.
					tagColType[key] = vectorized.ColumnTypeTagValue
				}
			}
		}
		for _, fs := range ms.GetFields() {
			colType, typeErr := fieldTypeToColumnTypeMG(fs.GetFieldType())
			if typeErr != nil {
				colType = vectorized.ColumnTypeFieldValue
			}
			if existing, seen := fieldColType[fs.GetName()]; !seen {
				fieldColType[fs.GetName()] = colType
				fieldOrder = append(fieldOrder, fs.GetName())
			} else if existing != colType {
				fieldColType[fs.GetName()] = vectorized.ColumnTypeFieldValue
			}
		}
	}

	// Apply TagProjection filter when present, preserving projection order.
	// A projected tag that resolves in NO group is a configuration error —
	// surface loudly rather than silently producing a merged schema that
	// drops the column. This mirrors the row path's ValidateMultiGroupProjection
	// in dispatch.go which rejects unknown projection names with
	// "<tag>: tag is not defined".
	tp := req.GetTagProjection()
	if tp != nil && len(tp.GetTagFamilies()) > 0 {
		projected := make(map[string]bool)
		var projOrder []tagKey
		for _, fam := range tp.GetTagFamilies() {
			for _, name := range fam.GetTags() {
				k := nativeKeyMulti(fam.GetName(), name)
				if _, inUnion := tagColType[k]; !inUnion {
					return nil, fmt.Errorf("%s: tag is not defined", name)
				}
				projected[k] = true
				projOrder = append(projOrder, tagKey{family: fam.GetName(), name: name})
			}
		}
		tagOrder = projOrder
		// Remove non-projected tags from the type map so the columns slice
		// stays consistent with tagOrder.
		newTagColType := make(map[string]vectorized.ColumnType, len(projected))
		for k, ct := range tagColType {
			if projected[k] {
				newTagColType[k] = ct
			}
		}
		tagColType = newTagColType
	}

	for _, tk := range tagOrder {
		key := nativeKeyMulti(tk.family, tk.name)
		colType, ok := tagColType[key]
		if !ok {
			continue
		}
		cols = append(cols, vectorized.ColumnDef{
			Role:      vectorized.RoleTag,
			TagFamily: tk.family,
			Name:      tk.name,
			Type:      colType,
		})
	}

	// Apply FieldProjection filter when present.
	fp := req.GetFieldProjection()
	if fp != nil && len(fp.GetNames()) > 0 {
		orderedFields := make([]string, 0, len(fp.GetNames()))
		for _, name := range fp.GetNames() {
			if _, inUnion := fieldColType[name]; inUnion {
				orderedFields = append(orderedFields, name)
			} else {
				// Field not in any group's schema — include as passthrough
				// (BuildBatchSchema's null-slot behaviour for unknown fields).
				orderedFields = append(orderedFields, name)
				fieldColType[name] = vectorized.ColumnTypeFieldValue
			}
		}
		fieldOrder = orderedFields
	}

	for _, name := range fieldOrder {
		colType, ok := fieldColType[name]
		if !ok {
			colType = vectorized.ColumnTypeFieldValue
		}
		cols = append(cols, vectorized.ColumnDef{
			Role: vectorized.RoleField,
			Name: name,
			Type: colType,
		})
	}

	return vectorized.NewBatchSchema(cols), nil
}

// nativeKeyMulti produces the composite lookup key for (family, name) without
// allocation in the common case.
func nativeKeyMulti(family, name string) string { return family + "\x00" + name }

// tagTypeToColumnTypeMG maps a TagType to its vec ColumnType. Returns an error
// for types that have no direct vec mapping (UNSPECIFIED, TIMESTAMP) so the
// caller can fall back to ColumnTypeTagValue.
func tagTypeToColumnTypeMG(t databasev1.TagType) (vectorized.ColumnType, error) {
	switch t {
	case databasev1.TagType_TAG_TYPE_INT:
		return vectorized.ColumnTypeInt64, nil
	case databasev1.TagType_TAG_TYPE_STRING:
		return vectorized.ColumnTypeString, nil
	case databasev1.TagType_TAG_TYPE_DATA_BINARY:
		return vectorized.ColumnTypeBytes, nil
	case databasev1.TagType_TAG_TYPE_INT_ARRAY:
		return vectorized.ColumnTypeInt64Array, nil
	case databasev1.TagType_TAG_TYPE_STRING_ARRAY:
		return vectorized.ColumnTypeStrArray, nil
	}
	return 0, fmt.Errorf("unsupported tag type %v", t)
}

// multiGroupTagProjection converts the proto TagProjection on a request into
// the model.TagProjection slice that BuildBatchSchema expects.
func multiGroupTagProjection(req *measurev1.QueryRequest) []model.TagProjection {
	tp := req.GetTagProjection()
	if tp == nil {
		return nil
	}
	families := tp.GetTagFamilies()
	out := make([]model.TagProjection, 0, len(families))
	for _, tf := range families {
		out = append(out, model.TagProjection{
			Family: tf.GetName(),
			Names:  append([]string(nil), tf.GetTags()...),
		})
	}
	return out
}

// fieldTypeToColumnTypeMG maps a FieldType to its vec ColumnType. Returns an
// error for FIELD_TYPE_UNSPECIFIED so the caller can fall back to
// ColumnTypeFieldValue — matching the row path's type-divergence fallback.
func fieldTypeToColumnTypeMG(t databasev1.FieldType) (vectorized.ColumnType, error) {
	switch t {
	case databasev1.FieldType_FIELD_TYPE_INT:
		return vectorized.ColumnTypeInt64, nil
	case databasev1.FieldType_FIELD_TYPE_FLOAT:
		return vectorized.ColumnTypeFloat64, nil
	case databasev1.FieldType_FIELD_TYPE_STRING:
		return vectorized.ColumnTypeString, nil
	case databasev1.FieldType_FIELD_TYPE_DATA_BINARY:
		return vectorized.ColumnTypeBytes, nil
	}
	return 0, fmt.Errorf("unsupported field type %v", t)
}
