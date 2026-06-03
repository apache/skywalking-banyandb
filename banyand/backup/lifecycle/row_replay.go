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

// Package lifecycle implements row-level replay paths used by migration visitors
// when a source segment maps to more than one target segment. The receiving
// data node owns segment creation per-row, which makes the per-target sender
// loop unnecessary.
package lifecycle

import (
	"fmt"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/dump"
	dumpmeasure "github.com/apache/skywalking-banyandb/banyand/internal/dump/measure"
	dumpstream "github.com/apache/skywalking-banyandb/banyand/internal/dump/stream"
	dumptrace "github.com/apache/skywalking-banyandb/banyand/internal/dump/trace"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

// decodeSeriesEntityValues recovers (subject, entityTagValues) from the raw
// EntityValues byte sequence stored in part-level smeta.bin or recovered via
// PartSeriesMap.
func decodeSeriesEntityValues(rawBytes []byte) (string, pbv1.EntityValues, error) {
	if len(rawBytes) == 0 {
		return "", nil, fmt.Errorf("empty entity values bytes")
	}
	var s pbv1.Series
	if err := s.Unmarshal(rawBytes); err != nil {
		return "", nil, fmt.Errorf("unmarshal series: %w", err)
	}
	if s.Subject == "" {
		return "", nil, fmt.Errorf("decoded subject is empty")
	}
	return s.Subject, s.EntityValues, nil
}

// deriveMergedRuleToTag builds an IndexRuleID -> IndexedTagSpec map covering the
// group's measures. A rule's tag is resolved within the measure it is bound to
// (via IndexRuleBinding), not against a group-wide tag set: tag names may
// collide across measures with a different family/type, so a global merge would
// decode an indexed value with the wrong type. Rules whose tag is not
// column-defined in the bound measure are skipped because the resolver cannot
// decode their value without a known type.
func deriveMergedRuleToTag(measures []*databasev1.Measure, rules []*databasev1.IndexRule,
	bindings []*databasev1.IndexRuleBinding,
) map[uint32]dump.IndexedTagSpec {
	if len(measures) == 0 || len(rules) == 0 || len(bindings) == 0 {
		return nil
	}
	type tagInfo struct {
		spec   *databasev1.TagSpec
		family string
	}
	// Per-measure tag index: tag name -> family/spec, scoped to each measure so
	// colliding names resolve to the right type.
	measureTags := make(map[string]map[string]tagInfo, len(measures))
	for _, m := range measures {
		tags := make(map[string]tagInfo)
		for _, fam := range m.TagFamilies {
			for _, ts := range fam.Tags {
				tags[ts.Name] = tagInfo{family: fam.Name, spec: ts}
			}
		}
		measureTags[m.GetMetadata().GetName()] = tags
	}
	ruleByName := make(map[string]*databasev1.IndexRule, len(rules))
	for _, rule := range rules {
		if rule.GetMetadata() != nil {
			ruleByName[rule.GetMetadata().GetName()] = rule
		}
	}
	out := make(map[uint32]dump.IndexedTagSpec)
	for _, binding := range bindings {
		subject := binding.GetSubject()
		if subject == nil || subject.GetCatalog() != commonv1.Catalog_CATALOG_MEASURE {
			continue
		}
		tags, ok := measureTags[subject.GetName()]
		if !ok {
			continue
		}
		for _, ruleName := range binding.GetRules() {
			rule, exists := ruleByName[ruleName]
			if !exists || rule.GetMetadata() == nil {
				continue
			}
			// The index stores a rule's value under a field keyed solely by its
			// IndexRuleID (FieldKey.Marshal), so the resolver recovers exactly
			// one value per rule. A multi-tag rule indexes a composite term that
			// cannot be split back into individual tag values, so only single-tag
			// rules are decodable; skip the rest rather than mis-decode tags[0].
			ruleTags := rule.GetTags()
			if len(ruleTags) != 1 {
				continue
			}
			info, found := tags[ruleTags[0]]
			if !found || info.spec == nil {
				continue
			}
			out[rule.GetMetadata().GetId()] = dump.IndexedTagSpec{
				Family: info.family,
				Name:   ruleTags[0],
				Type:   tagTypeToValueType(info.spec.Type),
			}
		}
	}
	return out
}

// tagTypeToValueType maps the schema TagType enum to the storage ValueType
// used by dump.DecodeTagValue. It mirrors the production write path's mapping.
func tagTypeToValueType(t databasev1.TagType) pbv1.ValueType {
	switch t {
	case databasev1.TagType_TAG_TYPE_STRING:
		return pbv1.ValueTypeStr
	case databasev1.TagType_TAG_TYPE_INT:
		return pbv1.ValueTypeInt64
	case databasev1.TagType_TAG_TYPE_DATA_BINARY:
		return pbv1.ValueTypeBinaryData
	case databasev1.TagType_TAG_TYPE_STRING_ARRAY:
		return pbv1.ValueTypeStrArr
	case databasev1.TagType_TAG_TYPE_INT_ARRAY:
		return pbv1.ValueTypeInt64Arr
	case databasev1.TagType_TAG_TYPE_TIMESTAMP:
		return pbv1.ValueTypeTimestamp
	default:
		return pbv1.ValueTypeUnknown
	}
}

// fieldTypeToValueType maps the schema FieldType enum to the storage ValueType
// used by dumpmeasure.DecodeFieldValue.
func fieldTypeToValueType(t databasev1.FieldType) pbv1.ValueType {
	switch t {
	case databasev1.FieldType_FIELD_TYPE_STRING:
		return pbv1.ValueTypeStr
	case databasev1.FieldType_FIELD_TYPE_INT:
		return pbv1.ValueTypeInt64
	case databasev1.FieldType_FIELD_TYPE_FLOAT:
		return pbv1.ValueTypeFloat64
	case databasev1.FieldType_FIELD_TYPE_DATA_BINARY:
		return pbv1.ValueTypeBinaryData
	default:
		return pbv1.ValueTypeUnknown
	}
}

// buildEntityTagIndex maps Entity.TagNames positionally onto the decoded
// EntityValues so the per-tag builders can backfill entity tags that live
// neither in the column store nor in the IndexResolver output.
func buildEntityTagIndex(entity *databasev1.Entity, evList pbv1.EntityValues) map[string]*modelv1.TagValue {
	if entity == nil || len(entity.TagNames) == 0 {
		return nil
	}
	out := make(map[string]*modelv1.TagValue, len(entity.TagNames))
	for i, name := range entity.TagNames {
		if i >= len(evList) {
			break
		}
		out[name] = evList[i]
	}
	return out
}

// buildMeasureTagFamilies rebuilds the TagFamilyForWrite list in schema order.
// Resolution priority: column store > IndexResolver > EntityValues > null.
func buildMeasureTagFamilies(
	families []*databasev1.TagFamilySpec,
	entity *databasev1.Entity,
	row dumpmeasure.Row,
	evList pbv1.EntityValues,
	indexedTyped map[string]*modelv1.TagValue,
) []*modelv1.TagFamilyForWrite {
	entityIdx := buildEntityTagIndex(entity, evList)
	out := make([]*modelv1.TagFamilyForWrite, 0, len(families))
	for _, fam := range families {
		tf := &modelv1.TagFamilyForWrite{Tags: make([]*modelv1.TagValue, 0, len(fam.Tags))}
		for _, t := range fam.Tags {
			tf.Tags = append(tf.Tags, resolveMeasureTagValue(fam.Name, t, row, entityIdx, indexedTyped))
		}
		out = append(out, tf)
	}
	return out
}

func resolveMeasureTagValue(
	familyName string,
	t *databasev1.TagSpec,
	row dumpmeasure.Row,
	entityIdx map[string]*modelv1.TagValue,
	indexedTyped map[string]*modelv1.TagValue,
) *modelv1.TagValue {
	fullName := familyName + "." + t.Name
	if raw, ok := row.Tags[fullName]; ok {
		vt, hasType := row.TagTypes[fullName]
		if !hasType {
			vt = tagTypeToValueType(t.Type)
		}
		return dump.DecodeTagValue(vt, raw, nil)
	}
	if tv, ok := indexedTyped[fullName]; ok && tv != nil {
		return tv
	}
	if tv, ok := entityIdx[t.Name]; ok && tv != nil {
		return tv
	}
	return pbv1.NullTagValue
}

// buildMeasureFields rebuilds the Fields list in schema order using the
// row.Fields raw bytes + row.FieldTypes. Missing fields produce a null
// FieldValue.
func buildMeasureFields(specs []*databasev1.FieldSpec, row dumpmeasure.Row) []*modelv1.FieldValue {
	if len(specs) == 0 {
		return nil
	}
	out := make([]*modelv1.FieldValue, 0, len(specs))
	for _, spec := range specs {
		raw, ok := row.Fields[spec.Name]
		if !ok {
			out = append(out, pbv1.NullFieldValue)
			continue
		}
		vt, hasType := row.FieldTypes[spec.Name]
		if !hasType {
			vt = fieldTypeToValueType(spec.FieldType)
		}
		out = append(out, dumpmeasure.DecodeFieldValue(vt, raw))
	}
	return out
}

// buildStreamTagFamilies rebuilds a stream row's TagFamilyForWrite list in
// schema order. Resolution priority: column store > EntityValues > null.
func buildStreamTagFamilies(
	families []*databasev1.TagFamilySpec,
	entity *databasev1.Entity,
	row dumpstream.Row,
	evList pbv1.EntityValues,
) []*modelv1.TagFamilyForWrite {
	entityIdx := buildEntityTagIndex(entity, evList)
	out := make([]*modelv1.TagFamilyForWrite, 0, len(families))
	for _, fam := range families {
		tf := &modelv1.TagFamilyForWrite{Tags: make([]*modelv1.TagValue, 0, len(fam.Tags))}
		for _, t := range fam.Tags {
			fullName := fam.Name + "." + t.Name
			if raw, ok := row.Tags[fullName]; ok {
				vt, hasType := row.TagTypes[fullName]
				if !hasType {
					vt = tagTypeToValueType(t.Type)
				}
				tf.Tags = append(tf.Tags, dump.DecodeTagValue(vt, raw, nil))
				continue
			}
			if tv, ok := entityIdx[t.Name]; ok && tv != nil {
				tf.Tags = append(tf.Tags, tv)
				continue
			}
			tf.Tags = append(tf.Tags, pbv1.NullTagValue)
		}
		out = append(out, tf)
	}
	return out
}

// buildTraceTags rebuilds a trace row's flat tags list in schema order.
// trace_id and span_id are promoted to top-level Row fields by the dump
// library, so we match them by schema-declared name rather than Tags lookup.
func buildTraceTags(specs []*databasev1.TraceTagSpec, row dumptrace.Row, traceIDName, spanIDName string) []*modelv1.TagValue {
	if len(specs) == 0 {
		return nil
	}
	out := make([]*modelv1.TagValue, 0, len(specs))
	for _, t := range specs {
		switch t.Name {
		case traceIDName:
			out = append(out, &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: row.TraceID}}})
			continue
		case spanIDName:
			out = append(out, &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: row.SpanID}}})
			continue
		}
		raw, ok := row.Tags[t.Name]
		if !ok {
			out = append(out, pbv1.NullTagValue)
			continue
		}
		vt, hasType := row.TagTypes[t.Name]
		if !hasType {
			vt = tagTypeToValueType(t.Type)
		}
		out = append(out, dump.DecodeTagValue(vt, raw, nil))
	}
	return out
}
