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
package tools

import (
	"fmt"
	"strings"
	"time"

	"google.golang.org/protobuf/encoding/protojson"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/session"
)

// The schema endpoints return one protobuf message per resource type. Everything below turns those
// messages into the type-neutral snapshot the planner and the TUI read.

func summarizeSchema(req SchemaRequest, body []byte, updatedAt time.Time) (session.SchemaSnapshot, error) {
	base := session.SchemaSnapshot{
		UpdatedAt: updatedAt,
		Type:      req.Type,
		Name:      req.Name,
		Groups:    append([]string(nil), req.Groups...),
	}
	switch req.Type {
	case session.ResourceTypeMeasure:
		measure, parseErr := parseMeasureSchema(body)
		if parseErr != nil {
			return session.SchemaSnapshot{}, parseErr
		}
		base.Tags = tagFamilies(measure.GetTagFamilies())
		base.EntityTags = entityTagNames(measure.GetEntity())
		base.Fields = fieldNames(measure.GetFields())
		base.Columns = append(tagFamilyColumns(measure.GetTagFamilies()), fieldColumns(measure.GetFields())...)
		return base, nil
	case session.ResourceTypeStream:
		stream, parseErr := parseStreamSchema(body)
		if parseErr != nil {
			return session.SchemaSnapshot{}, parseErr
		}
		base.Tags = tagFamilies(stream.GetTagFamilies())
		base.EntityTags = entityTagNames(stream.GetEntity())
		base.Columns = tagFamilyColumns(stream.GetTagFamilies())
		return base, nil
	case session.ResourceTypeTrace:
		trace, parseErr := parseTraceSchema(body)
		if parseErr != nil {
			return session.SchemaSnapshot{}, parseErr
		}
		base.Tags = traceTagNames(trace.GetTags())
		base.Columns = traceTagColumns(trace.GetTags())
		base.TraceIDTag = strings.TrimSpace(trace.GetTraceIdTagName())
		base.TimestampTag = strings.TrimSpace(trace.GetTimestampTagName())
		return base, nil
	case session.ResourceTypeProperty:
		property, parseErr := parsePropertySchema(body)
		if parseErr != nil {
			return session.SchemaSnapshot{}, parseErr
		}
		base.Tags = tagNames(property.GetTags())
		base.Columns = tagColumns(property.GetTags(), session.SchemaColumnTag)
		return base, nil
	case session.ResourceTypeTopN:
		topN, parseErr := parseTopNSchema(body)
		if parseErr != nil {
			return session.SchemaSnapshot{}, parseErr
		}
		base.Tags = append([]string(nil), topN.GetGroupByTagNames()...)
		base.Fields = compactStrings([]string{topN.GetFieldName()})
		base.SourceMeasure = strings.TrimSpace(topN.GetSourceMeasure().GetName())
		base.SourceMeasureGroup = strings.TrimSpace(topN.GetSourceMeasure().GetGroup())
		base.FieldValueSort = topN.GetFieldValueSort().String()
		for _, tagName := range base.Tags {
			base.Columns = append(base.Columns, session.SchemaColumn{Name: tagName, Kind: session.SchemaColumnTag})
		}
		if fieldName := strings.TrimSpace(topN.GetFieldName()); fieldName != "" {
			base.Columns = append(base.Columns, session.SchemaColumn{Name: fieldName, Kind: session.SchemaColumnField})
		}
		return base, nil
	default:
		return session.SchemaSnapshot{}, fmt.Errorf("unsupported resource type: %s", req.Type)
	}
}

func parseMeasureSchema(body []byte) (*databasev1.Measure, error) {
	wrapped := new(databasev1.MeasureRegistryServiceGetResponse)
	if unmarshalErr := protojson.Unmarshal(body, wrapped); unmarshalErr == nil {
		if measure := wrapped.GetMeasure(); measure != nil {
			return measure, nil
		}
	}
	measure := new(databasev1.Measure)
	if unmarshalErr := protojson.Unmarshal(body, measure); unmarshalErr != nil {
		return nil, unmarshalErr
	}
	if measure.GetMetadata() == nil {
		return nil, fmt.Errorf("measure schema missing in response")
	}
	return measure, nil
}

func parseStreamSchema(body []byte) (*databasev1.Stream, error) {
	wrapped := new(databasev1.StreamRegistryServiceGetResponse)
	if unmarshalErr := protojson.Unmarshal(body, wrapped); unmarshalErr == nil {
		if stream := wrapped.GetStream(); stream != nil {
			return stream, nil
		}
	}
	stream := new(databasev1.Stream)
	if unmarshalErr := protojson.Unmarshal(body, stream); unmarshalErr != nil {
		return nil, unmarshalErr
	}
	if stream.GetMetadata() == nil {
		return nil, fmt.Errorf("stream schema missing in response")
	}
	return stream, nil
}

func parseTraceSchema(body []byte) (*databasev1.Trace, error) {
	wrapped := new(databasev1.TraceRegistryServiceGetResponse)
	if unmarshalErr := protojson.Unmarshal(body, wrapped); unmarshalErr == nil {
		if trace := wrapped.GetTrace(); trace != nil {
			return trace, nil
		}
	}
	trace := new(databasev1.Trace)
	if unmarshalErr := protojson.Unmarshal(body, trace); unmarshalErr != nil {
		return nil, unmarshalErr
	}
	if trace.GetMetadata() == nil {
		return nil, fmt.Errorf("trace schema missing in response")
	}
	return trace, nil
}

func parsePropertySchema(body []byte) (*databasev1.Property, error) {
	wrapped := new(databasev1.PropertyRegistryServiceGetResponse)
	if unmarshalErr := protojson.Unmarshal(body, wrapped); unmarshalErr == nil {
		if property := wrapped.GetProperty(); property != nil {
			return property, nil
		}
	}
	property := new(databasev1.Property)
	if unmarshalErr := protojson.Unmarshal(body, property); unmarshalErr != nil {
		return nil, unmarshalErr
	}
	if property.GetMetadata() == nil {
		return nil, fmt.Errorf("property schema missing in response")
	}
	return property, nil
}

func parseTopNSchema(body []byte) (*databasev1.TopNAggregation, error) {
	wrapped := new(databasev1.TopNAggregationRegistryServiceGetResponse)
	if unmarshalErr := protojson.Unmarshal(body, wrapped); unmarshalErr == nil {
		if topN := wrapped.GetTopNAggregation(); topN != nil {
			return topN, nil
		}
	}
	topN := new(databasev1.TopNAggregation)
	if unmarshalErr := protojson.Unmarshal(body, topN); unmarshalErr != nil {
		return nil, unmarshalErr
	}
	if topN.GetMetadata() == nil {
		return nil, fmt.Errorf("topn schema missing in response")
	}
	return topN, nil
}

func tagFamilies(families []*databasev1.TagFamilySpec) []string {
	var tags []string
	for _, family := range families {
		familyName := strings.TrimSpace(family.GetName())
		for _, tag := range family.GetTags() {
			tagName := strings.TrimSpace(tag.GetName())
			if tagName == "" {
				continue
			}
			if familyName != "" {
				tags = append(tags, familyName+"."+tagName)
				continue
			}
			tags = append(tags, tagName)
		}
	}
	return compactStrings(tags)
}

func tagFamilyColumns(families []*databasev1.TagFamilySpec) []session.SchemaColumn {
	var columns []session.SchemaColumn
	for _, family := range families {
		familyName := strings.TrimSpace(family.GetName())
		for _, tag := range family.GetTags() {
			tagName := strings.TrimSpace(tag.GetName())
			if tagName == "" {
				continue
			}
			if familyName != "" {
				tagName = familyName + "." + tagName
			}
			columns = append(columns, session.SchemaColumn{
				Name: tagName,
				Kind: session.SchemaColumnTag,
				Type: tagValueType(tag.GetType()),
			})
		}
	}
	return columns
}

func entityTagNames(entity *databasev1.Entity) []string {
	if entity == nil {
		return nil
	}
	return compactStrings(entity.GetTagNames())
}

func tagNames(tags []*databasev1.TagSpec) []string {
	var names []string
	for _, tag := range tags {
		names = append(names, tag.GetName())
	}
	return compactStrings(names)
}

func tagColumns(tags []*databasev1.TagSpec, kind session.SchemaColumnKind) []session.SchemaColumn {
	columns := make([]session.SchemaColumn, 0, len(tags))
	for _, tag := range tags {
		tagName := strings.TrimSpace(tag.GetName())
		if tagName == "" {
			continue
		}
		columns = append(columns, session.SchemaColumn{Name: tagName, Kind: kind, Type: tagValueType(tag.GetType())})
	}
	return columns
}

func traceTagNames(tags []*databasev1.TraceTagSpec) []string {
	var names []string
	for _, tag := range tags {
		names = append(names, tag.GetName())
	}
	return compactStrings(names)
}

func traceTagColumns(tags []*databasev1.TraceTagSpec) []session.SchemaColumn {
	columns := make([]session.SchemaColumn, 0, len(tags))
	for _, tag := range tags {
		tagName := strings.TrimSpace(tag.GetName())
		if tagName == "" {
			continue
		}
		columns = append(columns, session.SchemaColumn{
			Name: tagName,
			Kind: session.SchemaColumnTag,
			Type: tagValueType(tag.GetType()),
		})
	}
	return columns
}

func fieldNames(fields []*databasev1.FieldSpec) []string {
	var names []string
	for _, field := range fields {
		names = append(names, field.GetName())
	}
	return compactStrings(names)
}

func fieldColumns(fields []*databasev1.FieldSpec) []session.SchemaColumn {
	columns := make([]session.SchemaColumn, 0, len(fields))
	for _, field := range fields {
		fieldName := strings.TrimSpace(field.GetName())
		if fieldName == "" {
			continue
		}
		columns = append(columns, session.SchemaColumn{
			Name: fieldName,
			Kind: session.SchemaColumnField,
			Type: fieldValueType(field.GetFieldType()),
		})
	}
	return columns
}

func markIndexedColumns(columns []session.SchemaColumn, indexedFields []string) []session.SchemaColumn {
	indexedColumns := append([]session.SchemaColumn(nil), columns...)
	for columnIndex := range indexedColumns {
		for _, indexedField := range indexedFields {
			if matchesColumnName(indexedColumns[columnIndex].Name, indexedField) {
				indexedColumns[columnIndex].Indexed = true
				break
			}
		}
	}
	return indexedColumns
}

func matchesColumnName(columnName, requestedName string) bool {
	if strings.EqualFold(strings.TrimSpace(columnName), strings.TrimSpace(requestedName)) {
		return true
	}
	lastDot := strings.LastIndex(columnName, ".")
	return lastDot >= 0 && strings.EqualFold(columnName[lastDot+1:], strings.TrimSpace(requestedName))
}

func tagValueType(tagType databasev1.TagType) session.SchemaValueType {
	switch tagType {
	case databasev1.TagType_TAG_TYPE_STRING:
		return session.SchemaValueTypeString
	case databasev1.TagType_TAG_TYPE_INT:
		return session.SchemaValueTypeInt
	case databasev1.TagType_TAG_TYPE_STRING_ARRAY:
		return session.SchemaValueTypeStringArray
	case databasev1.TagType_TAG_TYPE_INT_ARRAY:
		return session.SchemaValueTypeIntArray
	case databasev1.TagType_TAG_TYPE_TIMESTAMP:
		return session.SchemaValueTypeTimestamp
	case databasev1.TagType_TAG_TYPE_DATA_BINARY:
		return session.SchemaValueTypeBinary
	default:
		return session.SchemaValueTypeUnknown
	}
}

func fieldValueType(fieldType databasev1.FieldType) session.SchemaValueType {
	switch fieldType {
	case databasev1.FieldType_FIELD_TYPE_STRING:
		return session.SchemaValueTypeString
	case databasev1.FieldType_FIELD_TYPE_INT:
		return session.SchemaValueTypeInt
	case databasev1.FieldType_FIELD_TYPE_FLOAT:
		return session.SchemaValueTypeFloat
	case databasev1.FieldType_FIELD_TYPE_DATA_BINARY:
		return session.SchemaValueTypeBinary
	default:
		return session.SchemaValueTypeUnknown
	}
}

func compactStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	var compactedValues []string
	for _, value := range values {
		trimmedValue := strings.TrimSpace(value)
		if trimmedValue == "" {
			continue
		}
		if _, ok := seen[trimmedValue]; ok {
			continue
		}
		seen[trimmedValue] = struct{}{}
		compactedValues = append(compactedValues, trimmedValue)
	}
	return compactedValues
}
