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

// Package tools implements controlled schema discovery and BYDBQL execution for the TUI.
package tools

import (
	"fmt"
	"strings"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/session"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/tuitext"
)

// The schema endpoints return one protobuf message per resource type. Everything below turns those
// messages into the type-neutral snapshot the planner and the TUI read.

func summarizeSchema(req SchemaRequest, body []byte, updatedAt time.Time) (session.SchemaSnapshot, error) {
	codec, codecErr := resourceCodecFor(req.Type)
	if codecErr != nil {
		return session.SchemaSnapshot{}, codecErr
	}
	return codec.summarize(req, body, updatedAt)
}

func baseSchemaSnapshot(req SchemaRequest, updatedAt time.Time) session.SchemaSnapshot {
	return session.SchemaSnapshot{
		UpdatedAt: updatedAt,
		Type:      req.Type,
		Name:      req.Name,
		Groups:    append([]string(nil), req.Groups...),
	}
}

func summarizeMeasureSchema(req SchemaRequest, body []byte, updatedAt time.Time) (session.SchemaSnapshot, error) {
	measure, parseErr := parseMeasureSchema(body)
	if parseErr != nil {
		return session.SchemaSnapshot{}, parseErr
	}
	snapshot := baseSchemaSnapshot(req, updatedAt)
	snapshot.Tags = tagFamilies(measure.GetTagFamilies())
	snapshot.EntityTags = entityTagNames(measure.GetEntity())
	snapshot.Fields = fieldNames(measure.GetFields())
	snapshot.Columns = append(tagFamilyColumns(measure.GetTagFamilies()), fieldColumns(measure.GetFields())...)
	return snapshot, nil
}

func summarizeStreamSchema(req SchemaRequest, body []byte, updatedAt time.Time) (session.SchemaSnapshot, error) {
	stream, parseErr := parseStreamSchema(body)
	if parseErr != nil {
		return session.SchemaSnapshot{}, parseErr
	}
	snapshot := baseSchemaSnapshot(req, updatedAt)
	snapshot.Tags = tagFamilies(stream.GetTagFamilies())
	snapshot.EntityTags = entityTagNames(stream.GetEntity())
	snapshot.Columns = tagFamilyColumns(stream.GetTagFamilies())
	return snapshot, nil
}

func summarizeTraceSchema(req SchemaRequest, body []byte, updatedAt time.Time) (session.SchemaSnapshot, error) {
	trace, parseErr := parseTraceSchema(body)
	if parseErr != nil {
		return session.SchemaSnapshot{}, parseErr
	}
	snapshot := baseSchemaSnapshot(req, updatedAt)
	snapshot.Tags = traceTagNames(trace.GetTags())
	snapshot.Columns = traceTagColumns(trace.GetTags())
	snapshot.TraceIDTag = strings.TrimSpace(trace.GetTraceIdTagName())
	snapshot.TimestampTag = strings.TrimSpace(trace.GetTimestampTagName())
	return snapshot, nil
}

func summarizePropertySchema(req SchemaRequest, body []byte, updatedAt time.Time) (session.SchemaSnapshot, error) {
	property, parseErr := parsePropertySchema(body)
	if parseErr != nil {
		return session.SchemaSnapshot{}, parseErr
	}
	snapshot := baseSchemaSnapshot(req, updatedAt)
	snapshot.Tags = tagNames(property.GetTags())
	snapshot.Columns = tagColumns(property.GetTags(), session.SchemaColumnTag)
	return snapshot, nil
}

func summarizeTopNSchema(req SchemaRequest, body []byte, updatedAt time.Time) (session.SchemaSnapshot, error) {
	topN, parseErr := parseTopNSchema(body)
	if parseErr != nil {
		return session.SchemaSnapshot{}, parseErr
	}
	snapshot := baseSchemaSnapshot(req, updatedAt)
	snapshot.Tags = append([]string(nil), topN.GetGroupByTagNames()...)
	snapshot.Fields = tuitext.Compact([]string{topN.GetFieldName()})
	snapshot.SourceMeasure = strings.TrimSpace(topN.GetSourceMeasure().GetName())
	snapshot.SourceMeasureGroup = strings.TrimSpace(topN.GetSourceMeasure().GetGroup())
	snapshot.FieldValueSort = topN.GetFieldValueSort().String()
	for _, tagName := range snapshot.Tags {
		snapshot.Columns = append(snapshot.Columns, session.SchemaColumn{Name: tagName, Kind: session.SchemaColumnTag})
	}
	if fieldName := strings.TrimSpace(topN.GetFieldName()); fieldName != "" {
		snapshot.Columns = append(snapshot.Columns, session.SchemaColumn{Name: fieldName, Kind: session.SchemaColumnField})
	}
	return snapshot, nil
}

// parseSchema decodes a schema response that may be wrapped in its registry Get envelope.
//
// The HTTP gateway returns the envelope, while a cached or hand-supplied body may hold the bare
// resource, so both shapes are accepted and the presence of metadata is what proves a real schema.
func parseSchema[W proto.Message, R proto.Message](
	body []byte,
	newWrapped func() W,
	unwrap func(W) R,
	newBare func() R,
	metadataOf func(R) *commonv1.Metadata,
	resourceName string,
) (R, error) {
	wrapped := newWrapped()
	if unmarshalErr := protojson.Unmarshal(body, wrapped); unmarshalErr == nil {
		if resource := unwrap(wrapped); metadataOf(resource) != nil {
			return resource, nil
		}
	}
	bare := newBare()
	if unmarshalErr := protojson.Unmarshal(body, bare); unmarshalErr != nil {
		var zero R
		return zero, fmt.Errorf("failed to decode %s schema: %w", resourceName, unmarshalErr)
	}
	if metadataOf(bare) == nil {
		var zero R
		return zero, fmt.Errorf("%s schema missing in response", resourceName)
	}
	return bare, nil
}

func parseMeasureSchema(body []byte) (*databasev1.Measure, error) {
	return parseSchema(body,
		func() *databasev1.MeasureRegistryServiceGetResponse {
			return new(databasev1.MeasureRegistryServiceGetResponse)
		},
		(*databasev1.MeasureRegistryServiceGetResponse).GetMeasure,
		func() *databasev1.Measure { return new(databasev1.Measure) },
		(*databasev1.Measure).GetMetadata, "measure")
}

func parseStreamSchema(body []byte) (*databasev1.Stream, error) {
	return parseSchema(body,
		func() *databasev1.StreamRegistryServiceGetResponse {
			return new(databasev1.StreamRegistryServiceGetResponse)
		},
		(*databasev1.StreamRegistryServiceGetResponse).GetStream,
		func() *databasev1.Stream { return new(databasev1.Stream) },
		(*databasev1.Stream).GetMetadata, "stream")
}

func parseTraceSchema(body []byte) (*databasev1.Trace, error) {
	return parseSchema(body,
		func() *databasev1.TraceRegistryServiceGetResponse {
			return new(databasev1.TraceRegistryServiceGetResponse)
		},
		(*databasev1.TraceRegistryServiceGetResponse).GetTrace,
		func() *databasev1.Trace { return new(databasev1.Trace) },
		(*databasev1.Trace).GetMetadata, "trace")
}

func parsePropertySchema(body []byte) (*databasev1.Property, error) {
	return parseSchema(body,
		func() *databasev1.PropertyRegistryServiceGetResponse {
			return new(databasev1.PropertyRegistryServiceGetResponse)
		},
		(*databasev1.PropertyRegistryServiceGetResponse).GetProperty,
		func() *databasev1.Property { return new(databasev1.Property) },
		(*databasev1.Property).GetMetadata, "property")
}

func parseTopNSchema(body []byte) (*databasev1.TopNAggregation, error) {
	return parseSchema(body,
		func() *databasev1.TopNAggregationRegistryServiceGetResponse {
			return new(databasev1.TopNAggregationRegistryServiceGetResponse)
		},
		(*databasev1.TopNAggregationRegistryServiceGetResponse).GetTopNAggregation,
		func() *databasev1.TopNAggregation { return new(databasev1.TopNAggregation) },
		(*databasev1.TopNAggregation).GetMetadata, "topn")
}

// flattenTagFamilies visits every named tag in a family list under its qualified "family.tag" name.
//
// Measure and Stream nest their tags one level deep; a tag outside a named family keeps its bare
// name so it matches how BYDBQL refers to it.
func flattenTagFamilies(families []*databasev1.TagFamilySpec, visit func(qualifiedName string, tag *databasev1.TagSpec)) {
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
			visit(tagName, tag)
		}
	}
}

func tagFamilies(families []*databasev1.TagFamilySpec) []string {
	var tags []string
	flattenTagFamilies(families, func(qualifiedName string, _ *databasev1.TagSpec) {
		tags = append(tags, qualifiedName)
	})
	return tuitext.Compact(tags)
}

func tagFamilyColumns(families []*databasev1.TagFamilySpec) []session.SchemaColumn {
	var columns []session.SchemaColumn
	flattenTagFamilies(families, func(qualifiedName string, tag *databasev1.TagSpec) {
		columns = append(columns, session.SchemaColumn{
			Name: qualifiedName,
			Kind: session.SchemaColumnTag,
			Type: tagValueType(tag.GetType()),
		})
	})
	return columns
}

func entityTagNames(entity *databasev1.Entity) []string {
	if entity == nil {
		return nil
	}
	return tuitext.Compact(entity.GetTagNames())
}

// specNames collects the distinct trimmed names of a tag or field spec list.
func specNames[S any](specs []*S, nameOf func(*S) string) []string {
	names := make([]string, 0, len(specs))
	for _, spec := range specs {
		names = append(names, nameOf(spec))
	}
	return tuitext.Compact(names)
}

// specColumns turns a tag or field spec list into typed columns, skipping unnamed entries.
func specColumns[S any](specs []*S, kind session.SchemaColumnKind, nameOf func(*S) string, typeOf func(*S) session.SchemaValueType) []session.SchemaColumn {
	columns := make([]session.SchemaColumn, 0, len(specs))
	for _, spec := range specs {
		name := strings.TrimSpace(nameOf(spec))
		if name == "" {
			continue
		}
		columns = append(columns, session.SchemaColumn{Name: name, Kind: kind, Type: typeOf(spec)})
	}
	return columns
}

func tagNames(tags []*databasev1.TagSpec) []string {
	return specNames(tags, (*databasev1.TagSpec).GetName)
}

func tagColumns(tags []*databasev1.TagSpec, kind session.SchemaColumnKind) []session.SchemaColumn {
	return specColumns(tags, kind, (*databasev1.TagSpec).GetName, func(tag *databasev1.TagSpec) session.SchemaValueType {
		return tagValueType(tag.GetType())
	})
}

func traceTagNames(tags []*databasev1.TraceTagSpec) []string {
	return specNames(tags, (*databasev1.TraceTagSpec).GetName)
}

func traceTagColumns(tags []*databasev1.TraceTagSpec) []session.SchemaColumn {
	return specColumns(tags, session.SchemaColumnTag, (*databasev1.TraceTagSpec).GetName, func(tag *databasev1.TraceTagSpec) session.SchemaValueType {
		return tagValueType(tag.GetType())
	})
}

func fieldNames(fields []*databasev1.FieldSpec) []string {
	return specNames(fields, (*databasev1.FieldSpec).GetName)
}

func fieldColumns(fields []*databasev1.FieldSpec) []session.SchemaColumn {
	return specColumns(fields, session.SchemaColumnField, (*databasev1.FieldSpec).GetName, func(field *databasev1.FieldSpec) session.SchemaValueType {
		return fieldValueType(field.GetFieldType())
	})
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
