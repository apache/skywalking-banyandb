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

// Package property implements property-based metadata storage.
package property

import (
	"fmt"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
)

const (
	// SchemaGroup is the constant group name for schema storage.
	SchemaGroup = "_schema"

	// TagKeyGroup is the tag key for group name.
	TagKeyGroup = "group"
	// TagKeyName is the tag key for resource name.
	TagKeyName = "name"
	// TagKeySource is the tag key for serialized source.
	TagKeySource = "source"
	// TagKeyKind is the tag key for schema kind.
	TagKeyKind = "kind"
	// TagKeyUpdatedAt is the tag key for updated at timestamp.
	TagKeyUpdatedAt = "updated_at"
)

var errUnsupportedKind = errors.New("unsupported schema kind")

// SchemaToProperty converts a schema spec to Property format.
func SchemaToProperty(kind schema.Kind, spec proto.Message) (*propertyv1.Property, error) {
	metadata, schemaErr := getMetadataFromSpec(kind, spec)
	if schemaErr != nil {
		return nil, schemaErr
	}
	updateAt, schemaErr := getUpdateAtFromSpec(kind, spec)
	if schemaErr != nil {
		return nil, schemaErr
	}
	sourceBytes, marshalErr := protojson.Marshal(spec)
	if marshalErr != nil {
		return nil, fmt.Errorf("failed to marshal spec: %w", marshalErr)
	}
	propID := BuildPropertyID(kind, metadata)
	prop := &propertyv1.Property{
		Metadata: &commonv1.Metadata{
			Group:       SchemaGroup,
			Name:        kind.String(),
			ModRevision: metadata.GetModRevision(),
		},
		Id: propID,
		Tags: []*modelv1.Tag{
			{Key: TagKeyGroup, Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: metadata.GetGroup()}}}},
			{Key: TagKeyName, Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: metadata.GetName()}}}},
			{Key: TagKeySource, Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: string(sourceBytes)}}}},
			{Key: TagKeyKind, Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: kind.String()}}}},
			{Key: TagKeyUpdatedAt, Value: &modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: updateAt.AsTime().UnixNano()}}}},
		},
		UpdatedAt: updateAt,
	}
	return prop, nil
}

// ToSchema converts Property back to schema spec.
func ToSchema(kind schema.Kind, prop *propertyv1.Property) (schema.Metadata, error) {
	var sourceBytes string
	for _, tag := range prop.Tags {
		if tag.Key == TagKeySource {
			if strData := tag.Value.GetStr(); strData != nil {
				sourceBytes = strData.Value
			}
			break
		}
	}
	if len(sourceBytes) == 0 {
		return schema.Metadata{}, errors.New("source tag not found in property")
	}
	spec, unmarshalErr := unmarshalSpec(kind, sourceBytes)
	if unmarshalErr != nil {
		return schema.Metadata{}, unmarshalErr
	}
	metadata, metaErr := getMetadataFromSpec(kind, spec)
	if metaErr != nil {
		return schema.Metadata{}, metaErr
	}
	metadata.ModRevision = prop.Metadata.GetModRevision()
	return schema.Metadata{
		TypeMeta: schema.TypeMeta{
			Kind:        kind,
			Name:        metadata.GetName(),
			Group:       metadata.GetGroup(),
			ModRevision: metadata.GetModRevision(),
		},
		Spec: spec,
	}, nil
}

// BuildPropertyID creates unique ID: kind.String() + "_" + group + "/" + name.
func BuildPropertyID(kind schema.Kind, metadata *commonv1.Metadata) string {
	if kind == schema.KindGroup {
		return kind.String() + "_" + metadata.GetName()
	}
	return kind.String() + "_" + metadata.GetGroup() + "/" + metadata.GetName()
}

// BuildPropertyIDFromMeta creates unique ID from schema.TypeMeta.
func BuildPropertyIDFromMeta(meta schema.TypeMeta) string {
	if meta.Kind == schema.KindGroup {
		return meta.Kind.String() + "_" + meta.Name
	}
	return meta.Kind.String() + "_" + meta.Group + "/" + meta.Name
}

func getMetadataFromSpec(kind schema.Kind, spec proto.Message) (*commonv1.Metadata, error) {
	switch kind {
	case schema.KindGroup:
		if g, ok := spec.(*commonv1.Group); ok {
			return g.GetMetadata(), nil
		}
	case schema.KindStream:
		if s, ok := spec.(*databasev1.Stream); ok {
			return s.GetMetadata(), nil
		}
	case schema.KindMeasure:
		if m, ok := spec.(*databasev1.Measure); ok {
			return m.GetMetadata(), nil
		}
	case schema.KindTrace:
		if t, ok := spec.(*databasev1.Trace); ok {
			return t.GetMetadata(), nil
		}
	case schema.KindIndexRule:
		if ir, ok := spec.(*databasev1.IndexRule); ok {
			return ir.GetMetadata(), nil
		}
	case schema.KindIndexRuleBinding:
		if irb, ok := spec.(*databasev1.IndexRuleBinding); ok {
			return irb.GetMetadata(), nil
		}
	case schema.KindTopNAggregation:
		if t, ok := spec.(*databasev1.TopNAggregation); ok {
			return t.GetMetadata(), nil
		}
	case schema.KindProperty:
		if p, ok := spec.(*databasev1.Property); ok {
			return p.GetMetadata(), nil
		}
	case schema.KindNode:
	case schema.KindMask:
		return nil, fmt.Errorf("%w: %s", errUnsupportedKind, kind.String())
	}
	return nil, fmt.Errorf("%w: %s", errUnsupportedKind, kind.String())
}

func getUpdateAtFromSpec(kind schema.Kind, spec proto.Message) (*timestamppb.Timestamp, error) {
	switch kind {
	case schema.KindGroup:
		if g, ok := spec.(*commonv1.Group); ok {
			return g.GetUpdatedAt(), nil
		}
	case schema.KindStream:
		if s, ok := spec.(*databasev1.Stream); ok {
			return s.GetUpdatedAt(), nil
		}
	case schema.KindMeasure:
		if m, ok := spec.(*databasev1.Measure); ok {
			return m.GetUpdatedAt(), nil
		}
	case schema.KindTrace:
		if t, ok := spec.(*databasev1.Trace); ok {
			return t.GetUpdatedAt(), nil
		}
	case schema.KindIndexRule:
		if ir, ok := spec.(*databasev1.IndexRule); ok {
			return ir.GetUpdatedAt(), nil
		}
	case schema.KindIndexRuleBinding:
		if irb, ok := spec.(*databasev1.IndexRuleBinding); ok {
			return irb.GetUpdatedAt(), nil
		}
	case schema.KindTopNAggregation:
		if t, ok := spec.(*databasev1.TopNAggregation); ok {
			return t.GetUpdatedAt(), nil
		}
	case schema.KindProperty:
		if p, ok := spec.(*databasev1.Property); ok {
			return p.GetUpdatedAt(), nil
		}
	case schema.KindNode:
	case schema.KindMask:
		return nil, fmt.Errorf("%w: %s", errUnsupportedKind, kind.String())
	}
	return nil, fmt.Errorf("%w: %s", errUnsupportedKind, kind.String())
}

func unmarshalSpec(kind schema.Kind, data string) (proto.Message, error) {
	var spec proto.Message
	switch kind {
	case schema.KindGroup:
		spec = &commonv1.Group{}
	case schema.KindStream:
		spec = &databasev1.Stream{}
	case schema.KindMeasure:
		spec = &databasev1.Measure{}
	case schema.KindTrace:
		spec = &databasev1.Trace{}
	case schema.KindIndexRule:
		spec = &databasev1.IndexRule{}
	case schema.KindIndexRuleBinding:
		spec = &databasev1.IndexRuleBinding{}
	case schema.KindTopNAggregation:
		spec = &databasev1.TopNAggregation{}
	case schema.KindNode:
		spec = &databasev1.Node{}
	case schema.KindProperty:
		spec = &databasev1.Property{}
	default:
		return nil, fmt.Errorf("%w: %s", errUnsupportedKind, kind.String())
	}
	if unmarshalErr := protojson.Unmarshal([]byte(data), spec); unmarshalErr != nil {
		return nil, fmt.Errorf("failed to unmarshal spec: %w", unmarshalErr)
	}
	return spec, nil
}

// KindFromString converts a kind string to schema.Kind.
func KindFromString(kindStr string) (schema.Kind, error) {
	switch kindStr {
	case "group":
		return schema.KindGroup, nil
	case "stream":
		return schema.KindStream, nil
	case "measure":
		return schema.KindMeasure, nil
	case "trace":
		return schema.KindTrace, nil
	case "indexRuleBinding":
		return schema.KindIndexRuleBinding, nil
	case "indexRule":
		return schema.KindIndexRule, nil
	case "topNAggregation":
		return schema.KindTopNAggregation, nil
	case "node":
		return schema.KindNode, nil
	case "property":
		return schema.KindProperty, nil
	default:
		return 0, fmt.Errorf("unknown kind: %s", kindStr)
	}
}
