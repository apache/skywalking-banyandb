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

package property

import (
	"fmt"
	"hash/crc32"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	schemav1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/schema/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
)

// Tag key constants for schema properties.
const (
	TagKeyGroup     = "group"
	TagKeyName      = "name"
	TagKeySource    = "source"
	TagKeyKind      = "kind"
	TagKeyUpdatedAt = "updated_at"
)

// BuildPropertyID returns the property ID for a schema resource.
// Format: kind.String() + "_" + group + "/" + name; for KindGroup: kind.String() + "_" + name.
func BuildPropertyID(kind schema.Kind, metadata *commonv1.Metadata) string {
	if kind == schema.KindGroup {
		return kind.String() + "_" + metadata.GetName()
	}
	return kind.String() + "_" + metadata.GetGroup() + "/" + metadata.GetName()
}

// BuildPropertyIDFromMeta builds a property ID from a TypeMeta.
func BuildPropertyIDFromMeta(meta schema.TypeMeta) string {
	if meta.Kind == schema.KindGroup {
		return meta.Kind.String() + "_" + meta.Name
	}
	return meta.Kind.String() + "_" + meta.Group + "/" + meta.Name
}

// SchemaToProperty converts a schema proto to a property for storage in the schema server.
func SchemaToProperty(kind schema.Kind, spec proto.Message) (*propertyv1.Property, error) {
	metadata, metaErr := getMetadataFromSpec(kind, spec)
	if metaErr != nil {
		return nil, metaErr
	}
	data, marshalErr := protojson.Marshal(spec)
	if marshalErr != nil {
		return nil, fmt.Errorf("failed to marshal schema to protojson: %w", marshalErr)
	}
	updatedAt, updErr := getUpdatedAtFromSpec(kind, spec)
	if updErr != nil {
		return nil, updErr
	}
	group := metadata.GetGroup()
	name := metadata.GetName()
	var updatedAtNano int64
	if updatedAt != nil {
		updatedAtNano = updatedAt.AsTime().UnixNano()
	}
	return &propertyv1.Property{
		Metadata: &commonv1.Metadata{
			Name:        kind.String(),
			ModRevision: metadata.GetModRevision(),
		},
		Id: BuildPropertyID(kind, metadata),
		Tags: []*modelv1.Tag{
			{Key: TagKeyGroup, Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: group}}}},
			{Key: TagKeyName, Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: name}}}},
			{Key: TagKeySource, Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: string(data)}}}},
			{Key: TagKeyKind, Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: kind.String()}}}},
			{Key: TagKeyUpdatedAt, Value: &modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: updatedAtNano}}}},
		},
		UpdatedAt: updatedAt,
	}, nil
}

// ToSchema converts a property back to a schema Metadata.
func ToSchema(kind schema.Kind, prop *propertyv1.Property) (schema.Metadata, error) {
	if prop == nil {
		return schema.Metadata{}, fmt.Errorf("property is nil")
	}
	msg, newErr := newMessageForKind(kind)
	if newErr != nil {
		return schema.Metadata{}, newErr
	}
	parsed := ParseTags(prop.GetTags())
	if parsed.Source == "" {
		return schema.Metadata{}, fmt.Errorf("property %s/%s has no source tag", kind.String(), prop.GetId())
	}
	if unmarshalErr := protojson.Unmarshal([]byte(parsed.Source), msg); unmarshalErr != nil {
		return schema.Metadata{}, fmt.Errorf("failed to unmarshal schema from property: %w", unmarshalErr)
	}
	if hasMetadata, ok := msg.(schema.HasMetadata); ok && hasMetadata.GetMetadata() != nil {
		hasMetadata.GetMetadata().ModRevision = prop.GetMetadata().GetModRevision()
	}
	return schema.Metadata{
		TypeMeta: schema.TypeMeta{
			Kind:        kind,
			Name:        parsed.Name,
			Group:       parsed.Group,
			ModRevision: prop.GetMetadata().GetModRevision(),
		},
		Spec: msg,
	}, nil
}

// KindFromString returns the Kind for a given kind string (e.g. "stream", "measure").
func KindFromString(kindStr string) (schema.Kind, error) {
	for _, k := range schema.AllKinds() {
		if k.String() == kindStr {
			return k, nil
		}
	}
	return 0, fmt.Errorf("unknown kind string: %s", kindStr)
}

// ParsedTags holds pre-extracted tag values from a property.
type ParsedTags struct {
	Group     string
	Name      string
	Source    string
	Kind      string
	UpdatedAt int64
}

// ParseTags extracts all known tag values in a single pass.
func ParseTags(tags []*modelv1.Tag) ParsedTags {
	var pt ParsedTags
	for _, tag := range tags {
		switch tag.GetKey() {
		case TagKeyGroup:
			pt.Group = tag.GetValue().GetStr().GetValue()
		case TagKeyName:
			pt.Name = tag.GetValue().GetStr().GetValue()
		case TagKeySource:
			pt.Source = tag.GetValue().GetStr().GetValue()
		case TagKeyKind:
			pt.Kind = tag.GetValue().GetStr().GetValue()
		case TagKeyUpdatedAt:
			pt.UpdatedAt = tag.GetValue().GetInt().GetValue()
		}
	}
	return pt
}

func buildSchemaQuery(kind schema.Kind, group, name string, sinceRevision int64) *propertyv1.QueryRequest {
	query := &propertyv1.QueryRequest{
		Groups: []string{schema.SchemaGroup},
		Name:   kind.String(),
	}
	if name != "" {
		// for the group case, the group value could be empty
		metadata := &commonv1.Metadata{Group: group, Name: name}
		query.Ids = []string{BuildPropertyID(kind, metadata)}
		return query
	}
	var conditions []*modelv1.Criteria
	if group != "" {
		conditions = append(conditions, &modelv1.Criteria{
			Exp: &modelv1.Criteria_Condition{
				Condition: &modelv1.Condition{
					Name: TagKeyGroup,
					Op:   modelv1.Condition_BINARY_OP_EQ,
					Value: &modelv1.TagValue{
						Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: group}},
					},
				},
			},
		})
	}
	if sinceRevision > 0 {
		conditions = append(conditions, &modelv1.Criteria{
			Exp: &modelv1.Criteria_Condition{
				Condition: &modelv1.Condition{
					Name: TagKeyUpdatedAt,
					Op:   modelv1.Condition_BINARY_OP_GT,
					Value: &modelv1.TagValue{
						Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: sinceRevision}},
					},
				},
			},
		})
	}
	switch len(conditions) {
	case 1:
		query.Criteria = conditions[0]
	case 2:
		query.Criteria = &modelv1.Criteria{
			Exp: &modelv1.Criteria_Le{Le: &modelv1.LogicalExpression{
				Op:    modelv1.LogicalExpression_LOGICAL_OP_AND,
				Left:  conditions[0],
				Right: conditions[1],
			}},
		}
	}
	return query
}

func buildUpdatedSchemasQuery(sinceRevision int64) *propertyv1.QueryRequest {
	query := &propertyv1.QueryRequest{
		Groups: []string{schema.SchemaGroup},
	}
	if sinceRevision > 0 {
		query.Criteria = &modelv1.Criteria{
			Exp: &modelv1.Criteria_Condition{
				Condition: &modelv1.Condition{
					Name: TagKeyUpdatedAt,
					Op:   modelv1.Condition_BINARY_OP_GT,
					Value: &modelv1.TagValue{
						Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: sinceRevision}},
					},
				},
			},
		}
	}
	return query
}

// buildDeleteRequest builds a DeleteSchema request.
func buildDeleteRequest(kind schema.Kind, group, name string) *schemav1.DeleteSchemaRequest {
	metadata := &commonv1.Metadata{Group: group, Name: name}
	return &schemav1.DeleteSchemaRequest{
		Delete: &propertyv1.DeleteRequest{
			Group: schema.SchemaGroup,
			Name:  kind.String(),
			Id:    BuildPropertyID(kind, metadata),
		},
		UpdateAt: timestamppb.New(time.Now()),
	}
}

func getUpdatedAtFromSpec(kind schema.Kind, spec proto.Message) (*timestamppb.Timestamp, error) {
	var ts *timestamppb.Timestamp
	switch kind {
	case schema.KindGroup:
		if g, ok := spec.(*commonv1.Group); ok {
			ts = g.GetUpdatedAt()
		}
	case schema.KindStream:
		if s, ok := spec.(*databasev1.Stream); ok {
			ts = s.GetUpdatedAt()
		}
	case schema.KindMeasure:
		if m, ok := spec.(*databasev1.Measure); ok {
			ts = m.GetUpdatedAt()
		}
	case schema.KindTrace:
		if t, ok := spec.(*databasev1.Trace); ok {
			ts = t.GetUpdatedAt()
		}
	case schema.KindIndexRule:
		if ir, ok := spec.(*databasev1.IndexRule); ok {
			ts = ir.GetUpdatedAt()
		}
	case schema.KindIndexRuleBinding:
		if irb, ok := spec.(*databasev1.IndexRuleBinding); ok {
			ts = irb.GetUpdatedAt()
		}
	case schema.KindTopNAggregation:
		if t, ok := spec.(*databasev1.TopNAggregation); ok {
			ts = t.GetUpdatedAt()
		}
	case schema.KindProperty:
		if p, ok := spec.(*databasev1.Property); ok {
			ts = p.GetUpdatedAt()
		}
	case schema.KindNode:
		// Node does not have an UpdatedAt field.
	default:
		return nil, fmt.Errorf("unsupported kind: %s", kind)
	}
	if ts != nil {
		return ts, nil
	}
	return timestamppb.Now(), nil
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
	case schema.KindNode:
		if n, ok := spec.(*databasev1.Node); ok {
			return n.GetMetadata(), nil
		}
	case schema.KindProperty:
		if p, ok := spec.(*databasev1.Property); ok {
			return p.GetMetadata(), nil
		}
	case schema.KindMask:
		return nil, fmt.Errorf("unsupported kind: %s", kind)
	}
	return nil, fmt.Errorf("unsupported kind: %s", kind)
}

func newMessageForKind(kind schema.Kind) (proto.Message, error) {
	switch kind {
	case schema.KindGroup:
		return &commonv1.Group{}, nil
	case schema.KindStream:
		return &databasev1.Stream{}, nil
	case schema.KindMeasure:
		return &databasev1.Measure{}, nil
	case schema.KindTrace:
		return &databasev1.Trace{}, nil
	case schema.KindIndexRuleBinding:
		return &databasev1.IndexRuleBinding{}, nil
	case schema.KindIndexRule:
		return &databasev1.IndexRule{}, nil
	case schema.KindTopNAggregation:
		return &databasev1.TopNAggregation{}, nil
	case schema.KindNode:
		return &databasev1.Node{}, nil
	case schema.KindProperty:
		return &databasev1.Property{}, nil
	default:
		return nil, schema.ErrUnsupportedEntityType
	}
}

// generateCRC32ID generates a CRC32 ID for an index rule.
func generateCRC32ID(group, name string) uint32 {
	buf := []byte(group)
	buf = append(buf, name...)
	return crc32.ChecksumIEEE(buf)
}
