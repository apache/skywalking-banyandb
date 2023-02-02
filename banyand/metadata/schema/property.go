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

package schema

import (
	"context"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
)

const all = "*"

var propertyKeyPrefix = "/properties/"

func (e *etcdSchemaRegistry) GetProperty(ctx context.Context, metadata *propertyv1.Metadata, tags []string) (*propertyv1.Property, error) {
	var entity propertyv1.Property
	if err := e.get(ctx, formatPropertyKey(transformKey(metadata)), &entity); err != nil {
		return nil, err
	}
	return filterTags(&entity, tags), nil
}

func filterTags(property *propertyv1.Property, tags []string) *propertyv1.Property {
	if len(tags) == 0 || tags[0] == all {
		return property
	}
	filtered := &propertyv1.Property{
		Metadata:  property.Metadata,
		UpdatedAt: property.UpdatedAt,
	}

	for _, expectedTag := range tags {
		for _, t := range property.Tags {
			if t.Key == expectedTag {
				filtered.Tags = append(filtered.Tags, t)
			}
		}
	}
	return filtered
}

func (e *etcdSchemaRegistry) ListProperty(ctx context.Context, container *commonv1.Metadata, ids []string, tags []string) ([]*propertyv1.Property, error) {
	if container.Group == "" {
		return nil, BadRequest("container.group", "group should not be empty")
	}
	messages, err := e.listWithPrefix(ctx, listPrefixesForEntity(container.Group, propertyKeyPrefix+container.Name), func() proto.Message {
		return &propertyv1.Property{}
	})
	if err != nil {
		return nil, err
	}
	entities := make([]*propertyv1.Property, 0, len(messages))
	for _, message := range messages {
		p := message.(*propertyv1.Property)
		if len(ids) < 1 || ids[0] == all {
			entities = append(entities, filterTags(p, tags))
			continue
		}
		for _, id := range ids {
			if p.Metadata.Id == id {
				entities = append(entities, filterTags(p, tags))
			}
		}
	}
	return entities, nil
}

func (e *etcdSchemaRegistry) ApplyProperty(ctx context.Context, property *propertyv1.Property, strategy propertyv1.ApplyRequest_Strategy) (bool, uint32, error) {
	m := transformKey(property.GetMetadata())
	group := m.GetGroup()
	if _, getGroupErr := e.GetGroup(ctx, group); getGroupErr != nil {
		return false, 0, errors.Wrap(getGroupErr, "group is not exist")
	}
	if property.UpdatedAt != nil {
		property.UpdatedAt = timestamppb.Now()
	}
	md := Metadata{
		TypeMeta: TypeMeta{
			Kind:  KindProperty,
			Group: group,
			Name:  m.GetName(),
		},
		Spec: property,
	}
	tagsNum := uint32(len(property.Tags))
	err := e.create(ctx, md)
	if err == nil {
		return true, tagsNum, nil
	}
	if !errors.Is(err, errGRPCAlreadyExists) {
		return false, 0, err
	}
	if strategy != propertyv1.ApplyRequest_STRATEGY_REPLACE {
		existed, errGet := e.GetProperty(ctx, property.Metadata, nil)
		if errGet != nil {
			return false, 0, errGet
		}
		for i := 0; i < int(tagsNum); i++ {
			t := property.Tags[0]
			property.Tags = property.Tags[1:]
			for _, et := range existed.Tags {
				if et.Key == t.Key {
					et.Value = t.Value
				}
			}
		}
		existed.Tags = append(existed.Tags, property.Tags...)
		md.Spec = existed
	}
	if err = e.update(ctx, md); err != nil {
		return false, 0, err
	}
	return false, tagsNum, nil
}

func (e *etcdSchemaRegistry) DeleteProperty(ctx context.Context, metadata *propertyv1.Metadata, tags []string) (bool, uint32, error) {
	if len(tags) == 0 || tags[0] == all {
		m := transformKey(metadata)
		deleted, err := e.delete(ctx, Metadata{
			TypeMeta: TypeMeta{
				Kind:  KindProperty,
				Group: m.GetGroup(),
				Name:  m.GetName(),
			},
		})
		return deleted, 0, err
	}
	property, err := e.GetProperty(ctx, metadata, nil)
	if err != nil {
		return false, 0, err
	}
	filtered := &propertyv1.Property{
		Metadata:  property.Metadata,
		UpdatedAt: property.UpdatedAt,
	}

	for _, expectedTag := range tags {
		for _, t := range property.Tags {
			if t.Key != expectedTag {
				filtered.Tags = append(filtered.Tags, t)
			}
		}
	}
	_, num, err := e.ApplyProperty(ctx, filtered, propertyv1.ApplyRequest_STRATEGY_REPLACE)
	return true, num, err
}

func transformKey(metadata *propertyv1.Metadata) *commonv1.Metadata {
	return &commonv1.Metadata{
		Group: metadata.Container.GetGroup(),
		Name:  metadata.Container.Name + "/" + metadata.Id,
	}
}

func formatPropertyKey(metadata *commonv1.Metadata) string {
	return formatKey(propertyKeyPrefix, metadata)
}
