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
//
package schema

import (
	"context"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
)

var PropertyKeyPrefix = "/properties/"

func (e *etcdSchemaRegistry) GetProperty(ctx context.Context, metadata *propertyv1.Metadata) (*propertyv1.Property, error) {
	var entity propertyv1.Property
	if err := e.get(ctx, formatPropertyKey(transformKey(metadata)), &entity); err != nil {
		return nil, err
	}
	return &entity, nil
}

func (e *etcdSchemaRegistry) ListProperty(ctx context.Context, container *commonv1.Metadata) ([]*propertyv1.Property, error) {
	if container.Group == "" {
		return nil, errors.Wrap(ErrGroupAbsent, "list Property")
	}
	messages, err := e.listWithPrefix(ctx, listPrefixesForEntity(container.Group, PropertyKeyPrefix+container.Name), func() proto.Message {
		return &propertyv1.Property{}
	})
	if err != nil {
		return nil, err
	}
	entities := make([]*propertyv1.Property, 0, len(messages))
	for _, message := range messages {
		entities = append(entities, message.(*propertyv1.Property))
	}
	return entities, nil
}

func (e *etcdSchemaRegistry) UpdateProperty(ctx context.Context, property *propertyv1.Property) error {
	m := transformKey(property.GetMetadata())
	return e.update(ctx, Metadata{
		TypeMeta: TypeMeta{
			Kind:  KindProperty,
			Group: m.GetGroup(),
			Name:  m.GetName(),
		},
		Spec: property,
	})
}

func (e *etcdSchemaRegistry) DeleteProperty(ctx context.Context, metadata *propertyv1.Metadata) (bool, error) {
	m := transformKey(metadata)
	return e.delete(ctx, Metadata{
		TypeMeta: TypeMeta{
			Kind:  KindProperty,
			Group: m.GetGroup(),
			Name:  m.GetName(),
		},
	})
}

func transformKey(metadata *propertyv1.Metadata) *commonv1.Metadata {
	return &commonv1.Metadata{
		Group: metadata.Container.GetGroup(),
		Name:  metadata.Container.Name + "/" + metadata.Id,
	}
}

func formatPropertyKey(metadata *commonv1.Metadata) string {
	return formatKey(PropertyKeyPrefix, metadata)
}
