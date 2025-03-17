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
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/api/validate"
)

const propertyKeyPrefix = "/properties/"

func (e *etcdSchemaRegistry) CreateProperty(ctx context.Context, property *databasev1.Property) error {
	if property.UpdatedAt == nil {
		property.UpdatedAt = timestamppb.Now()
	}

	group := property.Metadata.GetGroup()
	g, err := e.GetGroup(ctx, group)
	if err != nil {
		return err
	}
	if err = validate.Group(g); err != nil {
		return err
	}
	_, err = e.create(ctx, Metadata{
		TypeMeta: TypeMeta{
			Kind:  KindProperty,
			Group: property.GetMetadata().GetGroup(),
			Name:  property.GetMetadata().GetName(),
		},
		Spec: property,
	})
	return err
}

func (e *etcdSchemaRegistry) DeleteProperty(ctx context.Context, metadata *commonv1.Metadata) (bool, error) {
	return e.delete(ctx, Metadata{
		TypeMeta: TypeMeta{
			Kind:  KindProperty,
			Group: metadata.GetGroup(),
			Name:  metadata.GetName(),
		},
	})
}

func (e *etcdSchemaRegistry) GetProperty(ctx context.Context, metadata *commonv1.Metadata) (*databasev1.Property, error) {
	var entity databasev1.Property
	if err := e.get(ctx, formatPropertyKey(metadata), &entity); err != nil {
		return nil, err
	}
	return &entity, nil
}

func (e *etcdSchemaRegistry) ListProperty(ctx context.Context, opt ListOpt) ([]*databasev1.Property, error) {
	if opt.Group == "" {
		return nil, BadRequest("group", "group should not be empty")
	}
	messages, err := e.listWithPrefix(ctx, listPrefixesForEntity(opt.Group, propertyKeyPrefix), KindProperty)
	if err != nil {
		return nil, err
	}
	entities := make([]*databasev1.Property, 0, len(messages))
	for _, message := range messages {
		entities = append(entities, message.(*databasev1.Property))
	}
	return entities, nil
}

func (e *etcdSchemaRegistry) UpdateProperty(ctx context.Context, property *databasev1.Property) error {
	if property.UpdatedAt == nil {
		property.UpdatedAt = timestamppb.Now()
	}

	group := property.Metadata.GetGroup()
	g, err := e.GetGroup(ctx, group)
	if err != nil {
		return err
	}
	if err = validate.Group(g); err != nil {
		return err
	}

	prev, err := e.GetProperty(ctx, property.GetMetadata())
	if err != nil {
		return err
	}
	if prev == nil {
		return errors.WithMessagef(ErrGRPCResourceNotFound, "property %s not found", property.GetMetadata().GetName())
	}

	_, err = e.update(ctx, Metadata{
		TypeMeta: TypeMeta{
			Kind:        KindProperty,
			Group:       property.GetMetadata().GetGroup(),
			Name:        property.GetMetadata().GetName(),
			ModRevision: property.GetMetadata().GetModRevision(),
		},
		Spec: property,
	})
	return err
}

func formatPropertyKey(metadata *commonv1.Metadata) string {
	return formatKey(propertyKeyPrefix, metadata)
}
