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

	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
)

var topNAggregationKeyPrefix = "/topnagg/"

func (e *etcdSchemaRegistry) GetTopNAggregation(ctx context.Context, metadata *commonv1.Metadata) (*databasev1.TopNAggregation, error) {
	var entity databasev1.TopNAggregation
	if err := e.get(ctx, formatTopNAggregationKey(metadata), &entity); err != nil {
		return nil, err
	}
	return &entity, nil
}

func (e *etcdSchemaRegistry) ListTopNAggregation(ctx context.Context, opt ListOpt) ([]*databasev1.TopNAggregation, error) {
	if opt.Group == "" {
		return nil, BadRequest("group", "group should not be empty")
	}
	messages, err := e.listWithPrefix(ctx, listPrefixesForEntity(opt.Group, topNAggregationKeyPrefix), KindTopNAggregation)
	if err != nil {
		return nil, err
	}
	entities := make([]*databasev1.TopNAggregation, 0, len(messages))
	for _, message := range messages {
		entities = append(entities, message.(*databasev1.TopNAggregation))
	}
	return entities, nil
}

func (e *etcdSchemaRegistry) CreateTopNAggregation(ctx context.Context, topNAggregation *databasev1.TopNAggregation) error {
	if topNAggregation.UpdatedAt != nil {
		topNAggregation.UpdatedAt = timestamppb.Now()
	}
	_, err := e.create(ctx, Metadata{
		TypeMeta: TypeMeta{
			Kind:  KindTopNAggregation,
			Group: topNAggregation.GetMetadata().GetGroup(),
			Name:  topNAggregation.GetMetadata().GetName(),
		},
		Spec: topNAggregation,
	})
	return err
}

func (e *etcdSchemaRegistry) UpdateTopNAggregation(ctx context.Context, topNAggregation *databasev1.TopNAggregation) error {
	_, err := e.update(ctx, Metadata{
		TypeMeta: TypeMeta{
			Kind:  KindTopNAggregation,
			Group: topNAggregation.GetMetadata().GetGroup(),
			Name:  topNAggregation.GetMetadata().GetName(),
		},
		Spec: topNAggregation,
	})
	return err
}

func (e *etcdSchemaRegistry) DeleteTopNAggregation(ctx context.Context, metadata *commonv1.Metadata) (bool, error) {
	return e.delete(ctx, Metadata{
		TypeMeta: TypeMeta{
			Kind:  KindTopNAggregation,
			Group: metadata.GetGroup(),
			Name:  metadata.GetName(),
		},
	})
}

func formatTopNAggregationKey(metadata *commonv1.Metadata) string {
	return formatKey(topNAggregationKeyPrefix, metadata)
}
