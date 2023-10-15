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
	"hash/crc32"

	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
)

var (
	indexRuleBindingKeyPrefix = "/index-rule-bindings/"
	indexRuleKeyPrefix        = "/index-rules/"
)

func (e *etcdSchemaRegistry) GetIndexRuleBinding(ctx context.Context, metadata *commonv1.Metadata) (*databasev1.IndexRuleBinding, error) {
	var indexRuleBinding databasev1.IndexRuleBinding
	if err := e.get(ctx, formatIndexRuleBindingKey(metadata), &indexRuleBinding); err != nil {
		return nil, err
	}
	return &indexRuleBinding, nil
}

func (e *etcdSchemaRegistry) ListIndexRuleBinding(ctx context.Context, opt ListOpt) ([]*databasev1.IndexRuleBinding, error) {
	if opt.Group == "" {
		return nil, BadRequest("group", "group should not be empty")
	}
	messages, err := e.listWithPrefix(ctx, listPrefixesForEntity(opt.Group, indexRuleBindingKeyPrefix), KindIndexRuleBinding)
	if err != nil {
		return nil, err
	}
	entities := make([]*databasev1.IndexRuleBinding, 0, len(messages))
	for _, message := range messages {
		entities = append(entities, message.(*databasev1.IndexRuleBinding))
	}
	return entities, nil
}

func (e *etcdSchemaRegistry) CreateIndexRuleBinding(ctx context.Context, indexRuleBinding *databasev1.IndexRuleBinding) error {
	if indexRuleBinding.UpdatedAt != nil {
		indexRuleBinding.UpdatedAt = timestamppb.Now()
	}
	_, err := e.create(ctx, Metadata{
		TypeMeta: TypeMeta{
			Kind:  KindIndexRuleBinding,
			Name:  indexRuleBinding.GetMetadata().GetName(),
			Group: indexRuleBinding.GetMetadata().GetGroup(),
		},
		Spec: indexRuleBinding,
	})
	return err
}

func (e *etcdSchemaRegistry) UpdateIndexRuleBinding(ctx context.Context, indexRuleBinding *databasev1.IndexRuleBinding) error {
	_, err := e.update(ctx, Metadata{
		TypeMeta: TypeMeta{
			Kind:  KindIndexRuleBinding,
			Name:  indexRuleBinding.GetMetadata().GetName(),
			Group: indexRuleBinding.GetMetadata().GetGroup(),
		},
		Spec: indexRuleBinding,
	})
	return err
}

func (e *etcdSchemaRegistry) DeleteIndexRuleBinding(ctx context.Context, metadata *commonv1.Metadata) (bool, error) {
	return e.delete(ctx, Metadata{
		TypeMeta: TypeMeta{
			Kind:  KindIndexRuleBinding,
			Name:  metadata.GetName(),
			Group: metadata.GetGroup(),
		},
	})
}

func (e *etcdSchemaRegistry) GetIndexRule(ctx context.Context, metadata *commonv1.Metadata) (*databasev1.IndexRule, error) {
	var entity databasev1.IndexRule
	if err := e.get(ctx, formatIndexRuleKey(metadata), &entity); err != nil {
		return nil, err
	}
	if entity.Metadata.Id == 0 {
		return nil, errGRPCDataLoss
	}
	return &entity, nil
}

func (e *etcdSchemaRegistry) ListIndexRule(ctx context.Context, opt ListOpt) ([]*databasev1.IndexRule, error) {
	if opt.Group == "" {
		return nil, BadRequest("group", "group should not be empty")
	}
	messages, err := e.listWithPrefix(ctx, listPrefixesForEntity(opt.Group, indexRuleKeyPrefix), KindIndexRule)
	if err != nil {
		return nil, err
	}
	entities := make([]*databasev1.IndexRule, 0, len(messages))
	for _, message := range messages {
		entity := message.(*databasev1.IndexRule)
		if entity.Metadata.Id == 0 {
			return nil, errGRPCDataLoss
		}
		entities = append(entities, entity)
	}
	return entities, nil
}

func (e *etcdSchemaRegistry) CreateIndexRule(ctx context.Context, indexRule *databasev1.IndexRule) error {
	if indexRule.UpdatedAt != nil {
		indexRule.UpdatedAt = timestamppb.Now()
	}
	if indexRule.Metadata.Id == 0 {
		buf := []byte(indexRule.Metadata.Group)
		buf = append(buf, indexRule.Metadata.Name...)
		indexRule.Metadata.Id = crc32.ChecksumIEEE(buf)
	}
	_, err := e.create(ctx, Metadata{
		TypeMeta: TypeMeta{
			Kind:  KindIndexRule,
			Name:  indexRule.GetMetadata().GetName(),
			Group: indexRule.GetMetadata().GetGroup(),
		},
		Spec: indexRule,
	})
	return err
}

func (e *etcdSchemaRegistry) UpdateIndexRule(ctx context.Context, indexRule *databasev1.IndexRule) error {
	if indexRule.Metadata.Id == 0 {
		existingIndexRule, err := e.GetIndexRule(ctx, indexRule.Metadata)
		if err != nil {
			return err
		}
		indexRule.Metadata.Id = existingIndexRule.Metadata.Id
	}
	_, err := e.update(ctx, Metadata{
		TypeMeta: TypeMeta{
			Kind:  KindIndexRule,
			Name:  indexRule.GetMetadata().GetName(),
			Group: indexRule.GetMetadata().GetGroup(),
		},
		Spec: indexRule,
	})
	return err
}

func (e *etcdSchemaRegistry) DeleteIndexRule(ctx context.Context, metadata *commonv1.Metadata) (bool, error) {
	return e.delete(ctx, Metadata{
		TypeMeta: TypeMeta{
			Kind:  KindIndexRule,
			Name:  metadata.GetName(),
			Group: metadata.GetGroup(),
		},
	})
}

func formatIndexRuleKey(metadata *commonv1.Metadata) string {
	return formatKey(indexRuleKeyPrefix, metadata)
}

func formatIndexRuleBindingKey(metadata *commonv1.Metadata) string {
	return formatKey(indexRuleBindingKeyPrefix, metadata)
}
