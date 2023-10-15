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
	"path"

	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
)

var groupsKeyPrefix = "/groups/"

func (e *etcdSchemaRegistry) GetGroup(ctx context.Context, group string) (*commonv1.Group, error) {
	var entity commonv1.Group
	err := e.get(ctx, formatGroupKey(group), &entity)
	if err != nil {
		return nil, errors.WithMessagef(err, "GetGroup[%s]", group)
	}
	return &entity, nil
}

func (e *etcdSchemaRegistry) ListGroup(ctx context.Context) ([]*commonv1.Group, error) {
	messages, err := e.listWithPrefix(ctx, groupsKeyPrefix, KindGroup)
	if err != nil {
		return nil, err
	}
	entities := make([]*commonv1.Group, 0, len(messages))
	for _, message := range messages {
		entities = append(entities, message.(*commonv1.Group))
	}
	return entities, nil
}

func (e *etcdSchemaRegistry) DeleteGroup(ctx context.Context, group string) (bool, error) {
	_, err := e.GetGroup(ctx, group)
	if err != nil {
		return false, errors.Wrap(err, group)
	}
	keysToDelete := allKeys()
	deleteOPs := make([]clientv3.Op, 0, len(keysToDelete)+1)
	for _, key := range keysToDelete {
		deleteOPs = append(deleteOPs, clientv3.OpDelete(e.prependNamespace(listPrefixesForEntity(group, key)), clientv3.WithPrefix()))
	}
	deleteOPs = append(deleteOPs, clientv3.OpDelete(e.prependNamespace(formatGroupKey(group)), clientv3.WithPrefix()))
	txnResponse, err := e.client.Txn(ctx).Then(deleteOPs...).Commit()
	if err != nil {
		return false, err
	}
	if !txnResponse.Succeeded {
		return false, errConcurrentModification
	}
	return true, nil
}

func (e *etcdSchemaRegistry) CreateGroup(ctx context.Context, group *commonv1.Group) error {
	if group.UpdatedAt != nil {
		group.UpdatedAt = timestamppb.Now()
	}
	_, err := e.create(ctx, Metadata{
		TypeMeta: TypeMeta{
			Kind: KindGroup,
			Name: group.GetMetadata().GetName(),
		},
		Spec: group,
	})
	return err
}

func (e *etcdSchemaRegistry) UpdateGroup(ctx context.Context, group *commonv1.Group) error {
	_, err := e.update(ctx, Metadata{
		TypeMeta: TypeMeta{
			Kind: KindGroup,
			Name: group.GetMetadata().GetName(),
		},
		Spec: group,
	})
	return err
}

func formatGroupKey(group string) string {
	return path.Join(groupsKeyPrefix, group)
}
