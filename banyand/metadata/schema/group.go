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
	"strings"

	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
)

var (
	groupsKeyPrefix  = "/groups/"
	groupMetadataKey = "/__meta_group__"
)

func (e *etcdSchemaRegistry) GetGroup(ctx context.Context, group string) (*commonv1.Group, error) {
	var entity commonv1.Group
	err := e.get(ctx, formatGroupKey(group), &entity)
	if err != nil {
		return nil, errors.WithMessagef(err, "GetGroup[%s]", group)
	}
	return &entity, nil
}

func (e *etcdSchemaRegistry) ListGroup(ctx context.Context) ([]*commonv1.Group, error) {
	messages, err := e.client.Get(ctx, groupsKeyPrefix, clientv3.WithFromKey(), clientv3.WithRange(incrementLastByte(groupsKeyPrefix)))
	if err != nil {
		return nil, err
	}

	var groups []*commonv1.Group
	for _, kv := range messages.Kvs {
		// kv.Key = "/groups/" + {group} + "/__meta_info__"
		if strings.HasSuffix(string(kv.Key), groupMetadataKey) {
			message := &commonv1.Group{}
			if innerErr := proto.Unmarshal(kv.Value, message); innerErr != nil {
				return nil, innerErr
			}
			groups = append(groups, message)
		}
	}

	return groups, nil
}

func (e *etcdSchemaRegistry) DeleteGroup(ctx context.Context, group string) (bool, error) {
	g, err := e.GetGroup(ctx, group)
	if err != nil {
		return false, errors.Wrap(err, group)
	}
	keyPrefix := groupsKeyPrefix + g.GetMetadata().GetName() + "/"
	resp, err := e.client.Delete(ctx, keyPrefix, clientv3.WithRange(incrementLastByte(keyPrefix)))
	if err != nil {
		return false, err
	}
	if resp.Deleted > 0 {
		e.notifyDelete(Metadata{
			TypeMeta: TypeMeta{
				Kind: KindGroup,
				Name: group,
			},
			Spec: g,
		})
	}

	return true, nil
}

func (e *etcdSchemaRegistry) CreateGroup(ctx context.Context, group *commonv1.Group) error {
	if group.UpdatedAt != nil {
		group.UpdatedAt = timestamppb.Now()
	}
	return e.create(ctx, Metadata{
		TypeMeta: TypeMeta{
			Kind: KindGroup,
			Name: group.GetMetadata().GetName(),
		},
		Spec: group,
	})
}

func (e *etcdSchemaRegistry) UpdateGroup(ctx context.Context, group *commonv1.Group) error {
	return e.update(ctx, Metadata{
		TypeMeta: TypeMeta{
			Kind: KindGroup,
			Name: group.GetMetadata().GetName(),
		},
		Spec: group,
	})
}

func formatGroupKey(group string) string {
	return groupsKeyPrefix + group + groupMetadataKey
}
