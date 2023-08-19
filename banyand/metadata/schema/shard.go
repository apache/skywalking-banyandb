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
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
)

var shardKeyPrefix = "/shards/"

func (e *etcdSchemaRegistry) CreateOrUpdateShard(ctx context.Context, shard *databasev1.Shard) error {
	if shard.UpdatedAt != nil {
		shard.UpdatedAt = timestamppb.Now()
	}
	md := Metadata{
		TypeMeta: TypeMeta{
			Kind:  KindShard,
			Group: shard.GetMetadata().GetGroup(),
			Name:  shard.GetMetadata().GetName(),
		},
		Spec: shard,
	}
	err := e.update(ctx, md)
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrGRPCResourceNotFound) {
		shard.CreatedAt = shard.UpdatedAt
		md.Spec = shard
		return e.create(ctx, md)
	}
	return err
}

func (e *etcdSchemaRegistry) ListShard(ctx context.Context, opt ListOpt) ([]*databasev1.Shard, error) {
	if opt.Group == "" {
		return nil, BadRequest("group", "group should not be empty")
	}
	messages, err := e.listWithPrefix(ctx, listPrefixesForEntity(opt.Group, shardKeyPrefix), func() proto.Message {
		return &databasev1.Shard{}
	})
	if err != nil {
		return nil, err
	}
	entities := make([]*databasev1.Shard, 0, len(messages))
	for _, message := range messages {
		entities = append(entities, message.(*databasev1.Shard))
	}
	return entities, nil
}

func formatShardKey(metadata *commonv1.Metadata) string {
	return formatKey(shardKeyPrefix, metadata)
}
