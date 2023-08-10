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

package metadata

import (
	"context"
	"time"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

var _ schema.EventHandler = (*allocator)(nil)

type allocator struct {
	schemaRegistry schema.Registry
	l              *logger.Logger
}

func newAllocator(schemaRegistry schema.Registry, logger *logger.Logger) *allocator {
	return &allocator{
		schemaRegistry: schemaRegistry,
		l:              logger,
	}
}

// OnAddOrUpdate implements EventHandler.
func (a *allocator) OnAddOrUpdate(metadata schema.Metadata) {
	switch metadata.Kind {
	case schema.KindGroup:
		groupSchema := metadata.Spec.(*commonv1.Group)
		if groupSchema.Catalog == commonv1.Catalog_CATALOG_UNSPECIFIED {
			return
		}
		shardNum := groupSchema.GetResourceOpts().GetShardNum()
		syncShard := func(id uint64) error {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			return a.schemaRegistry.CreateOrUpdateShard(ctx, &databasev1.Shard{
				Id:    id,
				Total: shardNum,
				Metadata: &commonv1.Metadata{
					Name: groupSchema.GetMetadata().GetName(),
				},
				Node: "TODO",
			})
		}
		for i := 0; i < int(shardNum); i++ {
			if err := syncShard(uint64(i)); err != nil {
				// TODO: handle error. retry? or do a full sync?
				a.l.Error().Err(err).Msg("failed to sync shard")
			}
		}
	case schema.KindNode:
		// TODO: handle node
	default:
		return
	}
}

// OnDelete implements EventHandler.
func (*allocator) OnDelete(schema.Metadata) {
	// TODO: handle delete
}
