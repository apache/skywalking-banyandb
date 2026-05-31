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

package partition

import (
	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/api/common"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

// ShardID calculates a shard id.
func ShardID(key []byte, shardNum uint32) (uint, error) {
	if shardNum < 1 {
		return 0, errors.New("invalid shardNum")
	}
	encodeKey := convert.Hash(key)
	return uint(encodeKey % uint64(shardNum)), nil
}

// TraceShardID returns the shard id derived from a trace id hash.
// A zero shardNum returns shard 0 to avoid a divide-by-zero panic.
func TraceShardID(traceID string, shardNum uint32) common.ShardID {
	if shardNum == 0 {
		return 0
	}
	return common.ShardID(convert.Hash([]byte(traceID)) % uint64(shardNum))
}

// Router is anything that can map a write to its target (entityValues, shardID).
type Router interface {
	Locate(subject string, tagFamilies []*modelv1.TagFamilyForWrite, shardNum uint32) (pbv1.EntityValues, common.ShardID, error)
}

// ApplyLocators routes a write by entity tags then, when a non-nil
// shardingKey Router is supplied, overrides the shard id with the
// sharding-key derived one. EntityValues always come from the entity router.
func ApplyLocators(subject string, tagFamilies []*modelv1.TagFamilyForWrite,
	entityRouter, shardingKeyRouter Router, shardNum uint32,
) (pbv1.EntityValues, common.ShardID, error) {
	entityValues, shardID, err := entityRouter.Locate(subject, tagFamilies, shardNum)
	if err != nil {
		return nil, 0, err
	}
	if shardingKeyRouter != nil {
		_, shardID, err = shardingKeyRouter.Locate(subject, tagFamilies, shardNum)
		if err != nil {
			return nil, 0, err
		}
	}
	return entityValues, shardID, nil
}
