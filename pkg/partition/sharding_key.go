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

// Package partition implements a location system to find a shard or index rule.
// This system reflects the entity identity which is a intermediate result in calculating the target shard.
package partition

import (
	"github.com/apache/skywalking-banyandb/api/common"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

// ShardingKeyLocator combines several TagLocators that help find the sharding key value.
type ShardingKeyLocator struct {
	TagLocators []TagLocator
	ModRevision int64
}

// NewShardingKeyLocator return a ShardingKeyLocator based on tag family spec and sharding key spec.
func NewShardingKeyLocator(families []*databasev1.TagFamilySpec, shardingKey *databasev1.ShardingKey, modRevision int64) ShardingKeyLocator {
	locator := make([]TagLocator, 0, len(shardingKey.GetTagNames()))
	for _, tagInShardingKey := range shardingKey.GetTagNames() {
		fIndex, tIndex, tag := pbv1.FindTagByName(families, tagInShardingKey)
		if tag != nil {
			locator = append(locator, TagLocator{FamilyOffset: fIndex, TagOffset: tIndex})
		}
	}
	return ShardingKeyLocator{TagLocators: locator, ModRevision: modRevision}
}

// IsEmpty returns true if the sharding key locator is empty.
func (s ShardingKeyLocator) IsEmpty() bool {
	return len(s.TagLocators) == 0
}

// Find the sharding key from a tag family, prepend a subject to the sharding key.
func (s ShardingKeyLocator) Find(subject string, value []*modelv1.TagFamilyForWrite) (pbv1.ShardingKey, error) {
	shardingKeyValues := make(pbv1.ShardingKeyValues, len(s.TagLocators)+1)
	shardingKeyValues[0] = pbv1.ShardingKeyStrValue(subject)
	for i, index := range s.TagLocators {
		tag, err := GetTagByOffset(value, index.FamilyOffset, index.TagOffset)
		if err != nil {
			return nil, err
		}
		shardingKeyValues[i+1] = tag
	}
	shardingKey, err := shardingKeyValues.ToShardingKey()
	if err != nil {
		return nil, err
	}
	return shardingKey, nil
}

// Locate a shard and find the sharding key from a tag family, prepend a subject to the sharding key.
func (s ShardingKeyLocator) Locate(subject string, value []*modelv1.TagFamilyForWrite, shardNum uint32) (common.ShardID, error) {
	shardingKey, err := s.Find(subject, value)
	if err != nil {
		return 0, err
	}
	id, err := ShardID(shardingKey.Marshal(), shardNum)
	if err != nil {
		return 0, err
	}
	return common.ShardID(id), nil
}
