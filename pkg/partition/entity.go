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
	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/api/common"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

// ErrMalformedElement indicates the element is malformed.
var ErrMalformedElement = errors.New("element is malformed")

// Locator combines several TagLocators that help find the entity or sharding key value.
type Locator struct {
	TagLocators []TagLocator
	ModRevision int64
}

// TagLocator contains offsets to retrieve a tag swiftly.
type TagLocator struct {
	FamilyOffset int
	TagOffset    int
}

// NewEntityLocator return a Locator based on tag family spec and entity spec.
func NewEntityLocator(families []*databasev1.TagFamilySpec, entity *databasev1.Entity, modRevision int64) Locator {
	locator := make([]TagLocator, 0, len(entity.GetTagNames()))
	for _, tagInEntity := range entity.GetTagNames() {
		fIndex, tIndex, tag := pbv1.FindTagByName(families, tagInEntity)
		if tag != nil {
			locator = append(locator, TagLocator{FamilyOffset: fIndex, TagOffset: tIndex})
		}
	}
	return Locator{TagLocators: locator, ModRevision: modRevision}
}

// NewShardingKeyLocator returns a Locator based on tag family spec and sharding key spec.
func NewShardingKeyLocator(families []*databasev1.TagFamilySpec, shardingKey *databasev1.ShardingKey) Locator {
	locator := make([]TagLocator, 0, len(shardingKey.GetTagNames()))
	for _, tagInShardingKey := range shardingKey.GetTagNames() {
		fIndex, tIndex, tag := pbv1.FindTagByName(families, tagInShardingKey)
		if tag != nil {
			locator = append(locator, TagLocator{FamilyOffset: fIndex, TagOffset: tIndex})
		}
	}
	return Locator{TagLocators: locator}
}

// Find the entity from a tag family, prepend a subject to the entity.
func (l Locator) Find(subject string, value []*modelv1.TagFamilyForWrite) (pbv1.Entity, pbv1.EntityValues, error) {
	entityValues := make(pbv1.EntityValues, len(l.TagLocators)+1)
	entityValues[0] = pbv1.EntityStrValue(subject)
	for i, index := range l.TagLocators {
		tag, err := GetTagByOffset(value, index.FamilyOffset, index.TagOffset)
		if err != nil {
			return nil, nil, err
		}
		entityValues[i+1] = tag
	}
	entity, err := entityValues.ToEntity()
	if err != nil {
		return nil, nil, err
	}
	return entity, entityValues, nil
}

// Locate a shard and find the entity or sharding key from a tag family, prepend a subject to the entity or sharding key.
func (l Locator) Locate(subject string, value []*modelv1.TagFamilyForWrite, shardNum uint32) (pbv1.EntityValues, common.ShardID, error) {
	entity, tagValues, err := l.Find(subject, value)
	if err != nil {
		return nil, 0, err
	}
	id, err := ShardID(entity.Marshal(), shardNum)
	if err != nil {
		return nil, 0, err
	}
	return tagValues, common.ShardID(id), nil
}

// GetTagByOffset gets a tag value based of a tag family offset and a tag offset in this family.
func GetTagByOffset(value []*modelv1.TagFamilyForWrite, fIndex, tIndex int) (*modelv1.TagValue, error) {
	if fIndex >= len(value) {
		return nil, errors.Wrap(ErrMalformedElement, "tag family offset is invalid")
	}
	family := value[fIndex]
	if tIndex >= len(family.GetTags()) {
		return nil, errors.Wrap(ErrMalformedElement, "tag offset is invalid")
	}
	return family.GetTags()[tIndex], nil
}
