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
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

var ErrMalformedElement = errors.New("element is malformed")

type EntityLocator []TagLocator

type TagLocator struct {
	FamilyOffset int
	TagOffset    int
}

func NewEntityLocator(families []*databasev1.TagFamilySpec, entity *databasev1.Entity) EntityLocator {
	locator := make(EntityLocator, 0, len(entity.GetTagNames()))
	for _, tagInEntity := range entity.GetTagNames() {
		fIndex, tIndex, tag := pbv1.FindTagByName(families, tagInEntity)
		if tag != nil {
			locator = append(locator, TagLocator{FamilyOffset: fIndex, TagOffset: tIndex})
		}
	}
	return locator
}

func (e EntityLocator) Find(subject string, value []*modelv1.TagFamilyForWrite) (tsdb.Entity, tsdb.EntityValues, error) {
	entityValues := make(tsdb.EntityValues, len(e)+1)
	entityValues[0] = tsdb.StrValue(subject)
	for i, index := range e {
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

func (e EntityLocator) Locate(subject string, value []*modelv1.TagFamilyForWrite, shardNum uint32) (tsdb.Entity, tsdb.EntityValues, common.ShardID, error) {
	entity, tagValues, err := e.Find(subject, value)
	if err != nil {
		return nil, nil, 0, err
	}
	id, err := ShardID(entity.Marshal(), shardNum)
	if err != nil {
		return nil, nil, 0, err
	}
	return entity, tagValues, common.ShardID(id), nil
}

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
