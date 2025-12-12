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

package grpc

import (
	"github.com/apache/skywalking-banyandb/api/common"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/partition"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

type tagFamilySpec interface {
	GetName() string
	GetTagNames() []string
}

type specLocator struct {
	tagLocators []partition.TagLocator
}

func newSpecLocator[T tagFamilySpec](schemaFamilies []*databasev1.TagFamilySpec, tagNames []string, specFamilies []T) *specLocator {
	specFamilyMap, specTagMaps := buildSpecMaps(specFamilies)
	locator := make([]partition.TagLocator, 0, len(tagNames))
	for _, tagName := range tagNames {
		familyIdx, tagIdx := findTagInSpec(schemaFamilies, tagName, specFamilyMap, specTagMaps)
		locator = append(locator, partition.TagLocator{FamilyOffset: familyIdx, TagOffset: tagIdx})
	}
	return &specLocator{tagLocators: locator}
}

func buildSpecMaps[T tagFamilySpec](specFamilies []T) (map[string]int, map[string]map[string]int) {
	specFamilyMap := make(map[string]int, len(specFamilies))
	specTagMaps := make(map[string]map[string]int, len(specFamilies))
	for i, specFamily := range specFamilies {
		specFamilyMap[specFamily.GetName()] = i
		tagMap := make(map[string]int, len(specFamily.GetTagNames()))
		for j, tagName := range specFamily.GetTagNames() {
			tagMap[tagName] = j
		}
		specTagMaps[specFamily.GetName()] = tagMap
	}
	return specFamilyMap, specTagMaps
}

func findTagInSpec(schemaFamilies []*databasev1.TagFamilySpec, tagName string,
	specFamilyMap map[string]int, specTagMaps map[string]map[string]int,
) (familyIdx, tagIdx int) {
	for _, schemaFamily := range schemaFamilies {
		for _, schemaTag := range schemaFamily.GetTags() {
			if schemaTag.GetName() != tagName {
				continue
			}
			fIdx, ok := specFamilyMap[schemaFamily.GetName()]
			if !ok {
				return -1, -1
			}
			tagMap := specTagMaps[schemaFamily.GetName()]
			if tagMap == nil {
				return -1, -1
			}
			tIdx, ok := tagMap[tagName]
			if !ok {
				return -1, -1
			}
			return fIdx, tIdx
		}
	}
	return -1, -1
}

// Locate finds the entity values and shard ID using the spec locator.
func (l *specLocator) Locate(subject string, tagFamilies []*modelv1.TagFamilyForWrite, shardNum uint32) (pbv1.EntityValues, common.ShardID, error) {
	entity, tagValues, err := l.Find(subject, tagFamilies)
	if err != nil {
		return nil, 0, err
	}
	id, err := partition.ShardID(entity.Marshal(), shardNum)
	if err != nil {
		return nil, 0, err
	}
	return tagValues, common.ShardID(id), nil
}

// Find finds the entity and entity values from tag families.
func (l *specLocator) Find(subject string, tagFamilies []*modelv1.TagFamilyForWrite) (pbv1.Entity, pbv1.EntityValues, error) {
	entityValues := make(pbv1.EntityValues, len(l.tagLocators)+1)
	entityValues[0] = pbv1.EntityStrValue(subject)
	for i, index := range l.tagLocators {
		if index.FamilyOffset < 0 || index.TagOffset < 0 {
			entityValues[i+1] = &modelv1.TagValue{Value: &modelv1.TagValue_Null{}}
			continue
		}
		tag, err := partition.GetTagByOffset(tagFamilies, index.FamilyOffset, index.TagOffset)
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
