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

package property

import (
	"fmt"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
)

// validateTagFamiliesUpdate rejects deletion of entity tags or of tag families that hold them.
func validateTagFamiliesUpdate(prevTFs, newTFs []*databasev1.TagFamilySpec, entityTagNames []string) error {
	entityTagSet := make(map[string]struct{}, len(entityTagNames))
	for _, tagName := range entityTagNames {
		entityTagSet[tagName] = struct{}{}
	}
	newTagFamilyMap := make(map[string]map[string]*databasev1.TagSpec, len(newTFs))
	for _, tf := range newTFs {
		tagMap := make(map[string]*databasev1.TagSpec, len(tf.GetTags()))
		for _, tag := range tf.GetTags() {
			tagMap[tag.GetName()] = tag
		}
		newTagFamilyMap[tf.GetName()] = tagMap
	}
	for _, prevTagFamily := range prevTFs {
		newTagMap, familyExists := newTagFamilyMap[prevTagFamily.GetName()]
		if !familyExists {
			for _, tag := range prevTagFamily.GetTags() {
				if _, isEntity := entityTagSet[tag.GetName()]; isEntity {
					return fmt.Errorf("cannot delete tag family %s: it contains entity tag %s", prevTagFamily.GetName(), tag.GetName())
				}
			}
			continue
		}
		for _, prevTag := range prevTagFamily.GetTags() {
			if _, tagExists := newTagMap[prevTag.GetName()]; tagExists {
				continue
			}
			if _, isEntity := entityTagSet[prevTag.GetName()]; isEntity {
				return fmt.Errorf("cannot delete entity tag %s in tag family %s", prevTag.GetName(), prevTagFamily.GetName())
			}
		}
	}
	return nil
}

func validateStreamUpdate(prevStream, newStream *databasev1.Stream) error {
	if prevStream.GetEntity().String() != newStream.GetEntity().String() {
		return fmt.Errorf("entity is different: %s != %s", prevStream.GetEntity().String(), newStream.GetEntity().String())
	}
	return validateTagFamiliesUpdate(prevStream.GetTagFamilies(), newStream.GetTagFamilies(), newStream.GetEntity().GetTagNames())
}

func validateMeasureUpdate(prevMeasure, newMeasure *databasev1.Measure) error {
	if prevMeasure.GetInterval() != newMeasure.GetInterval() {
		return fmt.Errorf("interval is different: %s != %s", prevMeasure.GetInterval(), newMeasure.GetInterval())
	}
	if prevMeasure.GetEntity().String() != newMeasure.GetEntity().String() {
		return fmt.Errorf("entity is different: %s != %s", prevMeasure.GetEntity().String(), newMeasure.GetEntity().String())
	}
	if prevMeasure.GetIndexMode() != newMeasure.GetIndexMode() {
		return fmt.Errorf("index mode is different: %v != %v", prevMeasure.GetIndexMode(), newMeasure.GetIndexMode())
	}
	if err := validateTagFamiliesUpdate(prevMeasure.GetTagFamilies(), newMeasure.GetTagFamilies(), newMeasure.GetEntity().GetTagNames()); err != nil {
		return err
	}
	newFieldMap := make(map[string]*databasev1.FieldSpec, len(newMeasure.GetFields()))
	for _, field := range newMeasure.GetFields() {
		newFieldMap[field.GetName()] = field
	}
	for _, prevField := range prevMeasure.GetFields() {
		newField, fieldExists := newFieldMap[prevField.GetName()]
		if !fieldExists {
			continue
		}
		if prevField.String() != newField.String() {
			return fmt.Errorf("field %s is different: %s != %s", prevField.GetName(), prevField.String(), newField.String())
		}
	}
	return nil
}
