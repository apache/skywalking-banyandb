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
	"fmt"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/api/validate"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

const (
	measureKeyPrefix = "/measures/"
	tagTypeID        = "id"
)

func (e *etcdSchemaRegistry) GetMeasure(ctx context.Context, metadata *commonv1.Metadata) (*databasev1.Measure, error) {
	var entity databasev1.Measure
	if err := e.get(ctx, formatMeasureKey(metadata), &entity); err != nil {
		return nil, err
	}
	return &entity, nil
}

func (e *etcdSchemaRegistry) ListMeasure(ctx context.Context, opt ListOpt) ([]*databasev1.Measure, error) {
	if opt.Group == "" {
		return nil, BadRequest("group", "group should not be empty")
	}
	messages, err := e.listWithPrefix(ctx, listPrefixesForEntity(opt.Group, measureKeyPrefix), KindMeasure)
	if err != nil {
		return nil, err
	}
	entities := make([]*databasev1.Measure, 0, len(messages))
	for _, message := range messages {
		entities = append(entities, message.(*databasev1.Measure))
	}
	return entities, nil
}

func (e *etcdSchemaRegistry) CreateMeasure(ctx context.Context, measure *databasev1.Measure) (int64, error) {
	if measure.UpdatedAt != nil {
		measure.UpdatedAt = timestamppb.Now()
	}
	if measure.GetInterval() != "" {
		if _, err := timestamp.ParseDuration(measure.GetInterval()); err != nil {
			return 0, errors.Wrap(err, "interval is malformed")
		}
	}
	if err := validate.Measure(measure); err != nil {
		return 0, err
	}
	group := measure.Metadata.GetGroup()
	g, err := e.GetGroup(ctx, group)
	if err != nil {
		return 0, err
	}
	if err := validate.GroupForNonProperty(g); err != nil {
		return 0, err
	}
	return e.create(ctx, Metadata{
		TypeMeta: TypeMeta{
			Kind:  KindMeasure,
			Group: measure.GetMetadata().GetGroup(),
			Name:  measure.GetMetadata().GetName(),
		},
		Spec: measure,
	})
}

func (e *etcdSchemaRegistry) UpdateMeasure(ctx context.Context, measure *databasev1.Measure) (int64, error) {
	if measure.UpdatedAt != nil {
		measure.UpdatedAt = timestamppb.Now()
	}
	if measure.GetInterval() != "" {
		if _, err := timestamp.ParseDuration(measure.GetInterval()); err != nil {
			return 0, errors.Wrap(err, "interval is malformed")
		}
	}
	if err := validate.Measure(measure); err != nil {
		return 0, err
	}
	group := measure.Metadata.GetGroup()
	g, err := e.GetGroup(ctx, group)
	if err != nil {
		return 0, err
	}
	if err = validate.GroupForNonProperty(g); err != nil {
		return 0, err
	}
	prev, err := e.GetMeasure(ctx, measure.GetMetadata())
	if err != nil {
		return 0, err
	}
	if prev == nil {
		return 0, errors.WithMessagef(ErrGRPCResourceNotFound, "measure %s not found", measure.GetMetadata().GetName())
	}
	if err := validateMeasureUpdate(prev, measure); err != nil {
		return 0, errors.WithMessagef(ErrInputInvalid, "validation failed: %s", err)
	}
	deletedTags := findDeletedTagsForMeasure(prev, measure)
	if err := e.cleanupIndexRulesForDeletedTags(ctx, group, measure.GetMetadata().GetName(), commonv1.Catalog_CATALOG_MEASURE, deletedTags); err != nil {
		return 0, errors.Wrap(err, "failed to cleanup index rules for deleted tags")
	}
	return e.update(ctx, Metadata{
		TypeMeta: TypeMeta{
			Kind:        KindMeasure,
			Group:       measure.GetMetadata().GetGroup(),
			Name:        measure.GetMetadata().GetName(),
			ModRevision: measure.GetMetadata().GetModRevision(),
		},
		Spec: measure,
	})
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

	entityTagSet := make(map[string]struct{})
	for _, tagName := range newMeasure.GetEntity().GetTagNames() {
		entityTagSet[tagName] = struct{}{}
	}
	newTagFamilyMap := make(map[string]map[string]*databasev1.TagSpec)
	for _, tf := range newMeasure.GetTagFamilies() {
		tagMap := make(map[string]*databasev1.TagSpec)
		for _, tag := range tf.GetTags() {
			tagMap[tag.GetName()] = tag
		}
		newTagFamilyMap[tf.GetName()] = tagMap
	}

	for _, prevTagFamily := range prevMeasure.GetTagFamilies() {
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
			newTag, tagExists := newTagMap[prevTag.GetName()]
			if !tagExists {
				if _, isEntity := entityTagSet[prevTag.GetName()]; isEntity {
					return fmt.Errorf("cannot delete entity tag %s in tag family %s", prevTag.GetName(), prevTagFamily.GetName())
				}
				continue
			}
			if prevTag.String() != newTag.String() {
				return fmt.Errorf("tag %s in tag family %s is different: %s != %s",
					prevTag.GetName(), prevTagFamily.GetName(), prevTag.String(), newTag.String())
			}
		}
	}

	newFieldMap := make(map[string]*databasev1.FieldSpec)
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

func (e *etcdSchemaRegistry) DeleteMeasure(ctx context.Context, metadata *commonv1.Metadata) (bool, error) {
	return e.delete(ctx, Metadata{
		TypeMeta: TypeMeta{
			Kind:  KindMeasure,
			Group: metadata.GetGroup(),
			Name:  metadata.GetName(),
		},
	})
}

func (e *etcdSchemaRegistry) TopNAggregations(ctx context.Context, metadata *commonv1.Metadata) ([]*databasev1.TopNAggregation, error) {
	aggregations, err := e.ListTopNAggregation(ctx, ListOpt{Group: metadata.GetGroup()})
	if err != nil {
		return nil, err
	}

	var result []*databasev1.TopNAggregation
	for _, aggrDef := range aggregations {
		// filter sourceMeasure
		if aggrDef.GetSourceMeasure().GetName() == metadata.GetName() {
			result = append(result, aggrDef)
		}
	}

	return result, nil
}

func formatMeasureKey(metadata *commonv1.Metadata) string {
	return formatKey(measureKeyPrefix, metadata)
}

func findDeletedTagsForMeasure(prevMeasure, newMeasure *databasev1.Measure) map[string]struct{} {
	newTagSet := make(map[string]struct{})
	for _, tf := range newMeasure.GetTagFamilies() {
		for _, tag := range tf.GetTags() {
			newTagSet[tag.GetName()] = struct{}{}
		}
	}

	deletedTags := make(map[string]struct{})
	for _, tf := range prevMeasure.GetTagFamilies() {
		for _, tag := range tf.GetTags() {
			if _, exists := newTagSet[tag.GetName()]; !exists {
				deletedTags[tag.GetName()] = struct{}{}
			}
		}
	}
	return deletedTags
}
