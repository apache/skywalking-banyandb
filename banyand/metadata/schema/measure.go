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
	if err := validate.GroupForStreamOrMeasure(g); err != nil {
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
	if err = validate.GroupForStreamOrMeasure(g); err != nil {
		return 0, err
	}
	prev, err := e.GetMeasure(ctx, measure.GetMetadata())
	if err != nil {
		return 0, err
	}
	if prev == nil {
		return 0, errors.WithMessagef(ErrGRPCResourceNotFound, "measure %s not found", measure.GetMetadata().GetName())
	}
	if err := validateEqualExceptAppendTagsAndFields(prev, measure); err != nil {
		return 0, errors.WithMessagef(ErrInputInvalid, "validation failed: %s", err)
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

func validateEqualExceptAppendTagsAndFields(prevMeasure, newMeasure *databasev1.Measure) error {
	if prevMeasure.GetInterval() != newMeasure.GetInterval() {
		return fmt.Errorf("interval is different: %s != %s", prevMeasure.GetInterval(), newMeasure.GetInterval())
	}
	if prevMeasure.GetEntity().String() != newMeasure.GetEntity().String() {
		return fmt.Errorf("entity is different: %s != %s", prevMeasure.GetEntity().String(), newMeasure.GetEntity().String())
	}
	if prevMeasure.GetIndexMode() != newMeasure.GetIndexMode() {
		return fmt.Errorf("index mode is different: %v != %v", prevMeasure.GetIndexMode(), newMeasure.GetIndexMode())
	}
	if len(prevMeasure.GetTagFamilies()) > len(newMeasure.GetTagFamilies()) {
		return fmt.Errorf("number of tag families is less in the new measure")
	}
	if len(prevMeasure.GetFields()) > len(newMeasure.GetFields()) {
		return fmt.Errorf("number of fields is less in the new measure")
	}
	for i, tagFamily := range prevMeasure.GetTagFamilies() {
		if tagFamily.Name != newMeasure.GetTagFamilies()[i].Name {
			return fmt.Errorf("tag family name is different: %s != %s", tagFamily.Name, newMeasure.GetTagFamilies()[i].Name)
		}
		if len(tagFamily.Tags) > len(newMeasure.GetTagFamilies()[i].Tags) {
			return fmt.Errorf("number of tags in tag family %s is less in the new measure", tagFamily.Name)
		}
		for j, tag := range tagFamily.Tags {
			if tag.String() != newMeasure.GetTagFamilies()[i].Tags[j].String() {
				return fmt.Errorf("tag %s in tag family %s is different: %s != %s", tag.Name, tagFamily.Name, tag.String(), newMeasure.GetTagFamilies()[i].Tags[j].String())
			}
		}
	}
	for i, field := range prevMeasure.GetFields() {
		if field.String() != newMeasure.GetFields()[i].String() {
			return fmt.Errorf("field is different: %s != %s", field.String(), newMeasure.GetFields()[i].String())
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
