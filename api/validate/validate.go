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

// Package validate provides functions to validate the provided objects.
package validate

import (
	"errors"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
)

// Group validates the provided Group object.
func Group(group *commonv1.Group) error {
	if group.Metadata == nil {
		return errors.New("metadata is required")
	}
	if group.Metadata.Name == "" {
		return errors.New("metadata.name is required")
	}
	if group.Catalog == commonv1.Catalog_CATALOG_UNSPECIFIED {
		return errors.New("catalog is unspecified")
	}
	if group.Catalog == commonv1.Catalog_CATALOG_PROPERTY {
		if group.ResourceOpts == nil {
			return errors.New("resourceOpts is nil")
		}
		if group.ResourceOpts.ShardNum <= 0 {
			return errors.New("shardNum is invalid")
		}
		if group.ResourceOpts.SegmentInterval != nil {
			return errors.New("segmentInterval should be nil")
		}
		if group.ResourceOpts.Ttl != nil {
			return errors.New("ttl should be nil")
		}
		return nil
	}
	return GroupForStreamOrMeasure(group)
}

// GroupForStreamOrMeasure validates the provided Group object for Stream or Measure.
// It checks for nil values, empty strings, and unspecified enum values.
func GroupForStreamOrMeasure(group *commonv1.Group) error {
	if group == nil {
		return errors.New("group is nil")
	}
	if group.Metadata == nil {
		return errors.New("group metadata is nil")
	}
	if group.Metadata.Name == "" {
		return errors.New("group name is empty")
	}
	if group.Catalog == commonv1.Catalog_CATALOG_UNSPECIFIED {
		return errors.New("group catalog is unspecified")
	}
	if group.ResourceOpts == nil {
		return errors.New("group resourceOpts is nil")
	}
	if group.ResourceOpts.ShardNum <= 0 {
		return errors.New("group shardNum is invalid")
	}
	if group.ResourceOpts.SegmentInterval == nil {
		return errors.New("group segmentInterval is nil")
	}
	if group.ResourceOpts.SegmentInterval.Num <= 0 {
		return errors.New("group segmentInterval num is invalid")
	}
	if group.ResourceOpts.SegmentInterval.Unit == commonv1.IntervalRule_UNIT_UNSPECIFIED {
		return errors.New("group segmentInterval unit is unspecified")
	}
	if group.ResourceOpts.Ttl == nil {
		return errors.New("group ttl is nil")
	}
	if group.ResourceOpts.Ttl.Num <= 0 {
		return errors.New("group ttl num is invalid")
	}
	if group.ResourceOpts.Ttl.Unit == commonv1.IntervalRule_UNIT_UNSPECIFIED {
		return errors.New("group ttl unit is unspecified")
	}
	return nil
}

// Stream validates the provided Stream object.
// It checks for nil values, empty strings, and unspecified enum values.
func Stream(stream *databasev1.Stream) error {
	if stream == nil {
		return errors.New("stream is nil")
	}
	if stream.Metadata == nil {
		return errors.New("stream metadata is nil")
	}
	if stream.Metadata.Name == "" {
		return errors.New("stream name is empty")
	}
	if stream.Metadata.Group == "" {
		return errors.New("stream group is empty")
	}
	if len(stream.TagFamilies) == 0 {
		return errors.New("stream tag families is empty")
	}
	if stream.Entity == nil {
		return errors.New("stream entity is nil")
	}
	if len(stream.Entity.TagNames) == 0 {
		return errors.New("stream entity tag names is empty")
	}
	return tagFamily(stream.TagFamilies)
}

// Measure validates the provided Measure object.
// It checks for nil values, empty strings, and unspecified enum values.
func Measure(measure *databasev1.Measure) error {
	if measure == nil {
		return errors.New("measure is nil")
	}
	if measure.Metadata == nil {
		return errors.New("measure metadata is nil")
	}
	if measure.Metadata.Name == "" {
		return errors.New("measure name is empty")
	}
	if measure.Metadata.Group == "" {
		return errors.New("measure group is empty")
	}
	if measure.Entity == nil {
		return errors.New("measure entity is nil")
	}
	if len(measure.Entity.TagNames) == 0 {
		return errors.New("measure entity tag names is empty")
	}
	for i := range measure.Fields {
		if measure.Fields[i].Name == "" {
			return errors.New("field name is empty")
		}
		if measure.Fields[i].FieldType == databasev1.FieldType_FIELD_TYPE_UNSPECIFIED {
			return errors.New("field type is unspecified")
		}
		if measure.Fields[i].CompressionMethod == databasev1.CompressionMethod_COMPRESSION_METHOD_UNSPECIFIED {
			return errors.New("compression method is unspecified")
		}
		if measure.Fields[i].CompressionMethod == databasev1.CompressionMethod_COMPRESSION_METHOD_UNSPECIFIED {
			return errors.New("compression method is unspecified")
		}
	}
	if len(measure.TagFamilies) == 0 {
		return errors.New("measure tag families is empty")
	}
	if measure.IndexMode && len(measure.Fields) > 0 {
		return errors.New("index mode is enabled, but fields are not empty")
	}
	return tagFamily(measure.TagFamilies)
}

func tagFamily(tagFamilies []*databasev1.TagFamilySpec) error {
	for i := range tagFamilies {
		if tagFamilies[i].Name == "" {
			return errors.New("tag family name is empty")
		}
		for j := range tagFamilies[i].Tags {
			if tagFamilies[i].Tags[j].Name == "" {
				return errors.New("tag name is empty")
			}
			if tagFamilies[i].Tags[j].Type == databasev1.TagType_TAG_TYPE_UNSPECIFIED {
				return errors.New("tag type is unspecified")
			}
		}
	}
	return nil
}

// IndexRule validates the provided IndexRule object.
// It checks for nil values, empty strings, and unspecified enum values.
func IndexRule(indexRule *databasev1.IndexRule) error {
	if indexRule == nil {
		return errors.New("indexRule is nil")
	}
	if indexRule.Metadata == nil {
		return errors.New("indexRule metadata is nil")
	}
	if indexRule.Metadata.Name == "" {
		return errors.New("indexRule name is empty")
	}
	if indexRule.Metadata.Group == "" {
		return errors.New("indexRule group is empty")
	}
	if indexRule.Metadata.Id <= 0 {
		return errors.New("indexRule id is invalid")
	}
	if len(indexRule.Tags) == 0 {
		return errors.New("indexRule tags is empty")
	}
	if indexRule.Type == databasev1.IndexRule_TYPE_UNSPECIFIED {
		return errors.New("indexRule type is unspecified")
	}
	return nil
}

// IndexRuleBinding validates the provided IndexRuleBinding object.
// It checks for nil values, empty strings, and unspecified enum values.
func IndexRuleBinding(indexRuleBinding *databasev1.IndexRuleBinding) error {
	if indexRuleBinding == nil {
		return errors.New("indexRuleBinding is nil")
	}
	if indexRuleBinding.Metadata == nil {
		return errors.New("indexRuleBinding metadata is nil")
	}
	if indexRuleBinding.Metadata.Name == "" {
		return errors.New("indexRuleBinding name is empty")
	}
	if indexRuleBinding.Metadata.Group == "" {
		return errors.New("indexRuleBinding group is empty")
	}
	if indexRuleBinding.Subject == nil {
		return errors.New("indexRuleBinding subject is nil")
	}
	if indexRuleBinding.Subject.Name == "" {
		return errors.New("indexRuleBinding subject name is empty")
	}
	if indexRuleBinding.Subject.Catalog == commonv1.Catalog_CATALOG_UNSPECIFIED {
		return errors.New("indexRuleBinding subject catalog is unspecified")
	}
	if len(indexRuleBinding.Rules) == 0 {
		return errors.New("indexRuleBinding rules is empty")
	}
	return nil
}

// TopNAggregation validates the provided TopNAggregation object.
// It checks for nil values, empty strings, and unspecified enum values.
func TopNAggregation(topNAggregation *databasev1.TopNAggregation) error {
	if topNAggregation == nil {
		return errors.New("topNAggregation is nil")
	}
	if topNAggregation.Metadata == nil {
		return errors.New("topNAggregation metadata is nil")
	}
	if topNAggregation.Metadata.Name == "" {
		return errors.New("topNAggregation name is empty")
	}
	if topNAggregation.Metadata.Group == "" {
		return errors.New("topNAggregation group is empty")
	}
	if topNAggregation.SourceMeasure == nil {
		return errors.New("topNAggregation sourceMeasure is nil")
	}
	if topNAggregation.SourceMeasure.Name == "" {
		return errors.New("topNAggregation sourceMeasure name is empty")
	}
	if topNAggregation.SourceMeasure.Group == "" {
		return errors.New("topNAggregation sourceMeasure group is empty")
	}
	if topNAggregation.CountersNumber <= 0 {
		return errors.New("topNAggregation countersNumber is invalid")
	}
	if topNAggregation.FieldName == "" {
		return errors.New("topNAggregation fieldName is empty")
	}
	return nil
}
