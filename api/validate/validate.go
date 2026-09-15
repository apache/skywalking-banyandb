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
	"fmt"
	"path/filepath"
	"regexp"
	"strings"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk"
)

const (
	reservedTagSeparator = "#"
	// maxResourceNameLen caps group and resource names used as filesystem path elements.
	maxResourceNameLen = 255
)

// validResourceNamePattern is the human-readable form of validResourceName.
// Names must be a single path element: start and end with alphanumeric, with
// only letters, digits, `_`, `-`, and `.` in between (no separators or `..`).
const validResourceNamePattern = `^[a-zA-Z0-9]([a-zA-Z0-9._-]*[a-zA-Z0-9])?$`

var validResourceName = regexp.MustCompile(validResourceNamePattern)

func validateTagName(name string) error {
	if strings.Contains(name, reservedTagSeparator) {
		return fmt.Errorf("tag name %q must not contain reserved character %q", name, reservedTagSeparator)
	}
	return nil
}

// validateResourceNameFormat reports whether name is a valid group or resource
// name for use as a single filesystem path element under a catalog data root.
func validateResourceNameFormat(name string) error {
	if len(name) > maxResourceNameLen {
		return fmt.Errorf("must be at most %d characters", maxResourceNameLen)
	}
	if !validResourceName.MatchString(name) {
		return fmt.Errorf("must match %s", validResourceNamePattern)
	}
	return nil
}

func validateResourceName(kind, name string) error {
	if name == "" {
		return fmt.Errorf("%s is empty", kind)
	}
	if formatErr := validateResourceNameFormat(name); formatErr != nil {
		return fmt.Errorf("%s %q is invalid: %w", kind, name, formatErr)
	}
	return nil
}

// Group validates the provided Group object.
func Group(group *commonv1.Group) error {
	if group.Metadata == nil {
		return errors.New("metadata is required")
	}
	if group.Metadata.Name == "" {
		return errors.New("metadata.name is required")
	}
	if nameErr := validateResourceNameFormat(group.Metadata.Name); nameErr != nil {
		return fmt.Errorf("metadata.name %q is invalid: %w", group.Metadata.Name, nameErr)
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
	return GroupForNonProperty(group)
}

// GroupForNonProperty validates the provided Group object for Stream or Measure.
// It checks for nil values, empty strings, and unspecified enum values.
func GroupForNonProperty(group *commonv1.Group) error {
	if group == nil {
		return errors.New("group is nil")
	}
	if group.Metadata == nil {
		return errors.New("group metadata is nil")
	}
	if nameErr := validateResourceName("group name", group.Metadata.Name); nameErr != nil {
		return nameErr
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
	if pipelineCfg := group.GetPipeline(); pipelineCfg != nil {
		if validateErr := validateTracePipelineConfig(pipelineCfg); validateErr != nil {
			return fmt.Errorf("group pipeline config is invalid: %w", validateErr)
		}
	}
	return nil
}

// validateTracePipelineConfig validates the embedded TracePipelineConfig on a Group.
// It checks each plugin's sampler for trusted-dir-safe path shape and ABI version,
// then delegates bounds validation to the generated proto validator.
func validateTracePipelineConfig(cfg *commonv1.TracePipelineConfig) error {
	if validateErr := cfg.Validate(); validateErr != nil {
		return fmt.Errorf("proto validation failed: %w", validateErr)
	}
	for idx, plugin := range cfg.GetPlugins() {
		sp := plugin.GetSampler()
		if sp == nil {
			continue
		}
		if sp.GetPath() == "" {
			return fmt.Errorf("plugins[%d].sampler.path is empty", idx)
		}
		// Reject absolute paths and path segments that escape via "..".
		cleaned := filepath.Clean(sp.GetPath())
		if filepath.IsAbs(cleaned) {
			return fmt.Errorf("plugins[%d].sampler.path must be a relative path, got %q", idx, sp.GetPath())
		}
		if cleaned == ".." || strings.HasPrefix(cleaned, ".."+string(filepath.Separator)) {
			return fmt.Errorf("plugins[%d].sampler.path %q escapes the trusted directory", idx, sp.GetPath())
		}
		if sp.GetAbiVersion() != sdk.ABIVersion {
			return fmt.Errorf("plugins[%d].sampler.abi_version must be %d, got %d", idx, sdk.ABIVersion, sp.GetAbiVersion())
		}
	}
	for idx, stage := range cfg.GetStages() {
		for jdx, plugin := range stage.GetPlugins() {
			if plugin.GetSampler() != nil {
				return fmt.Errorf("stages[%d].plugins[%d]: stages[].plugins are not supported in v1; declare plugins at the top level", idx, jdx)
			}
		}
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
	if nameErr := validateResourceName("stream name", stream.Metadata.Name); nameErr != nil {
		return nameErr
	}
	if groupErr := validateResourceName("stream group", stream.Metadata.Group); groupErr != nil {
		return groupErr
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
	if nameErr := validateResourceName("measure name", measure.Metadata.Name); nameErr != nil {
		return nameErr
	}
	if groupErr := validateResourceName("measure group", measure.Metadata.Group); groupErr != nil {
		return groupErr
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

// CheckShardingKeySubset checks whether every ShardingKey tag exists in Entity tags
// and the shared tags appear in the same relative order.
func CheckShardingKeySubset(measure *databasev1.Measure) error {
	if measure == nil || measure.Entity == nil || measure.ShardingKey == nil || len(measure.ShardingKey.TagNames) == 0 {
		return nil
	}
	// A single entity tag may represent a composite identifier, e.g. OAP's entity_id,
	// which can already encode the sharding key fields such as service_id.
	// In that case, literal tag-name subset validation would produce false positives.
	if len(measure.Entity.TagNames) == 1 {
		return nil
	}
	entityIndex := make(map[string]int, len(measure.Entity.TagNames))
	for idx, tag := range measure.Entity.TagNames {
		entityIndex[tag] = idx
	}
	prevPos := -1
	for _, shardTag := range measure.ShardingKey.TagNames {
		pos, exists := entityIndex[shardTag]
		if !exists {
			return fmt.Errorf("ShardingKey tag %q is not present in Entity tags %v", shardTag, measure.Entity.TagNames)
		}
		if pos <= prevPos {
			return fmt.Errorf("ShardingKey %v is not in the same relative order as Entity tags %v",
				measure.ShardingKey.TagNames, measure.Entity.TagNames)
		}
		prevPos = pos
	}
	return nil
}

// Trace validates the provided Trace object.
// It checks for nil values, empty strings, and unspecified enum values.
func Trace(trace *databasev1.Trace) error {
	if trace == nil {
		return errors.New("trace is nil")
	}
	if trace.Metadata == nil {
		return errors.New("trace metadata is nil")
	}
	if nameErr := validateResourceName("trace name", trace.Metadata.Name); nameErr != nil {
		return nameErr
	}
	if groupErr := validateResourceName("trace group", trace.Metadata.Group); groupErr != nil {
		return groupErr
	}
	if len(trace.Tags) == 0 {
		return errors.New("trace tags is empty")
	}
	if trace.TraceIdTagName == "" {
		return errors.New("trace_id_tag_name is empty")
	}
	if err := validateTagName(trace.TraceIdTagName); err != nil {
		return err
	}
	if trace.SpanIdTagName == "" {
		return errors.New("span_id_tag_name is empty")
	}
	if err := validateTagName(trace.SpanIdTagName); err != nil {
		return err
	}
	if trace.TimestampTagName == "" {
		return errors.New("timestamp_tag_name is empty")
	}
	if err := validateTagName(trace.TimestampTagName); err != nil {
		return err
	}
	for i := range trace.Tags {
		if trace.Tags[i].Name == "" {
			return errors.New("trace tag name is empty")
		}
		if err := validateTagName(trace.Tags[i].Name); err != nil {
			return err
		}
		if trace.Tags[i].Type == databasev1.TagType_TAG_TYPE_UNSPECIFIED {
			return errors.New("trace tag type is unspecified")
		}
	}
	return nil
}

// TraceUpdate validates the provided Trace update operation.
// It ensures that reserved tags cannot be deleted.
func TraceUpdate(prevTrace, newTrace *databasev1.Trace) error {
	reservedTags := map[string]struct{}{
		newTrace.GetTraceIdTagName():   {},
		newTrace.GetTimestampTagName(): {},
		newTrace.GetSpanIdTagName():    {},
	}
	newTagSet := make(map[string]struct{})
	for _, tag := range newTrace.GetTags() {
		newTagSet[tag.GetName()] = struct{}{}
	}
	for _, prevTag := range prevTrace.GetTags() {
		if _, exists := newTagSet[prevTag.GetName()]; !exists {
			if _, isReserved := reservedTags[prevTag.GetName()]; isReserved {
				return errors.New("cannot delete reserved tag " + prevTag.GetName())
			}
		}
	}
	return nil
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
			if err := validateTagName(tagFamilies[i].Tags[j].Name); err != nil {
				return err
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
	if nameErr := validateResourceName("indexRule name", indexRule.Metadata.Name); nameErr != nil {
		return nameErr
	}
	if groupErr := validateResourceName("indexRule group", indexRule.Metadata.Group); groupErr != nil {
		return groupErr
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
	if nameErr := validateResourceName("indexRuleBinding name", indexRuleBinding.Metadata.Name); nameErr != nil {
		return nameErr
	}
	if groupErr := validateResourceName("indexRuleBinding group", indexRuleBinding.Metadata.Group); groupErr != nil {
		return groupErr
	}
	if indexRuleBinding.Subject == nil {
		return errors.New("indexRuleBinding subject is nil")
	}
	if subjectErr := validateResourceName("indexRuleBinding subject name", indexRuleBinding.Subject.Name); subjectErr != nil {
		return subjectErr
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
	if nameErr := validateResourceName("topNAggregation name", topNAggregation.Metadata.Name); nameErr != nil {
		return nameErr
	}
	if groupErr := validateResourceName("topNAggregation group", topNAggregation.Metadata.Group); groupErr != nil {
		return groupErr
	}
	if topNAggregation.SourceMeasure == nil {
		return errors.New("topNAggregation sourceMeasure is nil")
	}
	if sourceNameErr := validateResourceName("topNAggregation sourceMeasure name", topNAggregation.SourceMeasure.Name); sourceNameErr != nil {
		return sourceNameErr
	}
	if sourceGroupErr := validateResourceName("topNAggregation sourceMeasure group", topNAggregation.SourceMeasure.Group); sourceGroupErr != nil {
		return sourceGroupErr
	}
	if topNAggregation.CountersNumber <= 0 {
		return errors.New("topNAggregation countersNumber is invalid")
	}
	if topNAggregation.FieldName == "" {
		return errors.New("topNAggregation fieldName is empty")
	}
	return nil
}
