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
const validResourceNamePattern = `^[a-zA-Z0-9_]([a-zA-Z0-9._-]*[a-zA-Z0-9])?$`

var validResourceName = regexp.MustCompile(validResourceNamePattern)

func validateTagName(name string) error {
	if strings.Contains(name, reservedTagSeparator) {
		return fmt.Errorf("tag name %q must not contain reserved character %q", name, reservedTagSeparator)
	}
	if formatErr := validateResourceNameFormat(name); formatErr != nil {
		return fmt.Errorf("tag name %q is invalid: %w", name, formatErr)
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
		if group.ResourceOpts.ShardNum > 1024 {
			return fmt.Errorf("shardNum %d exceeds maximum 1024", group.ResourceOpts.ShardNum)
		}
		if group.ResourceOpts.Replicas > 16 {
			return fmt.Errorf("replicas %d exceeds maximum 16", group.ResourceOpts.Replicas)
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
	if shardErr := validateResourceOptsBounds(group.ResourceOpts); shardErr != nil {
		return shardErr
	}
	if pipelineCfg := group.GetPipeline(); pipelineCfg != nil {
		if validateErr := validateTracePipelineConfig(pipelineCfg); validateErr != nil {
			return fmt.Errorf("group pipeline config is invalid: %w", validateErr)
		}
	}
	return nil
}

func validateResourceOptsBounds(opts *commonv1.ResourceOpts) error {
	if opts.ShardNum > 1024 {
		return fmt.Errorf("group shardNum %d exceeds maximum 1024", opts.ShardNum)
	}
	if opts.Replicas > 16 {
		return fmt.Errorf("group replicas %d exceeds maximum 16", opts.Replicas)
	}
	if opts.SegmentInterval != nil && opts.SegmentInterval.Num > 3650 {
		return fmt.Errorf("group segmentInterval num %d exceeds maximum 3650", opts.SegmentInterval.Num)
	}
	if opts.Ttl != nil && opts.Ttl.Num > 3650 {
		return fmt.Errorf("group ttl num %d exceeds maximum 3650", opts.Ttl.Num)
	}
	if len(opts.Stages) > 16 {
		return fmt.Errorf("group stages count %d exceeds maximum 16", len(opts.Stages))
	}
	for idx, stage := range opts.Stages {
		if nameErr := validateResourceName(fmt.Sprintf("group stages[%d].name", idx), stage.GetName()); nameErr != nil {
			return nameErr
		}
		if stage.GetShardNum() > 1024 {
			return fmt.Errorf("group stages[%d].shardNum %d exceeds maximum 1024", idx, stage.GetShardNum())
		}
		if stage.GetReplicas() > 16 {
			return fmt.Errorf("group stages[%d].replicas %d exceeds maximum 16", idx, stage.GetReplicas())
		}
		if stage.GetNodeSelector() != "" && len(stage.GetNodeSelector()) > 1024 {
			return fmt.Errorf("group stages[%d].node_selector exceeds maximum 1024 characters", idx)
		}
	}
	if len(opts.DefaultStages) > 16 {
		return fmt.Errorf("group default_stages count %d exceeds maximum 16", len(opts.DefaultStages))
	}
	for idx, stageName := range opts.DefaultStages {
		if nameErr := validateResourceName(fmt.Sprintf("group default_stages[%d]", idx), stageName); nameErr != nil {
			return nameErr
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
	for idx, tagName := range stream.Entity.TagNames {
		if tagErr := validateTagName(tagName); tagErr != nil {
			return fmt.Errorf("stream entity tag_names[%d]: %w", idx, tagErr)
		}
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
	for idx, tagName := range measure.Entity.TagNames {
		if tagErr := validateTagName(tagName); tagErr != nil {
			return fmt.Errorf("measure entity tag_names[%d]: %w", idx, tagErr)
		}
	}
	for i := range measure.Fields {
		if nameErr := validateResourceName("field name", measure.Fields[i].Name); nameErr != nil {
			return nameErr
		}
		if measure.Fields[i].FieldType == databasev1.FieldType_FIELD_TYPE_UNSPECIFIED {
			return errors.New("field type is unspecified")
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
	if measure.ShardingKey != nil {
		for idx, tagName := range measure.ShardingKey.TagNames {
			if tagErr := validateTagName(tagName); tagErr != nil {
				return fmt.Errorf("measure sharding_key tag_names[%d]: %w", idx, tagErr)
			}
		}
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
	if len(tagFamilies) > 32 {
		return fmt.Errorf("tag families count %d exceeds maximum 32", len(tagFamilies))
	}
	for i := range tagFamilies {
		if nameErr := validateResourceName("tag family name", tagFamilies[i].Name); nameErr != nil {
			return nameErr
		}
		if len(tagFamilies[i].Tags) > 512 {
			return fmt.Errorf("tag family %q tags count %d exceeds maximum 512", tagFamilies[i].Name, len(tagFamilies[i].Tags))
		}
		for j := range tagFamilies[i].Tags {
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

// UniqueTagNames rejects a tag name that appears more than once across a
// resource's tag families.
//
// A tag name identifies a tag across the whole resource, not merely within its
// own family: the query layer resolves a bare name against a flat map keyed by
// name alone -- logical.CommonSchema.CreateRef states the invariant outright,
// and BydbQL's transformer relies on it for every identifier it resolves. Two
// families sharing a name make those lookups depend on iteration order. A
// single resource-wide set also catches a name duplicated within one family.
//
// This is deliberately separate from Measure and Stream, which the data nodes
// also run when loading an already-persisted schema. A schema registered
// before this check existed must keep loading rather than be dropped, so only
// the registry's create and update paths call this.
func UniqueTagNames(tagFamilies []*databasev1.TagFamilySpec) error {
	familyOfTag := make(map[string]string)
	for i := range tagFamilies {
		for j := range tagFamilies[i].Tags {
			tagName := tagFamilies[i].Tags[j].Name
			previousFamily, duplicated := familyOfTag[tagName]
			if !duplicated {
				familyOfTag[tagName] = tagFamilies[i].Name
				continue
			}
			if previousFamily == tagFamilies[i].Name {
				return fmt.Errorf("tag name %q is duplicated in tag family %q", tagName, previousFamily)
			}
			return fmt.Errorf("tag name %q is duplicated in tag families %q and %q: tag names must be unique across all tag families",
				tagName, previousFamily, tagFamilies[i].Name)
		}
	}
	return nil
}

// Property validates the provided Property schema object.
func Property(property *databasev1.Property) error {
	if property == nil {
		return errors.New("property is nil")
	}
	if property.Metadata == nil {
		return errors.New("property metadata is nil")
	}
	if nameErr := validateResourceName("property name", property.Metadata.Name); nameErr != nil {
		return nameErr
	}
	if groupErr := validateResourceName("property group", property.Metadata.Group); groupErr != nil {
		return groupErr
	}
	if len(property.Tags) > 512 {
		return fmt.Errorf("property tags count %d exceeds maximum 512", len(property.Tags))
	}
	for i := range property.Tags {
		if err := validateTagName(property.Tags[i].Name); err != nil {
			return err
		}
		if property.Tags[i].Type == databasev1.TagType_TAG_TYPE_UNSPECIFIED {
			return errors.New("property tag type is unspecified")
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
	if len(indexRule.Tags) > 64 {
		return fmt.Errorf("indexRule tags count %d exceeds maximum 64", len(indexRule.Tags))
	}
	for idx, tagName := range indexRule.Tags {
		if tagErr := validateTagName(tagName); tagErr != nil {
			return fmt.Errorf("indexRule tags[%d]: %w", idx, tagErr)
		}
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
	if len(indexRuleBinding.Rules) > 128 {
		return fmt.Errorf("indexRuleBinding rules count %d exceeds maximum 128", len(indexRuleBinding.Rules))
	}
	for idx, ruleName := range indexRuleBinding.Rules {
		if ruleErr := validateResourceName(fmt.Sprintf("indexRuleBinding rules[%d]", idx), ruleName); ruleErr != nil {
			return ruleErr
		}
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
	if topNAggregation.CountersNumber > 100000 {
		return fmt.Errorf("topNAggregation countersNumber %d exceeds maximum 100000", topNAggregation.CountersNumber)
	}
	if fieldErr := validateResourceName("topNAggregation fieldName", topNAggregation.FieldName); fieldErr != nil {
		return fieldErr
	}
	if len(topNAggregation.GroupByTagNames) > 64 {
		return fmt.Errorf("topNAggregation group_by_tag_names count %d exceeds maximum 64", len(topNAggregation.GroupByTagNames))
	}
	for idx, tagName := range topNAggregation.GroupByTagNames {
		if tagErr := validateTagName(tagName); tagErr != nil {
			return fmt.Errorf("topNAggregation group_by_tag_names[%d]: %w", idx, tagErr)
		}
	}
	return nil
}
