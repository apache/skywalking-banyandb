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

package logical

import (
	"github.com/pkg/errors"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
)

// IndexChecker allows checking the existence of a specific index rule.
type IndexChecker interface {
	IndexDefined(tagName string) (bool, *databasev1.IndexRule)
	IndexRuleDefined(ruleName string) (bool, *databasev1.IndexRule)
}

type emptyIndexChecker struct{}

func (emptyIndexChecker) IndexDefined(_ string) (bool, *databasev1.IndexRule) {
	return false, nil
}

func (emptyIndexChecker) IndexRuleDefined(_ string) (bool, *databasev1.IndexRule) {
	return false, nil
}

// TagSpecRegistry enables to find TagSpec by its name.
type TagSpecRegistry interface {
	FindTagSpecByName(string) *TagSpec
}

// Schema allows retrieving schemas in a convenient way.
type Schema interface {
	TagSpecRegistry
	IndexChecker
	EntityList() []string
	CreateTagRef(tags ...[]*Tag) ([][]*TagRef, error)
	CreateFieldRef(fields ...*Field) ([]*FieldRef, error)
	ProjTags(refs ...[]*TagRef) Schema
	ProjFields(refs ...*FieldRef) Schema
	Children() []Schema
}

// TagSpec wraps offsets to access a tag in the raw data swiftly.
type TagSpec struct {
	Spec         *databasev1.TagSpec
	TagFamilyIdx int
	TagIdx       int
}

// Equal compares fs and other have the same fields.
func (fs *TagSpec) Equal(other *TagSpec) bool {
	return fs.TagFamilyIdx == other.TagFamilyIdx && fs.TagIdx == other.TagIdx &&
		fs.Spec.GetType() == other.Spec.GetType() && fs.Spec.GetName() == other.Spec.GetName()
}

// TagSpecMap is a map of TapSpec implements TagSpecRegistry.
type TagSpecMap map[string]*TagSpec

// FindTagSpecByName finds TagSpec by its name in the registry.
func (tagSpecMap TagSpecMap) FindTagSpecByName(name string) *TagSpec {
	if spec, ok := tagSpecMap[name]; ok {
		return spec
	}
	return nil
}

// RegisterTagFamilies registers the tag specs with a given slice of TagFamilySpec.
func (tagSpecMap TagSpecMap) RegisterTagFamilies(tagFamilies []*databasev1.TagFamilySpec) {
	for tagFamilyIdx, tagFamily := range tagFamilies {
		for tagIdx, spec := range tagFamily.GetTags() {
			tagSpecMap.RegisterTag(tagFamilyIdx, tagIdx, spec)
		}
	}
}

// RegisterTag registers the tag spec with given tagFamilyName, tagName and indexes.
func (tagSpecMap TagSpecMap) RegisterTag(tagFamilyIdx, tagIdx int, spec *databasev1.TagSpec) {
	tagSpecMap[spec.GetName()] = &TagSpec{
		TagIdx:       tagIdx,
		TagFamilyIdx: tagFamilyIdx,
		Spec:         spec,
	}
}

// CommonSchema represents a sharable fields between independent schemas.
// It provides common access methods at the same time.
type CommonSchema struct {
	TagSpecMap
	IndexRules []*databasev1.IndexRule
	EntityList []string
}

// ProjTags inits a dictionary for getting TagSpec by tag's name.
func (cs *CommonSchema) ProjTags(refs ...[]*TagRef) *CommonSchema {
	if len(refs) == 0 {
		return nil
	}
	newCommonSchema := &CommonSchema{
		IndexRules: cs.IndexRules,
		TagSpecMap: make(map[string]*TagSpec),
		EntityList: cs.EntityList,
	}
	for projFamilyIdx, refInFamily := range refs {
		for projIdx, ref := range refInFamily {
			newCommonSchema.TagSpecMap[ref.Tag.GetTagName()] = &TagSpec{
				TagFamilyIdx: projFamilyIdx,
				TagIdx:       projIdx,
				Spec:         ref.Spec.Spec,
			}
		}
	}
	return newCommonSchema
}

// IndexDefined checks whether the field given is indexed.
func (cs *CommonSchema) IndexDefined(tagName string) (bool, *databasev1.IndexRule) {
	for _, idxRule := range cs.IndexRules {
		for _, tn := range idxRule.GetTags() {
			if tn == tagName {
				return true, idxRule
			}
		}
	}
	return false, nil
}

// IndexRuleDefined return the IndexRule by its name.
func (cs *CommonSchema) IndexRuleDefined(indexRuleName string) (bool, *databasev1.IndexRule) {
	for _, idxRule := range cs.IndexRules {
		if idxRule.GetMetadata().GetName() == indexRuleName {
			return true, idxRule
		}
	}
	return false, nil
}

// CreateRef create TagRef to the given tags.
// The family name of the tag is actually not used
// since the uniqueness of the tag names can be guaranteed across families.
func (cs *CommonSchema) CreateRef(tags ...[]*Tag) ([][]*TagRef, error) {
	tagRefs := make([][]*TagRef, 0, len(tags))
	for _, tagInFamily := range tags {
		var tagRefsInFamily []*TagRef
		for _, tag := range tagInFamily {
			if ts, ok := cs.TagSpecMap[tag.GetTagName()]; ok {
				tagRefsInFamily = append(tagRefsInFamily, &TagRef{tag, ts})
			}
		}
		tagRefs = append(tagRefs, tagRefsInFamily)
	}
	return tagRefs, nil
}

func stringSliceEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// MergeSchemas merges multiple schemas into one.
func MergeSchemas(schemas []*CommonSchema) (*CommonSchema, error) {
	if len(schemas) == 0 {
		return nil, errors.New("no schemas to merge")
	}

	baseEntity := schemas[0].EntityList
	for _, s := range schemas[1:] {
		if !stringSliceEqual(baseEntity, s.EntityList) {
			return nil, errors.New("schemas have different entity lists")
		}
	}

	merged := &CommonSchema{
		TagSpecMap: make(map[string]*TagSpec),
		IndexRules: make([]*databasev1.IndexRule, 0),
		EntityList: baseEntity,
	}

	indexRuleMap := make(map[string]*databasev1.IndexRule)

	for _, s := range schemas {
		for _, rule := range s.IndexRules {
			if existedRule := indexRuleMap[rule.Metadata.Name]; existedRule == nil {
				indexRuleMap[rule.Metadata.Name] = rule
			} else if !stringSliceEqual(existedRule.GetTags(), rule.GetTags()) {
				return nil, errors.Errorf("index rule %s has different tags", rule.Metadata.Name)
			}
		}
	}
	for _, rule := range indexRuleMap {
		merged.IndexRules = append(merged.IndexRules, rule)
	}

	return merged, nil
}

func mergeTagSpecs(dst, src []*databasev1.TagSpec) []*databasev1.TagSpec {
	res := make([]*databasev1.TagSpec, 0, len(dst)+len(src))
	res = append(res, dst...)
	for _, s := range src {
		found := false
		for i, d := range dst {
			if d.Name == s.Name {
				if d.Type != s.Type {
					// If the type is different, the tag spec is not compatible.
					// We need to set the type to unspecifed.
					res[i].Type = databasev1.TagType_TAG_TYPE_UNSPECIFIED
				}
				found = true
				break
			}
		}
		if !found {
			res = append(res, s)
		}
	}
	return res
}

// MergeTagFamilySpecs merges two slices of TagFamilySpec.
func MergeTagFamilySpecs(dst []*databasev1.TagFamilySpec, src []*databasev1.TagFamilySpec) []*databasev1.TagFamilySpec {
	res := make([]*databasev1.TagFamilySpec, 0, len(dst)+len(src))
	res = append(res, dst...)
	for _, s := range src {
		found := false
		for i, d := range dst {
			if d.Name == s.Name {
				res[i].Tags = mergeTagSpecs(d.Tags, s.Tags)
				found = true
				break
			}
		}
		if !found {
			res = append(res, s)
		}
	}
	return res
}
