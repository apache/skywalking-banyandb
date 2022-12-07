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
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
)

// Schema allows retrieving schemas in a convenient way.
type Schema interface {
	Scope() tsdb.Entry
	EntityList() []string
	IndexDefined(tagName string) (bool, *databasev1.IndexRule)
	IndexRuleDefined(string) (bool, *databasev1.IndexRule)
	CreateTagRef(tags ...[]*Tag) ([][]*TagRef, error)
	CreateFieldRef(fields ...*Field) ([]*FieldRef, error)
	ProjTags(refs ...[]*TagRef) Schema
	ProjFields(refs ...*FieldRef) Schema
	Equal(Schema) bool
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

// CommonSchema represents a sharable fields between independent schemas.
// It provides common access methods at the same time.
type CommonSchema struct {
	IndexRules []*databasev1.IndexRule
	TagMap     map[string]*TagSpec
	EntityList []string
}

// ProjTags inits a dictionary for getting TagSpec by tag's name.
func (cs *CommonSchema) ProjTags(refs ...[]*TagRef) *CommonSchema {
	if len(refs) == 0 {
		return nil
	}
	newCommonSchema := &CommonSchema{
		IndexRules: cs.IndexRules,
		TagMap:     make(map[string]*TagSpec),
		EntityList: cs.EntityList,
	}
	for projFamilyIdx, refInFamily := range refs {
		for projIdx, ref := range refInFamily {
			newCommonSchema.TagMap[ref.Tag.getTagName()] = &TagSpec{
				TagFamilyIdx: projFamilyIdx,
				TagIdx:       projIdx,
				Spec:         ref.Spec.Spec,
			}
		}
	}
	return newCommonSchema
}

// RegisterTag registers the tag spec with given tagFamilyName, tagName and indexes.
func (cs *CommonSchema) RegisterTag(tagFamilyIdx, tagIdx int, spec *databasev1.TagSpec) {
	cs.TagMap[spec.GetName()] = &TagSpec{
		TagIdx:       tagIdx,
		TagFamilyIdx: tagFamilyIdx,
		Spec:         spec,
	}
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
	tagRefs := make([][]*TagRef, len(tags))
	for i, tagInFamily := range tags {
		var tagRefsInFamily []*TagRef
		for _, tag := range tagInFamily {
			if ts, ok := cs.TagMap[tag.getTagName()]; ok {
				tagRefsInFamily = append(tagRefsInFamily, &TagRef{tag, ts})
			} else {
				return nil, errors.Wrap(errTagNotDefined, tag.GetCompoundName())
			}
		}
		tagRefs[i] = tagRefsInFamily
	}
	return tagRefs, nil
}
