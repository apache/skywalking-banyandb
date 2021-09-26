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
	"github.com/google/go-cmp/cmp"
	"github.com/pkg/errors"

	databasev2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v2"
)

type Schema interface {
	EntityList() []string
	IndexDefined(*Tag) (bool, *databasev2.IndexRule)
	IndexRuleDefined(string) (bool, *databasev2.IndexRule)
	FieldSubscript(string) (bool, int)
	FieldDefined(string) bool
	CreateRef(tags ...[]*Tag) ([][]*FieldRef, error)
	Proj(refs ...[]*FieldRef) Schema
	Equal(Schema) bool
	ShardNumber() uint32
	TraceIDFieldName() string
}

type tagSpec struct {
	// Idx is defined as
	// 1) the field index based on the (trace) schema for the underlying plans which
	//    directly interact with the database and index modules,
	// 2) the projection index given by the users for those plans which can only access the data from parent plans,
	//    e.g. orderBy plan uses this projection index to access the data entities (normally a projection view)
	//    from the parent plan.
	TagFamilyIdx int
	TagIdx       int
	spec         *databasev2.TagSpec
}

func (fs *tagSpec) Equal(other *tagSpec) bool {
	return fs.TagFamilyIdx == other.TagFamilyIdx && fs.TagIdx == other.TagIdx &&
		fs.spec.GetType() == other.spec.GetType() && fs.spec.GetName() == other.spec.GetName()
}

var _ Schema = (*schema)(nil)

type schema struct {
	stream     *databasev2.Stream
	indexRules []*databasev2.IndexRule
	fieldMap   map[string]*tagSpec
	entityList []string
}

func (s *schema) IndexRuleDefined(indexRuleName string) (bool, *databasev2.IndexRule) {
	for _, idxRule := range s.indexRules {
		if idxRule.GetMetadata().GetName() == indexRuleName {
			return true, idxRule
		}
	}
	return false, nil
}

func (s *schema) EntityList() []string {
	return s.entityList
}

func (s *schema) TraceIDFieldName() string {
	// TODO: how to extract trace_id?
	return "trace_id"
}

// IndexDefined checks whether the field given is indexed
func (s *schema) IndexDefined(tag *Tag) (bool, *databasev2.IndexRule) {
	for _, idxRule := range s.indexRules {
		for _, tagName := range idxRule.GetTags() {
			if tag.GetTagName() == tagName {
				return true, idxRule
			}
		}
	}
	return false, nil
}

func (s *schema) FieldSubscript(field string) (bool, int) {
	// TODO: what does this mean
	for i, indexObj := range s.indexRules {
		for _, fieldName := range indexObj.GetTags() {
			if field == fieldName {
				return true, i
			}
		}
	}
	return false, -1
}

func (s *schema) Equal(s2 Schema) bool {
	if other, ok := s2.(*schema); ok {
		return cmp.Equal(other.fieldMap, s.fieldMap)
	}
	return false
}

// registerField registers the field spec with given tagFamilyName, tagName and indexes.
func (s *schema) registerField(tagName string, tagFamilyIdx, tagIdx int, spec *databasev2.TagSpec) {
	s.fieldMap[tagName] = &tagSpec{
		TagIdx:       tagIdx,
		TagFamilyIdx: tagFamilyIdx,
		spec:         spec,
	}
}

func (s *schema) FieldDefined(name string) bool {
	if _, ok := s.fieldMap[name]; ok {
		return true
	}
	return false
}

// CreateRef create FieldRef to the given tags.
// The family name of the tag is actually not used
// since the uniqueness of the tag names can be guaranteed across families.
func (s *schema) CreateRef(tags ...[]*Tag) ([][]*FieldRef, error) {
	fieldRefs := make([][]*FieldRef, len(tags))
	for i, tagInFamily := range tags {
		var fieldRefsInFamily []*FieldRef
		for _, tag := range tagInFamily {
			if fs, ok := s.fieldMap[tag.GetTagName()]; ok {
				fieldRefsInFamily = append(fieldRefsInFamily, &FieldRef{tag, fs})
			} else {
				return nil, errors.Wrap(ErrFieldNotDefined, tag.GetCompoundName())
			}
		}
		fieldRefs[i] = fieldRefsInFamily
	}
	return fieldRefs, nil
}

// Proj creates a projection view from the present schema
// with a given list of projections
func (s *schema) Proj(refs ...[]*FieldRef) Schema {
	if len(refs) == 0 {
		return nil
	}
	newSchema := &schema{
		stream:     s.stream,
		indexRules: s.indexRules,
		fieldMap:   make(map[string]*tagSpec),
		entityList: s.entityList,
	}
	for projFamilyIdx, refInFamily := range refs {
		for projIdx, ref := range refInFamily {
			newSchema.fieldMap[ref.tag.GetTagName()] = &tagSpec{
				TagFamilyIdx: projFamilyIdx,
				TagIdx:       projIdx,
				spec:         ref.Spec.spec,
			}
		}
	}
	return newSchema
}

func (s *schema) ShardNumber() uint32 {
	return s.stream.ShardNum
}
