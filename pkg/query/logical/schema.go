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

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
)

type Schema interface {
	Scope() tsdb.Entry
	EntityList() []string
	IndexDefined(*Tag) (bool, *databasev1.IndexRule)
	IndexRuleDefined(string) (bool, *databasev1.IndexRule)
	CreateTagRef(tags ...[]*Tag) ([][]*TagRef, error)
	CreateFieldRef(fields ...*Field) ([]*FieldRef, error)
	ProjTags(refs ...[]*TagRef) Schema
	ProjField(*FieldRef) Schema
	Equal(Schema) bool
	ShardNumber() uint32
	TraceIDFieldName() string
}

type tagSpec struct {
	// Idx is defined as
	// 1) the field index based on the (stream/measure) schema for the underlying plans which
	//    directly interact with the database and index modules,
	// 2) the projection index given by the users for those plans which can only access the data from parent plans,
	//    e.g. orderBy plan uses this projection index to access the data entities (normally a projection view)
	//    from the parent plan.
	TagFamilyIdx int
	TagIdx       int
	spec         *databasev1.TagSpec
}

func (fs *tagSpec) Equal(other *tagSpec) bool {
	return fs.TagFamilyIdx == other.TagFamilyIdx && fs.TagIdx == other.TagIdx &&
		fs.spec.GetType() == other.spec.GetType() && fs.spec.GetName() == other.spec.GetName()
}

var _ Schema = (*streamSchema)(nil)

type commonSchema struct {
	group      *commonv1.Group
	indexRules []*databasev1.IndexRule
	tagMap     map[string]*tagSpec
	entityList []string
}

func (cs *commonSchema) ProjTags(refs ...[]*TagRef) *commonSchema {
	if len(refs) == 0 {
		return nil
	}
	newCommonSchema := &commonSchema{
		indexRules: cs.indexRules,
		tagMap:     make(map[string]*tagSpec),
		entityList: cs.entityList,
	}
	for projFamilyIdx, refInFamily := range refs {
		for projIdx, ref := range refInFamily {
			newCommonSchema.tagMap[ref.tag.GetTagName()] = &tagSpec{
				TagFamilyIdx: projFamilyIdx,
				TagIdx:       projIdx,
				spec:         ref.Spec.spec,
			}
		}
	}
	return newCommonSchema
}

// registerTag registers the tag spec with given tagFamilyName, tagName and indexes.
func (cs *commonSchema) registerTag(tagFamilyIdx, tagIdx int, spec *databasev1.TagSpec) {
	cs.tagMap[spec.GetName()] = &tagSpec{
		TagIdx:       tagIdx,
		TagFamilyIdx: tagFamilyIdx,
		spec:         spec,
	}
}

func (cs *commonSchema) ShardNumber() uint32 {
	return cs.group.ResourceOpts.ShardNum
}

func (cs *commonSchema) EntityList() []string {
	return cs.entityList
}

// IndexDefined checks whether the field given is indexed
func (cs *commonSchema) IndexDefined(tag *Tag) (bool, *databasev1.IndexRule) {
	for _, idxRule := range cs.indexRules {
		for _, tagName := range idxRule.GetTags() {
			if tag.GetTagName() == tagName {
				return true, idxRule
			}
		}
	}
	return false, nil
}

func (cs *commonSchema) IndexRuleDefined(indexRuleName string) (bool, *databasev1.IndexRule) {
	for _, idxRule := range cs.indexRules {
		if idxRule.GetMetadata().GetName() == indexRuleName {
			return true, idxRule
		}
	}
	return false, nil
}

// CreateRef create TagRef to the given tags.
// The family name of the tag is actually not used
// since the uniqueness of the tag names can be guaranteed across families.
func (cs *commonSchema) CreateRef(tags ...[]*Tag) ([][]*TagRef, error) {
	tagRefs := make([][]*TagRef, len(tags))
	for i, tagInFamily := range tags {
		var tagRefsInFamily []*TagRef
		for _, tag := range tagInFamily {
			if ts, ok := cs.tagMap[tag.GetTagName()]; ok {
				tagRefsInFamily = append(tagRefsInFamily, &TagRef{tag, ts})
			} else {
				return nil, errors.Wrap(ErrTagNotDefined, tag.GetCompoundName())
			}
		}
		tagRefs[i] = tagRefsInFamily
	}
	return tagRefs, nil
}

type streamSchema struct {
	stream *databasev1.Stream
	common *commonSchema
}

func (s *streamSchema) CreateFieldRef(fields ...*Field) ([]*FieldRef, error) {
	panic("no field for stream")
}

func (s *streamSchema) IndexRuleDefined(indexRuleName string) (bool, *databasev1.IndexRule) {
	return s.common.IndexRuleDefined(indexRuleName)
}

func (s *streamSchema) EntityList() []string {
	return s.common.EntityList()
}

func (s *streamSchema) TraceIDFieldName() string {
	// TODO: how to extract trace_id?
	return "trace_id"
}

// IndexDefined checks whether the field given is indexed
func (s *streamSchema) IndexDefined(tag *Tag) (bool, *databasev1.IndexRule) {
	return s.common.IndexDefined(tag)
}

func (s *streamSchema) Equal(s2 Schema) bool {
	if other, ok := s2.(*streamSchema); ok {
		return cmp.Equal(other.common.tagMap, s.common.tagMap)
	}
	return false
}

// registerTag registers the tag spec with given tagFamilyName, tagName and indexes.
func (s *streamSchema) registerTag(tagFamilyIdx, tagIdx int, spec *databasev1.TagSpec) {
	s.common.registerTag(tagFamilyIdx, tagIdx, spec)
}

// CreateTagRef create TagRef to the given tags.
// The family name of the tag is actually not used
// since the uniqueness of the tag names can be guaranteed across families.
func (s *streamSchema) CreateTagRef(tags ...[]*Tag) ([][]*TagRef, error) {
	return s.common.CreateRef(tags...)
}

// ProjTags creates a projection view from the present streamSchema
// with a given list of projections
func (s *streamSchema) ProjTags(refs ...[]*TagRef) Schema {
	if len(refs) == 0 {
		return nil
	}
	newSchema := &streamSchema{
		stream: s.stream,
		common: s.common.ProjTags(refs...),
	}
	return newSchema
}

func (s *streamSchema) ProjField(*FieldRef) Schema {
	panic("stream does not support field")
}

func (s *streamSchema) ShardNumber() uint32 {
	return s.common.ShardNumber()
}

func (s *streamSchema) Scope() tsdb.Entry {
	return tsdb.Entry(s.stream.Metadata.Name)
}

var _ Schema = (*measureSchema)(nil)

type fieldSpec struct {
	FieldIdx int
	spec     *databasev1.FieldSpec
}

type measureSchema struct {
	measure  *databasev1.Measure
	fieldMap map[string]*fieldSpec
	common   *commonSchema
}

func (m *measureSchema) Scope() tsdb.Entry {
	return tsdb.Entry(m.measure.Metadata.Name)
}

func (m *measureSchema) EntityList() []string {
	return m.common.EntityList()
}

func (m *measureSchema) IndexDefined(tag *Tag) (bool, *databasev1.IndexRule) {
	return m.common.IndexDefined(tag)
}

func (m *measureSchema) IndexRuleDefined(indexRuleName string) (bool, *databasev1.IndexRule) {
	return m.common.IndexRuleDefined(indexRuleName)
}

func (m *measureSchema) CreateTagRef(tags ...[]*Tag) ([][]*TagRef, error) {
	return m.common.CreateRef(tags...)
}

func (m *measureSchema) CreateFieldRef(fields ...*Field) ([]*FieldRef, error) {
	fieldRefs := make([]*FieldRef, len(fields))
	for idx, field := range fields {
		if fs, ok := m.fieldMap[field.name]; ok {
			fieldRefs[idx] = &FieldRef{field, fs}
		} else {
			return nil, errors.Wrap(ErrFieldNotDefined, field.name)
		}
	}
	return fieldRefs, nil
}

func (m *measureSchema) ProjTags(refs ...[]*TagRef) Schema {
	if len(refs) == 0 {
		return nil
	}
	newSchema := &measureSchema{
		measure:  m.measure,
		common:   m.common.ProjTags(refs...),
		fieldMap: m.fieldMap,
	}
	return newSchema
}

func (m *measureSchema) ProjField(fieldRef *FieldRef) Schema {
	newFieldMap := make(map[string]*fieldSpec)
	if spec, ok := m.fieldMap[fieldRef.field.name]; ok {
		newFieldMap[fieldRef.field.name] = spec
	}
	return &measureSchema{
		measure:  m.measure,
		common:   m.common,
		fieldMap: newFieldMap,
	}
}

func (m *measureSchema) Equal(s2 Schema) bool {
	if other, ok := s2.(*measureSchema); ok {
		// TODO: add more equality checks
		return cmp.Equal(other.common.tagMap, m.common.tagMap)
	}
	return false
}

func (m *measureSchema) ShardNumber() uint32 {
	return m.common.ShardNumber()
}

// registerTag registers the tag spec with given tagFamilyIdx and tagIdx.
func (m *measureSchema) registerTag(tagFamilyIdx, tagIdx int, spec *databasev1.TagSpec) {
	m.common.registerTag(tagFamilyIdx, tagIdx, spec)
}

// registerField registers the field spec with given index.
func (m *measureSchema) registerField(fieldIdx int, spec *databasev1.FieldSpec) {
	m.fieldMap[spec.GetName()] = &fieldSpec{
		FieldIdx: fieldIdx,
		spec:     spec,
	}
}

func (m *measureSchema) TraceIDFieldName() string {
	// We don't have traceID for measure
	panic("implement me")
}
