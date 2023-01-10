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

package measure

import (
	"github.com/google/go-cmp/cmp"
	"github.com/pkg/errors"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
)

// ErrFieldNotDefined indicated the field is not defined in the measure schema.
var ErrFieldNotDefined = errors.New("field is not defined")

type measureSchema struct {
	measure  *databasev1.Measure
	fieldMap map[string]*logical.FieldSpec
	common   *logical.CommonSchema
}

func (m *measureSchema) Scope() tsdb.Entry {
	return tsdb.Entry(m.measure.Metadata.Name)
}

func (m *measureSchema) EntityList() []string {
	return m.common.EntityList
}

func (m *measureSchema) FindTagSpecByName(name string) *logical.TagSpec {
	return m.common.FindTagSpecByName(name)
}

func (m *measureSchema) IndexDefined(tagName string) (bool, *databasev1.IndexRule) {
	return m.common.IndexDefined(tagName)
}

func (m *measureSchema) IndexRuleDefined(indexRuleName string) (bool, *databasev1.IndexRule) {
	return m.common.IndexRuleDefined(indexRuleName)
}

func (m *measureSchema) CreateTagRef(tags ...[]*logical.Tag) ([][]*logical.TagRef, error) {
	return m.common.CreateRef(tags...)
}

func (m *measureSchema) CreateFieldRef(fields ...*logical.Field) ([]*logical.FieldRef, error) {
	fieldRefs := make([]*logical.FieldRef, len(fields))
	for idx, field := range fields {
		if fs, ok := m.fieldMap[field.Name]; ok {
			fieldRefs[idx] = &logical.FieldRef{Field: field, Spec: fs}
		} else {
			return nil, errors.Wrap(ErrFieldNotDefined, field.Name)
		}
	}
	return fieldRefs, nil
}

func (m *measureSchema) ProjTags(refs ...[]*logical.TagRef) logical.Schema {
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

func (m *measureSchema) ProjFields(fieldRefs ...*logical.FieldRef) logical.Schema {
	newFieldMap := make(map[string]*logical.FieldSpec)
	i := 0
	for _, fr := range fieldRefs {
		if spec, ok := m.fieldMap[fr.Field.Name]; ok {
			spec.FieldIdx = i
			newFieldMap[fr.Field.Name] = spec
		}
		i++
	}
	return &measureSchema{
		measure:  m.measure,
		common:   m.common,
		fieldMap: newFieldMap,
	}
}

func (m *measureSchema) Equal(s2 logical.Schema) bool {
	if other, ok := s2.(*measureSchema); ok {
		// TODO: add more equality checks
		return cmp.Equal(other.common.TagMap, m.common.TagMap)
	}
	return false
}

// registerTag registers the tag spec with given tagFamilyIdx and tagIdx.
func (m *measureSchema) registerTag(tagFamilyIdx, tagIdx int, spec *databasev1.TagSpec) {
	m.common.RegisterTag(tagFamilyIdx, tagIdx, spec)
}

// registerField registers the field spec with given index.
func (m *measureSchema) registerField(fieldIdx int, spec *databasev1.FieldSpec) {
	m.fieldMap[spec.GetName()] = &logical.FieldSpec{
		FieldIdx: fieldIdx,
		Spec:     spec,
	}
}
