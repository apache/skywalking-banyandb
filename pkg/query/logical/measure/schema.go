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

// Package measure implements execution operations for querying measure data.
package measure

import (
	"github.com/google/go-cmp/cmp"
	"github.com/pkg/errors"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
)

// errFieldNotDefined indicated the field is not defined in the measure schema.
var errFieldNotDefined = errors.New("field is not defined")

type schema struct {
	measure  *databasev1.Measure
	fieldMap map[string]*logical.FieldSpec
	common   *logical.CommonSchema
	children []logical.Schema
}

func (m *schema) EntityList() []string {
	return m.common.EntityList
}

func (m *schema) FindTagSpecByName(name string) *logical.TagSpec {
	return m.common.FindTagSpecByName(name)
}

func (m *schema) IndexDefined(tagName string) (bool, *databasev1.IndexRule) {
	return m.common.IndexDefined(tagName)
}

func (m *schema) IndexRuleDefined(indexRuleName string) (bool, *databasev1.IndexRule) {
	return m.common.IndexRuleDefined(indexRuleName)
}

func (m *schema) CreateTagRef(tags ...[]*logical.Tag) ([][]*logical.TagRef, error) {
	return m.common.CreateRef(tags...)
}

func (m *schema) CreateFieldRef(fields ...*logical.Field) ([]*logical.FieldRef, error) {
	fieldRefs := make([]*logical.FieldRef, 0, len(fields))
	for _, field := range fields {
		if fs, ok := m.fieldMap[field.Name]; ok {
			fieldRefs = append(fieldRefs, &logical.FieldRef{Field: field, Spec: fs})
		}
	}
	return fieldRefs, nil
}

func (m *schema) ProjTags(refs ...[]*logical.TagRef) logical.Schema {
	if len(refs) == 0 {
		return nil
	}
	newSchema := &schema{
		measure:  m.measure,
		common:   m.common.ProjTags(refs...),
		fieldMap: m.fieldMap,
	}
	return newSchema
}

func (m *schema) ProjFields(fieldRefs ...*logical.FieldRef) logical.Schema {
	newFieldMap := make(map[string]*logical.FieldSpec)
	i := 0
	for _, fr := range fieldRefs {
		if spec, ok := m.fieldMap[fr.Field.Name]; ok {
			newFieldMap[fr.Field.Name] = &logical.FieldSpec{
				FieldIdx: i,
				Spec:     spec.Spec,
			}
		}
		i++
	}
	return &schema{
		measure:  m.measure,
		common:   m.common,
		fieldMap: newFieldMap,
	}
}

func (m *schema) Equal(s2 logical.Schema) bool {
	if other, ok := s2.(*schema); ok {
		return cmp.Equal(other.common.TagSpecMap, m.common.TagSpecMap)
	}
	return false
}

// registerField registers the field spec with given index.
func (m *schema) registerField(fieldIdx int, spec *databasev1.FieldSpec) {
	m.fieldMap[spec.GetName()] = &logical.FieldSpec{
		FieldIdx: fieldIdx,
		Spec:     spec,
	}
}

func (m *schema) Children() []logical.Schema {
	return m.children
}

func mergeSchema(schemas []logical.Schema) (logical.Schema, error) {
	if len(schemas) == 0 {
		return nil, nil
	}
	if len(schemas) == 1 {
		return schemas[0], nil
	}

	var commonSchemas []*logical.CommonSchema
	var tagFamilies []*databasev1.TagFamilySpec
	fieldMap := make(map[string]*logical.FieldSpec)

	for _, sm := range schemas {
		if sm == nil {
			continue
		}
		mSchema, ok := sm.(*schema)
		if !ok {
			continue
		}

		tagFamilies = logical.MergeTagFamilySpecs(tagFamilies, mSchema.measure.GetTagFamilies())

		commonSchemas = append(commonSchemas, mSchema.common)

		for name, spec := range mSchema.fieldMap {
			if existing, exists := fieldMap[name]; exists {
				if existing.Spec.FieldType != spec.Spec.FieldType {
					existing.Spec.FieldType = databasev1.FieldType_FIELD_TYPE_UNSPECIFIED
				}
			} else {
				fieldMap[name] = &logical.FieldSpec{
					FieldIdx: spec.FieldIdx,
					Spec:     spec.Spec,
				}
			}
		}
	}

	mergedCommon, err := logical.MergeSchemas(commonSchemas)
	if err != nil {
		return nil, err
	}

	ret := &schema{
		common:   mergedCommon,
		children: schemas,
		fieldMap: fieldMap,
	}
	ret.common.RegisterTagFamilies(tagFamilies)
	return ret, nil
}
