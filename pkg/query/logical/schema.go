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

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	apischema "github.com/apache/skywalking-banyandb/api/schema"
)

type Schema interface {
	IndexDefined(string) (bool, *databasev1.IndexObject)
	FieldSubscript(string) (bool, int)
	FieldDefined(string) bool
	CreateRef(names ...string) ([]*FieldRef, error)
	Proj(refs ...*FieldRef) Schema
	Equal(Schema) bool
	ShardNumber() uint32
	TraceIDFieldName() string
	TraceStateFieldName() string
}

type fieldSpec struct {
	// Idx is defined as
	// 1) the field index based on the (trace) schema for the underlying plans which
	//    directly interact with the database and index modules,
	// 2) the projection index given by the users for those plans which can only access the data from parent plans,
	//    e.g. orderBy plan uses this projection index to access the data entities (normally a projection view)
	//    from the parent plan.
	Idx  int
	spec *databasev1.FieldSpec
}

func (fs *fieldSpec) Equal(other *fieldSpec) bool {
	return fs.Idx == other.Idx && fs.spec.GetType() == other.spec.GetType() && fs.spec.GetName() == other.spec.GetName()
}

var _ Schema = (*schema)(nil)

type schema struct {
	traceSeries *databasev1.TraceSeries
	indexRule   apischema.IndexRule
	fieldMap    map[string]*fieldSpec
}

func (s *schema) TraceIDFieldName() string {
	return s.traceSeries.GetReservedFieldsMap().GetTraceId()
}

func (s *schema) TraceStateFieldName() string {
	return s.traceSeries.GetReservedFieldsMap().GetState().GetField()
}

// IndexDefined checks whether the field given is indexed
func (s *schema) IndexDefined(field string) (bool, *databasev1.IndexObject) {
	idxRule := s.indexRule.Spec
	for _, indexObj := range idxRule.GetObjects() {
		for _, fieldName := range indexObj.GetFields() {
			if field == fieldName {
				return true, indexObj
			}
		}
	}
	return false, nil
}

func (s *schema) FieldSubscript(field string) (bool, int) {
	idxRule := s.indexRule.Spec
	for i, indexObj := range idxRule.GetObjects() {
		for _, fieldName := range indexObj.GetFields() {
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

func (s *schema) registerField(name string, i int, spec *databasev1.FieldSpec) {
	s.fieldMap[name] = &fieldSpec{
		Idx:  i,
		spec: spec,
	}
}

func (s *schema) FieldDefined(name string) bool {
	if _, ok := s.fieldMap[name]; ok {
		return true
	}
	return false
}

func (s *schema) CreateRef(names ...string) ([]*FieldRef, error) {
	var fieldRefs []*FieldRef
	for _, name := range names {
		if fs, ok := s.fieldMap[name]; ok {
			fieldRefs = append(fieldRefs, &FieldRef{name, fs})
		} else {
			return nil, errors.Wrap(ErrFieldNotDefined, name)
		}
	}
	return fieldRefs, nil
}

// Proj creates a projection view from the present schema
// with a given list of projections
func (s *schema) Proj(refs ...*FieldRef) Schema {
	if len(refs) == 0 {
		return nil
	}
	newSchema := &schema{
		traceSeries: s.traceSeries,
		indexRule:   s.indexRule,
		fieldMap:    make(map[string]*fieldSpec),
	}
	for projIdx, ref := range refs {
		newSchema.fieldMap[ref.name] = &fieldSpec{
			Idx:  projIdx,
			spec: ref.Spec.spec,
		}
	}
	return newSchema
}

func (s *schema) ShardNumber() uint32 {
	return s.traceSeries.Shard.Number
}
