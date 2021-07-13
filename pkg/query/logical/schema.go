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
	"bytes"

	"github.com/google/go-cmp/cmp"
	"github.com/pkg/errors"

	apiv1 "github.com/apache/skywalking-banyandb/api/fbs/v1"
	apischema "github.com/apache/skywalking-banyandb/api/schema"
)

type Schema interface {
	IndexDefined(string) (bool, *apiv1.IndexObject)
	FieldDefined(string) bool
	CreateRef(names ...string) ([]*fieldRef, error)
	Map(refs ...*fieldRef) Schema
	Equal(Schema) bool
}

type fieldSpec struct {
	idx  int
	spec *apiv1.FieldSpec
}

func (fs *fieldSpec) Equal(other *fieldSpec) bool {
	return fs.idx == other.idx && fs.spec.Type() == other.spec.Type() && bytes.Equal(fs.spec.Name(), other.spec.Name())
}

var _ Schema = (*schema)(nil)

type schema struct {
	indexRule apischema.IndexRule
	fieldMap  map[string]*fieldSpec
}

// IndexDefined checks whether the field given is indexed
func (s *schema) IndexDefined(field string) (bool, *apiv1.IndexObject) {
	idxRule := s.indexRule.Spec
	for i := 0; i < idxRule.ObjectsLength(); i++ {
		var idxObj apiv1.IndexObject
		if ok := idxRule.Objects(&idxObj, i); ok {
			for j := 0; j < idxObj.FieldsLength(); j++ {
				if field == string(idxObj.Fields(j)) {
					return true, &idxObj
				}
			}
		} else {
			return false, nil
		}
	}
	return false, nil
}

func (s *schema) Equal(s2 Schema) bool {
	if other, ok := s2.(*schema); ok {
		return cmp.Equal(other.fieldMap, s.fieldMap)
	}
	return false
}

func (s *schema) RegisterField(name string, i int, spec *apiv1.FieldSpec) {
	s.fieldMap[name] = &fieldSpec{
		idx:  i,
		spec: spec,
	}
}

func (s *schema) FieldDefined(name string) bool {
	if _, ok := s.fieldMap[name]; ok {
		return true
	}
	return false
}

func (s *schema) CreateRef(names ...string) ([]*fieldRef, error) {
	var fieldRefs []*fieldRef
	for _, name := range names {
		if fs, ok := s.fieldMap[name]; ok {
			fieldRefs = append(fieldRefs, &fieldRef{name, fs})
		} else {
			return nil, errors.Wrap(FieldNotDefinedErr, name)
		}
	}
	return fieldRefs, nil
}

func (s *schema) Map(refs ...*fieldRef) Schema {
	if refs == nil || len(refs) == 0 {
		return nil
	}
	newS := &schema{
		indexRule: s.indexRule,
		fieldMap:  make(map[string]*fieldSpec),
	}
	for _, ref := range refs {
		newS.fieldMap[ref.name] = ref.spec
	}
	return newS
}
