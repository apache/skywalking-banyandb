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
	"fmt"

	database_v1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
)

var _ ResolvableExpr = (*TagRef)(nil)

// TagRef is the reference to the field
// also it holds the definition (derived from the streamSchema, measureSchema) of the tag
type TagRef struct {
	// Tag defines the family name and name of the Tag
	Tag *Tag
	// spec contains the index of the key in the streamSchema/measureSchema, as well as the underlying tagSpec
	Spec *TagSpec
}

func (f *TagRef) Equal(expr Expr) bool {
	if other, ok := expr.(*TagRef); ok {
		return other.Tag.GetTagName() == f.Tag.GetTagName() && other.Spec.Spec.GetType() == f.Spec.Spec.GetType()
	}
	return false
}

func (f *TagRef) DataType() int32 {
	if f.Spec == nil {
		panic("should be resolved first")
	}
	return int32(f.Spec.Spec.GetType())
}

func (f *TagRef) Resolve(s Schema) error {
	specs, err := s.CreateTagRef([]*Tag{f.Tag})
	if err != nil {
		return err
	}
	f.Spec = specs[0][0].Spec
	return nil
}

func (f *TagRef) String() string {
	return fmt.Sprintf("#%s<%s>", f.Tag.GetCompoundName(), f.Spec.Spec.GetType().String())
}

func NewTagRef(familyName, tagName string) *TagRef {
	return &TagRef{
		Tag: NewTag(familyName, tagName),
	}
}

// NewSearchableTagRef is a short-handed method for creating a TagRef to the tag in the searchable family
func NewSearchableTagRef(tagName string) *TagRef {
	return &TagRef{
		Tag: NewTag("searchable", tagName),
	}
}

var _ ResolvableExpr = (*FieldRef)(nil)

// FieldRef is the reference to the field
// also it holds the definition (derived from measureSchema) of the field
type FieldRef struct {
	// Field defines the name of the Field
	Field *Field
	// spec contains the index of the key in the measureSchema, as well as the underlying FieldSpec
	Spec *FieldSpec
}

func (f *FieldRef) String() string {
	return fmt.Sprintf("#%s<%s>", f.Spec.Spec.GetName(), f.Spec.Spec.GetFieldType().String())
}

func (f *FieldRef) DataType() int32 {
	if f.Spec == nil {
		panic("should be resolved first")
	}
	return int32(f.Spec.Spec.GetFieldType())
}

func (f *FieldRef) Equal(expr Expr) bool {
	if other, ok := expr.(*FieldRef); ok {
		return other.Field.Name == f.Field.Name && other.Spec.Spec.GetFieldType() == f.Spec.Spec.GetFieldType()
	}
	return false
}

func (f *FieldRef) Resolve(s Schema) error {
	specs, err := s.CreateFieldRef(f.Field)
	if err != nil {
		return err
	}
	f.Spec = specs[0].Spec
	return nil
}

type FieldSpec struct {
	FieldIdx int
	Spec     *database_v1.FieldSpec
}
