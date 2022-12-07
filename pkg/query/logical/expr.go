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

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
)

var _ Expr = (*TagRef)(nil)

// TagRef is the reference to the tag
// also it holds the definition (derived from the streamSchema, measureSchema) of the tag.
type TagRef struct {
	// Tag defines the family name and name of the Tag
	Tag *Tag
	// spec contains the index of the key in the streamSchema/measureSchema, as well as the underlying tagSpec
	Spec *TagSpec
}

// Equal reports whether f and expr have the same name and data type.
func (f *TagRef) Equal(expr Expr) bool {
	if other, ok := expr.(*TagRef); ok {
		return other.Tag.getTagName() == f.Tag.getTagName() && other.Spec.Spec.GetType() == f.Spec.Spec.GetType()
	}
	return false
}

// DataType shows the type of the tag's value.
func (f *TagRef) DataType() int32 {
	if f.Spec == nil {
		panic("should be resolved first")
	}
	return int32(f.Spec.Spec.GetType())
}

// String shows the string representation.
func (f *TagRef) String() string {
	return fmt.Sprintf("#%s<%s>", f.Tag.GetCompoundName(), f.Spec.Spec.GetType().String())
}

// NewTagRef returns a new TagRef.
func NewTagRef(familyName, tagName string) *TagRef {
	return &TagRef{
		Tag: NewTag(familyName, tagName),
	}
}

// NewSearchableTagRef is a short-handed method for creating a TagRef to the tag in the searchable family.
func NewSearchableTagRef(tagName string) *TagRef {
	return &TagRef{
		Tag: NewTag("searchable", tagName),
	}
}

var _ Expr = (*FieldRef)(nil)

// FieldRef is the reference to the field
// also it holds the definition (derived from measureSchema) of the field.
type FieldRef struct {
	// Field defines the name of the Field
	Field *Field
	// spec contains the index of the key in the measureSchema, as well as the underlying FieldSpec
	Spec *FieldSpec
}

// String shows the string representation.
func (f *FieldRef) String() string {
	return fmt.Sprintf("#%s<%s>", f.Spec.Spec.GetName(), f.Spec.Spec.GetFieldType().String())
}

// DataType shows the type of the filed's value.
func (f *FieldRef) DataType() int32 {
	if f.Spec == nil {
		panic("should be resolved first")
	}
	return int32(f.Spec.Spec.GetFieldType())
}

// Equal reports whether f and expr have the same name and data type.
func (f *FieldRef) Equal(expr Expr) bool {
	if other, ok := expr.(*FieldRef); ok {
		return other.Field.Name == f.Field.Name && other.Spec.Spec.GetFieldType() == f.Spec.Spec.GetFieldType()
	}
	return false
}

// FieldSpec is the reference to the field.
// It also holds the definition of the field.
type FieldSpec struct {
	Spec     *databasev1.FieldSpec
	FieldIdx int
}
