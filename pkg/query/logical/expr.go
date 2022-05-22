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

	"github.com/pkg/errors"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

var binaryOpFactory = map[modelv1.Condition_BinaryOp]func(l, r Expr) Expr{
	modelv1.Condition_BINARY_OP_EQ:         Eq,
	modelv1.Condition_BINARY_OP_NE:         Ne,
	modelv1.Condition_BINARY_OP_LT:         Lt,
	modelv1.Condition_BINARY_OP_GT:         Gt,
	modelv1.Condition_BINARY_OP_LE:         Le,
	modelv1.Condition_BINARY_OP_GE:         Ge,
	modelv1.Condition_BINARY_OP_HAVING:     Having,
	modelv1.Condition_BINARY_OP_NOT_HAVING: NotHaving,
}

var _ ResolvableExpr = (*TagRef)(nil)

// TagRef is the reference to the field
// also it holds the definition (derived from the streamSchema, measureSchema) of the tag
type TagRef struct {
	// tag defines the family name and name of the Tag
	tag *Tag
	// spec contains the index of the key in the streamSchema/measureSchema, as well as the underlying tagSpec
	Spec *tagSpec
}

func (f *TagRef) Equal(expr Expr) bool {
	if other, ok := expr.(*TagRef); ok {
		return other.tag.GetTagName() == f.tag.GetTagName() && other.Spec.spec.GetType() == f.Spec.spec.GetType()
	}
	return false
}

func (f *TagRef) DataType() int32 {
	if f.Spec == nil {
		panic("should be resolved first")
	}
	return int32(f.Spec.spec.GetType())
}

func (f *TagRef) Resolve(s Schema) error {
	specs, err := s.CreateTagRef([]*Tag{f.tag})
	if err != nil {
		return err
	}
	f.Spec = specs[0][0].Spec
	return nil
}

func (f *TagRef) String() string {
	return fmt.Sprintf("#%s<%s>", f.tag.GetCompoundName(), f.Spec.spec.GetType().String())
}

func NewTagRef(familyName, tagName string) *TagRef {
	return &TagRef{
		tag: NewTag(familyName, tagName),
	}
}

// NewSearchableFieldRef is a short-handed method for creating a TagRef to the tag in the searchable family
func NewSearchableFieldRef(tagName string) *TagRef {
	return &TagRef{
		tag: NewTag("searchable", tagName),
	}
}

var _ ResolvableExpr = (*FieldRef)(nil)

// FieldRef is the reference to the field
// also it holds the definition (derived from measureSchema) of the field
type FieldRef struct {
	// field defines the name of the Field
	field *Field
	// spec contains the index of the key in the measureSchema, as well as the underlying FieldSpec
	Spec *fieldSpec
}

func (f *FieldRef) String() string {
	return fmt.Sprintf("#%s<%s>", f.Spec.spec.GetName(), f.Spec.spec.GetFieldType().String())
}

func (f *FieldRef) DataType() int32 {
	if f.Spec == nil {
		panic("should be resolved first")
	}
	return int32(f.Spec.spec.GetFieldType())
}

func (f *FieldRef) Equal(expr Expr) bool {
	if other, ok := expr.(*FieldRef); ok {
		return other.field.name == f.field.name && other.Spec.spec.GetFieldType() == f.Spec.spec.GetFieldType()
	}
	return false
}

func (f *FieldRef) Resolve(s Schema) error {
	specs, err := s.CreateFieldRef(f.field)
	if err != nil {
		return err
	}
	f.Spec = specs[0].Spec
	return nil
}

var _ ResolvableExpr = (*binaryExpr)(nil)

// binaryExpr is composed of two operands with one op as the operator
// l is normally a reference to a field, while r is usually literals
type binaryExpr struct {
	op modelv1.Condition_BinaryOp
	l  Expr
	r  Expr
}

func (b *binaryExpr) Equal(expr Expr) bool {
	if other, ok := expr.(*binaryExpr); ok {
		return b.op == other.op && b.l.Equal(other.l) && b.r.Equal(other.r)
	}
	return false
}

func (b *binaryExpr) DataType() int32 {
	panic("Boolean should be added")
}

func (b *binaryExpr) Resolve(s Schema) error {
	if lr, ok := b.l.(ResolvableExpr); ok {
		err := lr.Resolve(s)
		if err != nil {
			return err
		}
	}
	if rr, ok := b.l.(ResolvableExpr); ok {
		err := rr.Resolve(s)
		if err != nil {
			return err
		}
	}
	if b.l.DataType() != b.r.DataType() {
		return errors.Wrapf(ErrIncompatibleQueryCondition, "left is %d while right is %d",
			b.l.DataType(),
			b.r.DataType(),
		)
	}
	return nil
}

func (b *binaryExpr) String() string {
	return fmt.Sprintf("%s %s %s", b.l.String(), b.op.String(), b.r.String())
}

func Eq(l, r Expr) Expr {
	return &binaryExpr{
		op: modelv1.Condition_BINARY_OP_EQ,
		l:  l,
		r:  r,
	}
}

func Ne(l, r Expr) Expr {
	return &binaryExpr{
		op: modelv1.Condition_BINARY_OP_NE,
		l:  l,
		r:  r,
	}
}

func Lt(l, r Expr) Expr {
	return &binaryExpr{
		op: modelv1.Condition_BINARY_OP_LT,
		l:  l,
		r:  r,
	}
}

func Le(l, r Expr) Expr {
	return &binaryExpr{
		op: modelv1.Condition_BINARY_OP_LE,
		l:  l,
		r:  r,
	}
}

func Gt(l, r Expr) Expr {
	return &binaryExpr{
		op: modelv1.Condition_BINARY_OP_GT,
		l:  l,
		r:  r,
	}
}

func Ge(l, r Expr) Expr {
	return &binaryExpr{
		op: modelv1.Condition_BINARY_OP_GE,
		l:  l,
		r:  r,
	}
}

func Having(l, r Expr) Expr {
	return &binaryExpr{
		op: modelv1.Condition_BINARY_OP_HAVING,
		l:  l,
		r:  r,
	}
}

func NotHaving(l, r Expr) Expr {
	return &binaryExpr{
		op: modelv1.Condition_BINARY_OP_NOT_HAVING,
		l:  l,
		r:  r,
	}
}
