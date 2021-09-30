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

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
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

var _ ResolvableExpr = (*FieldRef)(nil)

// FieldRef is the reference to the field
// also it holds the definition/schema of the field
type FieldRef struct {
	// tag defines the family name and name of the Tag
	tag *Tag
	// spec contains the index of the key in the schema, as well as the underlying FieldSpec
	Spec *tagSpec
}

func (f *FieldRef) Equal(expr Expr) bool {
	if other, ok := expr.(*FieldRef); ok {
		return other.tag.GetTagName() == f.tag.GetTagName() && other.Spec.spec.GetType() == f.Spec.spec.GetType()
	}
	return false
}

func (f *FieldRef) FieldType() databasev1.TagType {
	if f.Spec == nil {
		panic("should be resolved first")
	}
	return f.Spec.spec.GetType()
}

func (f *FieldRef) Resolve(s Schema) error {
	specs, err := s.CreateRef([]*Tag{f.tag})
	if err != nil {
		return err
	}
	f.Spec = specs[0][0].Spec
	return nil
}

func (f *FieldRef) String() string {
	return fmt.Sprintf("#%s<%s>", f.tag.GetCompoundName(), f.Spec.spec.GetType().String())
}

func NewFieldRef(familyName, tagName string) *FieldRef {
	return &FieldRef{
		tag: NewTag(familyName, tagName),
	}
}

// NewSearchableFieldRef is a short-hand method for creating a FieldRef to the tag in the searchable family
func NewSearchableFieldRef(tagName string) *FieldRef {
	return &FieldRef{
		tag: NewTag("searchable", tagName),
	}
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

func (b *binaryExpr) FieldType() databasev1.TagType {
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
	if b.l.FieldType() != b.r.FieldType() {
		return errors.Wrapf(ErrIncompatibleQueryCondition, "left is %s while right is %s",
			b.l.FieldType().String(),
			b.r.FieldType().String(),
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
