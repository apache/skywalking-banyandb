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

	apiv1 "github.com/apache/skywalking-banyandb/api/fbs/v1"
)

var binaryOpFactory = map[apiv1.BinaryOp]func(l, r Expr) Expr{
	apiv1.BinaryOpEQ:         Eq,
	apiv1.BinaryOpNE:         Ne,
	apiv1.BinaryOpLT:         Lt,
	apiv1.BinaryOpGT:         Gt,
	apiv1.BinaryOpLE:         Le,
	apiv1.BinaryOpGE:         Ge,
	apiv1.BinaryOpHAVING:     Having,
	apiv1.BinaryOpNOT_HAVING: NotHaving,
}

var _ ResolvableExpr = (*fieldRef)(nil)

// fieldRef is the reference to the field
// also it holds the definition/schema of the field
type fieldRef struct {
	// name defines the key of the field
	name string
	// spec contains the index of the key in the schema, as well as the underlying FieldSpec
	spec *fieldSpec
}

func (f *fieldRef) Equal(expr Expr) bool {
	if other, ok := expr.(*fieldRef); ok {
		return other.name == f.name && other.spec.spec.Type() == f.spec.spec.Type()
	}
	return false
}

func (f *fieldRef) FieldType() apiv1.FieldType {
	if f.spec == nil {
		panic("should be resolved first")
	}
	return f.spec.spec.Type()
}

func (f *fieldRef) Resolve(plan Plan) error {
	s := plan.Schema()
	specs, err := s.CreateRef(f.name)
	if err != nil {
		return err
	}
	f.spec = specs[0].spec
	return nil
}

func (f *fieldRef) String() string {
	return fmt.Sprintf("#%s<%s>", f.name, apiv1.EnumNamesFieldType[f.spec.spec.Type()])
}

func NewFieldRef(fieldName string) *fieldRef {
	return &fieldRef{
		name: fieldName,
	}
}

var _ ResolvableExpr = (*binaryExpr)(nil)

// binaryExpr is composed of two operands with one op as the operator
// l is normally a reference to a field, while r is usually literals
type binaryExpr struct {
	op apiv1.BinaryOp
	l  Expr
	r  Expr
}

func (b *binaryExpr) Equal(expr Expr) bool {
	if other, ok := expr.(*binaryExpr); ok {
		return b.op == other.op && b.l.Equal(other.l) && b.r.Equal(other.r)
	}
	return false
}

func (b *binaryExpr) FieldType() apiv1.FieldType {
	panic("Boolean should be added")
}

func (b *binaryExpr) Resolve(plan Plan) error {
	if lr, ok := b.l.(ResolvableExpr); ok {
		err := lr.Resolve(plan)
		if err != nil {
			return err
		}
	}
	if rr, ok := b.l.(ResolvableExpr); ok {
		err := rr.Resolve(plan)
		if err != nil {
			return err
		}
	}
	if b.l.FieldType() != b.r.FieldType() {
		return errors.Wrapf(IncompatibleQueryConditionErr, "left is %v while right is %v",
			apiv1.EnumNamesFieldType[b.l.FieldType()],
			apiv1.EnumNamesFieldType[b.r.FieldType()],
		)
	}
	return nil
}

func (b *binaryExpr) String() string {
	return fmt.Sprintf("%s %s %s", b.l.String(), apiv1.EnumNamesBinaryOp[b.op], b.r.String())
}

func Eq(l, r Expr) Expr {
	return &binaryExpr{
		op: apiv1.BinaryOpEQ,
		l:  l,
		r:  r,
	}
}

func Ne(l, r Expr) Expr {
	return &binaryExpr{
		op: apiv1.BinaryOpNE,
		l:  l,
		r:  r,
	}
}

func Lt(l, r Expr) Expr {
	return &binaryExpr{
		op: apiv1.BinaryOpLT,
		l:  l,
		r:  r,
	}
}

func Le(l, r Expr) Expr {
	return &binaryExpr{
		op: apiv1.BinaryOpLE,
		l:  l,
		r:  r,
	}
}

func Gt(l, r Expr) Expr {
	return &binaryExpr{
		op: apiv1.BinaryOpGT,
		l:  l,
		r:  r,
	}
}

func Ge(l, r Expr) Expr {
	return &binaryExpr{
		op: apiv1.BinaryOpGE,
		l:  l,
		r:  r,
	}
}

func Having(l, r Expr) Expr {
	return &binaryExpr{
		op: apiv1.BinaryOpHAVING,
		l:  l,
		r:  r,
	}
}

func NotHaving(l, r Expr) Expr {
	return &binaryExpr{
		op: apiv1.BinaryOpNOT_HAVING,
		l:  l,
		r:  r,
	}
}
