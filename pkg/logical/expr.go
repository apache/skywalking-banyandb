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
	"errors"
	"fmt"
	"strconv"
	"strings"

	apiv1 "github.com/apache/skywalking-banyandb/api/fbs/v1"
	"github.com/apache/skywalking-banyandb/pkg/types"
)

var (
	NoSuchField          = errors.New("no such field")
	NotSupporterBinaryOp = errors.New("not supported binary operator")
)

type BinaryOpFactory func(left, right Expr) *BinaryExpr

var (
	operatorFactory map[apiv1.BinaryOp]BinaryOpFactory
)

func init() {
	operatorFactory = map[apiv1.BinaryOp]BinaryOpFactory{
		apiv1.BinaryOpEQ:         Eq,
		apiv1.BinaryOpNE:         Ne,
		apiv1.BinaryOpLT:         Lt,
		apiv1.BinaryOpGT:         Gt,
		apiv1.BinaryOpLE:         Le,
		apiv1.BinaryOpGE:         Ge,
		apiv1.BinaryOpHAVING:     Having,
		apiv1.BinaryOpNOT_HAVING: NotHaving,
	}
}

var _ Expr = (*FieldRef)(nil)

type FieldRef struct {
	fieldName string
}

func NewFieldRef(keyName string) Expr {
	return &FieldRef{fieldName: keyName}
}

func (ref FieldRef) String() string {
	return "#" + ref.fieldName
}

func (ref FieldRef) ToField(plan Plan) (types.Field, error) {
	schema, err := plan.Schema()
	if err != nil {
		return nil, err
	}
	for _, f := range schema.GetFields() {
		if f.Name() == ref.fieldName {
			return f, nil
		}
	}
	return nil, NoSuchField
}

var _ Expr = (*StringLit)(nil)

type StringLit struct {
	literal string
}

func Str(lit string) Expr {
	return &StringLit{lit}
}

func (s *StringLit) String() string {
	return fmt.Sprintf("'%s'", s.literal)
}

func (s *StringLit) ToField(Plan) (types.Field, error) {
	return types.NewField(s.literal, types.STRING), nil
}

var _ Expr = (*StringsLit)(nil)

type StringsLit struct {
	literal []string
}

func Strs(lit ...string) Expr {
	return &StringsLit{lit}
}

func (s *StringsLit) String() string {
	return fmt.Sprintf("['%s']", strings.Join(s.literal, "', '"))
}

func (s *StringsLit) ToField(Plan) (types.Field, error) {
	return types.NewField(strings.Join(s.literal, ","), types.STRING), nil
}

var _ Expr = (*Int64Lit)(nil)

type Int64Lit struct {
	literal int64
}

func Long(lit int64) Expr {
	return &Int64Lit{lit}
}

func (i *Int64Lit) String() string {
	return strconv.FormatInt(i.literal, 10)
}

func (i *Int64Lit) ToField(Plan) (types.Field, error) {
	return types.NewField(strconv.FormatInt(i.literal, 10), types.INT64), nil
}

var _ Expr = (*BinaryExpr)(nil)

type BinaryExpr struct {
	name  string
	op    apiv1.BinaryOp
	left  Expr
	right Expr
}

func (b BinaryExpr) String() string {
	return fmt.Sprintf("%s %s %s", b.left, b.op.String(), b.right)
}

func (b BinaryExpr) ToField(Plan) (types.Field, error) {
	switch b.op {
	case apiv1.BinaryOpEQ, apiv1.BinaryOpNE, apiv1.BinaryOpLT, apiv1.BinaryOpGT, apiv1.BinaryOpGE, apiv1.BinaryOpHAVING, apiv1.BinaryOpNOT_HAVING:
		return types.NewField(b.name, types.BOOLEAN), nil
	default:
		return nil, NotSupporterBinaryOp
	}
}

func Eq(l, r Expr) *BinaryExpr {
	return &BinaryExpr{
		name:  "eq",
		op:    apiv1.BinaryOpEQ,
		left:  l,
		right: r,
	}
}

func Ne(l, r Expr) *BinaryExpr {
	return &BinaryExpr{
		name:  "ne",
		op:    apiv1.BinaryOpNE,
		left:  l,
		right: r,
	}
}

func Gt(l, r Expr) *BinaryExpr {
	return &BinaryExpr{
		name:  "gt",
		op:    apiv1.BinaryOpGT,
		left:  l,
		right: r,
	}
}

func Lt(l, r Expr) *BinaryExpr {
	return &BinaryExpr{
		name:  "lt",
		op:    apiv1.BinaryOpLT,
		left:  l,
		right: r,
	}
}

func Ge(l, r Expr) *BinaryExpr {
	return &BinaryExpr{
		name:  "ge",
		op:    apiv1.BinaryOpGE,
		left:  l,
		right: r,
	}
}

func Le(l, r Expr) *BinaryExpr {
	return &BinaryExpr{
		name:  "le",
		op:    apiv1.BinaryOpLE,
		left:  l,
		right: r,
	}
}

func Having(l, r Expr) *BinaryExpr {
	return &BinaryExpr{
		name:  "having",
		op:    apiv1.BinaryOpHAVING,
		left:  l,
		right: r,
	}
}

func NotHaving(l, r Expr) *BinaryExpr {
	return &BinaryExpr{
		name:  "not having",
		op:    apiv1.BinaryOpHAVING,
		left:  l,
		right: r,
	}
}
