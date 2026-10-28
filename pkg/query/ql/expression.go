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
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package ql

import "github.com/apache/skywalking-banyandb/pkg/query/vectorized"

// ExpressionType distinguishes SQL values without assigning new wire types.
type ExpressionType uint8

// Expression types include local booleans and nanosecond UTC timestamps.
const (
	ExpressionNull ExpressionType = iota
	ExpressionInt
	ExpressionFloat
	ExpressionBool
	ExpressionString
	ExpressionBytes
	ExpressionTimestamp
	ExpressionIntArray
	ExpressionStringArray
)

// ExpressionScalar holds one typed literal; Expression.Type selects its payload.
type ExpressionScalar struct {
	String  string
	Bytes   []byte
	Ints    []int64
	Strings []string
	Int64   int64
	Float64 float64
	Bool    bool
	Null    bool
}

// Expression is an immutable, schema-resolved expression recipe.
// Op is literal, column, cast, case, simple_case, coalesce, +, -, *, /, %,
// negate, positive, =, !=, <, <=, >, >=, and, or, not, is_null, is_not_null,
// in, not_in, contains or not_contains. Cast Type names the target. CASE arguments are condition/value
// pairs followed by ELSE; simple_case prepends the comparison value.
type Expression struct {
	Op      string
	Args    []*Expression
	Source  vectorized.ColumnDef
	Literal ExpressionScalar
	Type    ExpressionType
}

// ExpressionOutput pairs an expression with its existing response column identity.
type ExpressionOutput struct {
	Expr   *Expression
	Column vectorized.ColumnDef
}

// ExpressionPlan projects typed expressions while retaining native row metadata.
type ExpressionPlan struct {
	Outputs []ExpressionOutput
}
