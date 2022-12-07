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
)

// UnresolvedPlan denotes an logical expression.
// It could be analyzed to a Plan(executable operation) with the Schema.
type UnresolvedPlan interface {
	Analyze(Schema) (Plan, error)
}

// Plan is the executable operation. It belongs to a execution tree.
type Plan interface {
	fmt.Stringer
	Children() []Plan
	Schema() Schema
}

// Expr represents a predicate in criteria.
type Expr interface {
	fmt.Stringer
	DataType() int32
	Equal(Expr) bool
}

// LiteralExpr allows getting raw data represented as bytes.
type LiteralExpr interface {
	Expr
	Bytes() [][]byte
}

// ComparableExpr allows comparing Expr and Expr arrays.
type ComparableExpr interface {
	LiteralExpr
	Compare(LiteralExpr) (int, bool)
	BelongTo(LiteralExpr) bool
	Contains(LiteralExpr) bool
}
