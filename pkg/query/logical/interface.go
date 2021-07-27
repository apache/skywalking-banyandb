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

	apiv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
)

type PlanType uint8

const (
	PlanLimit PlanType = iota
	PlanOffset
	PlanTableScan
	PlanOrderBy
	PlanIndexScan
	PlanTraceIDFetch
)

//go:generate mockgen -destination=./plan_unresolved_mock.go -package=logical . UnresolvedPlan
type UnresolvedPlan interface {
	Type() PlanType
	Analyze(Schema) (Plan, error)
}

//go:generate mockgen -destination=./plan_mock.go -package=logical . Plan
type Plan interface {
	fmt.Stringer
	executor.Executable
	Type() PlanType
	Children() []Plan
	Schema() Schema
	Equal(Plan) bool
}

type Expr interface {
	fmt.Stringer
	FieldType() apiv1.FieldSpec_FieldType
	Equal(Expr) bool
}

type LiteralExpr interface {
	Expr
	Bytes() [][]byte
}

type ResolvableExpr interface {
	Expr
	Resolve(Schema) error
}
