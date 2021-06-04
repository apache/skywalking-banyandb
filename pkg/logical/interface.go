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

	"github.com/apache/skywalking-banyandb/pkg/types"
)

type ExprType int8

const (
	Unary ExprType = iota
	Binary
)

type validationOpts struct {
}

//go:generate mockgen -destination=./plan_mock.go -package=logical . Plan
type Plan interface {
	fmt.Stringer
	Schema() (types.Schema, error)
	Children() []Plan
	Validate(*validationOpts) (bool, error)
}

//go:generate mockgen -destination=./expr_mock.go -package=logical . Expr
type Expr interface {
	fmt.Stringer
	ToField(plan Plan) (types.Field, error)
	Validate(types.Schema, *validationOpts) (bool, error)
}
