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

	apiv1 "github.com/apache/skywalking-banyandb/api/fbs/v1"
)

var _ Plan = (*orderBy)(nil)

type orderBy struct {
	input  Plan
	sort   apiv1.Sort
	target Expr
}

func (o *orderBy) String() string {
	return fmt.Sprintf("OrderBy: %s, sort=%s", o.target.String(), apiv1.EnumNamesSort[o.sort])
}

func (o *orderBy) Children() []Plan {
	return []Plan{o.input}
}

func (o *orderBy) Type() PlanType {
	return PlanOrderBy
}

func NewOrderBy(input Plan, target Expr, sort apiv1.Sort) Plan {
	return &orderBy{
		input:  input,
		sort:   sort,
		target: target,
	}
}
