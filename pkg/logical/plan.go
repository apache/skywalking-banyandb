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
	"strings"

	apiv1 "github.com/apache/skywalking-banyandb/api/fbs/v1"
	"github.com/apache/skywalking-banyandb/pkg/types"
)

var _ Plan = (*Selection)(nil)

type Selection struct {
	input Plan
	exprs []Expr
}

func NewSelection(input Plan, exprs ...Expr) Plan {
	return &Selection{
		input: input,
		exprs: exprs,
	}
}

func (s *Selection) String() string {
	var exprStrs []string
	for _, expr := range s.exprs {
		exprStrs = append(exprStrs, expr.String())
	}
	return fmt.Sprintf("Selection: %s", strings.Join(exprStrs, " And "))
}

func (s *Selection) Schema() (types.Schema, error) {
	return s.input.Schema()
}

func (s *Selection) Children() []Plan {
	return []Plan{s.input}
}

var _ Plan = (*Projection)(nil)

type Projection struct {
	input Plan
	exprs []Expr
}

func NewProjection(input Plan, exprs ...Expr) Plan {
	return &Projection{
		input: input,
		exprs: exprs,
	}
}

func (p *Projection) String() string {
	var expressStr []string
	for _, e := range p.exprs {
		expressStr = append(expressStr, e.String())
	}
	return "Projection: " + strings.Join(expressStr, ", ")
}

func (p *Projection) Schema() (types.Schema, error) {
	var fields []types.Field
	for _, e := range p.exprs {
		f, err := e.ToField(p.input)
		if err != nil {
			return nil, err
		}
		fields = append(fields, f)
	}
	return types.NewSchema(fields...), nil
}

func (p *Projection) Children() []Plan {
	return []Plan{p.input}
}

var _ Plan = (*Scan)(nil)

type Scan struct {
	metadata   *apiv1.Metadata
	startTime  uint64
	endTime    uint64
	projection []string
}

func NewScan(metadata *apiv1.Metadata, startTime, endTime uint64) Plan {
	return &Scan{
		metadata:   metadata,
		startTime:  startTime,
		endTime:    endTime,
		projection: make([]string, 0),
	}
}

func (s *Scan) String() string {
	if len(s.projection) == 0 {
		return fmt.Sprintf("Scan: Metadata{group=%s, name=%s}; startTime=%d ,endTime=%d; projection=None",
			s.metadata.Group(), s.metadata.Name(), s.startTime, s.endTime)
	} else {
		return fmt.Sprintf("Scan: Metadata{group=%s, name=%s}; startTime=%d ,endTime=%d; projection=%v",
			s.metadata.Group(), s.metadata.Name(), s.startTime, s.endTime, s.projection)
	}
}

func (s *Scan) Schema() (types.Schema, error) {
	panic("implement me")
}

func (s *Scan) Children() []Plan {
	return []Plan{}
}

func FormatPlan(plan Plan) string {
	return formatPlan(plan, 0)
}

func formatPlan(plan Plan, indent int) string {
	result := ""
	for i := 0; i < indent; i++ {
		result += "\t"
	}
	result += plan.String() + "\n"
	children := plan.Children()
	for _, child := range children {
		result += formatPlan(child, indent+1)
	}
	return result
}
