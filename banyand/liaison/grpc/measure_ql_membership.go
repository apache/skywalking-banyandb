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

package grpc

import (
	"context"
	"fmt"
	"slices"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	"github.com/apache/skywalking-banyandb/pkg/bydbql"
	"github.com/apache/skywalking-banyandb/pkg/query/ql"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

func (b *bydbQLService) queryQLMeasure(ctx context.Context, request *measurev1.QueryRequest, plan *ql.RawPlan) (*measurev1.QueryResponse, error) {
	if err := b.validateQLIndexes(ctx, request, plan); err != nil {
		return nil, planQueryStatus(err)
	}
	return b.measureSvc.queryWithRawPlan(ctx, request, true, plan, true)
}

// Typed QL membership retains its indexed-array restriction across binding changes.
func validateArrayMembership(rules []*databasev1.IndexRule, tags map[string]bool) error {
	for _, rule := range rules {
		for tag := range tags {
			if slices.Contains(rule.GetTags(), tag) {
				return fmt.Errorf("%w: QL membership on indexed array %q is unsupported", bydbql.ErrInvalidMeasureQuery, tag)
			}
		}
	}
	return nil
}

func collectMembershipArrays(expression *ql.Expression, tags map[string]bool) {
	if expression == nil {
		return
	}
	if (expression.Op == "contains" || expression.Op == "not_contains") && len(expression.Args) > 0 {
		source := expression.Args[0].Source
		if source.Role == vectorized.RoleTag && (source.Type == vectorized.ColumnTypeInt64Array || source.Type == vectorized.ColumnTypeStrArray) {
			tags[source.Name], tags[source.TagFamily+"."+source.Name] = true, true
		}
	}
	for _, argument := range expression.Args {
		collectMembershipArrays(argument, tags)
	}
}
