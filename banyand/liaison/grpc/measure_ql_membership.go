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

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/bydbql"
	"github.com/apache/skywalking-banyandb/pkg/query/ql"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

func (b *bydbQLService) queryQLMeasure(ctx context.Context, request *measurev1.QueryRequest, plan *ql.RawPlan) (*measurev1.QueryResponse, error) {
	if err := b.validateArrayMembership(ctx, request, plan); err != nil {
		return nil, planQueryStatus(err)
	}
	return b.measureSvc.queryWithRawPlan(ctx, request, true, plan, true)
}

// validateArrayMembership checks current bindings on every execution, including
// cache hits: index bindings are not dependencies of the compiled measure plan.
// Indexed array projection can lose elements before the liaison receives a batch.
func (b *bydbQLService) validateArrayMembership(ctx context.Context, request *measurev1.QueryRequest, plan *ql.RawPlan) error {
	if plan == nil {
		return nil
	}
	tags := make(map[string]bool)
	collectMembershipArrays(plan.Filter, tags)
	if plan.Having != nil {
		collectMembershipArrays(plan.Having.Predicate, tags)
	}
	if len(tags) == 0 {
		return nil
	}
	if b.repo == nil {
		return fmt.Errorf("cannot verify indexed array membership without a metadata repository")
	}
	for _, group := range request.Groups {
		bindings, err := b.repo.IndexRuleBindingRegistry().ListIndexRuleBinding(ctx, schema.ListOpt{Group: group})
		if err != nil {
			return fmt.Errorf("load array membership index bindings for %s/%s: %w", group, request.Name, err)
		}
		seen := make(map[string]bool)
		for _, binding := range bindings {
			if binding.GetSubject().GetCatalog() != commonv1.Catalog_CATALOG_MEASURE || binding.GetSubject().GetName() != request.Name {
				continue
			}
			// Native schema caches apply bindings without filtering their validity
			// windows; an expired binding does not prove full array storage exists.
			for _, name := range binding.Rules {
				if seen[name] {
					continue
				}
				seen[name] = true
				rule, ruleErr := b.repo.IndexRuleRegistry().GetIndexRule(ctx, &commonv1.Metadata{Group: group, Name: name})
				if ruleErr != nil {
					return fmt.Errorf("load array membership index rule %s/%s: %w", group, name, ruleErr)
				}
				if rule == nil {
					return fmt.Errorf("array membership index rule %s/%s is unavailable", group, name)
				}
				for tag := range tags {
					if slices.Contains(rule.GetTags(), tag) {
						return fmt.Errorf("%w: QL membership on indexed array %q is unsupported until native projection preserves all elements",
							bydbql.ErrInvalidMeasureQuery, tag)
					}
				}
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
