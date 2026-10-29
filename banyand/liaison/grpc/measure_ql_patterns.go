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

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/bydbql"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/ql"
	vmeasure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"
)

// Index rules are not compiled-plan dependencies. Resolve current bindings on
// every call, including cache hits and queries whose bound predicate is empty.
func (b *bydbQLService) validateQLIndexes(ctx context.Context, request *measurev1.QueryRequest, plan *ql.RawPlan) error {
	if plan == nil {
		return nil
	}
	tags := make(map[string]bool)
	collectMembershipArrays(plan.Filter, tags)
	if plan.Having != nil {
		collectMembershipArrays(plan.Having.Predicate, tags)
	}
	hasPatterns := len(plan.Patterns) > 0
	if !hasPatterns && len(tags) == 0 {
		return nil
	}
	purpose := "array membership"
	if hasPatterns {
		purpose = "LIKE"
	}
	if b.repo == nil {
		if hasPatterns {
			return fmt.Errorf("cannot verify LIKE indexes without a metadata repository")
		}
		return fmt.Errorf("cannot verify indexed array membership without a metadata repository")
	}
	for _, group := range request.Groups {
		var measure *databasev1.Measure
		var err error
		if hasPatterns {
			measure, err = b.repo.MeasureRegistry().GetMeasure(ctx, &commonv1.Metadata{Group: group, Name: request.Name})
			if err != nil {
				return fmt.Errorf("load LIKE source %s/%s: %w", group, request.Name, err)
			}
		}
		rules, err := b.loadQLIndexRules(ctx, group, request.Name, purpose)
		if err != nil {
			return err
		}
		if hasPatterns {
			if err = model.ValidateIndexedPatterns(measure, rules, plan.Patterns); err != nil {
				return fmt.Errorf("%w: %w", bydbql.ErrInvalidMeasureQuery, err)
			}
		}
		if err = validateArrayMembership(rules, tags); err != nil {
			return err
		}
	}
	return nil
}

func (b *bydbQLService) loadQLIndexRules(ctx context.Context, group, measure, purpose string) ([]*databasev1.IndexRule, error) {
	bindings, err := b.repo.IndexRuleBindingRegistry().ListIndexRuleBinding(ctx, schema.ListOpt{Group: group})
	if err != nil {
		return nil, fmt.Errorf("load %s index bindings for %s/%s: %w", purpose, group, measure, err)
	}
	var rules []*databasev1.IndexRule
	seen := make(map[string]bool)
	for _, binding := range bindings {
		if binding.GetSubject().GetCatalog() != commonv1.Catalog_CATALOG_MEASURE || binding.GetSubject().GetName() != measure {
			continue
		}
		// Native schema caches apply bindings without filtering validity
		// windows. Both validators use that same binding visibility.
		for _, name := range binding.GetRules() {
			if seen[name] {
				continue
			}
			seen[name] = true
			rule, ruleErr := b.repo.IndexRuleRegistry().GetIndexRule(ctx, &commonv1.Metadata{Group: group, Name: name})
			if ruleErr != nil {
				return nil, fmt.Errorf("load %s index rule %s/%s: %w", purpose, group, name, ruleErr)
			}
			if rule == nil {
				return nil, fmt.Errorf("%s index rule %s/%s is unavailable", purpose, group, name)
			}
			rules = append(rules, rule)
		}
	}
	return rules, nil
}

func measureBatchRequest(request *measurev1.QueryRequest, plan *ql.RawPlan) *vmeasure.BatchQueryRequest {
	result := &vmeasure.BatchQueryRequest{Request: request}
	if plan != nil {
		result.Patterns = plan.Patterns
	}
	return result
}
