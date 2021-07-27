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

package trace

import (
	"context"
	"time"

	"go.uber.org/multierr"

	"github.com/apache/skywalking-banyandb/api/common"
	v1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/v1"
	"github.com/apache/skywalking-banyandb/banyand/series"
	"github.com/apache/skywalking-banyandb/banyand/series/schema"
	"github.com/apache/skywalking-banyandb/banyand/series/schema/sw"
)

//Methods for query objects in the schema

func (s *service) TraceSeries() schema.TraceSeries {
	return sw.NewTraceSeries()
}

func (s *service) IndexRule() schema.IndexRule {
	return sw.NewIndexRule()
}

func (s *service) IndexRuleBinding() schema.IndexRuleBinding {
	return sw.NewIndexRuleBinding()
}

func (s *service) IndexRules(ctx context.Context, subject *v1.Series, filter series.IndexObjectFilter) ([]*v1.IndexRule, error) {
	group := subject.Series.GetGroup()
	bindings, err := s.IndexRuleBinding().List(ctx, schema.ListOpt{Group: group})
	if err != nil {
		return nil, err
	}
	subjectSeries := subject.GetSeries()
	if subjectSeries == nil {
		return nil, nil
	}
	now := time.Now()
	foundRules := make([]*v1.Metadata, 0)
	for _, binding := range bindings {
		spec := binding.Spec
		if spec.GetBeginAt().AsTime().After(now) ||
			spec.GetExpireAt().AsTime().Before(now) {
			continue
		}
		for _, sub := range spec.GetSubjects() {
			if sub != nil && sub.GetCatalog() == subject.GetCatalog() {
				s1 := sub.GetSeries()
				if s1 != nil &&
					s1.GetName() == subjectSeries.GetName() &&
					s1.GetGroup() == subjectSeries.GetGroup() {
					ruleRef := spec.GetRuleRef()
					if ruleRef != nil {
						foundRules = append(foundRules, ruleRef)
					}
				}
				break
			}
		}
	}
	result := make([]*v1.IndexRule, 0)
	var indexRuleErr error
	for _, rule := range foundRules {
		object, getErr := s.IndexRule().Get(ctx, common.Metadata{KindVersion: common.MetadataKindVersion, Spec: rule})
		if getErr != nil {
			indexRuleErr = multierr.Append(indexRuleErr, err)
			continue
		}
		r := object.Spec
		if filter == nil {
			result = append(result, r)
			continue
		}
		for _, obj := range r.GetObjects() {
			if filter(obj) {
				result = append(result, r)
				continue
			}
		}
	}
	return result, indexRuleErr
}
