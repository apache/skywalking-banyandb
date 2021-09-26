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

package metadata

import (
	"context"
	"time"

	"go.uber.org/multierr"

	commonv2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v2"
	databasev2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v2"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

//IndexFilter provides methods to find a specific index related objects
type IndexFilter interface {
	//IndexRules fetches v2.IndexRule by subject defined in IndexRuleBinding
	IndexRules(ctx context.Context, subject *commonv2.Metadata) ([]*databasev2.IndexRule, error)
}

type Repo interface {
	IndexFilter
	Stream() schema.Stream
}

type Service interface {
	Repo
	run.Unit
}

type service struct {
	stream           schema.Stream
	indexRule        schema.IndexRule
	indexRuleBinding schema.IndexRuleBinding
}

func NewService(_ context.Context) (Service, error) {
	stream, err := schema.NewStream()
	if err != nil {
		return nil, err
	}
	indexRule, err := schema.NewIndexRule()
	if err != nil {
		return nil, err
	}
	indexRuleBinding, err := schema.NewIndexRuleBinding()
	if err != nil {
		return nil, err
	}
	return &service{
		stream:           stream,
		indexRule:        indexRule,
		indexRuleBinding: indexRuleBinding,
	}, nil
}

func (s *service) Stream() schema.Stream {
	return s.stream
}

func (s *service) Name() string {
	return "metadata"
}

func (s *service) IndexRules(ctx context.Context, subject *commonv2.Metadata) ([]*databasev2.IndexRule, error) {
	bindings, err := s.indexRuleBinding.List(ctx, schema.ListOpt{Group: subject.Group})
	if err != nil {
		return nil, err
	}
	now := time.Now()
	foundRules := make([]string, 0)
	for _, binding := range bindings {
		if binding.GetBeginAt().AsTime().After(now) ||
			binding.GetExpireAt().AsTime().Before(now) {
			continue
		}
		sub := binding.GetSubject()
		if sub.Name != subject.Name {
			continue
		}
		foundRules = append(foundRules, binding.Rules...)
	}
	result := make([]*databasev2.IndexRule, 0, len(foundRules))
	var indexRuleErr error
	for _, rule := range foundRules {
		r, getErr := s.indexRule.Get(ctx, &commonv2.Metadata{
			Name:  rule,
			Group: subject.Group,
		})
		if getErr != nil {
			indexRuleErr = multierr.Append(indexRuleErr, err)
			continue
		}
		result = append(result, r)

	}
	return result, indexRuleErr
}
