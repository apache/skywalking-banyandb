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

package series

import (
	"bytes"
	"context"
	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	v1 "github.com/apache/skywalking-banyandb/api/fbs/v1"
	"github.com/apache/skywalking-banyandb/banyand/series/schema"
	"github.com/apache/skywalking-banyandb/banyand/series/schema/sw"
	"github.com/apache/skywalking-banyandb/banyand/storage"
	"github.com/apache/skywalking-banyandb/pkg/run"
	"go.uber.org/multierr"
	"time"
)

var _ Service = (*service)(nil)

type service struct {
	db storage.Database
	addr string
}

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

func (s *service) IndexRules(ctx context.Context, subject v1.Series, filter IndexObjectFilter) ([]v1.IndexRule, error) {
	group := subject.Series(nil).Group()
	var groupStr string
	if group != nil {
		groupStr = string(group)
	}
	bindings, err := s.IndexRuleBinding().List(ctx, schema.ListOpt{Group: groupStr})
	if err != nil {
		return nil, err
	}
	subjectSeries := subject.Series(nil)
	if subjectSeries == nil {
		return nil, nil
	}
	now := uint64(time.Now().UnixNano())
	foundRules := make([]v1.Metadata, 0)
	for _, binding := range bindings {
		spec := binding.Spec
		if spec.BeginAtNanoseconds() > now ||
			spec.ExpireAtNanoseconds() < now {
			continue
		}
		for i := 0; i < spec.SubjectsLength(); i++ {
			sub := &v1.Series{}
			if spec.Subjects(sub, i) &&
				sub.Catalog() == subject.Catalog() {
				s1 := sub.Series(nil)
				if s1 != nil &&
					bytes.Equal(s1.Name(), subjectSeries.Name()) &&
					bytes.Equal(s1.Group(), subjectSeries.Group()) {
					ruleRef := spec.RuleRef(nil)
					if ruleRef != nil {
						foundRules = append(foundRules, *ruleRef)
					}
				}
				break
			}
		}
	}
	result := make([]v1.IndexRule, 0)
	var indexRuleErr error
	for _, rule := range foundRules {
		object, getErr := s.IndexRule().Get(ctx, common.Metadata{KindVersion: common.MetadataKindVersion, Spec: rule})
		if getErr != nil {
			indexRuleErr = multierr.Append(indexRuleErr, err)
			continue
		}
		object.Spec.ObjectsLength()
		r := object.Spec
		if filter == nil {
			result = append(result, r)
			continue
		}
		for i := 0; i < r.ObjectsLength(); i++ {
			indexObject := &v1.IndexObject{}
			if !r.Objects(indexObject, i) {
				continue
			}
			if filter(*indexObject) {
				result = append(result, r)
				continue
			}
		}
	}
	return result, indexRuleErr
}

func (s *service) FetchTrace(traceSeries common.Metadata, traceID string) (data.Trace, error) {
	panic("implement me")
}

func (s *service) FetchEntity(traceSeries common.Metadata, chunkIDs []common.ChunkID, opt ScanOptions) ([]data.Entity, error) {
	panic("implement me")
}

func (s *service) ScanEntity(traceSeries common.Metadata, startTime, endTime uint64, opt ScanOptions) ([]data.Entity, error) {
	panic("implement me")
}

func (s *service) Name() string {
	return "series"
}

func (s *service) FlagSet() *run.FlagSet {
	fs := run.NewFlagSet("series")
	fs.StringVarP(&s.addr, "series", "", ":17911", "the address of banyand listens")
	return fs
}

func (s *service) Validate() error {
	return nil
}

func (s *service) PreRun() error {
	return nil
}
