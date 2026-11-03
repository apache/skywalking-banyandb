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
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	bydbqlv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/bydbql/v1"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/bydbql"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

type qlIndexMetadata struct {
	metadata.Repo
	schema.Measure
	schema.IndexRule
	schema.IndexRuleBinding
	measure      *databasev1.Measure
	rules        map[string]*databasev1.IndexRule
	ruleLoads    map[string]int
	bindings     []*databasev1.IndexRuleBinding
	bindingLoads int
}

func (r *qlIndexMetadata) MeasureRegistry() schema.Measure                   { return r }
func (r *qlIndexMetadata) IndexRuleRegistry() schema.IndexRule               { return r }
func (r *qlIndexMetadata) IndexRuleBindingRegistry() schema.IndexRuleBinding { return r }
func (r *qlIndexMetadata) GetMeasure(context.Context, *commonv1.Metadata) (*databasev1.Measure, error) {
	return r.measure, nil
}

func (r *qlIndexMetadata) GetIndexRule(_ context.Context, metadata *commonv1.Metadata) (*databasev1.IndexRule, error) {
	r.ruleLoads[metadata.Name]++
	return r.rules[metadata.Name], nil
}

func (r *qlIndexMetadata) ListIndexRuleBinding(context.Context, schema.ListOpt) ([]*databasev1.IndexRuleBinding, error) {
	r.bindingLoads++
	return r.bindings, nil
}

func newQLIndexService(t *testing.T, cached bool) (*bydbQLService, *qlIndexMetadata) {
	t.Helper()
	measure := &databasev1.Measure{
		Metadata: &commonv1.Metadata{Group: "g", Name: "m", ModRevision: 1},
		Entity:   &databasev1.Entity{TagNames: []string{"name"}},
		TagFamilies: []*databasev1.TagFamilySpec{{Name: "default", Tags: []*databasev1.TagSpec{
			{Name: "name", Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: "members", Type: databasev1.TagType_TAG_TYPE_STRING_ARRAY},
		}}},
	}
	repo := &qlIndexMetadata{
		measure: measure,
		rules: map[string]*databasev1.IndexRule{"names": {
			Metadata: &commonv1.Metadata{Group: "g", Name: "names", Id: 9},
			Tags:     []string{"name"}, Type: databasev1.IndexRule_TYPE_INVERTED, Analyzer: "keyword",
		}},
		bindings: []*databasev1.IndexRuleBinding{{
			Subject: &databasev1.Subject{Catalog: commonv1.Catalog_CATALOG_MEASURE, Name: "m"},
			Rules:   []string{"names"},
		}},
		ruleLoads: make(map[string]int),
	}
	entities := newEmptyEntityRepo()
	entities.OnAddOrUpdate(measureAddEvent(measure))
	source := &entityRepoSchemaSource{measures: entities}
	service := newTestBydbQLService()
	service.l = logger.GetLogger("ql-index-test")
	service.repo = repo
	service.transformer = bydbql.NewTransformerWithSource(nil, source)
	service.measureSvc = newTestMeasureService(entities, 0)
	service.measureSvc.groupRepo = &groupRepo{inflight: make(map[string]*groupInflight)}
	if cached {
		service.planCache = newCompiledPlanCache(planCacheConfig{
			MaxEntries: 4, MaxBytes: 1 << 20, MaxCompiles: 2, MaxInFlight: 8,
		}, source, service.transformer, nil)
		t.Cleanup(service.planCache.Close)
	}
	return service, repo
}

func TestQLIndexesShareMetadataOnEveryEmptyExecution(t *testing.T) {
	for _, cached := range []bool{false, true} {
		name := "uncached"
		if cached {
			name = "cached"
		}
		t.Run(name, func(t *testing.T) {
			service, repo := newQLIndexService(t, cached)
			repo.bindings = append(repo.bindings, repo.bindings[0])
			request := &bydbqlv1.QueryRequest{
				Query: "SELECT name FROM g.m WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z'" +
					" AND name LIKE ? AND members CONTAINS 'member' LIMIT 0",
				Params: []*modelv1.TagValue{bydbqlStrParam("%")},
			}
			for execution := 1; execution <= 2; execution++ {
				response, err := service.Query(t.Context(), request)
				require.NoError(t, err)
				require.Empty(t, response.GetMeasureResult().GetDataPoints())
				require.Equal(t, execution, repo.bindingLoads)
				require.Equal(t, execution, repo.ruleLoads["names"])
			}
			if cached {
				require.EqualValues(t, 1, service.planCache.snapshot().Hits)
			}
		})
	}
}

func TestQLIndexesRevalidateChangedBindings(t *testing.T) {
	for _, test := range []struct {
		change  func(*qlIndexMetadata)
		name    string
		message string
		code    codes.Code
	}{
		{
			name: "missing index", code: codes.InvalidArgument, message: "requires an active single-tag whole-value inverted index",
			change: func(repo *qlIndexMetadata) { repo.bindings = nil },
		},
		{
			name: "indexed array", code: codes.InvalidArgument, message: `membership on indexed array "members" is unsupported`,
			change: func(repo *qlIndexMetadata) {
				repo.rules["members"] = &databasev1.IndexRule{Tags: []string{"members"}}
				repo.bindings[0].Rules = append(repo.bindings[0].Rules, "members")
			},
		},
		{
			name: "ambiguous pattern index", code: codes.InvalidArgument, message: "multiple active index rules",
			change: func(repo *qlIndexMetadata) {
				repo.rules["other"] = &databasev1.IndexRule{
					Metadata: &commonv1.Metadata{Group: "g", Name: "other", Id: 1},
					Tags:     []string{"name"}, Type: databasev1.IndexRule_TYPE_INVERTED, Analyzer: "keyword",
				}
				repo.bindings[0].Rules = append(repo.bindings[0].Rules, "other")
			},
		},
		{
			name: "unavailable rule", code: codes.Internal, message: "LIKE index rule g/names is unavailable",
			change: func(repo *qlIndexMetadata) { delete(repo.rules, "names") },
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			service, repo := newQLIndexService(t, true)
			request := &bydbqlv1.QueryRequest{
				Query: "SELECT name FROM g.m WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z'" +
					" AND name LIKE ? AND members CONTAINS 'member' LIMIT 0",
				Params: []*modelv1.TagValue{bydbqlStrParam("%")},
			}
			_, err := service.Query(t.Context(), request)
			require.NoError(t, err)
			test.change(repo)
			response, err := service.Query(t.Context(), request)
			require.Nil(t, response)
			require.Equal(t, test.code, status.Code(err))
			require.ErrorContains(t, err, test.message)
			require.Equal(t, 2, repo.bindingLoads)
			require.EqualValues(t, 1, service.planCache.snapshot().Hits)
		})
	}
}
