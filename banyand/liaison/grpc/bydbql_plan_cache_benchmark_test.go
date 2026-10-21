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

package grpc

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/bydbql"
)

// BenchmarkQLBPlanReuse measures fresh parameter binding and native-request construction.
func BenchmarkQLBPlanReuse(b *testing.B) {
	b.Run("T05", func(b *testing.B) {
		for _, shape := range []string{"legacy_simple", "typed_wide"} {
			b.Run(shape, func(b *testing.B) {
				for _, enabled := range []bool{false, true} {
					name := "cache_off"
					if enabled {
						name = "cache_on"
					}
					b.Run(name, func(b *testing.B) { benchmarkQLBT05Planning(b, shape, enabled) })
				}
			})
		}
	})
}

func benchmarkQLBT05Planning(b *testing.B, shape string, enabled bool) {
	b.Helper()
	service, source := qlbT05PlanningService()
	cacheSize := 0
	if enabled {
		cacheSize = 4000
	}
	service.planCache = newCompiledPlanCache(planCacheConfig{
		MaxEntries: cacheSize, MaxBytes: 64 << 20, MaxCompiles: 4, MaxInFlight: 64,
	}, source, service.transformer, nil)
	b.Cleanup(service.planCache.Close)
	query, params := qlbT05PlanningQuery(shape)
	statement, prepareErr := bydbql.Prepare(query)
	if prepareErr != nil {
		b.Fatal(prepareErr)
	}
	for _, values := range params {
		bound, bindErr := statement.Bind(values)
		if bindErr != nil {
			b.Fatal(bindErr)
		}
		_, _, handle, _, planErr := service.planQueryWithCache(context.Background(), statement, bound)
		if planErr != nil {
			b.Fatal(planErr)
		}
		if handle != nil {
			handle.Release()
		}
	}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		bound, bindErr := statement.Bind(params[iteration%2])
		if bindErr != nil {
			b.Fatal(bindErr)
		}
		_, _, handle, _, planErr := service.planQueryWithCache(ctx, statement, bound)
		if planErr != nil {
			b.Fatal(planErr)
		}
		if handle != nil {
			handle.Release()
		}
	}
}

func qlbT05PlanningService() (*bydbQLService, *entityRepoSchemaSource) {
	repo := newEmptyEntityRepo()
	repo.OnAddOrUpdate(measureAddEvent(&databasev1.Measure{
		Metadata: &commonv1.Metadata{Name: "service_cpm_minute", Group: "sw_metric", ModRevision: 1},
		Entity:   &databasev1.Entity{TagNames: []string{"entity_id"}},
		TagFamilies: []*databasev1.TagFamilySpec{{
			Name: "default", Tags: []*databasev1.TagSpec{
				{Name: "id", Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: "entity_id", Type: databasev1.TagType_TAG_TYPE_STRING},
			},
		}},
		Fields: []*databasev1.FieldSpec{{Name: "value", FieldType: databasev1.FieldType_FIELD_TYPE_INT}},
	}))
	source := &entityRepoSchemaSource{measures: repo}
	return &bydbQLService{transformer: bydbql.NewTransformerWithSource(nil, source)}, source
}

func qlbT05PlanningQuery(shape string) (string, [][]*modelv1.TagValue) {
	query := "SELECT id, value::FIELD FROM MEASURE service_cpm_minute IN sw_metric TIME BETWEEN ? AND ? WHERE id = ? LIMIT 1024"
	if shape == "typed_wide" {
		columns := []string{"id AS node", "entity_id AS entity"}
		for index := 0; index < 16; index++ {
			columns = append(columns, fmt.Sprintf("value::FIELD AS value%02d", index))
		}
		query = "SELECT " + strings.Join(columns, ", ") +
			" FROM sw_metric.service_cpm_minute WHERE id = ? AND time >= ? AND time < ?" +
			" AND time >= ? AND time < ? AND time >= ? AND time < ? AND time >= ? AND time < ?"
	}
	values := make([][]*modelv1.TagValue, 2)
	base := time.Unix(1700000000, 0)
	for binding := range values {
		paramTime := func(offset int, adjustment time.Duration) *modelv1.TagValue {
			return &modelv1.TagValue{Value: &modelv1.TagValue_Timestamp{
				Timestamp: timestamppb.New(base.Add(time.Duration(offset)*time.Second + adjustment)),
			}}
		}
		begin := binding * 8
		end := begin + 16
		service := bydbqlStrParam(fmt.Sprintf("svc%d", binding))
		if shape == "legacy_simple" {
			values[binding] = []*modelv1.TagValue{paramTime(begin, 0), paramTime(end, -time.Millisecond), service}
			continue
		}
		values[binding] = []*modelv1.TagValue{service}
		for margin := 0; margin < 4; margin++ {
			values[binding] = append(values[binding], paramTime(begin-margin, 0), paramTime(end+margin, 0))
		}
	}
	return query, values
}
