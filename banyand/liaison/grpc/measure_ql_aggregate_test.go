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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	"github.com/apache/skywalking-banyandb/pkg/bydbql"
)

func TestNativeAggregateLifecycleSelection(t *testing.T) {
	execution := nativeAggregateStageExecution(t)
	warm := &commonv1.LifecycleStage{Name: "warm", NodeSelector: "role=data-warm"}
	cold := &commonv1.LifecycleStage{Name: "cold", NodeSelector: "role=data-cold"}
	cases := []struct {
		name        string
		options     *commonv1.ResourceOpts
		stages      []string
		groups      []string
		withoutRepo bool
		want        bool
	}{
		{name: "implicit hot", options: &commonv1.ResourceOpts{}, want: true},
		{name: "unavailable metadata"},
		{name: "missing group repository", withoutRepo: true},
		{name: "multiple measure groups", options: &commonv1.ResourceOpts{}, groups: []string{"g", "other"}},
		{name: "explicit multiple stages", options: &commonv1.ResourceOpts{Stages: []*commonv1.LifecycleStage{warm, cold}}, stages: []string{"warm", "cold"}},
		{name: "default multiple stages", options: &commonv1.ResourceOpts{
			Stages: []*commonv1.LifecycleStage{warm, cold}, DefaultStages: []string{"warm", "cold"},
		}},
		{name: "explicit hot", options: &commonv1.ResourceOpts{}, stages: []string{"HoT"}},
		{name: "default hot", options: &commonv1.ResourceOpts{DefaultStages: []string{"HOT"}}},
		{name: "configured hot", options: &commonv1.ResourceOpts{
			Stages: []*commonv1.LifecycleStage{{Name: "hot", NodeSelector: "role=data-hot"}}, DefaultStages: []string{"hot"},
		}},
		{name: "unknown stage", options: &commonv1.ResourceOpts{Stages: []*commonv1.LifecycleStage{warm}}, stages: []string{"missing"}},
		{name: "unknown default", options: &commonv1.ResourceOpts{Stages: []*commonv1.LifecycleStage{warm}, DefaultStages: []string{"missing"}}},
		{name: "blank selector", options: &commonv1.ResourceOpts{
			Stages: []*commonv1.LifecycleStage{{Name: "warm", NodeSelector: " \t "}}, DefaultStages: []string{"warm"},
		}},
		{name: "configured without selection", options: &commonv1.ResourceOpts{Stages: []*commonv1.LifecycleStage{warm}}},
		{name: "one requested named stage", options: &commonv1.ResourceOpts{Stages: []*commonv1.LifecycleStage{warm, cold}}, stages: []string{"WARM"}, want: true},
		{name: "one default named stage", options: &commonv1.ResourceOpts{
			Stages: []*commonv1.LifecycleStage{warm, cold}, DefaultStages: []string{"warm"},
		}, want: true},
		{name: "request overrides multiple defaults", options: &commonv1.ResourceOpts{
			Stages: []*commonv1.LifecycleStage{warm, cold}, DefaultStages: []string{"warm", "cold"},
		}, stages: []string{"cold"}, want: true},
		{name: "ambiguous duplicate stage", options: &commonv1.ResourceOpts{
			Stages: []*commonv1.LifecycleStage{warm, {Name: "WARM", NodeSelector: "role=other"}}, DefaultStages: []string{"warm"},
		}},
	}
	for _, example := range cases {
		t.Run(example.name, func(t *testing.T) {
			request := proto.Clone(execution.Request).(*measurev1.QueryRequest)
			request.Stages = example.stages
			if example.groups != nil {
				request.Groups = example.groups
			}
			repo := &groupRepo{resourceOpts: map[string]*commonv1.ResourceOpts{"g": example.options}}
			if example.withoutRepo {
				repo = nil
			}
			service := &measureService{discoveryService: &discoveryService{groupRepo: repo}}
			require.Equal(t, example.want, service.canUseNativeAggregate(request, execution.Plan))
		})
	}
}

func TestNativeAggregateLifecycleSelectionRereadsOptionsForBoundPlan(t *testing.T) {
	execution := nativeAggregateStageExecution(t)
	candidate := execution.Plan.NativeAggregate
	repo := &groupRepo{resourceOpts: make(map[string]*commonv1.ResourceOpts)}
	service := &measureService{discoveryService: &discoveryService{groupRepo: repo}}
	updates := []struct {
		options *commonv1.ResourceOpts
		want    bool
	}{
		{&commonv1.ResourceOpts{}, true},
		{&commonv1.ResourceOpts{
			Stages: []*commonv1.LifecycleStage{{Name: "warm", NodeSelector: "role=data-warm"}}, DefaultStages: []string{"warm"},
		}, true},
		{&commonv1.ResourceOpts{DefaultStages: []string{"warm", "cold"}}, false},
		{&commonv1.ResourceOpts{DefaultStages: []string{"hot"}}, false},
		{nil, false},
		{&commonv1.ResourceOpts{}, true},
	}
	for index, update := range updates {
		repo.Lock()
		repo.resourceOpts["g"] = update.options
		repo.Unlock()
		require.Equal(t, update.want, service.canUseNativeAggregate(execution.Request, execution.Plan), "metadata update %d", index)
		require.Same(t, candidate, execution.Plan.NativeAggregate)
	}
}

func nativeAggregateStageExecution(t *testing.T) *bydbql.MeasureExecution {
	t.Helper()
	schema := minimalMeasure("g", "m", 100)
	schema.Fields = []*databasev1.FieldSpec{{Name: "v", FieldType: databasev1.FieldType_FIELD_TYPE_INT}}
	statement, err := bydbql.Prepare("SELECT COUNT(v) AS c FROM g.m WHERE svc = 'a' " +
		"AND time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:06Z'")
	require.NoError(t, err)
	template, err := bydbql.NewTransformer(nil).CompileMeasureWithSchemas(t.Context(), statement,
		map[bydbql.MeasureSource]*databasev1.Measure{{Group: "g", Name: "m"}: schema})
	require.NoError(t, err)
	execution, err := template.BindParams(nil, time.UnixMilli(0))
	require.NoError(t, err)
	require.NotNil(t, execution.Plan.NativeAggregate)
	return execution
}
