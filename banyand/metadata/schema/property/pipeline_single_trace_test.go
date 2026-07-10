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

package property_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema/property"
)

// samplerPipeline builds a trace pipeline config with a single sampler plugin.
// When enabled, it triggers the single-trace-per-group constraint.
func samplerPipeline(enabled bool) *commonv1.TracePipelineConfig {
	return &commonv1.TracePipelineConfig{
		Enabled:       enabled,
		EnabledEvents: []commonv1.PipelineEvent{commonv1.PipelineEvent_PIPELINE_EVENT_MERGE},
		Plugins: []*commonv1.Plugin{
			{
				Name: "latency-status",
				Kind: &commonv1.Plugin_Sampler{
					Sampler: &commonv1.SamplerPlugin{
						Path:       "latencystatussampler.so",
						AbiVersion: 1,
					},
				},
			},
		},
	}
}

// traceTestGroup is the group name used across the single-trace-per-group tests.
const traceTestGroup = "tg"

// traceGroup returns a CATALOG_TRACE group, optionally carrying a pipeline.
func traceGroup(pipeline *commonv1.TracePipelineConfig) *commonv1.Group {
	return &commonv1.Group{
		Metadata: &commonv1.Metadata{Name: traceTestGroup},
		Catalog:  commonv1.Catalog_CATALOG_TRACE,
		ResourceOpts: &commonv1.ResourceOpts{
			ShardNum:        1,
			SegmentInterval: &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 1},
			Ttl:             &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 3},
		},
		Pipeline: pipeline,
	}
}

// traceNamed clones the standard test trace with the given name in traceTestGroup.
func traceNamed(name string) *databasev1.Trace {
	tr := testTrace(traceTestGroup)
	tr.Metadata.Name = name
	tr.UpdatedAt = timestamppb.Now()
	return tr
}

func requireFailedPrecondition(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	require.Equalf(t, codes.FailedPrecondition, status.Code(err), "expected FailedPrecondition, got: %v", err)
}

func newTraceRegistry(t *testing.T) (*property.SchemaRegistry, context.Context) {
	addr := startTestSchemaServer(t)
	return newTestRegistry(t, addr), context.Background()
}

// TestCreateTrace_RejectsSecondTraceWhenSamplerEnabled verifies direction 1:
// a group whose pipeline is enabled with a sampler plugin may hold only one trace.
func TestCreateTrace_RejectsSecondTraceWhenSamplerEnabled(t *testing.T) {
	reg, ctx := newTraceRegistry(t)
	_, groupErr := reg.CreateGroup(ctx, traceGroup(samplerPipeline(true)))
	require.NoError(t, groupErr)

	_, firstErr := reg.CreateTrace(ctx, traceNamed("trace-a"))
	require.NoError(t, firstErr)

	_, secondErr := reg.CreateTrace(ctx, traceNamed("trace-b"))
	requireFailedPrecondition(t, secondErr)
}

// TestCreateTrace_AllowedWithoutSamplerPipeline verifies the constraint does not
// apply when the group has no pipeline.
func TestCreateTrace_AllowedWithoutSamplerPipeline(t *testing.T) {
	reg, ctx := newTraceRegistry(t)
	_, groupErr := reg.CreateGroup(ctx, traceGroup(nil))
	require.NoError(t, groupErr)

	_, firstErr := reg.CreateTrace(ctx, traceNamed("trace-a"))
	require.NoError(t, firstErr)
	_, secondErr := reg.CreateTrace(ctx, traceNamed("trace-b"))
	require.NoError(t, secondErr)
}

// TestCreateTrace_AllowedWhenPipelineDisabled verifies the trigger is gated on
// Enabled: a disabled sampler pipeline does not constrain the trace count.
func TestCreateTrace_AllowedWhenPipelineDisabled(t *testing.T) {
	reg, ctx := newTraceRegistry(t)
	_, groupErr := reg.CreateGroup(ctx, traceGroup(samplerPipeline(false)))
	require.NoError(t, groupErr)

	_, firstErr := reg.CreateTrace(ctx, traceNamed("trace-a"))
	require.NoError(t, firstErr)
	_, secondErr := reg.CreateTrace(ctx, traceNamed("trace-b"))
	require.NoError(t, secondErr)
}

// TestUpdateTrace_AllowsSelf verifies updating the single existing trace is not
// rejected (self is excluded by name).
func TestUpdateTrace_AllowsSelf(t *testing.T) {
	reg, ctx := newTraceRegistry(t)
	_, groupErr := reg.CreateGroup(ctx, traceGroup(samplerPipeline(true)))
	require.NoError(t, groupErr)
	_, createErr := reg.CreateTrace(ctx, traceNamed("trace-a"))
	require.NoError(t, createErr)

	_, updateErr := reg.UpdateTrace(ctx, traceNamed("trace-a"))
	require.NoError(t, updateErr)
}

// TestUpdateGroup_RejectsEnablingSamplerWithMultipleTraces verifies direction 2:
// a sampler pipeline cannot be enabled on a group that already holds >1 trace.
func TestUpdateGroup_RejectsEnablingSamplerWithMultipleTraces(t *testing.T) {
	reg, ctx := newTraceRegistry(t)
	_, groupErr := reg.CreateGroup(ctx, traceGroup(nil))
	require.NoError(t, groupErr)
	_, aErr := reg.CreateTrace(ctx, traceNamed("trace-a"))
	require.NoError(t, aErr)
	_, bErr := reg.CreateTrace(ctx, traceNamed("trace-b"))
	require.NoError(t, bErr)

	_, updateErr := reg.UpdateGroup(ctx, traceGroup(samplerPipeline(true)))
	requireFailedPrecondition(t, updateErr)
}

// TestUpdateGroup_AllowsEnablingSamplerWithOneTrace verifies enabling a sampler
// pipeline on a single-trace group succeeds.
func TestUpdateGroup_AllowsEnablingSamplerWithOneTrace(t *testing.T) {
	reg, ctx := newTraceRegistry(t)
	_, groupErr := reg.CreateGroup(ctx, traceGroup(nil))
	require.NoError(t, groupErr)
	_, aErr := reg.CreateTrace(ctx, traceNamed("trace-a"))
	require.NoError(t, aErr)

	_, updateErr := reg.UpdateGroup(ctx, traceGroup(samplerPipeline(true)))
	require.NoError(t, updateErr)
}

// TestCreateGroup_WithSamplerPipelineNoTraces verifies a group may be created
// with an enabled sampler pipeline while it holds no traces yet.
func TestCreateGroup_WithSamplerPipelineNoTraces(t *testing.T) {
	reg, ctx := newTraceRegistry(t)
	_, groupErr := reg.CreateGroup(ctx, traceGroup(samplerPipeline(true)))
	require.NoError(t, groupErr)
}
