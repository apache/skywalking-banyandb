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

package tracepipeline_test

import (
	"context"
	"time"

	gm "github.com/onsi/gomega"
	grpclib "google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
)

// RegisterSamplerRuntime writes a TracePipelineConfig onto the named group via
// UpdateGroup, activating the sampler pipeline on the data node through the
// KindGroup watch path. Asserts no error.
func RegisterSamplerRuntime(ctx context.Context, conn *grpclib.ClientConn, group string, cfg *commonv1.TracePipelineConfig) {
	applyPipelineConfig(ctx, conn, group, cfg)
}

// UpdateSamplerRuntime replaces the TracePipelineConfig on the named group with
// cfg (e.g. a variant with a different threshold), delivering the change via
// UpdateGroup. Asserts no error.
func UpdateSamplerRuntime(ctx context.Context, conn *grpclib.ClientConn, group string, cfg *commonv1.TracePipelineConfig) {
	applyPipelineConfig(ctx, conn, group, cfg)
}

// RemoveSamplerRuntime clears the pipeline config from the named group by
// setting Pipeline to nil and calling UpdateGroup. After convergence the data
// node's sampler registry for the group is empty and all traces are retained.
// Asserts no error.
func RemoveSamplerRuntime(ctx context.Context, conn *grpclib.ClientConn, group string) {
	applyPipelineConfig(ctx, conn, group, nil)
}

// applyPipelineConfig fetches the current group, sets its Pipeline field to cfg
// (nil = clear), and persists via UpdateGroup. The caller decides whether this
// is a register, update, or remove operation.
func applyPipelineConfig(ctx context.Context, conn *grpclib.ClientConn, group string, cfg *commonv1.TracePipelineConfig) {
	client := databasev1.NewGroupRegistryServiceClient(conn)

	getCtx, getCancel := context.WithTimeout(ctx, 10*time.Second)
	defer getCancel()
	getResp, getErr := client.Get(getCtx, &databasev1.GroupRegistryServiceGetRequest{Group: group})
	gm.Expect(getErr).NotTo(gm.HaveOccurred(), "GetGroup %q", group)

	grp := getResp.GetGroup()
	gm.Expect(grp).NotTo(gm.BeNil(), "GetGroup %q returned nil group", group)

	grp.Pipeline = cfg

	updateCtx, updateCancel := context.WithTimeout(ctx, 10*time.Second)
	defer updateCancel()
	_, updateErr := client.Update(updateCtx, &databasev1.GroupRegistryServiceUpdateRequest{Group: grp})
	gm.Expect(updateErr).NotTo(gm.HaveOccurred(), "UpdateGroup %q with pipeline=%v", group, cfg != nil)
}

// Fixture builders.

// PluginSOName is the bare filename of the latency-status sampler .so. Suites
// must stage this file inside the --trace-pipeline-trusted-plugin-dir before
// registering. The path value written to the config is the trusted-dir-relative
// filename; the data node resolves it against the trusted dir at load time.
const PluginSOName = "latencystatussampler.so"

// DefaultMergeGrace is the merge_grace used by the base fixture. It is
// intentionally short so integration tests do not wait long for parts to cool.
const DefaultMergeGrace = 0 * time.Second

// NewBasePipelineConfig returns the base TracePipelineConfig used by
// RegisterSamplerRuntime in suite setup. It points at pluginSOName with
// thresholdMs=500 (the canonical latency-status sampler threshold) and
// merge_grace=graceOverride. Pass 0 to use the instantaneous grace path.
//
// The .so must already be staged in the trusted plugin dir under the same name.
func NewBasePipelineConfig(soPath string, mergeGrace time.Duration) *commonv1.TracePipelineConfig {
	return newLatencyStatusConfig(soPath, 500, mergeGrace)
}

// NewVariantPipelineConfig returns a variant TracePipelineConfig with a
// different thresholdMs (200) for the UpdateSamplerRuntime case. Traces with
// duration ≥ 200 ms and status == "success" are now KEPT instead of the
// baseline's ≥ 500 ms threshold.
//
// US-007 / US-008: suites call this to verify that Update delivers new drop
// verdicts on the next merge.
func NewVariantPipelineConfig(soPath string, mergeGrace time.Duration) *commonv1.TracePipelineConfig {
	return newLatencyStatusConfig(soPath, 200, mergeGrace)
}

// NewPanicPipelineConfig returns a TracePipelineConfig whose sampler panics
// unconditionally in Decide. The engine's fail-open recover wrapper catches the
// panic and retains all traces — this config is used exclusively by the US-010
// soak to verify engine resilience (the node must survive, merges must not
// stall, and traces must be retained after the panic is absorbed).
//
// The .so must be the same latencystatussampler staged in the trusted dir; the
// "panic":true config field activates the panic path added for US-010.
func NewPanicPipelineConfig(soPath string, mergeGrace time.Duration) *commonv1.TracePipelineConfig {
	cfgStruct, structErr := structpb.NewStruct(map[string]interface{}{
		"thresholdMs": float64(500),
		"panic":       true,
	})
	gm.Expect(structErr).NotTo(gm.HaveOccurred(), "structpb.NewStruct for latencystatussampler panic config")

	cfg := &commonv1.TracePipelineConfig{
		Enabled: true,
		Plugins: []*commonv1.Plugin{
			{
				Name: "latencystatussampler-panic",
				Kind: &commonv1.Plugin_Sampler{
					Sampler: &commonv1.SamplerPlugin{
						Path:       soPath,
						Symbol:     "NewSampler",
						AbiVersion: 1,
						Config:     cfgStruct,
					},
				},
			},
		},
	}
	if mergeGrace > 0 {
		cfg.MergeGrace = durationpb.New(mergeGrace)
	}
	return cfg
}

// newLatencyStatusConfig builds a TracePipelineConfig for the
// latencystatussampler plugin with the given .so path, thresholdMs, and
// merge_grace. The sampler drops traces whose duration (ms) is below
// thresholdMs AND whose status is "success".
func newLatencyStatusConfig(soPath string, thresholdMs int64, mergeGrace time.Duration) *commonv1.TracePipelineConfig {
	cfgStruct, structErr := structpb.NewStruct(map[string]interface{}{
		"thresholdMs": float64(thresholdMs),
	})
	gm.Expect(structErr).NotTo(gm.HaveOccurred(), "structpb.NewStruct for latencystatussampler config")

	cfg := &commonv1.TracePipelineConfig{
		Enabled: true,
		Plugins: []*commonv1.Plugin{
			{
				Name: "latencystatussampler",
				Kind: &commonv1.Plugin_Sampler{
					Sampler: &commonv1.SamplerPlugin{
						Path:       soPath,
						Symbol:     "NewSampler",
						AbiVersion: 1,
						Config:     cfgStruct,
					},
				},
			},
		},
	}
	if mergeGrace > 0 {
		cfg.MergeGrace = durationpb.New(mergeGrace)
	}
	return cfg
}
