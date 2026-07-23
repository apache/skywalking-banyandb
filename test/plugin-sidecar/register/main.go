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

// Command register creates the trace group + schema and attaches a trace
// pipeline (a single latency-status sampler) so a standalone data node
// reconciles it and calls plugin.Open on the carrier-mounted .so. It is used
// by the plugin-sidecar kind ship gate to drive assertion (c).
//
// It writes directly to the property-based schema registry (the single schema
// store — same path test/cases/tracepipeline's PreloadSchemaViaProperty uses)
// via the schema server the standalone pod exposes on :17916. That is a
// TEST-HARNESS convenience: this helper only has that schema-server address
// wired, not a liaison registry endpoint. The supported OPERATOR path is the
// public GroupRegistryService (gRPC / REST / bydbctl), which writes through the
// same store and preserves the nested pipeline field — see
// docs/operation/plugins-development.md. Either way, the group add/update fires
// the data node's KindGroup reconcile, which calls plugin.Open on the .so.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/types/known/structpb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema/property"
	"github.com/apache/skywalking-banyandb/banyand/observability"
)

func main() {
	propAddr := flag.String("property-schema-addr", "localhost:17916",
		"banyandb property-schema gRPC address (PropertySchemaGrpcAddress)")
	nodeName := flag.String("node-name", "banyandb-plugin:17912", "target data node name (schema server identity)")
	group := flag.String("group", "test-trace-pipeline", "target trace group")
	soName := flag.String("so", "latencystatussampler.so", "trusted-dir-relative .so path")
	threshold := flag.Float64("threshold-ms", 500, "latency sampler thresholdMs")
	successValue := flag.String("success-value", "success", "latency sampler successValue")
	flag.Parse()

	if err := run(*propAddr, *nodeName, *group, *soName, *threshold, *successValue); err != nil {
		fmt.Fprintf(os.Stderr, "register: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("registered pipeline on group %q referencing %q (via property schema registry)\n", *group, *soName)
}

// nodeRegistry implements schema.Node: it advertises exactly one ROLE_META
// schema server at the given property-schema gRPC address.
type nodeRegistry struct {
	nodes []*databasev1.Node
}

func (r *nodeRegistry) ListNode(_ context.Context, role databasev1.Role) ([]*databasev1.Node, error) {
	var out []*databasev1.Node
	for _, n := range r.nodes {
		for _, nr := range n.GetRoles() {
			if nr == role {
				out = append(out, n)
				break
			}
		}
	}
	return out, nil
}
func (r *nodeRegistry) RegisterNode(_ context.Context, _ *databasev1.Node, _ bool) error { return nil }
func (r *nodeRegistry) GetNode(_ context.Context, name string) (*databasev1.Node, error) {
	// This register helper's SchemaRegistryClient only exercises ListNode; return
	// an explicit error rather than an ambiguous (nil, nil) so any future call
	// path fails fast instead of silently treating it as a successful nil lookup.
	return nil, errors.Errorf("nodeRegistry.GetNode(%q): not implemented (register helper only requires ListNode)", name)
}
func (r *nodeRegistry) UpdateNode(_ context.Context, _ *databasev1.Node) error { return nil }

func run(propAddr, nodeName, group, soName string, threshold float64, successValue string) error {
	reg, regErr := property.NewSchemaRegistryClient(&property.ClientConfig{
		OMR:         observability.BypassRegistry,
		GRPCTimeout: 10 * time.Second,
		NodeRegistry: &nodeRegistry{nodes: []*databasev1.Node{{
			Metadata:                  &commonv1.Metadata{Name: nodeName},
			Roles:                     []databasev1.Role{databasev1.Role_ROLE_META},
			PropertySchemaGrpcAddress: propAddr,
		}}},
	})
	if regErr != nil {
		return errors.Wrap(regErr, "new property schema registry client")
	}
	defer func() { _ = reg.Close() }()

	// Wait for the schema server node to become active.
	deadline := time.Now().Add(30 * time.Second)
	for len(reg.ActiveNodeNames()) == 0 {
		if time.Now().After(deadline) {
			return errors.New("no active property schema server node within 30s")
		}
		time.Sleep(200 * time.Millisecond)
	}

	ctx := context.Background()

	// Mirror the proven integration-test sequence
	// (PreloadSchemaViaProperty + RegisterSamplerRuntime): first create the
	// group WITHOUT the pipeline, then attach the pipeline via UpdateGroup. The
	// pipeline is delivered on the group-UPDATE watch event, which is the path
	// that reliably fires the data node's reconcilePipeline (create-with-pipeline
	// does not).
	base := &commonv1.Group{
		Metadata: &commonv1.Metadata{Name: group},
		Catalog:  commonv1.Catalog_CATALOG_TRACE,
		ResourceOpts: &commonv1.ResourceOpts{
			ShardNum:        1,
			SegmentInterval: &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 1},
			Ttl:             &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 3},
		},
	}
	if _, createErr := reg.CreateGroup(ctx, base); createErr != nil && !errors.Is(createErr, schema.ErrGRPCAlreadyExists) {
		return errors.Wrap(createErr, "create group")
	}

	// Create the trace schema (harmless if already present); mirrors
	// test/cases/tracepipeline/testdata/traces/filter.json.
	trace := traceSchema(group)
	if _, traceErr := reg.CreateTrace(ctx, trace); traceErr != nil && !errors.Is(traceErr, schema.ErrGRPCAlreadyExists) {
		return errors.Wrap(traceErr, "create trace schema")
	}

	// Wait for the freshly-created group to propagate to (and be established
	// on) the data node before attaching the pipeline — mirrors the
	// integration suite's waitForSchemaSync between PreloadSchemaViaProperty
	// and RegisterSamplerRuntime. Without this settle, the pipeline UpdateGroup
	// races the group create and the data node's reconcilePipeline does not
	// fire for it.
	syncDeadline := time.Now().Add(30 * time.Second)
	for {
		got, getErr := reg.GetGroup(ctx, group)
		if getErr == nil && got != nil {
			break
		}
		if time.Now().After(syncDeadline) {
			return errors.New("group did not become visible within 30s")
		}
		time.Sleep(500 * time.Millisecond)
	}
	time.Sleep(3 * time.Second)

	// Attach the pipeline via UpdateGroup → fires reconcilePipeline → plugin.Open.
	pc, pcErr := pipelineConfig(soName, threshold, successValue)
	if pcErr != nil {
		return errors.Wrap(pcErr, "build pipeline config")
	}
	withPipeline := &commonv1.Group{
		Metadata:     &commonv1.Metadata{Name: group},
		Catalog:      commonv1.Catalog_CATALOG_TRACE,
		ResourceOpts: base.ResourceOpts,
		Pipeline:     pc,
	}
	if _, updateErr := reg.UpdateGroup(ctx, withPipeline); updateErr != nil {
		return errors.Wrap(updateErr, "update group with pipeline")
	}
	return nil
}

func pipelineConfig(soName string, threshold float64, successValue string) (*commonv1.TracePipelineConfig, error) {
	cfgStruct, err := structpb.NewStruct(map[string]any{
		"thresholdMs":  threshold,
		"successValue": successValue,
	})
	if err != nil {
		// NewStruct fails on non-finite numbers (NaN/Inf); fail fast rather than
		// silently shipping a nil config Struct (which would fall back to plugin
		// defaults with no signal).
		return nil, errors.Wrap(err, "structpb.NewStruct(sampler config)")
	}
	return &commonv1.TracePipelineConfig{
		Enabled:       true,
		EnabledEvents: []commonv1.PipelineEvent{commonv1.PipelineEvent_PIPELINE_EVENT_MERGE},
		Plugins: []*commonv1.Plugin{
			{
				Name: "latency-status",
				Kind: &commonv1.Plugin_Sampler{
					Sampler: &commonv1.SamplerPlugin{
						Path:       soName,
						AbiVersion: 1,
						Config:     cfgStruct,
					},
				},
			},
		},
	}, nil
}

func traceSchema(group string) *databasev1.Trace {
	strTag := func(name string) *databasev1.TraceTagSpec {
		return &databasev1.TraceTagSpec{Name: name, Type: databasev1.TagType_TAG_TYPE_STRING}
	}
	return &databasev1.Trace{
		Metadata: &commonv1.Metadata{Name: "filter", Group: group},
		Tags: []*databasev1.TraceTagSpec{
			strTag("trace_id"),
			strTag("span_id"),
			{Name: "timestamp", Type: databasev1.TagType_TAG_TYPE_TIMESTAMP},
			strTag("service_id"),
			{Name: "duration", Type: databasev1.TagType_TAG_TYPE_INT},
			strTag("status"),
		},
		TraceIdTagName:   "trace_id",
		SpanIdTagName:    "span_id",
		TimestampTagName: "timestamp",
	}
}
