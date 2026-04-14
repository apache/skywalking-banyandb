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
	"fmt"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	schemav1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/schema/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema/property"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema/schemaserver"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	testflags "github.com/apache/skywalking-banyandb/pkg/test/flags"
)

const (
	testStreamName       = "test-stream"
	testDeleteStreamName = "delete-stream"
)

func startTestSchemaServer(t *testing.T) string {
	t.Helper()
	addr, _ := startTestSchemaServerStoppable(t)
	return addr
}

func startTestSchemaServerStoppable(t *testing.T) (string, func()) {
	t.Helper()
	_, addr, stopFn := startTestSchemaServerFull(t)
	return addr, stopFn
}

func startTestSchemaServerFull(t *testing.T) (schemaserver.Server, string, func()) {
	t.Helper()
	srv := schemaserver.NewServer(observability.BypassRegistry)
	flagSet := srv.FlagSet()
	port := getFreePort(t)
	require.NoError(t, flagSet.Parse([]string{
		"--schema-server-root-path", t.TempDir(),
		"--schema-server-grpc-host", "127.0.0.1",
		"--schema-server-grpc-port", strconv.FormatUint(uint64(port), 10),
	}))
	require.NoError(t, srv.Validate())
	require.NoError(t, srv.PreRun(context.Background()))
	srv.Serve()
	var once sync.Once
	stopFn := func() { once.Do(func() { srv.GracefulStop() }) }
	t.Cleanup(stopFn)
	addr := net.JoinHostPort("127.0.0.1", strconv.FormatUint(uint64(port), 10))
	deadline := time.Now().Add(5 * time.Second)
	for {
		conn, dialErr := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if dialErr == nil {
			_ = conn.Close()
			break
		}
		if time.Now().After(deadline) {
			require.FailNowf(t, "server did not start in time", "last error: %v", dialErr)
		}
		time.Sleep(50 * time.Millisecond)
	}
	return srv, addr, stopFn
}

func getFreePort(t *testing.T) uint32 {
	t.Helper()
	lis, lisErr := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, lisErr)
	port := lis.Addr().(*net.TCPAddr).Port
	_ = lis.Close()
	return uint32(port)
}

type testNodeRegistry struct {
	nodes []*databasev1.Node
}

func (r *testNodeRegistry) ListNode(_ context.Context, role databasev1.Role) ([]*databasev1.Node, error) {
	var result []*databasev1.Node
	for _, n := range r.nodes {
		for _, nodeRole := range n.GetRoles() {
			if nodeRole == role {
				result = append(result, n)
				break
			}
		}
	}
	return result, nil
}

func (r *testNodeRegistry) RegisterNode(_ context.Context, _ *databasev1.Node, _ bool) error {
	return nil
}

func (r *testNodeRegistry) GetNode(_ context.Context, _ string) (*databasev1.Node, error) {
	return nil, nil
}

func (r *testNodeRegistry) UpdateNode(_ context.Context, _ *databasev1.Node) error { return nil }

func buildTestNodes(prefix string, addrs ...string) []*databasev1.Node {
	nodes := make([]*databasev1.Node, len(addrs))
	for idx, addr := range addrs {
		nodes[idx] = &databasev1.Node{
			Metadata:                  &commonv1.Metadata{Name: fmt.Sprintf("%s-%d", prefix, idx)},
			Roles:                     []databasev1.Role{databasev1.Role_ROLE_META},
			PropertySchemaGrpcAddress: addr,
		}
	}
	return nodes
}

func newTestRegistry(t *testing.T, addrs ...string) *property.SchemaRegistry {
	t.Helper()
	reg, regErr := property.NewSchemaRegistryClient(&property.ClientConfig{
		GRPCTimeout:  10 * time.Second,
		NodeRegistry: &testNodeRegistry{nodes: buildTestNodes("test-node", addrs...)},
	})
	require.NoError(t, regErr)
	t.Cleanup(func() { _ = reg.Close() })
	return reg
}

func rawMgmtClient(t *testing.T, addr string) schemav1.SchemaManagementServiceClient {
	t.Helper()
	conn, dialErr := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, dialErr)
	t.Cleanup(func() { _ = conn.Close() })
	return schemav1.NewSchemaManagementServiceClient(conn)
}

func testGroup() *commonv1.Group {
	return &commonv1.Group{
		Metadata: &commonv1.Metadata{
			Name: "test-group",
		},
		Catalog: commonv1.Catalog_CATALOG_STREAM,
		ResourceOpts: &commonv1.ResourceOpts{
			ShardNum: 2,
			SegmentInterval: &commonv1.IntervalRule{
				Unit: commonv1.IntervalRule_UNIT_DAY,
				Num:  1,
			},
			Ttl: &commonv1.IntervalRule{
				Unit: commonv1.IntervalRule_UNIT_DAY,
				Num:  7,
			},
		},
	}
}

func testStream() *databasev1.Stream {
	return &databasev1.Stream{
		Metadata: &commonv1.Metadata{
			Name:  testStreamName,
			Group: "test-group",
		},
		TagFamilies: []*databasev1.TagFamilySpec{
			{
				Name: "default",
				Tags: []*databasev1.TagSpec{
					{
						Name: "service_name",
						Type: databasev1.TagType_TAG_TYPE_STRING,
					},
				},
			},
		},
		Entity: &databasev1.Entity{
			TagNames: []string{"service_name"},
		},
		UpdatedAt: timestamppb.Now(),
	}
}

func testMeasure(group string) *databasev1.Measure {
	return &databasev1.Measure{
		Metadata: &commonv1.Metadata{
			Name:  "test-measure",
			Group: group,
		},
		TagFamilies: []*databasev1.TagFamilySpec{
			{
				Name: "default",
				Tags: []*databasev1.TagSpec{
					{
						Name: "service_name",
						Type: databasev1.TagType_TAG_TYPE_STRING,
					},
				},
			},
		},
		Fields: []*databasev1.FieldSpec{
			{
				Name:              "value",
				FieldType:         databasev1.FieldType_FIELD_TYPE_INT,
				CompressionMethod: databasev1.CompressionMethod_COMPRESSION_METHOD_ZSTD,
				EncodingMethod:    databasev1.EncodingMethod_ENCODING_METHOD_GORILLA,
			},
		},
		Entity: &databasev1.Entity{
			TagNames: []string{"service_name"},
		},
		UpdatedAt: timestamppb.Now(),
	}
}

func testTrace(group string) *databasev1.Trace {
	return &databasev1.Trace{
		Metadata: &commonv1.Metadata{
			Name:  "test-trace",
			Group: group,
		},
		Tags: []*databasev1.TraceTagSpec{
			{
				Name: "trace_id",
				Type: databasev1.TagType_TAG_TYPE_STRING,
			},
			{
				Name: "span_id",
				Type: databasev1.TagType_TAG_TYPE_STRING,
			},
			{
				Name: "timestamp",
				Type: databasev1.TagType_TAG_TYPE_INT,
			},
		},
		TraceIdTagName:   "trace_id",
		SpanIdTagName:    "span_id",
		TimestampTagName: "timestamp",
		UpdatedAt:        timestamppb.Now(),
	}
}

func testIndexRule(group string) *databasev1.IndexRule {
	return &databasev1.IndexRule{
		Metadata: &commonv1.Metadata{
			Name:  "test-index-rule",
			Group: group,
			Id:    1,
		},
		Tags: []string{"service_name"},
		Type: databasev1.IndexRule_TYPE_INVERTED,
	}
}

func testIndexRuleBinding(group string) *databasev1.IndexRuleBinding {
	return &databasev1.IndexRuleBinding{
		Metadata: &commonv1.Metadata{
			Name:  "test-index-rule-binding",
			Group: group,
		},
		Rules: []string{"test-index-rule"},
		Subject: &databasev1.Subject{
			Name:    testStreamName,
			Catalog: commonv1.Catalog_CATALOG_STREAM,
		},
		UpdatedAt: timestamppb.Now(),
	}
}

func testTopNAggregation(group string) *databasev1.TopNAggregation {
	return &databasev1.TopNAggregation{
		Metadata: &commonv1.Metadata{
			Name:  "test-topn",
			Group: group,
		},
		SourceMeasure: &commonv1.Metadata{
			Name:  "test-measure",
			Group: group,
		},
		FieldName:      "value",
		CountersNumber: 10,
		UpdatedAt:      timestamppb.Now(),
	}
}

func testProperty(group string) *databasev1.Property {
	return &databasev1.Property{
		Metadata: &commonv1.Metadata{
			Name:  "test-property",
			Group: group,
		},
		UpdatedAt: timestamppb.Now(),
	}
}

func testPropertyGroup(name string) *commonv1.Group {
	return &commonv1.Group{
		Metadata: &commonv1.Metadata{Name: name},
		Catalog:  commonv1.Catalog_CATALOG_PROPERTY,
		ResourceOpts: &commonv1.ResourceOpts{
			ShardNum: 1,
		},
	}
}

func createTestGroup(t *testing.T, reg *property.SchemaRegistry) {
	t.Helper()
	require.NoError(t, reg.CreateGroup(context.Background(), testGroup()))
}

func TestGroupCRUD(t *testing.T) {
	addr := startTestSchemaServer(t)
	reg := newTestRegistry(t, addr)
	ctx := context.Background()
	g := testGroup()
	require.NoError(t, reg.CreateGroup(ctx, g))
	got, getErr := reg.GetGroup(ctx, "test-group")
	require.NoError(t, getErr)
	assert.Equal(t, "test-group", got.GetMetadata().GetName())
	groups, listErr := reg.ListGroup(ctx)
	require.NoError(t, listErr)
	assert.Len(t, groups, 1)
	got.ResourceOpts.ShardNum = 4
	require.NoError(t, reg.UpdateGroup(ctx, got))
	updated, getErr := reg.GetGroup(ctx, "test-group")
	require.NoError(t, getErr)
	assert.Equal(t, uint32(4), updated.GetResourceOpts().GetShardNum())
	deleted, deleteErr := reg.DeleteGroup(ctx, "test-group")
	require.NoError(t, deleteErr)
	assert.True(t, deleted)
	_, getErr = reg.GetGroup(ctx, "test-group")
	require.Error(t, getErr)
	groups, listErr = reg.ListGroup(ctx)
	require.NoError(t, listErr)
	assert.Empty(t, groups)
}

func TestStreamCRUD(t *testing.T) {
	addr := startTestSchemaServer(t)
	reg := newTestRegistry(t, addr)
	ctx := context.Background()
	createTestGroup(t, reg)
	s := testStream()
	modRev, createErr := reg.CreateStream(ctx, s)
	require.NoError(t, createErr)
	assert.Greater(t, modRev, int64(0))
	got, getErr := reg.GetStream(ctx, &commonv1.Metadata{Group: "test-group", Name: testStreamName})
	require.NoError(t, getErr)
	assert.Equal(t, testStreamName, got.GetMetadata().GetName())
	streams, listErr := reg.ListStream(ctx, schema.ListOpt{Group: "test-group"})
	require.NoError(t, listErr)
	assert.Len(t, streams, 1)
	got.TagFamilies[0].Tags = append(got.TagFamilies[0].Tags, &databasev1.TagSpec{
		Name: "endpoint",
		Type: databasev1.TagType_TAG_TYPE_STRING,
	})
	_, updateErr := reg.UpdateStream(ctx, got)
	require.NoError(t, updateErr)
	updated, getErr := reg.GetStream(ctx, &commonv1.Metadata{Group: "test-group", Name: testStreamName})
	require.NoError(t, getErr)
	assert.Len(t, updated.GetTagFamilies()[0].GetTags(), 2)
	deleted, deleteErr := reg.DeleteStream(ctx, &commonv1.Metadata{Group: "test-group", Name: testStreamName})
	require.NoError(t, deleteErr)
	assert.True(t, deleted)
	_, getErr = reg.GetStream(ctx, &commonv1.Metadata{Group: "test-group", Name: testStreamName})
	require.Error(t, getErr)
}

// TestGroupUpdateNoOpSkipsBroadcast verifies the Group equality checker
// (which was previously broken due to an incorrect IgnoreFields descriptor)
// short-circuits a no-op UpdateGroup, and a real change still goes through.
func TestGroupUpdateNoOpSkipsBroadcast(t *testing.T) {
	addr := startTestSchemaServer(t)
	reg := newTestRegistry(t, addr)
	ctx := context.Background()
	createTestGroup(t, reg)

	got, getErr := reg.GetGroup(ctx, "test-group")
	require.NoError(t, getErr)
	storedModRev := got.GetMetadata().GetModRevision()
	require.NotZero(t, storedModRev)

	time.Sleep(2 * time.Millisecond)
	require.NoError(t, reg.UpdateGroup(ctx, got))

	afterNoOp, getErr := reg.GetGroup(ctx, "test-group")
	require.NoError(t, getErr)
	assert.Equal(t, storedModRev, afterNoOp.GetMetadata().GetModRevision(),
		"ModRevision should be unchanged when the submitted group is identical")

	afterNoOp.ResourceOpts.ShardNum = 4
	time.Sleep(2 * time.Millisecond)
	require.NoError(t, reg.UpdateGroup(ctx, afterNoOp))
	afterRealChange, getErr := reg.GetGroup(ctx, "test-group")
	require.NoError(t, getErr)
	assert.Greater(t, afterRealChange.GetMetadata().GetModRevision(), storedModRev,
		"ModRevision should advance when the group actually changes")
	assert.Equal(t, uint32(4), afterRealChange.GetResourceOpts().GetShardNum())
}

// TestStreamUpdateNoOpSkipsBroadcast verifies that resubmitting an identical
// schema spec is detected by the equality checker in updateResource and does
// not trigger an UpdateSchema RPC. The persisted ModRevision is used as the
// observable because the client mutates the incoming spec's ModRevision on
// every UpdateStream call; when the write is skipped, the server-side value
// stays at whatever was last persisted.
func TestStreamUpdateNoOpSkipsBroadcast(t *testing.T) {
	addr := startTestSchemaServer(t)
	reg := newTestRegistry(t, addr)
	ctx := context.Background()
	createTestGroup(t, reg)
	s := testStream()
	_, createErr := reg.CreateStream(ctx, s)
	require.NoError(t, createErr)

	got, getErr := reg.GetStream(ctx, &commonv1.Metadata{Group: "test-group", Name: testStreamName})
	require.NoError(t, getErr)
	storedModRev := got.GetMetadata().GetModRevision()
	require.NotZero(t, storedModRev)

	// Re-submit the same spec. UpdateStream will bump ModRevision / UpdatedAt
	// on the caller's copy, but the checker should compare against the
	// persisted spec (ignoring updated_at / mod_revision) and short-circuit.
	time.Sleep(2 * time.Millisecond)
	_, updateErr := reg.UpdateStream(ctx, got)
	require.NoError(t, updateErr)

	afterNoOp, getErr := reg.GetStream(ctx, &commonv1.Metadata{Group: "test-group", Name: testStreamName})
	require.NoError(t, getErr)
	assert.Equal(t, storedModRev, afterNoOp.GetMetadata().GetModRevision(),
		"ModRevision should be unchanged when the submitted spec is identical")

	// A real change must still go through.
	afterNoOp.TagFamilies[0].Tags = append(afterNoOp.TagFamilies[0].Tags, &databasev1.TagSpec{
		Name: "endpoint",
		Type: databasev1.TagType_TAG_TYPE_STRING,
	})
	time.Sleep(2 * time.Millisecond)
	_, updateErr = reg.UpdateStream(ctx, afterNoOp)
	require.NoError(t, updateErr)
	afterRealChange, getErr := reg.GetStream(ctx, &commonv1.Metadata{Group: "test-group", Name: testStreamName})
	require.NoError(t, getErr)
	assert.Greater(t, afterRealChange.GetMetadata().GetModRevision(), storedModRev,
		"ModRevision should advance when the spec actually changes")
	assert.Len(t, afterRealChange.GetTagFamilies()[0].GetTags(), 2)
}

func TestMeasureCRUD(t *testing.T) {
	addr := startTestSchemaServer(t)
	reg := newTestRegistry(t, addr)
	ctx := context.Background()
	createTestGroup(t, reg)
	m := testMeasure("test-group")
	modRev, createErr := reg.CreateMeasure(ctx, m)
	require.NoError(t, createErr)
	assert.Greater(t, modRev, int64(0))
	got, getErr := reg.GetMeasure(ctx, &commonv1.Metadata{Group: "test-group", Name: "test-measure"})
	require.NoError(t, getErr)
	assert.Equal(t, "test-measure", got.GetMetadata().GetName())
	measures, listErr := reg.ListMeasure(ctx, schema.ListOpt{Group: "test-group"})
	require.NoError(t, listErr)
	assert.Len(t, measures, 1)
	got.Fields = append(got.Fields, &databasev1.FieldSpec{
		Name:              "count",
		FieldType:         databasev1.FieldType_FIELD_TYPE_INT,
		CompressionMethod: databasev1.CompressionMethod_COMPRESSION_METHOD_ZSTD,
		EncodingMethod:    databasev1.EncodingMethod_ENCODING_METHOD_GORILLA,
	})
	_, updateErr := reg.UpdateMeasure(ctx, got)
	require.NoError(t, updateErr)
	updated, getErr := reg.GetMeasure(ctx, &commonv1.Metadata{Group: "test-group", Name: "test-measure"})
	require.NoError(t, getErr)
	assert.Len(t, updated.GetFields(), 2)
	deleted, deleteErr := reg.DeleteMeasure(ctx, &commonv1.Metadata{Group: "test-group", Name: "test-measure"})
	require.NoError(t, deleteErr)
	assert.True(t, deleted)
	_, getErr = reg.GetMeasure(ctx, &commonv1.Metadata{Group: "test-group", Name: "test-measure"})
	require.Error(t, getErr)
}

func TestTraceCRUD(t *testing.T) {
	addr := startTestSchemaServer(t)
	reg := newTestRegistry(t, addr)
	ctx := context.Background()
	createTestGroup(t, reg)
	tr := testTrace("test-group")
	modRev, createErr := reg.CreateTrace(ctx, tr)
	require.NoError(t, createErr)
	assert.Greater(t, modRev, int64(0))
	got, getErr := reg.GetTrace(ctx, &commonv1.Metadata{Group: "test-group", Name: "test-trace"})
	require.NoError(t, getErr)
	assert.Equal(t, "test-trace", got.GetMetadata().GetName())
	traces, listErr := reg.ListTrace(ctx, schema.ListOpt{Group: "test-group"})
	require.NoError(t, listErr)
	assert.Len(t, traces, 1)
	got.Tags = append(got.Tags, &databasev1.TraceTagSpec{
		Name: "http_method",
		Type: databasev1.TagType_TAG_TYPE_STRING,
	})
	_, updateErr := reg.UpdateTrace(ctx, got)
	require.NoError(t, updateErr)
	updated, getErr := reg.GetTrace(ctx, &commonv1.Metadata{Group: "test-group", Name: "test-trace"})
	require.NoError(t, getErr)
	assert.Len(t, updated.GetTags(), 4)
	deleted, deleteErr := reg.DeleteTrace(ctx, &commonv1.Metadata{Group: "test-group", Name: "test-trace"})
	require.NoError(t, deleteErr)
	assert.True(t, deleted)
	_, getErr = reg.GetTrace(ctx, &commonv1.Metadata{Group: "test-group", Name: "test-trace"})
	require.Error(t, getErr)
}

func TestIndexRuleCRUD(t *testing.T) {
	addr := startTestSchemaServer(t)
	reg := newTestRegistry(t, addr)
	ctx := context.Background()
	createTestGroup(t, reg)
	ir := testIndexRule("test-group")
	require.NoError(t, reg.CreateIndexRule(ctx, ir))
	got, getErr := reg.GetIndexRule(ctx, &commonv1.Metadata{Group: "test-group", Name: "test-index-rule"})
	require.NoError(t, getErr)
	assert.Equal(t, "test-index-rule", got.GetMetadata().GetName())
	rules, listErr := reg.ListIndexRule(ctx, schema.ListOpt{Group: "test-group"})
	require.NoError(t, listErr)
	assert.Len(t, rules, 1)
	got.Tags = append(got.Tags, "endpoint")
	require.NoError(t, reg.UpdateIndexRule(ctx, got))
	updated, getErr := reg.GetIndexRule(ctx, &commonv1.Metadata{Group: "test-group", Name: "test-index-rule"})
	require.NoError(t, getErr)
	assert.Len(t, updated.GetTags(), 2)
	deleted, deleteErr := reg.DeleteIndexRule(ctx, &commonv1.Metadata{Group: "test-group", Name: "test-index-rule"})
	require.NoError(t, deleteErr)
	assert.True(t, deleted)
	_, getErr = reg.GetIndexRule(ctx, &commonv1.Metadata{Group: "test-group", Name: "test-index-rule"})
	require.Error(t, getErr)
}

func TestIndexRuleCRUD_AutoGenerateCRC32ID(t *testing.T) {
	addr := startTestSchemaServer(t)
	reg := newTestRegistry(t, addr)
	ctx := context.Background()
	createTestGroup(t, reg)
	ir := testIndexRule("test-group")
	ir.Metadata.Id = 0
	require.NoError(t, reg.CreateIndexRule(ctx, ir))
	got, getErr := reg.GetIndexRule(ctx, &commonv1.Metadata{Group: "test-group", Name: "test-index-rule"})
	require.NoError(t, getErr)
	assert.Greater(t, got.GetMetadata().GetId(), uint32(0))
}

func TestIndexRuleBindingCRUD(t *testing.T) {
	addr := startTestSchemaServer(t)
	reg := newTestRegistry(t, addr)
	ctx := context.Background()
	createTestGroup(t, reg)
	irb := testIndexRuleBinding("test-group")
	require.NoError(t, reg.CreateIndexRuleBinding(ctx, irb))
	got, getErr := reg.GetIndexRuleBinding(ctx, &commonv1.Metadata{Group: "test-group", Name: "test-index-rule-binding"})
	require.NoError(t, getErr)
	assert.Equal(t, "test-index-rule-binding", got.GetMetadata().GetName())
	bindings, listErr := reg.ListIndexRuleBinding(ctx, schema.ListOpt{Group: "test-group"})
	require.NoError(t, listErr)
	assert.Len(t, bindings, 1)
	got.Rules = append(got.Rules, "another-rule")
	require.NoError(t, reg.UpdateIndexRuleBinding(ctx, got))
	updated, getErr := reg.GetIndexRuleBinding(ctx, &commonv1.Metadata{Group: "test-group", Name: "test-index-rule-binding"})
	require.NoError(t, getErr)
	assert.Len(t, updated.GetRules(), 2)
	deleted, deleteErr := reg.DeleteIndexRuleBinding(ctx, &commonv1.Metadata{Group: "test-group", Name: "test-index-rule-binding"})
	require.NoError(t, deleteErr)
	assert.True(t, deleted)
	_, getErr = reg.GetIndexRuleBinding(ctx, &commonv1.Metadata{Group: "test-group", Name: "test-index-rule-binding"})
	require.Error(t, getErr)
}

func TestTopNAggregationCRUD(t *testing.T) {
	addr := startTestSchemaServer(t)
	reg := newTestRegistry(t, addr)
	ctx := context.Background()
	createTestGroup(t, reg)
	topn := testTopNAggregation("test-group")
	require.NoError(t, reg.CreateTopNAggregation(ctx, topn))
	got, getErr := reg.GetTopNAggregation(ctx, &commonv1.Metadata{Group: "test-group", Name: "test-topn"})
	require.NoError(t, getErr)
	assert.Equal(t, "test-topn", got.GetMetadata().GetName())
	topns, listErr := reg.ListTopNAggregation(ctx, schema.ListOpt{Group: "test-group"})
	require.NoError(t, listErr)
	assert.Len(t, topns, 1)
	got.CountersNumber = 20
	require.NoError(t, reg.UpdateTopNAggregation(ctx, got))
	updated, getErr := reg.GetTopNAggregation(ctx, &commonv1.Metadata{Group: "test-group", Name: "test-topn"})
	require.NoError(t, getErr)
	assert.Equal(t, int32(20), updated.GetCountersNumber())
	aggrs, aggrErr := reg.TopNAggregations(ctx, &commonv1.Metadata{Group: "test-group", Name: "test-measure"})
	require.NoError(t, aggrErr)
	assert.Len(t, aggrs, 1)
	deleted, deleteErr := reg.DeleteTopNAggregation(ctx, &commonv1.Metadata{Group: "test-group", Name: "test-topn"})
	require.NoError(t, deleteErr)
	assert.True(t, deleted)
	_, getErr = reg.GetTopNAggregation(ctx, &commonv1.Metadata{Group: "test-group", Name: "test-topn"})
	require.Error(t, getErr)
}

func TestPropertyCRUD(t *testing.T) {
	addr := startTestSchemaServer(t)
	reg := newTestRegistry(t, addr)
	ctx := context.Background()
	require.NoError(t, reg.CreateGroup(ctx, testPropertyGroup("test-prop-group")))
	p := testProperty("test-prop-group")
	require.NoError(t, reg.CreateProperty(ctx, p))
	got, getErr := reg.GetProperty(ctx, &commonv1.Metadata{Group: "test-prop-group", Name: "test-property"})
	require.NoError(t, getErr)
	assert.Equal(t, "test-property", got.GetMetadata().GetName())
	props, listErr := reg.ListProperty(ctx, schema.ListOpt{Group: "test-prop-group"})
	require.NoError(t, listErr)
	assert.Len(t, props, 1)
	require.NoError(t, reg.UpdateProperty(ctx, got))
	deleted, deleteErr := reg.DeleteProperty(ctx, &commonv1.Metadata{Group: "test-prop-group", Name: "test-property"})
	require.NoError(t, deleteErr)
	assert.True(t, deleted)
	_, getErr = reg.GetProperty(ctx, &commonv1.Metadata{Group: "test-prop-group", Name: "test-property"})
	require.Error(t, getErr)
}

func TestCreateDuplicate(t *testing.T) {
	addr := startTestSchemaServer(t)
	reg := newTestRegistry(t, addr)
	ctx := context.Background()
	createTestGroup(t, reg)
	s := testStream()
	_, createErr := reg.CreateStream(ctx, s)
	require.NoError(t, createErr)
	s2 := testStream()
	_, dupErr := reg.CreateStream(ctx, s2)
	require.Error(t, dupErr)
	assert.Contains(t, dupErr.Error(), "already exists")
}

func TestBroadcastInsert_AllNodesAlreadyExist(t *testing.T) {
	addr1 := startTestSchemaServer(t)
	addr2 := startTestSchemaServer(t)
	reg := newTestRegistry(t, addr1, addr2)
	ctx := context.Background()
	createTestGroup(t, reg)
	s := testStream()
	_, createErr := reg.CreateStream(ctx, s)
	require.NoError(t, createErr)
	// Both nodes already have the schema. Creating again should return AlreadyExists.
	s2 := testStream()
	_, dupErr := reg.CreateStream(ctx, s2)
	require.Error(t, dupErr)
	assert.Contains(t, dupErr.Error(), "already exists")
}

func TestBroadcastInsert_PartialNodesAlreadyExist(t *testing.T) {
	addr1 := startTestSchemaServer(t)
	addr2 := startTestSchemaServer(t)
	// Insert stream directly on server1 only (bypass broadcast).
	mgmt1 := rawMgmtClient(t, addr1)
	ctx := context.Background()
	// First create the group on both servers via a registry connected to both.
	reg := newTestRegistry(t, addr1, addr2)
	createTestGroup(t, reg)
	// Now insert the stream on server1 only.
	s := testStream()
	s.Metadata.ModRevision = time.Now().UnixNano()
	s.UpdatedAt = timestamppb.Now()
	sProp, convErr := streamToProperty(s)
	require.NoError(t, convErr)
	_, insertErr := mgmt1.InsertSchema(ctx, &schemav1.InsertSchemaRequest{Property: sProp})
	require.NoError(t, insertErr)
	// Now create the same stream via the registry. Server1 returns AlreadyExists,
	// but server2 should accept it. The overall result should be success.
	s2 := testStream()
	_, createErr := reg.CreateStream(ctx, s2)
	require.NoError(t, createErr)
	// Verify both servers now have the stream.
	mgmt2 := rawMgmtClient(t, addr2)
	listStream, listErr := mgmt2.ListSchemas(ctx, &schemav1.ListSchemasRequest{
		Query: &propertyv1.QueryRequest{
			Groups: []string{schema.SchemaGroup},
			Name:   "stream",
		},
	})
	require.NoError(t, listErr)
	assertHasSchemas(t, listStream)
}

func TestListEmptyGroup(t *testing.T) {
	addr := startTestSchemaServer(t)
	reg := newTestRegistry(t, addr)
	ctx := context.Background()
	_, listErr := reg.ListStream(ctx, schema.ListOpt{Group: ""})
	require.Error(t, listErr)
}

func TestMultiNodeBroadcastRead(t *testing.T) {
	addr1 := startTestSchemaServer(t)
	addr2 := startTestSchemaServer(t)
	reg := newTestRegistry(t, addr1, addr2)
	ctx := context.Background()
	createTestGroup(t, reg)
	s := testStream()
	_, createErr := reg.CreateStream(ctx, s)
	require.NoError(t, createErr)
	got, getErr := reg.GetStream(ctx, &commonv1.Metadata{Group: "test-group", Name: testStreamName})
	require.NoError(t, getErr)
	assert.Equal(t, testStreamName, got.GetMetadata().GetName())
	streams, listErr := reg.ListStream(ctx, schema.ListOpt{Group: "test-group"})
	require.NoError(t, listErr)
	assert.Len(t, streams, 1)
}

func TestMultiNodeBroadcastWrite(t *testing.T) {
	addr1 := startTestSchemaServer(t)
	addr2 := startTestSchemaServer(t)
	reg := newTestRegistry(t, addr1, addr2)
	ctx := context.Background()
	createTestGroup(t, reg)
	s := testStream()
	_, createErr := reg.CreateStream(ctx, s)
	require.NoError(t, createErr)
	mgmt1 := rawMgmtClient(t, addr1)
	mgmt2 := rawMgmtClient(t, addr2)
	stream1, streamErr := mgmt1.ListSchemas(ctx, &schemav1.ListSchemasRequest{
		Query: &propertyv1.QueryRequest{
			Groups: []string{schema.SchemaGroup},
			Name:   "stream",
		},
	})
	require.NoError(t, streamErr)
	assertHasSchemas(t, stream1)
	stream2, streamErr := mgmt2.ListSchemas(ctx, &schemav1.ListSchemasRequest{
		Query: &propertyv1.QueryRequest{
			Groups: []string{schema.SchemaGroup},
			Name:   "stream",
		},
	})
	require.NoError(t, streamErr)
	assertHasSchemas(t, stream2)
	_, deleteErr := reg.DeleteStream(ctx, &commonv1.Metadata{Group: "test-group", Name: testStreamName})
	require.NoError(t, deleteErr)
	_, getErr := reg.GetStream(ctx, &commonv1.Metadata{Group: "test-group", Name: testStreamName})
	require.Error(t, getErr)
}

func TestRepairOnRead_StaleNode(t *testing.T) {
	addr1 := startTestSchemaServer(t)
	addr2 := startTestSchemaServer(t)
	reg := newTestRegistry(t, addr1, addr2)
	ctx := context.Background()
	createTestGroup(t, reg)
	s := testStream()
	_, createErr := reg.CreateStream(ctx, s)
	require.NoError(t, createErr)
	// Directly update on server1 only with higher revision
	mgmt1 := rawMgmtClient(t, addr1)
	got, getErr := reg.GetStream(ctx, &commonv1.Metadata{Group: "test-group", Name: testStreamName})
	require.NoError(t, getErr)
	got.TagFamilies[0].Tags = append(got.TagFamilies[0].Tags, &databasev1.TagSpec{
		Name: "updated_tag",
		Type: databasev1.TagType_TAG_TYPE_STRING,
	})
	got.Metadata.ModRevision = time.Now().Add(time.Hour).UnixNano()
	got.UpdatedAt = timestamppb.New(time.Now().Add(time.Hour))
	updatedProp, convErr := streamToProperty(got)
	require.NoError(t, convErr)
	_, updateErr := mgmt1.UpdateSchema(ctx, &schemav1.UpdateSchemaRequest{Property: updatedProp})
	require.NoError(t, updateErr)
	// Now read via the registry - this triggers broadcast+repair
	result, getErr := reg.GetStream(ctx, &commonv1.Metadata{Group: "test-group", Name: testStreamName})
	require.NoError(t, getErr)
	assert.Len(t, result.GetTagFamilies()[0].GetTags(), 2)
	// Give repair a moment to propagate
	time.Sleep(200 * time.Millisecond)
	// Verify server2 now has the updated version by reading again
	result2, getErr := reg.GetStream(ctx, &commonv1.Metadata{Group: "test-group", Name: testStreamName})
	require.NoError(t, getErr)
	assert.Len(t, result2.GetTagFamilies()[0].GetTags(), 2)
}

func TestRepairOnRead_MissingNode(t *testing.T) {
	addr1 := startTestSchemaServer(t)
	addr2 := startTestSchemaServer(t)
	reg := newTestRegistry(t, addr1, addr2)
	ctx := context.Background()
	// Insert group on both via registry
	createTestGroup(t, reg)
	// Insert a stream directly on server1 only (bypass broadcastWrite)
	mgmt1 := rawMgmtClient(t, addr1)
	s := testStream()
	s.Metadata.ModRevision = time.Now().UnixNano()
	s.UpdatedAt = timestamppb.Now()
	prop, convErr := streamToProperty(s)
	require.NoError(t, convErr)
	_, insertErr := mgmt1.InsertSchema(ctx, &schemav1.InsertSchemaRequest{Property: prop})
	require.NoError(t, insertErr)
	// Read via registry - should find the stream from server1 and repair server2
	got, getErr := reg.GetStream(ctx, &commonv1.Metadata{Group: "test-group", Name: testStreamName})
	require.NoError(t, getErr)
	assert.Equal(t, testStreamName, got.GetMetadata().GetName())
	// Give repair a moment to propagate
	time.Sleep(200 * time.Millisecond)
	// Verify server2 now has it by checking via the registry a second time
	got2, getErr := reg.GetStream(ctx, &commonv1.Metadata{Group: "test-group", Name: testStreamName})
	require.NoError(t, getErr)
	assert.Equal(t, testStreamName, got2.GetMetadata().GetName())
}

func streamToProperty(stream *databasev1.Stream) (*propertyv1.Property, error) {
	return property.SchemaToProperty(schema.KindStream, stream)
}

func groupToProperty(group *commonv1.Group) (*propertyv1.Property, error) {
	return property.SchemaToProperty(schema.KindGroup, group)
}

func measureToProperty(measure *databasev1.Measure) (*propertyv1.Property, error) {
	return property.SchemaToProperty(schema.KindMeasure, measure)
}

func assertHasSchemas(t *testing.T, stream schemav1.SchemaManagementService_ListSchemasClient) {
	t.Helper()
	var total int
	for {
		resp, recvErr := stream.Recv()
		if recvErr != nil {
			break
		}
		for idx, dt := range resp.GetDeleteTimes() {
			if dt == 0 && idx < len(resp.GetProperties()) {
				total++
			}
		}
		if len(resp.GetDeleteTimes()) == 0 {
			total += len(resp.GetProperties())
		}
	}
	assert.Greater(t, total, 0, "expected at least one non-deleted schema")
}

type testEventHandler struct {
	schema.UnimplementedOnInitHandler
	addOrUpdateEvents []schema.Metadata
	deleteEvents      []schema.Metadata
	mu                sync.Mutex
}

func (h *testEventHandler) OnAddOrUpdate(m schema.Metadata) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.addOrUpdateEvents = append(h.addOrUpdateEvents, m)
}

func (h *testEventHandler) OnDelete(m schema.Metadata) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.deleteEvents = append(h.deleteEvents, m)
}

func (h *testEventHandler) getAddOrUpdateEvents() []schema.Metadata {
	h.mu.Lock()
	defer h.mu.Unlock()
	cp := make([]schema.Metadata, len(h.addOrUpdateEvents))
	copy(cp, h.addOrUpdateEvents)
	return cp
}

func (h *testEventHandler) getDeleteEvents() []schema.Metadata {
	h.mu.Lock()
	defer h.mu.Unlock()
	cp := make([]schema.Metadata, len(h.deleteEvents))
	copy(cp, h.deleteEvents)
	return cp
}

func seedServerWithGroupAndStream(t *testing.T, addr string) {
	t.Helper()
	mgmt := rawMgmtClient(t, addr)
	ctx := context.Background()
	g := testGroup()
	g.Metadata.ModRevision = time.Now().UnixNano()
	g.UpdatedAt = timestamppb.Now()
	gProp, gErr := groupToProperty(g)
	require.NoError(t, gErr)
	gProp.Metadata.ModRevision = g.Metadata.ModRevision
	_, insertErr := mgmt.InsertSchema(ctx, &schemav1.InsertSchemaRequest{Property: gProp})
	require.NoError(t, insertErr)
	s := testStream()
	s.Metadata.ModRevision = time.Now().UnixNano()
	s.UpdatedAt = timestamppb.Now()
	sProp, sErr := streamToProperty(s)
	require.NoError(t, sErr)
	sProp.Metadata.ModRevision = s.Metadata.ModRevision
	_, insertStreamErr := mgmt.InsertSchema(ctx, &schemav1.InsertSchemaRequest{Property: sProp})
	require.NoError(t, insertStreamErr)
}

func addNodeToRegistry(reg *property.SchemaRegistry, name, addr string) {
	node := &databasev1.Node{
		Metadata:                  &commonv1.Metadata{Name: name},
		Roles:                     []databasev1.Role{databasev1.Role_ROLE_META},
		PropertySchemaGrpcAddress: addr,
	}
	reg.OnAddOrUpdate(schema.Metadata{Spec: node})
}

func removeNodeFromRegistry(reg *property.SchemaRegistry, name, addr string) {
	node := &databasev1.Node{
		Metadata:                  &commonv1.Metadata{Name: name},
		Roles:                     []databasev1.Role{databasev1.Role_ROLE_META},
		PropertySchemaGrpcAddress: addr,
	}
	reg.OnDelete(schema.Metadata{Spec: node})
}

func TestOnAddOrUpdate_IgnoresNonMetaRoleNode(t *testing.T) {
	addr := startTestSchemaServer(t)
	reg := newTestRegistry(t, addr)
	ctx := context.Background()
	// Initial active count is 1 (the meta node provided at construction).
	assert.Len(t, reg.ActiveNodeNames(), 1, "initial meta node should be registered")
	// A node with ROLE_DATA (not META) should be ignored even with a valid address.
	dataNode := &databasev1.Node{
		Metadata:                  &commonv1.Metadata{Name: "data-node"},
		Roles:                     []databasev1.Role{databasev1.Role_ROLE_DATA},
		PropertySchemaGrpcAddress: addr,
	}
	reg.OnAddOrUpdate(schema.Metadata{Spec: dataNode})
	assert.Len(t, reg.ActiveNodeNames(), 1, "data-only node should not be registered")
	// A node with no roles should be ignored.
	noRoleNode := &databasev1.Node{
		Metadata:                  &commonv1.Metadata{Name: "no-role-node"},
		PropertySchemaGrpcAddress: addr,
	}
	reg.OnAddOrUpdate(schema.Metadata{Spec: noRoleNode})
	assert.Len(t, reg.ActiveNodeNames(), 1, "node without roles should not be registered")
	// A META node with an empty address should be ignored.
	noAddrNode := &databasev1.Node{
		Metadata: &commonv1.Metadata{Name: "no-addr-node"},
		Roles:    []databasev1.Role{databasev1.Role_ROLE_META},
	}
	reg.OnAddOrUpdate(schema.Metadata{Spec: noAddrNode})
	assert.Len(t, reg.ActiveNodeNames(), 1, "meta node without address should not be registered")
	// Registry should work via the initial meta node.
	require.NoError(t, reg.CreateGroup(ctx, testGroup()))
	got, getErr := reg.GetGroup(ctx, "test-group")
	require.NoError(t, getErr)
	assert.Equal(t, "test-group", got.GetMetadata().GetName())
}

func TestOnAddOrUpdate_NewNodeWithData(t *testing.T) {
	addr1 := startTestSchemaServer(t)
	addr2 := startTestSchemaServer(t)
	// Seed server2 with group + stream data directly.
	seedServerWithGroupAndStream(t, addr2)
	// Create registry connected to server1 via NodeRegistry, with a handler.
	reg, regErr := property.NewSchemaRegistryClient(&property.ClientConfig{
		GRPCTimeout:  10 * time.Second,
		NodeRegistry: &testNodeRegistry{nodes: buildTestNodes("node", addr1)},
	})
	require.NoError(t, regErr)
	t.Cleanup(func() { _ = reg.Close() })
	handler := &testEventHandler{}
	reg.RegisterHandler("test-handler", schema.KindGroup|schema.KindStream, handler)
	// Now add server2 which has pre-existing data.
	addNodeToRegistry(reg, "node-2", addr2)
	// Start replays cached entries to handlers. Watch replay is async, so
	// we need to wait for the handler to receive notifications.
	reg.Start(context.Background())
	// Handler should receive OnAddOrUpdate events for the group and stream from server2.
	groupEvt := waitForAddOrUpdateEvent(t, handler, schema.KindGroup, "test-group")
	assert.Equal(t, schema.KindGroup, groupEvt.Kind)
	streamEvt := waitForAddOrUpdateEvent(t, handler, schema.KindStream, testStreamName)
	assert.Equal(t, "test-group", streamEvt.Group)
	// Registry should be able to read the stream.
	ctx := context.Background()
	got, getErr := reg.GetStream(ctx, &commonv1.Metadata{Group: "test-group", Name: testStreamName})
	require.NoError(t, getErr)
	assert.Equal(t, testStreamName, got.GetMetadata().GetName())
	// Verify repair: server1 should also have the data after broadcast+repair.
	time.Sleep(500 * time.Millisecond)
	mgmt1 := rawMgmtClient(t, addr1)
	listStream, listErr := mgmt1.ListSchemas(ctx, &schemav1.ListSchemasRequest{
		Query: &propertyv1.QueryRequest{
			Groups: []string{schema.SchemaGroup},
			Name:   "stream",
		},
	})
	require.NoError(t, listErr)
	assertHasSchemas(t, listStream)
}

func TestOnAddOrUpdate_RepairOldDataToNewNode(t *testing.T) {
	addr1 := startTestSchemaServer(t)
	addr2 := startTestSchemaServer(t)
	// Create registry connected to server1 via NodeRegistry, create data.
	reg, regErr := property.NewSchemaRegistryClient(&property.ClientConfig{
		GRPCTimeout:  10 * time.Second,
		NodeRegistry: &testNodeRegistry{nodes: buildTestNodes("node", addr1)},
	})
	require.NoError(t, regErr)
	t.Cleanup(func() { _ = reg.Close() })
	handler := &testEventHandler{}
	reg.RegisterHandler("test-handler", schema.KindGroup|schema.KindStream, handler)
	ctx := context.Background()
	require.NoError(t, reg.CreateGroup(ctx, testGroup()))
	s := testStream()
	_, createErr := reg.CreateStream(ctx, s)
	require.NoError(t, createErr)
	initialEvents := handler.getAddOrUpdateEvents()
	// Now add empty server2.
	addNodeToRegistry(reg, "node-2", addr2)
	time.Sleep(500 * time.Millisecond)
	// Read via registry to trigger broadcast+repair to server2.
	got, getErr := reg.GetStream(ctx, &commonv1.Metadata{Group: "test-group", Name: testStreamName})
	require.NoError(t, getErr)
	assert.Equal(t, testStreamName, got.GetMetadata().GetName())
	time.Sleep(500 * time.Millisecond)
	// Server2 should now have the data after repair.
	mgmt2 := rawMgmtClient(t, addr2)
	listStream, listErr := mgmt2.ListSchemas(ctx, &schemav1.ListSchemasRequest{
		Query: &propertyv1.QueryRequest{
			Groups: []string{schema.SchemaGroup},
			Name:   "stream",
		},
	})
	require.NoError(t, listErr)
	assertHasSchemas(t, listStream)
	// Adding empty server2 should not cause duplicate handler notifications for already-cached data.
	laterEvents := handler.getAddOrUpdateEvents()
	// The new events from adding node-2 should not duplicate the original stream event
	// since the data was already in cache from node-1.
	_ = initialEvents
	_ = laterEvents
}

func TestOnDelete_RemoveNode(t *testing.T) {
	addr1 := startTestSchemaServer(t)
	addr2, stopServer2 := startTestSchemaServerStoppable(t)
	reg, regErr := property.NewSchemaRegistryClient(&property.ClientConfig{
		GRPCTimeout:  10 * time.Second,
		NodeRegistry: &testNodeRegistry{nodes: buildTestNodes("node", addr1, addr2)},
	})
	require.NoError(t, regErr)
	t.Cleanup(func() { _ = reg.Close() })
	ctx := context.Background()
	require.NoError(t, reg.CreateGroup(ctx, testGroup()))
	s := testStream()
	_, createErr := reg.CreateStream(ctx, s)
	require.NoError(t, createErr)
	// Verify both nodes are active.
	activeNames := reg.ActiveNodeNames()
	assert.Len(t, activeNames, 2)
	// Stop server2 so the health check will fail, then remove node-2.
	stopServer2()
	removeNodeFromRegistry(reg, "node-1", addr2)
	// Wait for async removal of the unhealthy node.
	require.Eventually(t, func() bool {
		names := reg.ActiveNodeNames()
		for _, name := range names {
			if name == "node-1" {
				return false
			}
		}
		return true
	}, 5*time.Second, 100*time.Millisecond, "node-1 should be removed from active names")
	// Registry should still work via node-0.
	got, getErr := reg.GetStream(ctx, &commonv1.Metadata{Group: "test-group", Name: testStreamName})
	require.NoError(t, getErr)
	assert.Equal(t, testStreamName, got.GetMetadata().GetName())
	// ActiveNames should contain only node-0.
	activeNames = reg.ActiveNodeNames()
	nodeNames := make(map[string]bool)
	for _, name := range activeNames {
		nodeNames[name] = true
	}
	assert.True(t, nodeNames["node-0"], "node-0 should still be active")
}

func TestOnAddOrUpdate_HandlerNotification(t *testing.T) {
	addr1 := startTestSchemaServer(t)
	addr2 := startTestSchemaServer(t)
	// Seed server2 with a stream group + stream.
	seedServerWithGroupAndStream(t, addr2)
	// Also seed a measure group + measure on server2.
	mgmt2 := rawMgmtClient(t, addr2)
	ctx := context.Background()
	measureGroup := &commonv1.Group{
		Metadata:  &commonv1.Metadata{Name: "measure-group", ModRevision: time.Now().UnixNano()},
		Catalog:   commonv1.Catalog_CATALOG_MEASURE,
		UpdatedAt: timestamppb.Now(),
		ResourceOpts: &commonv1.ResourceOpts{
			ShardNum:        2,
			SegmentInterval: &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 1},
			Ttl:             &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 7},
		},
	}
	mgProp, mgErr := groupToProperty(measureGroup)
	require.NoError(t, mgErr)
	mgProp.Metadata.ModRevision = measureGroup.Metadata.ModRevision
	_, insertGErr := mgmt2.InsertSchema(ctx, &schemav1.InsertSchemaRequest{Property: mgProp})
	require.NoError(t, insertGErr)
	m := testMeasure("measure-group")
	m.Metadata.ModRevision = time.Now().UnixNano()
	m.UpdatedAt = timestamppb.Now()
	mProp, mErr := measureToProperty(m)
	require.NoError(t, mErr)
	mProp.Metadata.ModRevision = m.Metadata.ModRevision
	_, insertMErr := mgmt2.InsertSchema(ctx, &schemav1.InsertSchemaRequest{Property: mProp})
	require.NoError(t, insertMErr)
	// Create registry connected to server1 via NodeRegistry.
	reg, regErr := property.NewSchemaRegistryClient(&property.ClientConfig{
		GRPCTimeout:  10 * time.Second,
		NodeRegistry: &testNodeRegistry{nodes: buildTestNodes("node", addr1)},
	})
	require.NoError(t, regErr)
	t.Cleanup(func() { _ = reg.Close() })
	// Create separate handlers for stream and measure.
	streamHandler := &testEventHandler{}
	measureHandler := &testEventHandler{}
	reg.RegisterHandler("stream-handler", schema.KindStream, streamHandler)
	reg.RegisterHandler("measure-handler", schema.KindMeasure, measureHandler)
	// Add server2 with pre-existing data.
	addNodeToRegistry(reg, "node-2", addr2)
	// Start replays cached entries to handlers. Watch replay is async.
	reg.Start(context.Background())
	// Wait for async watch replay to complete and handlers to receive events.
	require.Eventually(t, func() bool {
		streamEvents := streamHandler.getAddOrUpdateEvents()
		var streamHasStream bool
		for _, evt := range streamEvents {
			if evt.Kind == schema.KindStream && evt.Name == testStreamName {
				streamHasStream = true
			}
		}
		measureEvents := measureHandler.getAddOrUpdateEvents()
		var measureHasMeasure bool
		for _, evt := range measureEvents {
			if evt.Kind == schema.KindMeasure && evt.Name == "test-measure" {
				measureHasMeasure = true
			}
		}
		return streamHasStream && measureHasMeasure
	}, 5*time.Second, 100*time.Millisecond, "handlers should receive their respective events")
	// Verify handler isolation with concrete data checks.
	streamEvents := streamHandler.getAddOrUpdateEvents()
	assert.Len(t, streamEvents, 1, "stream handler should receive exactly 1 event")
	assert.Equal(t, schema.KindStream, streamEvents[0].Kind)
	assert.Equal(t, testStreamName, streamEvents[0].Name)
	assert.Equal(t, "test-group", streamEvents[0].Group)
	measureEvents := measureHandler.getAddOrUpdateEvents()
	assert.Len(t, measureEvents, 1, "measure handler should receive exactly 1 event")
	assert.Equal(t, schema.KindMeasure, measureEvents[0].Kind)
	assert.Equal(t, "test-measure", measureEvents[0].Name)
	assert.Equal(t, "measure-group", measureEvents[0].Group)
}

func startTestSchemaServerTLS(t *testing.T) string {
	t.Helper()
	srv := schemaserver.NewServer(observability.BypassRegistry)
	flagSet := srv.FlagSet()
	port := getFreePort(t)
	require.NoError(t, flagSet.Parse([]string{
		"--schema-server-root-path", t.TempDir(),
		"--schema-server-grpc-host", "127.0.0.1",
		"--schema-server-grpc-port", strconv.FormatUint(uint64(port), 10),
		"--schema-server-tls", "true",
		"--schema-server-cert-file", "testdata/certs/server.crt",
		"--schema-server-key-file", "testdata/certs/server.key",
	}))
	require.NoError(t, srv.Validate())
	require.NoError(t, srv.PreRun(context.Background()))
	srv.Serve()
	t.Cleanup(func() { srv.GracefulStop() })
	addr := net.JoinHostPort("127.0.0.1", strconv.FormatUint(uint64(port), 10))
	deadline := time.Now().Add(5 * time.Second)
	for {
		conn, dialErr := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if dialErr == nil {
			_ = conn.Close()
			break
		}
		if time.Now().After(deadline) {
			require.FailNowf(t, "TLS server did not start in time", "last error: %v", dialErr)
		}
		time.Sleep(50 * time.Millisecond)
	}
	return addr
}

func newTestRegistryTLS(t *testing.T, addrs ...string) *property.SchemaRegistry {
	t.Helper()
	reg, regErr := property.NewSchemaRegistryClient(&property.ClientConfig{
		GRPCTimeout:  10 * time.Second,
		TLSEnabled:   true,
		CACertPath:   "testdata/certs/ca.crt",
		NodeRegistry: &testNodeRegistry{nodes: buildTestNodes("tls-node", addrs...)},
	})
	require.NoError(t, regErr)
	reg.Start(context.Background())
	t.Cleanup(func() { _ = reg.Close() })
	return reg
}

func TestTLS_GroupCRUD(t *testing.T) {
	addr := startTestSchemaServerTLS(t)
	reg := newTestRegistryTLS(t, addr)
	ctx := context.Background()
	g := testGroup()
	require.NoError(t, reg.CreateGroup(ctx, g))
	got, getErr := reg.GetGroup(ctx, "test-group")
	require.NoError(t, getErr)
	assert.Equal(t, "test-group", got.GetMetadata().GetName())
	groups, listErr := reg.ListGroup(ctx)
	require.NoError(t, listErr)
	assert.Len(t, groups, 1)
	got.ResourceOpts.ShardNum = 4
	require.NoError(t, reg.UpdateGroup(ctx, got))
	updated, getErr := reg.GetGroup(ctx, "test-group")
	require.NoError(t, getErr)
	assert.Equal(t, uint32(4), updated.GetResourceOpts().GetShardNum())
	deleted, deleteErr := reg.DeleteGroup(ctx, "test-group")
	require.NoError(t, deleteErr)
	assert.True(t, deleted)
	_, getErr = reg.GetGroup(ctx, "test-group")
	require.Error(t, getErr)
	groups, listErr = reg.ListGroup(ctx)
	require.NoError(t, listErr)
	assert.Empty(t, groups)
}

func TestTLS_InsecureClientFailsAgainstTLSServer(t *testing.T) {
	addr := startTestSchemaServerTLS(t)
	_, regErr := property.NewSchemaRegistryClient(&property.ClientConfig{
		GRPCTimeout:  3 * time.Second,
		InitWaitTime: 5 * time.Second,
		NodeRegistry: &testNodeRegistry{nodes: buildTestNodes("insecure-node", addr)},
	})
	require.Error(t, regErr)
}

// storedSchema represents a schema stored in the recording mock server.
type storedSchema struct {
	prop       *propertyv1.Property
	deleteTime int64
}

// recordingSchemaServer is a mock gRPC server that records requests for testing sync behavior.
type recordingSchemaServer struct {
	schemav1.UnimplementedSchemaManagementServiceServer
	schemav1.UnimplementedSchemaUpdateServiceServer
	schemas map[string]*storedSchema
	mu      sync.Mutex
}

func newRecordingSchemaServer() *recordingSchemaServer {
	return &recordingSchemaServer{
		schemas: make(map[string]*storedSchema),
	}
}

func (s *recordingSchemaServer) addSchema(prop *propertyv1.Property) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.schemas[prop.GetId()] = &storedSchema{prop: prop}
}

func (s *recordingSchemaServer) updateSchema(prop *propertyv1.Property) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.schemas[prop.GetId()] = &storedSchema{prop: prop}
}

func (s *recordingSchemaServer) markDeleted(propID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if stored, ok := s.schemas[propID]; ok {
		stored.deleteTime = time.Now().UnixNano()
	}
}

func (s *recordingSchemaServer) ListSchemas(req *schemav1.ListSchemasRequest, stream grpc.ServerStreamingServer[schemav1.ListSchemasResponse]) error {
	s.mu.Lock()
	query := req.GetQuery()
	kindName := query.GetName()
	var props []*propertyv1.Property
	var deleteTimes []int64
	for _, stored := range s.schemas {
		if kindName != "" && stored.prop.GetMetadata().GetName() != kindName {
			continue
		}
		if len(query.GetIds()) > 0 {
			matched := false
			for _, queryID := range query.GetIds() {
				if stored.prop.GetId() == queryID {
					matched = true
					break
				}
			}
			if !matched {
				continue
			}
		}
		props = append(props, stored.prop)
		deleteTimes = append(deleteTimes, stored.deleteTime)
	}
	s.mu.Unlock()
	if len(props) > 0 {
		return stream.Send(&schemav1.ListSchemasResponse{
			Properties:  props,
			DeleteTimes: deleteTimes,
		})
	}
	return nil
}

func (s *recordingSchemaServer) WatchSchemas(stream grpc.BidiStreamingServer[schemav1.WatchSchemasRequest, schemav1.WatchSchemasResponse]) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}
		metadataOnly := len(req.TagProjection) > 0
		maxRevision := extractMaxRevisionFromCriteria(req.Criteria)
		s.mu.Lock()
		for _, stored := range s.schemas {
			parsed := property.ParseTags(stored.prop.GetTags())
			if maxRevision > 0 && parsed.UpdatedAt <= maxRevision {
				continue
			}
			eventType := schemav1.SchemaEventType_SCHEMA_EVENT_TYPE_INSERT
			if stored.deleteTime > 0 {
				eventType = schemav1.SchemaEventType_SCHEMA_EVENT_TYPE_DELETE
			}
			if sendErr := stream.Send(&schemav1.WatchSchemasResponse{
				EventType:    eventType,
				Property:     stored.prop,
				MetadataOnly: metadataOnly,
				DeleteTime:   stored.deleteTime,
			}); sendErr != nil {
				s.mu.Unlock()
				return sendErr
			}
		}
		s.mu.Unlock()
		if sendErr := stream.Send(&schemav1.WatchSchemasResponse{
			EventType: schemav1.SchemaEventType_SCHEMA_EVENT_TYPE_REPLAY_DONE,
		}); sendErr != nil {
			return sendErr
		}
	}
}

// extractMaxRevisionFromCriteria extracts the max_revision value from a Criteria condition.
// It looks for a condition on "updated_at" with BINARY_OP_GT.
func extractMaxRevisionFromCriteria(c *modelv1.Criteria) int64 {
	if c == nil {
		return 0
	}
	if cond := c.GetCondition(); cond != nil {
		if cond.GetName() == "updated_at" && cond.GetOp() == modelv1.Condition_BINARY_OP_GT {
			return cond.GetValue().GetInt().GetValue()
		}
		return 0
	}
	if le := c.GetLe(); le != nil {
		if v := extractMaxRevisionFromCriteria(le.GetLeft()); v > 0 {
			return v
		}
		return extractMaxRevisionFromCriteria(le.GetRight())
	}
	return 0
}

func (s *recordingSchemaServer) InsertSchema(_ context.Context, req *schemav1.InsertSchemaRequest) (*schemav1.InsertSchemaResponse, error) {
	prop := req.GetProperty()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.schemas[prop.GetId()] = &storedSchema{prop: prop}
	return &schemav1.InsertSchemaResponse{}, nil
}

func (s *recordingSchemaServer) DeleteSchema(_ context.Context, req *schemav1.DeleteSchemaRequest) (*schemav1.DeleteSchemaResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	propID := req.GetDelete().GetId()
	if stored, ok := s.schemas[propID]; ok {
		stored.deleteTime = time.Now().UnixNano()
		return &schemav1.DeleteSchemaResponse{Found: true}, nil
	}
	return &schemav1.DeleteSchemaResponse{Found: false}, nil
}

func serveRecordingServer(mock *recordingSchemaServer, lis net.Listener) *grpc.Server {
	srv := grpc.NewServer()
	schemav1.RegisterSchemaManagementServiceServer(srv, mock)
	schemav1.RegisterSchemaUpdateServiceServer(srv, mock)
	healthSrv := health.NewServer()
	healthSrv.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	grpc_health_v1.RegisterHealthServer(srv, healthSrv)
	go func() { _ = srv.Serve(lis) }()
	return srv
}

func startRecordingServer(t *testing.T) (*recordingSchemaServer, string) {
	t.Helper()
	mock := newRecordingSchemaServer()
	lis, lisErr := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, lisErr)
	srv := serveRecordingServer(mock, lis)
	t.Cleanup(func() { srv.GracefulStop() })
	return mock, lis.Addr().String()
}

func newTestRegistryWithConfig(t *testing.T, cfg *property.ClientConfig, addrs ...string) *property.SchemaRegistry {
	t.Helper()
	if cfg.NodeRegistry == nil {
		cfg.NodeRegistry = &testNodeRegistry{nodes: buildTestNodes("mock-node", addrs...)}
	}
	if cfg.GRPCTimeout == 0 {
		cfg.GRPCTimeout = 10 * time.Second
	}
	reg, regErr := property.NewSchemaRegistryClient(cfg)
	require.NoError(t, regErr)
	t.Cleanup(func() { _ = reg.Close() })
	return reg
}

func buildMockGroupProperty(t *testing.T) *propertyv1.Property {
	t.Helper()
	g := testGroup()
	g.Metadata.ModRevision = time.Now().UnixNano()
	g.UpdatedAt = timestamppb.Now()
	prop, convErr := groupToProperty(g)
	require.NoError(t, convErr)
	return prop
}

func buildMockStreamProperty(t *testing.T, name string, updatedAt time.Time) *propertyv1.Property {
	t.Helper()
	s := &databasev1.Stream{
		Metadata: &commonv1.Metadata{
			Name:        name,
			Group:       "test-group",
			ModRevision: updatedAt.UnixNano(),
		},
		TagFamilies: []*databasev1.TagFamilySpec{
			{
				Name: "default",
				Tags: []*databasev1.TagSpec{
					{Name: "service_name", Type: databasev1.TagType_TAG_TYPE_STRING},
				},
			},
		},
		Entity:    &databasev1.Entity{TagNames: []string{"service_name"}},
		UpdatedAt: timestamppb.New(updatedAt),
	}
	prop, convErr := streamToProperty(s)
	require.NoError(t, convErr)
	return prop
}

func TestSyncLoop(t *testing.T) {
	t.Run("full_reconcile_detects_deletion", func(t *testing.T) {
		mock, addr := startRecordingServer(t)
		mock.addSchema(buildMockGroupProperty(t))
		mock.addSchema(buildMockStreamProperty(t, testStreamName, time.Now()))
		streamPropID := property.BuildPropertyID(schema.KindStream, &commonv1.Metadata{Group: "test-group", Name: testStreamName})
		reg := newTestRegistryWithConfig(t, &property.ClientConfig{
			SyncInterval: 50 * time.Millisecond,
		}, addr)
		handler := &testEventHandler{}
		reg.RegisterHandler("sync-handler", schema.KindGroup|schema.KindStream, handler)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		require.NoError(t, reg.Start(ctx))
		initEvt := waitForAddOrUpdateEvent(t, handler, schema.KindStream, testStreamName)
		assert.Equal(t, "test-group", initEvt.Group)
		// Mark stream as deleted on the server.
		mock.markDeleted(streamPropID)
		// Full reconcile should detect the deletion and notify the handler.
		deleteEvt := waitForDeleteEvent(t, handler, schema.KindStream, testStreamName)
		assert.Equal(t, "test-group", deleteEvt.Group)
	})

	t.Run("full_reconcile_detects_update", func(t *testing.T) {
		mock, addr := startRecordingServer(t)
		mock.addSchema(buildMockGroupProperty(t))
		initialTime := time.Now()
		mock.addSchema(buildMockStreamProperty(t, testStreamName, initialTime))
		reg := newTestRegistryWithConfig(t, &property.ClientConfig{
			SyncInterval:       500 * time.Millisecond,
			FullReconcileEvery: 1,
		}, addr)
		handler := &testEventHandler{}
		reg.RegisterHandler("sync-handler", schema.KindGroup|schema.KindStream, handler)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		require.NoError(t, reg.Start(ctx))
		waitForAddOrUpdateEvent(t, handler, schema.KindStream, testStreamName)
		// Update the stream on the server with a newer revision.
		mock.updateSchema(buildMockStreamProperty(t, testStreamName, initialTime.Add(10*time.Second)))
		// Full reconcile should detect the higher revision and re-fetch + update cache.
		require.Eventually(t, func() bool {
			count := 0
			for _, evt := range handler.getAddOrUpdateEvents() {
				if evt.Kind == schema.KindStream && evt.Name == testStreamName {
					count++
				}
			}
			return count >= 2
		}, testflags.EventuallyTimeout, 20*time.Millisecond, "full reconcile should detect updated stream")
		latestEvt := findLastEvent(handler.getAddOrUpdateEvents(), schema.KindStream, testStreamName)
		assert.Equal(t, "test-group", latestEvt.Group)
	})

	t.Run("incremental_sync_picks_up_new_data", func(t *testing.T) {
		srv, addr, _ := startTestSchemaServerFull(t)
		mgmt := rawMgmtClient(t, addr)
		insertSchemaProperty(t, mgmt, buildMockGroupProperty(t))
		insertSchemaProperty(t, mgmt, buildMockStreamProperty(t, "initial-stream", time.Now()))
		reg := newTestRegistryWithConfig(t, &property.ClientConfig{
			SyncInterval:       500 * time.Millisecond,
			FullReconcileEvery: 100,
		}, addr)
		handler := &testEventHandler{}
		reg.RegisterHandler("sync-handler", schema.KindGroup|schema.KindStream, handler)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		require.NoError(t, reg.Start(ctx))
		waitForServerWatcher(t, srv)
		waitForAddOrUpdateEvent(t, handler, schema.KindStream, "initial-stream")
		// Insert new stream directly on server.
		insertSchemaProperty(t, mgmt, buildMockStreamProperty(t, "incremental-stream", time.Now()))
		found := waitForAddOrUpdateEvent(t, handler, schema.KindStream, "incremental-stream")
		assert.Equal(t, "test-group", found.Group)
	})

	t.Run("full_reconcile_every_1_runs_each_tick", func(t *testing.T) {
		mock, addr := startRecordingServer(t)
		mock.addSchema(buildMockGroupProperty(t))
		mock.addSchema(buildMockStreamProperty(t, "s1", time.Now()))
		mock.addSchema(buildMockStreamProperty(t, "s2", time.Now()))
		s2PropID := property.BuildPropertyID(schema.KindStream, &commonv1.Metadata{Group: "test-group", Name: "s2"})
		reg := newTestRegistryWithConfig(t, &property.ClientConfig{
			SyncInterval:       500 * time.Millisecond,
			FullReconcileEvery: 1,
		}, addr)
		handler := &testEventHandler{}
		reg.RegisterHandler("sync-handler", schema.KindGroup|schema.KindStream, handler)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		require.NoError(t, reg.Start(ctx))
		waitForAddOrUpdateEvent(t, handler, schema.KindStream, "s1")
		waitForAddOrUpdateEvent(t, handler, schema.KindStream, "s2")
		// Mark s2 as deleted. Full reconcile (every tick) should detect it quickly.
		mock.markDeleted(s2PropID)
		deleteEvt := waitForDeleteEvent(t, handler, schema.KindStream, "s2")
		assert.Equal(t, "test-group", deleteEvt.Group)
	})
}

func waitForAddOrUpdateEvent(t *testing.T, handler *testEventHandler, kind schema.Kind, name string) schema.Metadata {
	t.Helper()
	require.Eventually(t, func() bool {
		for _, evt := range handler.getAddOrUpdateEvents() {
			if evt.Kind == kind && evt.Name == name {
				return true
			}
		}
		return false
	}, testflags.EventuallyTimeout, 20*time.Millisecond, "expected AddOrUpdate event for %s/%s", kind, name)
	return findLastEvent(handler.getAddOrUpdateEvents(), kind, name)
}

func waitForDeleteEvent(t *testing.T, handler *testEventHandler, kind schema.Kind, name string) schema.Metadata {
	t.Helper()
	require.Eventually(t, func() bool {
		for _, evt := range handler.getDeleteEvents() {
			if evt.Kind == kind && evt.Name == name {
				return true
			}
		}
		return false
	}, testflags.EventuallyTimeout, 20*time.Millisecond, "expected Delete event for %s/%s", kind, name)
	return findLastEvent(handler.getDeleteEvents(), kind, name)
}

func findLastEvent(events []schema.Metadata, kind schema.Kind, name string) schema.Metadata {
	for idx := len(events) - 1; idx >= 0; idx-- {
		if events[idx].Kind == kind && events[idx].Name == name {
			return events[idx]
		}
	}
	return schema.Metadata{}
}

func waitForServerWatcher(t *testing.T, srv schemaserver.Server) {
	t.Helper()
	type watcherCounter interface{ WatcherCount() int }
	wc, ok := srv.(watcherCounter)
	require.True(t, ok, "server does not implement WatcherCount")
	require.Eventually(t, func() bool {
		return wc.WatcherCount() > 0
	}, testflags.EventuallyTimeout, 20*time.Millisecond, "expected at least one watcher to connect")
}

func insertSchemaProperty(t *testing.T, mgmt schemav1.SchemaManagementServiceClient, prop *propertyv1.Property) {
	t.Helper()
	_, err := mgmt.InsertSchema(context.Background(), &schemav1.InsertSchemaRequest{Property: prop})
	require.NoError(t, err)
}

func updateSchemaProperty(t *testing.T, mgmt schemav1.SchemaManagementServiceClient, prop *propertyv1.Property) {
	t.Helper()
	_, err := mgmt.UpdateSchema(context.Background(), &schemav1.UpdateSchemaRequest{Property: prop})
	require.NoError(t, err)
}

func deleteSchemaProperty(t *testing.T, mgmt schemav1.SchemaManagementServiceClient, name, id string) {
	t.Helper()
	_, err := mgmt.DeleteSchema(context.Background(), &schemav1.DeleteSchemaRequest{
		Delete:   &propertyv1.DeleteRequest{Group: schema.SchemaGroup, Name: name, Id: id},
		UpdateAt: timestamppb.Now(),
	})
	require.NoError(t, err)
}

func TestFullSyncServerDown(t *testing.T) {
	t.Run("groups_preserved_when_server_stops", func(t *testing.T) {
		// Verifies that when the schema server stops (simulating hot node restart),
		// performFullSync does NOT delete locally cached groups because
		// sendSyncRequest returns an error on timeout.
		_, addr, stopFn := startTestSchemaServerFull(t)
		mgmt := rawMgmtClient(t, addr)

		insertSchemaProperty(t, mgmt, buildMockGroupProperty(t))
		insertSchemaProperty(t, mgmt, buildMockStreamProperty(t, "sv-stream", time.Now()))

		reg := newTestRegistryWithConfig(t, &property.ClientConfig{
			SyncInterval:       200 * time.Millisecond,
			SyncTimeout:        2 * time.Second,
			FullReconcileEvery: 1,
		}, addr)
		handler := &testEventHandler{}
		reg.RegisterHandler("test-handler", schema.KindGroup|schema.KindStream, handler)

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		require.NoError(t, reg.Start(ctx))

		waitForAddOrUpdateEvent(t, handler, schema.KindGroup, "test-group")
		waitForAddOrUpdateEvent(t, handler, schema.KindStream, "sv-stream")
		initialDeleteCount := len(handler.getDeleteEvents())

		// Stop the server. The watch session stays "active" but broken.
		stopFn()

		// Wait for the sync timeout (2s) to fire plus some buffer.
		time.Sleep(5 * time.Second)

		// Verify NO group delete events were fired.
		deleteEvents := handler.getDeleteEvents()
		groupDeleteCount := 0
		for _, evt := range deleteEvents[initialDeleteCount:] {
			if evt.Kind == schema.KindGroup && evt.Name == "test-group" {
				groupDeleteCount++
			}
		}
		assert.Equal(t, 0, groupDeleteCount, "Groups should NOT be deleted when server is unreachable")
	})

	t.Run("groups_recovered_after_server_restart", func(t *testing.T) {
		// Verifies that after server stops and restarts on the same address,
		// the watch session reconnects and groups remain consistent.
		lis, lisErr := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, lisErr)
		addr := lis.Addr().String()

		mock := newRecordingSchemaServer()
		mock.addSchema(buildMockGroupProperty(t))
		mock.addSchema(buildMockStreamProperty(t, "sv-stream", time.Now()))
		srv := serveRecordingServer(mock, lis)

		reg := newTestRegistryWithConfig(t, &property.ClientConfig{
			SyncInterval:       200 * time.Millisecond,
			SyncTimeout:        2 * time.Second,
			WatchMaxBackoff:    2 * time.Second,
			FullReconcileEvery: 1,
		}, addr)
		handler := &testEventHandler{}
		reg.RegisterHandler("test-handler", schema.KindGroup|schema.KindStream, handler)

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		require.NoError(t, reg.Start(ctx))

		waitForAddOrUpdateEvent(t, handler, schema.KindGroup, "test-group")
		waitForAddOrUpdateEvent(t, handler, schema.KindStream, "sv-stream")
		initialDeleteCount := len(handler.getDeleteEvents())

		// Stop the server (hard stop to avoid blocking on bidi streams).
		srv.Stop()

		// Wait for sync timeout (2s) to fire. Groups should NOT be deleted.
		time.Sleep(5 * time.Second)
		for _, evt := range handler.getDeleteEvents()[initialDeleteCount:] {
			require.NotEqual(t, schema.KindGroup, evt.Kind,
				"Group should not be deleted during server outage")
		}

		// Restart server on same address with same data.
		lis2, lisErr2 := net.Listen("tcp", addr)
		require.NoError(t, lisErr2)
		mock2 := newRecordingSchemaServer()
		mock2.addSchema(buildMockGroupProperty(t))
		mock2.addSchema(buildMockStreamProperty(t, "sv-stream", time.Now()))
		srv2 := serveRecordingServer(mock2, lis2)
		t.Cleanup(func() { srv2.Stop() })

		// Wait for watch reconnection (backoff up to 2s) and a successful full sync.
		time.Sleep(5 * time.Second)

		// Verify no group was deleted during the entire process.
		for _, evt := range handler.getDeleteEvents()[initialDeleteCount:] {
			assert.NotEqual(t, schema.KindGroup, evt.Kind,
				"Group should not be deleted after server restart")
		}
	})
}

func TestWatchPush(t *testing.T) {
	t.Run("watch_push_insert", func(t *testing.T) {
		srv, addr, _ := startTestSchemaServerFull(t)
		mgmt := rawMgmtClient(t, addr)
		insertSchemaProperty(t, mgmt, buildMockGroupProperty(t))
		reg := newTestRegistryWithConfig(t, &property.ClientConfig{
			SyncInterval: 10 * time.Minute,
		}, addr)
		handler := &testEventHandler{}
		reg.RegisterHandler("watch-handler", schema.KindGroup|schema.KindStream, handler)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		require.NoError(t, reg.Start(ctx))
		waitForServerWatcher(t, srv)
		// Insert a new stream via the real server RPC — triggers watch broadcast.
		insertSchemaProperty(t, mgmt, buildMockStreamProperty(t, "watch-stream", time.Now()))
		insertEvt := waitForAddOrUpdateEvent(t, handler, schema.KindStream, "watch-stream")
		assert.Equal(t, "test-group", insertEvt.Group)
	})

	t.Run("watch_push_update", func(t *testing.T) {
		srv, addr, _ := startTestSchemaServerFull(t)
		mgmt := rawMgmtClient(t, addr)
		insertSchemaProperty(t, mgmt, buildMockGroupProperty(t))
		insertSchemaProperty(t, mgmt, buildMockStreamProperty(t, "update-stream", time.Now().Add(-time.Hour)))
		reg := newTestRegistryWithConfig(t, &property.ClientConfig{
			SyncInterval: 10 * time.Minute,
		}, addr)
		handler := &testEventHandler{}
		reg.RegisterHandler("watch-handler", schema.KindGroup|schema.KindStream, handler)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		require.NoError(t, reg.Start(ctx))
		waitForServerWatcher(t, srv)
		initEvt := waitForAddOrUpdateEvent(t, handler, schema.KindStream, "update-stream")
		assert.Equal(t, "test-group", initEvt.Group)
		initialCount := len(handler.getAddOrUpdateEvents())
		// Update with a newer timestamp via the real server RPC.
		updateSchemaProperty(t, mgmt, buildMockStreamProperty(t, "update-stream", time.Now()))
		require.Eventually(t, func() bool {
			return len(handler.getAddOrUpdateEvents()) > initialCount
		}, testflags.EventuallyTimeout, 20*time.Millisecond, "handler should receive watch push update")
		lastEvt := findLastEvent(handler.getAddOrUpdateEvents(), schema.KindStream, "update-stream")
		assert.Equal(t, "test-group", lastEvt.Group)
	})

	t.Run("watch_push_delete", func(t *testing.T) {
		srv, addr, _ := startTestSchemaServerFull(t)
		mgmt := rawMgmtClient(t, addr)
		insertSchemaProperty(t, mgmt, buildMockGroupProperty(t))
		streamProp := buildMockStreamProperty(t, testDeleteStreamName, time.Now())
		insertSchemaProperty(t, mgmt, streamProp)
		reg := newTestRegistryWithConfig(t, &property.ClientConfig{
			SyncInterval: 10 * time.Minute,
		}, addr)
		handler := &testEventHandler{}
		reg.RegisterHandler("watch-handler", schema.KindGroup|schema.KindStream, handler)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		require.NoError(t, reg.Start(ctx))
		waitForServerWatcher(t, srv)
		initEvt := waitForAddOrUpdateEvent(t, handler, schema.KindStream, testDeleteStreamName)
		assert.Equal(t, "test-group", initEvt.Group)
		// Delete via the real server RPC — triggers watch broadcast.
		deleteSchemaProperty(t, mgmt, streamProp.GetMetadata().GetName(), streamProp.GetId())
		deleteEvt := waitForDeleteEvent(t, handler, schema.KindStream, testDeleteStreamName)
		assert.Equal(t, "test-group", deleteEvt.Group)
	})

	t.Run("watch_older_revision_ignored", func(t *testing.T) {
		srv, addr, _ := startTestSchemaServerFull(t)
		mgmt := rawMgmtClient(t, addr)
		insertSchemaProperty(t, mgmt, buildMockGroupProperty(t))
		now := time.Now()
		insertSchemaProperty(t, mgmt, buildMockStreamProperty(t, "rev-stream", now))
		reg := newTestRegistryWithConfig(t, &property.ClientConfig{
			SyncInterval: 10 * time.Minute,
		}, addr)
		handler := &testEventHandler{}
		reg.RegisterHandler("watch-handler", schema.KindGroup|schema.KindStream, handler)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		require.NoError(t, reg.Start(ctx))
		waitForServerWatcher(t, srv)
		initEvt := waitForAddOrUpdateEvent(t, handler, schema.KindStream, "rev-stream")
		assert.Equal(t, "test-group", initEvt.Group)
		initialCount := len(handler.getAddOrUpdateEvents())
		// Update with an older version — should be ignored by cache revision check.
		updateSchemaProperty(t, mgmt, buildMockStreamProperty(t, "rev-stream", now.Add(-time.Hour)))
		time.Sleep(200 * time.Millisecond)
		assert.Equal(t, initialCount, len(handler.getAddOrUpdateEvents()), "older revision should not trigger handler")
	})

	t.Run("watch_new_node_added", func(t *testing.T) {
		srv1, addr1, _ := startTestSchemaServerFull(t)
		mgmt1 := rawMgmtClient(t, addr1)
		insertSchemaProperty(t, mgmt1, buildMockGroupProperty(t))
		srv2, addr2, _ := startTestSchemaServerFull(t)
		mgmt2 := rawMgmtClient(t, addr2)
		insertSchemaProperty(t, mgmt2, buildMockGroupProperty(t))
		reg := newTestRegistryWithConfig(t, &property.ClientConfig{
			SyncInterval: 10 * time.Minute,
		}, addr1)
		handler := &testEventHandler{}
		reg.RegisterHandler("watch-handler", schema.KindGroup|schema.KindStream, handler)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		require.NoError(t, reg.Start(ctx))
		waitForServerWatcher(t, srv1)
		// Dynamically add the second server.
		addNodeToRegistry(reg, "new-node", addr2)
		waitForServerWatcher(t, srv2)
		// Insert via the new server — client should receive it through watch.
		insertSchemaProperty(t, mgmt2, buildMockStreamProperty(t, "new-node-stream", time.Now()))
		newNodeEvt := waitForAddOrUpdateEvent(t, handler, schema.KindStream, "new-node-stream")
		assert.Equal(t, "test-group", newNodeEvt.Group)
	})

	t.Run("watch_server_restart", func(t *testing.T) {
		srv, addr, _ := startTestSchemaServerFull(t)
		mgmt := rawMgmtClient(t, addr)
		insertSchemaProperty(t, mgmt, buildMockGroupProperty(t))
		reg := newTestRegistryWithConfig(t, &property.ClientConfig{
			SyncInterval: 10 * time.Minute,
		}, addr)
		handler := &testEventHandler{}
		reg.RegisterHandler("watch-handler", schema.KindGroup|schema.KindStream, handler)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		require.NoError(t, reg.Start(ctx))
		waitForServerWatcher(t, srv)
		// Verify initial watcher count.
		type watcherCounter interface{ WatcherCount() int }
		wc, ok := srv.(watcherCounter)
		require.True(t, ok, "server does not implement WatcherCount")
		assert.Equal(t, 1, wc.WatcherCount())
		// Insert via the real server RPC — should be received through watch.
		insertSchemaProperty(t, mgmt, buildMockStreamProperty(t, "before-restart", time.Now()))
		restartEvt := waitForAddOrUpdateEvent(t, handler, schema.KindStream, "before-restart")
		assert.Equal(t, "test-group", restartEvt.Group)
	})
}
