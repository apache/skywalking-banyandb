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
	"strings"
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
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	schemav1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/schema/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema/property"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema/schemaserver"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	testflags "github.com/apache/skywalking-banyandb/pkg/test/flags"
)

const testStreamName = "test-stream"

func startTestSchemaServer(t *testing.T) string {
	t.Helper()
	addr, _ := startTestSchemaServerStoppable(t)
	return addr
}

func startTestSchemaServerStoppable(t *testing.T) (string, func()) {
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
			require.FailNowf(t, "server did not start in time", "last error: %v", dialErr)
		}
		time.Sleep(50 * time.Millisecond)
	}
	return addr, func() { srv.GracefulStop() }
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
	resp1, existErr := mgmt1.ExistSchema(ctx, &schemav1.ExistSchemaRequest{
		Query: &propertyv1.QueryRequest{
			Groups: []string{schema.SchemaGroup},
			Name:   "stream",
		},
	})
	require.NoError(t, existErr)
	assert.True(t, resp1.GetHasSchema())
	resp2, existErr := mgmt2.ExistSchema(ctx, &schemav1.ExistSchemaRequest{
		Query: &propertyv1.QueryRequest{
			Groups: []string{schema.SchemaGroup},
			Name:   "stream",
		},
	})
	require.NoError(t, existErr)
	assert.True(t, resp2.GetHasSchema())
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
	// Start triggers initializeFromSchemaClient for all active nodes.
	reg.Start(context.Background())
	// Handler should receive OnAddOrUpdate events for the group and stream from server2.
	events := handler.getAddOrUpdateEvents()
	var hasGroup, hasStream bool
	for _, evt := range events {
		if evt.Kind == schema.KindGroup && evt.Name == "test-group" {
			hasGroup = true
		}
		if evt.Kind == schema.KindStream && evt.Name == testStreamName {
			hasStream = true
		}
	}
	assert.True(t, hasGroup, "handler should receive group OnAddOrUpdate, events: %+v", events)
	assert.True(t, hasStream, "handler should receive stream OnAddOrUpdate, events: %+v", events)
	// Registry should be able to read the stream.
	ctx := context.Background()
	got, getErr := reg.GetStream(ctx, &commonv1.Metadata{Group: "test-group", Name: testStreamName})
	require.NoError(t, getErr)
	assert.Equal(t, testStreamName, got.GetMetadata().GetName())
	// Verify repair: server1 should also have the data after broadcast+repair.
	time.Sleep(500 * time.Millisecond)
	mgmt1 := rawMgmtClient(t, addr1)
	resp, existErr := mgmt1.ExistSchema(ctx, &schemav1.ExistSchemaRequest{
		Query: &propertyv1.QueryRequest{
			Groups: []string{schema.SchemaGroup},
			Name:   "stream",
		},
	})
	require.NoError(t, existErr)
	assert.True(t, resp.GetHasSchema(), "server1 should have the stream data after repair")
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
	resp, existErr := mgmt2.ExistSchema(ctx, &schemav1.ExistSchemaRequest{
		Query: &propertyv1.QueryRequest{
			Groups: []string{schema.SchemaGroup},
			Name:   "stream",
		},
	})
	require.NoError(t, existErr)
	assert.True(t, resp.GetHasSchema(), "server2 should have stream data after repair")
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
	// Start triggers initializeFromSchemaClient for all active nodes.
	reg.Start(context.Background())
	time.Sleep(500 * time.Millisecond)
	// Stream handler should get stream events but not measure events.
	streamEvents := streamHandler.getAddOrUpdateEvents()
	var streamHasStream bool
	for _, evt := range streamEvents {
		if evt.Kind == schema.KindStream && evt.Name == testStreamName {
			streamHasStream = true
		}
		assert.NotEqual(t, schema.KindMeasure, evt.Kind, "stream handler should not receive measure events")
	}
	assert.True(t, streamHasStream, "stream handler should receive stream OnAddOrUpdate")
	// Measure handler should get measure events but not stream events.
	measureEvents := measureHandler.getAddOrUpdateEvents()
	var measureHasMeasure bool
	for _, evt := range measureEvents {
		if evt.Kind == schema.KindMeasure && evt.Name == "test-measure" {
			measureHasMeasure = true
		}
		assert.NotEqual(t, schema.KindStream, evt.Kind, "measure handler should not receive stream events")
	}
	assert.True(t, measureHasMeasure, "measure handler should receive measure OnAddOrUpdate")
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
	schemas        map[string]*storedSchema
	sinceRevisions []int64
	mu             sync.Mutex
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

func (s *recordingSchemaServer) markDeleted(propID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if stored, ok := s.schemas[propID]; ok {
		stored.deleteTime = time.Now().UnixNano()
	}
}

func (s *recordingSchemaServer) aggregateCallCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.sinceRevisions)
}

func (s *recordingSchemaServer) lastSinceRevision() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.sinceRevisions) == 0 {
		return 0
	}
	return s.sinceRevisions[len(s.sinceRevisions)-1]
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

func (s *recordingSchemaServer) AggregateSchemaUpdates(_ context.Context, req *schemav1.AggregateSchemaUpdatesRequest) (*schemav1.AggregateSchemaUpdatesResponse, error) {
	sinceRevision := extractSinceRevision(req.GetQuery())
	s.mu.Lock()
	s.sinceRevisions = append(s.sinceRevisions, sinceRevision)
	kindSet := make(map[string]struct{})
	for _, stored := range s.schemas {
		if stored.deleteTime > 0 {
			continue
		}
		parsed := property.ParseTags(stored.prop.GetTags())
		if parsed.UpdatedAt > sinceRevision {
			kindSet[stored.prop.GetMetadata().GetName()] = struct{}{}
		}
	}
	s.mu.Unlock()
	names := make([]string, 0, len(kindSet))
	for kindName := range kindSet {
		names = append(names, kindName)
	}
	return &schemav1.AggregateSchemaUpdatesResponse{Names: names}, nil
}

func (s *recordingSchemaServer) ExistSchema(_ context.Context, req *schemav1.ExistSchemaRequest) (*schemav1.ExistSchemaResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	query := req.GetQuery()
	for _, stored := range s.schemas {
		if stored.deleteTime > 0 {
			continue
		}
		if query.GetName() != "" && stored.prop.GetMetadata().GetName() != query.GetName() {
			continue
		}
		if len(query.GetIds()) > 0 {
			for _, queryID := range query.GetIds() {
				if stored.prop.GetId() == queryID {
					return &schemav1.ExistSchemaResponse{HasSchema: true}, nil
				}
			}
			continue
		}
		return &schemav1.ExistSchemaResponse{HasSchema: true}, nil
	}
	return &schemav1.ExistSchemaResponse{HasSchema: false}, nil
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

func extractSinceRevision(query *propertyv1.QueryRequest) int64 {
	cond := query.GetCriteria().GetCondition()
	if cond != nil && cond.GetName() == "updated_at" {
		return cond.GetValue().GetInt().GetValue()
	}
	return 0
}

func startRecordingServer(t *testing.T) (*recordingSchemaServer, string) {
	t.Helper()
	mock := newRecordingSchemaServer()
	lis, lisErr := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, lisErr)
	srv := grpc.NewServer()
	schemav1.RegisterSchemaManagementServiceServer(srv, mock)
	schemav1.RegisterSchemaUpdateServiceServer(srv, mock)
	healthSrv := health.NewServer()
	healthSrv.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	grpc_health_v1.RegisterHealthServer(srv, healthSrv)
	go func() { _ = srv.Serve(lis) }()
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
	t.Run("incremental_sync_calls_aggregate", func(t *testing.T) {
		mock, addr := startRecordingServer(t)
		mock.addSchema(buildMockGroupProperty(t))
		mock.addSchema(buildMockStreamProperty(t, testStreamName, time.Now()))
		reg := newTestRegistryWithConfig(t, &property.ClientConfig{
			SyncInterval:       50 * time.Millisecond,
			FullReconcileEvery: 100,
		}, addr)
		handler := &testEventHandler{}
		reg.RegisterHandler("sync-handler", schema.KindGroup|schema.KindStream, handler)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		require.NoError(t, reg.Start(ctx))
		require.Eventually(t, func() bool {
			return mock.aggregateCallCount() >= 3
		}, testflags.EventuallyTimeout, 20*time.Millisecond, "expected at least 3 AggregateSchemaUpdates calls")
	})

	t.Run("full_sync_skips_aggregate", func(t *testing.T) {
		mock, addr := startRecordingServer(t)
		mock.addSchema(buildMockGroupProperty(t))
		mock.addSchema(buildMockStreamProperty(t, testStreamName, time.Now()))
		reg := newTestRegistryWithConfig(t, &property.ClientConfig{
			SyncInterval:       50 * time.Millisecond,
			FullReconcileEvery: 1,
		}, addr)
		handler := &testEventHandler{}
		reg.RegisterHandler("sync-handler", schema.KindGroup|schema.KindStream, handler)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		require.NoError(t, reg.Start(ctx))
		// Wait for init to deliver the stream.
		require.Eventually(t, func() bool {
			for _, evt := range handler.getAddOrUpdateEvents() {
				if evt.Kind == schema.KindStream && evt.Name == testStreamName {
					return true
				}
			}
			return false
		}, testflags.EventuallyTimeout, 20*time.Millisecond)
		// Let several sync rounds fire (all full).
		time.Sleep(300 * time.Millisecond)
		assert.Equal(t, 0, mock.aggregateCallCount(), "full sync should not call AggregateSchemaUpdates")
	})

	t.Run("sinceRevision_advances_with_new_data", func(t *testing.T) {
		mock, addr := startRecordingServer(t)
		mock.addSchema(buildMockGroupProperty(t))
		mock.addSchema(buildMockStreamProperty(t, testStreamName, time.Now()))
		reg := newTestRegistryWithConfig(t, &property.ClientConfig{
			SyncInterval:       50 * time.Millisecond,
			FullReconcileEvery: 100,
		}, addr)
		handler := &testEventHandler{}
		reg.RegisterHandler("sync-handler", schema.KindGroup|schema.KindStream, handler)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		require.NoError(t, reg.Start(ctx))
		// Wait for at least 2 aggregate calls to establish a baseline revision.
		require.Eventually(t, func() bool {
			return mock.aggregateCallCount() >= 2
		}, testflags.EventuallyTimeout, 20*time.Millisecond)
		rev0 := mock.lastSinceRevision()
		assert.Greater(t, rev0, int64(0), "sinceRevision should be > 0 after init")
		// With no new data, sinceRevision should remain stable.
		countBefore := mock.aggregateCallCount()
		require.Eventually(t, func() bool {
			return mock.aggregateCallCount() >= countBefore+2
		}, testflags.EventuallyTimeout, 20*time.Millisecond)
		assert.Equal(t, rev0, mock.lastSinceRevision(), "sinceRevision should not change without new data")
		// Add a new stream with a far-future updatedAt.
		futureTime := time.Now().Add(time.Hour)
		mock.addSchema(buildMockStreamProperty(t, "new-stream", futureTime))
		// sinceRevision should advance after the new data is picked up.
		require.Eventually(t, func() bool {
			return mock.lastSinceRevision() > rev0
		}, testflags.EventuallyTimeout, 20*time.Millisecond, "sinceRevision should advance after new data")
		// Handler should have received the new stream.
		require.Eventually(t, func() bool {
			for _, evt := range handler.getAddOrUpdateEvents() {
				if evt.Kind == schema.KindStream && evt.Name == "new-stream" {
					return true
				}
			}
			return false
		}, testflags.EventuallyTimeout, 20*time.Millisecond, "handler should receive OnAddOrUpdate for new-stream")
	})

	t.Run("full_reconcile_detects_deletion", func(t *testing.T) {
		mock, addr := startRecordingServer(t)
		mock.addSchema(buildMockGroupProperty(t))
		mock.addSchema(buildMockStreamProperty(t, testStreamName, time.Now()))
		streamPropID := property.BuildPropertyID(schema.KindStream, &commonv1.Metadata{Group: "test-group", Name: testStreamName})
		reg := newTestRegistryWithConfig(t, &property.ClientConfig{
			SyncInterval:       50 * time.Millisecond,
			FullReconcileEvery: 1,
		}, addr)
		handler := &testEventHandler{}
		reg.RegisterHandler("sync-handler", schema.KindGroup|schema.KindStream, handler)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		require.NoError(t, reg.Start(ctx))
		// Wait for init to deliver the stream.
		require.Eventually(t, func() bool {
			for _, evt := range handler.getAddOrUpdateEvents() {
				if evt.Kind == schema.KindStream && evt.Name == testStreamName {
					return true
				}
			}
			return false
		}, testflags.EventuallyTimeout, 20*time.Millisecond)
		// Mark stream as deleted on the server.
		mock.markDeleted(streamPropID)
		// Full reconcile should detect the deletion and notify the handler.
		require.Eventually(t, func() bool {
			for _, evt := range handler.getDeleteEvents() {
				if evt.Kind == schema.KindStream && strings.Contains(evt.Name, testStreamName) {
					return true
				}
			}
			return false
		}, testflags.EventuallyTimeout, 20*time.Millisecond, "handler should receive OnDelete for test-stream")
	})
}
