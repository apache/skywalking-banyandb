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

package schema_test

import (
	"context"
	"embed"
	"fmt"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/embeddedetcd"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

const indexRuleDir = "testdata/index_rules"

var (
	//go:embed testdata/index_rules/*.json
	indexRuleStore embed.FS
	//go:embed testdata/index_rule_binding.json
	indexRuleBindingJSON string
	//go:embed testdata/stream.json
	streamJSON string
	//go:embed testdata/group.json
	groupJSON string
)

func preloadSchema(e schema.Registry) error {
	g := &commonv1.Group{}
	if err := protojson.Unmarshal([]byte(groupJSON), g); err != nil {
		return err
	}
	if err := e.CreateGroup(context.TODO(), g); err != nil {
		return err
	}

	s := &databasev1.Stream{}
	if err := protojson.Unmarshal([]byte(streamJSON), s); err != nil {
		return err
	}
	_, err := e.CreateStream(context.Background(), s)
	if err != nil {
		return err
	}

	indexRuleBinding := &databasev1.IndexRuleBinding{}
	if err = protojson.Unmarshal([]byte(indexRuleBindingJSON), indexRuleBinding); err != nil {
		return err
	}
	err = e.CreateIndexRuleBinding(context.Background(), indexRuleBinding)
	if err != nil {
		return err
	}

	entries, err := indexRuleStore.ReadDir(indexRuleDir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		data, err := indexRuleStore.ReadFile(path.Join(indexRuleDir, entry.Name()))
		if err != nil {
			return err
		}
		var idxRule databasev1.IndexRule
		err = protojson.Unmarshal(data, &idxRule)
		if err != nil {
			return err
		}
		err = e.CreateIndexRule(context.Background(), &idxRule)
		if err != nil {
			return err
		}
	}

	return nil
}

func initServerAndRegister(t *testing.T) (schema.Registry, func()) {
	path, defFn := test.Space(require.New(t))
	req := require.New(t)
	ports, err := test.AllocateFreePorts(2)
	if err != nil {
		panic("fail to find free ports")
	}
	endpoints := []string{fmt.Sprintf("http://127.0.0.1:%d", ports[0])}
	server, err := embeddedetcd.NewServer(
		embeddedetcd.ConfigureListener(endpoints, []string{fmt.Sprintf("http://127.0.0.1:%d", ports[1])}),
		embeddedetcd.RootDir(path),
		embeddedetcd.AutoCompactionMode("periodic"),
		embeddedetcd.AutoCompactionRetention("1h"),
		embeddedetcd.QuotaBackendBytes(2*1024*1024*1024),
	)
	req.NoError(err)
	req.NotNil(server)
	<-server.ReadyNotify()
	schemaRegistry, err := schema.NewEtcdSchemaRegistry(schema.ConfigureServerEndpoints(endpoints))
	req.NoError(err)
	req.NotNil(server)
	return schemaRegistry, func() {
		server.Close()
		<-server.StopNotify()
		schemaRegistry.Close()
		defFn()
	}
}

func Test_Etcd_Entity_Get(t *testing.T) {
	tester := assert.New(t)
	registry, closer := initServerAndRegister(t)
	defer closer()
	err := preloadSchema(registry)
	tester.NoError(err)

	tests := []struct {
		meta        *commonv1.Metadata
		get         func(schema.Registry, *commonv1.Metadata) (schema.HasMetadata, error)
		name        string
		expectedErr bool
	}{
		{
			name: "Get Group",
			meta: &commonv1.Metadata{Name: "default"},
			get: func(_ schema.Registry, meta *commonv1.Metadata) (schema.HasMetadata, error) {
				stm, innerErr := registry.GetGroup(context.TODO(), meta.GetName())
				if innerErr != nil {
					return nil, innerErr
				}
				return schema.HasMetadata(stm), nil
			},
		},
		{
			name: "Get Stream",
			meta: &commonv1.Metadata{Name: "sw", Group: "default"},
			get: func(_ schema.Registry, meta *commonv1.Metadata) (schema.HasMetadata, error) {
				stm, innerErr := registry.GetStream(context.TODO(), meta)
				if innerErr != nil {
					return nil, innerErr
				}
				return schema.HasMetadata(stm), nil
			},
		},
		{
			name: "Get IndexRuleBinding",
			meta: &commonv1.Metadata{Name: "sw-index-rule-binding", Group: "default"},
			get: func(_ schema.Registry, meta *commonv1.Metadata) (schema.HasMetadata, error) {
				e, innerErr := registry.GetIndexRuleBinding(context.TODO(), meta)
				if innerErr != nil {
					return nil, innerErr
				}
				return schema.HasMetadata(e), nil
			},
		},
		{
			name: "Get IndexRule",
			meta: &commonv1.Metadata{Name: "db.instance", Group: "default"},
			get: func(_ schema.Registry, meta *commonv1.Metadata) (schema.HasMetadata, error) {
				e, innerErr := registry.GetIndexRule(context.TODO(), meta)
				if innerErr != nil {
					return nil, innerErr
				}
				return schema.HasMetadata(e), nil
			},
		},
		{
			name: "Get unknown Measure",
			meta: &commonv1.Metadata{Name: "unknown-stream", Group: "default"},
			get: func(_ schema.Registry, meta *commonv1.Metadata) (schema.HasMetadata, error) {
				e, innerErr := registry.GetMeasure(context.TODO(), meta)
				if innerErr != nil {
					return nil, innerErr
				}
				return schema.HasMetadata(e), nil
			},
			expectedErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := require.New(t)
			entity, err := tt.get(registry, tt.meta)
			if !tt.expectedErr {
				req.NoError(err)
				req.NotNil(entity)
				req.Greater(entity.GetMetadata().GetCreateRevision(), int64(0))
				req.Greater(entity.GetMetadata().GetModRevision(), int64(0))
				req.Equal(entity.GetMetadata().GetGroup(), tt.meta.GetGroup())
				req.Equal(entity.GetMetadata().GetName(), tt.meta.GetName())
			} else {
				req.Error(err)
			}
		})
	}
}

func Test_Etcd_Entity_List(t *testing.T) {
	tester := assert.New(t)
	registry, closer := initServerAndRegister(t)
	defer closer()

	err := preloadSchema(registry)
	tester.NoError(err)

	tests := []struct {
		list        func(schema.Registry) (int, error)
		name        string
		expectedLen int
	}{
		{
			name: "List Group",
			list: func(r schema.Registry) (int, error) {
				entities, innerErr := r.ListGroup(context.TODO())
				if innerErr != nil {
					return 0, innerErr
				}
				return len(entities), nil
			},
			expectedLen: 1,
		},
		{
			name: "List Stream",
			list: func(r schema.Registry) (int, error) {
				entities, innerErr := r.ListStream(context.TODO(), schema.ListOpt{Group: "default"})
				if innerErr != nil {
					return 0, innerErr
				}
				return len(entities), nil
			},
			expectedLen: 1,
		},
		{
			name: "List IndexRuleBinding",
			list: func(r schema.Registry) (int, error) {
				entities, innerErr := r.ListIndexRuleBinding(context.TODO(), schema.ListOpt{Group: "default"})
				if innerErr != nil {
					return 0, innerErr
				}
				return len(entities), nil
			},
			expectedLen: 1,
		},
		{
			name: "List IndexRule",
			list: func(r schema.Registry) (int, error) {
				entities, innerErr := r.ListIndexRule(context.TODO(), schema.ListOpt{Group: "default"})
				if innerErr != nil {
					return 0, innerErr
				}
				return len(entities), nil
			},
			expectedLen: 10,
		},
		{
			name: "List Measure",
			list: func(r schema.Registry) (int, error) {
				entities, innerErr := r.ListMeasure(context.TODO(), schema.ListOpt{Group: "default"})
				if innerErr != nil {
					return 0, innerErr
				}
				return len(entities), nil
			},
			expectedLen: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := require.New(t)
			entitiesLen, listErr := tt.list(registry)
			req.NoError(listErr)
			req.Equal(entitiesLen, tt.expectedLen)
		})
	}
}

func Test_Etcd_Delete(t *testing.T) {
	tester := assert.New(t)
	registry, closer := initServerAndRegister(t)
	defer closer()

	err := preloadSchema(registry)
	tester.NoError(err)

	tests := []struct {
		list              func(schema.Registry) (int, error)
		delete            func(schema.Registry) error
		name              string
		expectedLenBefore int
		expectedLenAfter  int
	}{
		{
			name: "Delete IndexRule",
			list: func(r schema.Registry) (int, error) {
				entities, innerErr := r.ListIndexRule(context.TODO(), schema.ListOpt{Group: "default"})
				if innerErr != nil {
					return 0, innerErr
				}
				return len(entities), nil
			},
			delete: func(r schema.Registry) error {
				_, innerErr := r.DeleteIndexRule(context.TODO(), &commonv1.Metadata{
					Name:  "db.instance",
					Group: "default",
				})
				return innerErr
			},
			expectedLenBefore: 10,
			expectedLenAfter:  9,
		},
		{
			name: "Delete Group",
			list: func(r schema.Registry) (int, error) {
				entities, innerErr := r.ListIndexRule(context.TODO(), schema.ListOpt{Group: "default"})
				if innerErr != nil {
					return 0, innerErr
				}
				return len(entities), nil
			},
			delete: func(r schema.Registry) error {
				_, innerErr := r.DeleteGroup(context.TODO(), "default")
				return innerErr
			},
			expectedLenBefore: 9,
			expectedLenAfter:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ast := assert.New(t)
			num, err := tt.list(registry)
			ast.NoError(err)
			ast.Equal(num, tt.expectedLenBefore)
			err = tt.delete(registry)
			ast.NoError(err)
			num, err = tt.list(registry)
			ast.NoError(err)
			ast.Equal(num, tt.expectedLenAfter)
		})
	}
}

func Test_Etcd_Entity_Update(t *testing.T) {
	tester := assert.New(t)
	registry, closer := initServerAndRegister(t)
	defer closer()

	err := preloadSchema(registry)
	tester.NoError(err)

	tests := []struct {
		updateFunc     func(context.Context, schema.Registry) error
		validationFunc func(context.Context, schema.Registry) bool
		name           string
	}{
		{
			name: "update indexRule when none metadata-id",
			updateFunc: func(ctx context.Context, r schema.Registry) error {
				ir, err := r.GetIndexRule(ctx, &commonv1.Metadata{
					Name:  "db.instance",
					Group: "default",
				})
				if err != nil {
					return err
				}
				// reset
				ir.Metadata.Id = 0
				ir.Type = databasev1.IndexRule_TYPE_INVERTED
				return r.UpdateIndexRule(ctx, ir)
			},
			validationFunc: func(ctx context.Context, r schema.Registry) bool {
				ir, err := r.GetIndexRule(ctx, &commonv1.Metadata{
					Name:  "db.instance",
					Group: "default",
				})
				if err != nil {
					return false
				}
				if ir.Metadata.Id != 1 {
					return false
				}
				if ir.Type != databasev1.IndexRule_TYPE_INVERTED {
					return false
				}
				return true
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := require.New(t)

			err := tt.updateFunc(context.TODO(), registry)
			req.NoError(err)

			req.True(tt.validationFunc(context.TODO(), registry))
		})
	}
}

func Test_Etcd_Stream_GroupPrefixMatching(t *testing.T) {
	tester := assert.New(t)
	registry, closer := initServerAndRegister(t)
	defer closer()

	// Create groups with similar names to test prefix matching
	groups := []string{"records", "recordsTrace", "recordsLog"}
	for _, groupName := range groups {
		group := &commonv1.Group{
			Metadata: &commonv1.Metadata{
				Name: groupName,
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
		err := registry.CreateGroup(context.TODO(), group)
		tester.NoError(err)
	}

	// Create streams in each group
	streams := []struct {
		name  string
		group string
	}{
		{"alarm_record", "records"},
		{"top_n_cache_read_command", "records"},
		{"log", "recordsLog"},
		{"sw_span_attached_event_record", "recordsTrace"},
	}

	for _, streamData := range streams {
		stream := &databasev1.Stream{
			Metadata: &commonv1.Metadata{
				Name:  streamData.name,
				Group: streamData.group,
			},
			TagFamilies: []*databasev1.TagFamilySpec{
				{
					Name: "searchable",
					Tags: []*databasev1.TagSpec{
						{
							Name: "timestamp",
							Type: databasev1.TagType_TAG_TYPE_INT,
						},
					},
				},
			},
			Entity: &databasev1.Entity{
				TagNames: []string{"timestamp"},
			},
		}
		_, err := registry.CreateStream(context.Background(), stream)
		tester.NoError(err)
	}

	// Test that listing streams for "records" group only returns streams from "records" group
	recordsStreams, err := registry.ListStream(context.TODO(), schema.ListOpt{Group: "records"})
	tester.NoError(err)
	tester.Equal(2, len(recordsStreams), "Should only return streams from 'records' group")

	// Verify the returned streams are from the correct group
	for _, stream := range recordsStreams {
		tester.Equal("records", stream.Metadata.Group, "All returned streams should be from 'records' group")
	}

	// Test that listing streams for "recordsTrace" group only returns streams from "recordsTrace" group
	recordsTraceStreams, err := registry.ListStream(context.TODO(), schema.ListOpt{Group: "recordsTrace"})
	tester.NoError(err)
	tester.Equal(1, len(recordsTraceStreams), "Should only return streams from 'recordsTrace' group")
	tester.Equal("recordsTrace", recordsTraceStreams[0].Metadata.Group)
	tester.Equal("sw_span_attached_event_record", recordsTraceStreams[0].Metadata.Name)

	// Test that listing streams for "recordsLog" group only returns streams from "recordsLog" group
	recordsLogStreams, err := registry.ListStream(context.TODO(), schema.ListOpt{Group: "recordsLog"})
	tester.NoError(err)
	tester.Equal(1, len(recordsLogStreams), "Should only return streams from 'recordsLog' group")
	tester.Equal("recordsLog", recordsLogStreams[0].Metadata.Group)
	tester.Equal("log", recordsLogStreams[0].Metadata.Name)
}

func Test_Etcd_Measure_GroupPrefixMatching(t *testing.T) {
	tester := assert.New(t)
	registry, closer := initServerAndRegister(t)
	defer closer()

	// Create groups with similar names to test prefix matching
	groups := []string{"metrics", "metricsTrace", "metricsLog"}
	for _, groupName := range groups {
		group := &commonv1.Group{
			Metadata: &commonv1.Metadata{
				Name: groupName,
			},
			Catalog: commonv1.Catalog_CATALOG_MEASURE,
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
		err := registry.CreateGroup(context.TODO(), group)
		tester.NoError(err)
	}

	// Create measures in each group
	measures := []struct {
		name  string
		group string
	}{
		{"service_cpm", "metrics"},
		{"service_resp_time", "metrics"},
		{"trace_metrics", "metricsTrace"},
		{"log_metrics", "metricsLog"},
	}

	for _, measureData := range measures {
		measure := &databasev1.Measure{
			Metadata: &commonv1.Metadata{
				Name:  measureData.name,
				Group: measureData.group,
			},
			TagFamilies: []*databasev1.TagFamilySpec{
				{
					Name: "default",
					Tags: []*databasev1.TagSpec{
						{
							Name: "service_id",
							Type: databasev1.TagType_TAG_TYPE_STRING,
						},
					},
				},
			},
			Fields: []*databasev1.FieldSpec{
				{
					Name:              "value",
					FieldType:         databasev1.FieldType_FIELD_TYPE_INT,
					EncodingMethod:    databasev1.EncodingMethod_ENCODING_METHOD_GORILLA,
					CompressionMethod: databasev1.CompressionMethod_COMPRESSION_METHOD_ZSTD,
				},
			},
			Entity: &databasev1.Entity{
				TagNames: []string{"service_id"},
			},
			Interval: "1m",
		}
		_, err := registry.CreateMeasure(context.Background(), measure)
		tester.NoError(err)
	}

	// Test that listing measures for "metrics" group only returns measures from "metrics" group
	metricsResults, err := registry.ListMeasure(context.TODO(), schema.ListOpt{Group: "metrics"})
	tester.NoError(err)
	tester.Equal(2, len(metricsResults), "Should only return measures from 'metrics' group")

	// Verify the returned measures are from the correct group
	for _, measure := range metricsResults {
		tester.Equal("metrics", measure.Metadata.Group, "All returned measures should be from 'metrics' group")
	}

	// Test that listing measures for "metricsTrace" group only returns measures from "metricsTrace" group
	metricsTraceResults, err := registry.ListMeasure(context.TODO(), schema.ListOpt{Group: "metricsTrace"})
	tester.NoError(err)
	tester.Equal(1, len(metricsTraceResults), "Should only return measures from 'metricsTrace' group")
	tester.Equal("metricsTrace", metricsTraceResults[0].Metadata.Group)
	tester.Equal("trace_metrics", metricsTraceResults[0].Metadata.Name)

	// Test that listing measures for "metricsLog" group only returns measures from "metricsLog" group
	metricsLogResults, err := registry.ListMeasure(context.TODO(), schema.ListOpt{Group: "metricsLog"})
	tester.NoError(err)
	tester.Equal(1, len(metricsLogResults), "Should only return measures from 'metricsLog' group")
	tester.Equal("metricsLog", metricsLogResults[0].Metadata.Group)
	tester.Equal("log_metrics", metricsLogResults[0].Metadata.Name)
}

func Test_Etcd_GroupPrefixMatching_EdgeCases(t *testing.T) {
	tester := assert.New(t)
	registry, closer := initServerAndRegister(t)
	defer closer()

	// Test edge cases with various group name patterns
	groups := []string{
		"a",
		"ab",
		"abc",
		"a_",
		"a_b",
		"test",
		"testGroup",
		"testGroupSuffix",
	}

	for _, groupName := range groups {
		group := &commonv1.Group{
			Metadata: &commonv1.Metadata{
				Name: groupName,
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
		err := registry.CreateGroup(context.TODO(), group)
		tester.NoError(err)

		// Create one stream per group
		stream := &databasev1.Stream{
			Metadata: &commonv1.Metadata{
				Name:  "stream_" + groupName,
				Group: groupName,
			},
			TagFamilies: []*databasev1.TagFamilySpec{
				{
					Name: "default",
					Tags: []*databasev1.TagSpec{
						{
							Name: "id",
							Type: databasev1.TagType_TAG_TYPE_STRING,
						},
					},
				},
			},
			Entity: &databasev1.Entity{
				TagNames: []string{"id"},
			},
		}
		_, err = registry.CreateStream(context.Background(), stream)
		tester.NoError(err)
	}

	// Test each group individually to ensure exact matching
	for _, groupName := range groups {
		streams, err := registry.ListStream(context.TODO(), schema.ListOpt{Group: groupName})
		tester.NoError(err)
		tester.Equal(1, len(streams), "Group '%s' should have exactly 1 stream", groupName)
		tester.Equal(groupName, streams[0].Metadata.Group, "Stream should belong to group '%s'", groupName)
		tester.Equal("stream_"+groupName, streams[0].Metadata.Name, "Stream name should match expected pattern")
	}

	// Specifically test the problematic case from the GitHub issue
	// Ensure "a" group doesn't return streams from "ab" or "abc" groups
	aStreams, err := registry.ListStream(context.TODO(), schema.ListOpt{Group: "a"})
	tester.NoError(err)
	tester.Equal(1, len(aStreams), "Group 'a' should have exactly 1 stream")
	tester.Equal("a", aStreams[0].Metadata.Group)

	// Ensure "test" group doesn't return streams from "testGroup" or "testGroupSuffix" groups
	testStreams, err := registry.ListStream(context.TODO(), schema.ListOpt{Group: "test"})
	tester.NoError(err)
	tester.Equal(1, len(testStreams), "Group 'test' should have exactly 1 stream")
	tester.Equal("test", testStreams[0].Metadata.Group)
}
