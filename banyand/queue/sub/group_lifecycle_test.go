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

package sub

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	fodcv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/fodc/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

type mockGroupRegistry struct {
	schema.Group
	err    error
	groups []*commonv1.Group
}

func (m *mockGroupRegistry) ListGroup(_ context.Context) ([]*commonv1.Group, error) {
	return m.groups, m.err
}

type mockMetadataRepo struct {
	metadata.Repo
	groupRegistry    *mockGroupRegistry
	dataInfoMap      map[string][]*databasev1.DataInfo
	dataInfoErr      map[string]error
	collectionErrors map[string][]string
	slowGroups       map[string]time.Duration
	collectStarts    atomic.Int32
	concurrentMax    atomic.Int32
	concurrentNow    atomic.Int32
}

func (m *mockMetadataRepo) GroupRegistry() schema.Group {
	return m.groupRegistry
}

func (m *mockMetadataRepo) CollectDataInfo(_ context.Context, group string) ([]*databasev1.DataInfo, []string, error) {
	m.collectStarts.Add(1)
	current := m.concurrentNow.Add(1)
	defer m.concurrentNow.Add(-1)
	for {
		hi := m.concurrentMax.Load()
		if current <= hi || m.concurrentMax.CompareAndSwap(hi, current) {
			break
		}
	}
	if m.slowGroups != nil {
		if d, ok := m.slowGroups[group]; ok && d > 0 {
			time.Sleep(d)
		}
	}
	if m.dataInfoErr != nil {
		if err, ok := m.dataInfoErr[group]; ok {
			return nil, nil, err
		}
	}
	var dataInfo []*databasev1.DataInfo
	if m.dataInfoMap != nil {
		dataInfo = m.dataInfoMap[group]
	}
	var collectionErrs []string
	if m.collectionErrors != nil {
		collectionErrs = m.collectionErrors[group]
	}
	return dataInfo, collectionErrs, nil
}

func TestCatalogToString(t *testing.T) {
	tests := []struct {
		name     string
		expected string
		catalog  commonv1.Catalog
	}{
		{
			name:     "unspecified",
			catalog:  commonv1.Catalog_CATALOG_UNSPECIFIED,
			expected: "CATALOG_UNSPECIFIED",
		},
		{
			name:     "stream",
			catalog:  commonv1.Catalog_CATALOG_STREAM,
			expected: "CATALOG_STREAM",
		},
		{
			name:     "measure",
			catalog:  commonv1.Catalog_CATALOG_MEASURE,
			expected: "CATALOG_MEASURE",
		},
		{
			name:     "unknown value",
			catalog:  commonv1.Catalog(999),
			expected: "UNKNOWN_999",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := catalogToString(tt.catalog)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestInspectAll_NilMetadataRepo(t *testing.T) {
	s := &server{
		log: logger.GetLogger("test"),
	}
	resp, err := s.InspectAll(context.Background(), &fodcv1.InspectAllRequest{})
	require.Error(t, err)
	assert.Nil(t, resp)
	assert.Contains(t, err.Error(), "metadata repository not available")
}

func TestInspectAll_EmptyGroups(t *testing.T) {
	repo := &mockMetadataRepo{
		groupRegistry: &mockGroupRegistry{
			groups: []*commonv1.Group{},
		},
	}
	s := &server{
		log:          logger.GetLogger("test"),
		metadataRepo: repo,
	}
	resp, err := s.InspectAll(context.Background(), &fodcv1.InspectAllRequest{})
	require.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Empty(t, resp.Groups)
}

func TestInspectAll_SkipsNilGroups(t *testing.T) {
	repo := &mockMetadataRepo{
		groupRegistry: &mockGroupRegistry{
			groups: []*commonv1.Group{
				nil,
				{Metadata: nil},
				{
					Metadata: &commonv1.Metadata{Name: "valid_group"},
					Catalog:  commonv1.Catalog_CATALOG_STREAM,
				},
			},
		},
		dataInfoMap: map[string][]*databasev1.DataInfo{
			"valid_group": {},
		},
	}
	s := &server{
		log:          logger.GetLogger("test"),
		metadataRepo: repo,
	}
	resp, err := s.InspectAll(context.Background(), &fodcv1.InspectAllRequest{})
	require.NoError(t, err)
	require.Len(t, resp.Groups, 1)
	assert.Equal(t, "valid_group", resp.Groups[0].Name)
	assert.Equal(t, "CATALOG_STREAM", resp.Groups[0].Catalog)
}

func TestInspectAll_MultipleGroups(t *testing.T) {
	resourceOpts := &commonv1.ResourceOpts{
		ShardNum: 2,
		SegmentInterval: &commonv1.IntervalRule{
			Unit: commonv1.IntervalRule_UNIT_DAY,
			Num:  1,
		},
		Ttl: &commonv1.IntervalRule{
			Unit: commonv1.IntervalRule_UNIT_DAY,
			Num:  7,
		},
	}
	dataInfo := []*databasev1.DataInfo{
		{DataSizeBytes: 1024},
	}
	repo := &mockMetadataRepo{
		groupRegistry: &mockGroupRegistry{
			groups: []*commonv1.Group{
				{
					Metadata:     &commonv1.Metadata{Name: "sw_metric"},
					Catalog:      commonv1.Catalog_CATALOG_MEASURE,
					ResourceOpts: resourceOpts,
				},
				{
					Metadata: &commonv1.Metadata{Name: "sw_record"},
					Catalog:  commonv1.Catalog_CATALOG_STREAM,
				},
			},
		},
		dataInfoMap: map[string][]*databasev1.DataInfo{
			"sw_metric": dataInfo,
			"sw_record": {},
		},
		dataInfoErr: map[string]error{
			"sw_record": fmt.Errorf("collect error"),
		},
	}
	s := &server{
		log:          logger.GetLogger("test"),
		metadataRepo: repo,
	}
	resp, err := s.InspectAll(context.Background(), &fodcv1.InspectAllRequest{})
	require.NoError(t, err)
	require.Len(t, resp.Groups, 2)

	assert.Equal(t, "sw_metric", resp.Groups[0].Name)
	assert.Equal(t, "CATALOG_MEASURE", resp.Groups[0].Catalog)
	assert.Equal(t, resourceOpts, resp.Groups[0].ResourceOpts)
	assert.Equal(t, dataInfo, resp.Groups[0].DataInfo)

	assert.Equal(t, "sw_record", resp.Groups[1].Name)
	assert.Equal(t, "CATALOG_STREAM", resp.Groups[1].Catalog)
	assert.Nil(t, resp.Groups[1].DataInfo)
}

func TestInspectAll_RunsGroupsInParallel(t *testing.T) {
	const groupCount = 8
	const sleep = 80 * time.Millisecond
	groups := make([]*commonv1.Group, 0, groupCount)
	slow := make(map[string]time.Duration, groupCount)
	for i := range groupCount {
		name := fmt.Sprintf("g_%d", i)
		groups = append(groups, &commonv1.Group{
			Metadata: &commonv1.Metadata{Name: name},
			Catalog:  commonv1.Catalog_CATALOG_STREAM,
		})
		slow[name] = sleep
	}
	repo := &mockMetadataRepo{
		groupRegistry: &mockGroupRegistry{groups: groups},
		slowGroups:    slow,
	}
	s := &server{
		log:          logger.GetLogger("test"),
		metadataRepo: repo,
	}

	start := time.Now()
	resp, err := s.InspectAll(context.Background(), &fodcv1.InspectAllRequest{})
	elapsed := time.Since(start)
	require.NoError(t, err)
	require.Len(t, resp.Groups, groupCount)

	// Sequential execution would take groupCount*sleep; parallel must finish
	// in roughly one sleep interval. Allow 4x headroom for CI jitter.
	assert.Less(t, elapsed, 4*sleep, "InspectAll must run groups in parallel; total wall time was %s", elapsed)
	assert.Equal(t, int32(groupCount), repo.collectStarts.Load())
	assert.GreaterOrEqual(t, repo.concurrentMax.Load(), int32(2),
		"at least two CollectDataInfo invocations must overlap; observed max concurrency was %d",
		repo.concurrentMax.Load())
	for _, g := range resp.Groups {
		assert.Empty(t, g.Errors, "successful group %s must not carry errors", g.Name)
	}
}

// TestInspectAll_SurfacesTopLevelError asserts that a top-level
// CollectDataInfo error is reported through GroupLifecycleInfo.errors with
// a "top-level:" prefix and that DataInfo stays empty for that group.
func TestInspectAll_SurfacesTopLevelError(t *testing.T) {
	repo := &mockMetadataRepo{
		groupRegistry: &mockGroupRegistry{
			groups: []*commonv1.Group{
				{Metadata: &commonv1.Metadata{Name: "ok_group"}, Catalog: commonv1.Catalog_CATALOG_MEASURE},
				{Metadata: &commonv1.Metadata{Name: "bad_group"}, Catalog: commonv1.Catalog_CATALOG_STREAM},
			},
		},
		dataInfoMap: map[string][]*databasev1.DataInfo{
			"ok_group": {{DataSizeBytes: 1024}},
		},
		dataInfoErr: map[string]error{
			"bad_group": fmt.Errorf("metadataRepo: simulated load failure"),
		},
	}
	s := &server{log: logger.GetLogger("test"), metadataRepo: repo}

	resp, err := s.InspectAll(context.Background(), &fodcv1.InspectAllRequest{})
	require.NoError(t, err)
	require.Len(t, resp.Groups, 2)

	byName := map[string]*fodcv1.GroupLifecycleInfo{}
	for _, g := range resp.Groups {
		byName[g.Name] = g
	}

	ok := byName["ok_group"]
	require.NotNil(t, ok)
	assert.Empty(t, ok.Errors, "ok_group must not carry errors")
	assert.NotEmpty(t, ok.DataInfo)

	bad := byName["bad_group"]
	require.NotNil(t, bad)
	assert.Empty(t, bad.DataInfo, "failed group must not expose stale DataInfo")
	require.NotEmpty(t, bad.Errors, "failed group must surface the CollectDataInfo error")
	assert.Contains(t, bad.Errors[0], "top-level:",
		"top-level failures must be tagged with the top-level prefix")
	assert.Contains(t, bad.Errors[0], "simulated load failure",
		"the original error message must be preserved")
}

// TestInspectAll_PropagatesPartialFailureErrors asserts that per-node
// failures reported by mockMetadataRepo.collectionErrors are passed
// through to GroupLifecycleInfo.errors for the originating group, while
// DataInfo still carries the entries from nodes that succeeded.
func TestInspectAll_PropagatesPartialFailureErrors(t *testing.T) {
	repo := &mockMetadataRepo{
		groupRegistry: &mockGroupRegistry{
			groups: []*commonv1.Group{
				{Metadata: &commonv1.Metadata{Name: "partial_group"}, Catalog: commonv1.Catalog_CATALOG_MEASURE},
			},
		},
		dataInfoMap: map[string][]*databasev1.DataInfo{
			"partial_group": {{DataSizeBytes: 512}},
		},
		collectionErrors: map[string][]string{
			"partial_group": {
				"future error: rpc error: nil pointer dereference",
				"node error: cold-0 panic",
			},
		},
	}
	s := &server{log: logger.GetLogger("test"), metadataRepo: repo}

	resp, err := s.InspectAll(context.Background(), &fodcv1.InspectAllRequest{})
	require.NoError(t, err)
	require.Len(t, resp.Groups, 1)
	g := resp.Groups[0]
	assert.NotEmpty(t, g.DataInfo, "partial success must still expose the DataInfo entries it did get")
	assert.Equal(t,
		[]string{
			"future error: rpc error: nil pointer dereference",
			"node error: cold-0 panic",
		},
		g.Errors,
		"per-node collection errors must be passed through to GroupLifecycleInfo.errors")
	for _, e := range g.Errors {
		assert.NotContains(t, e, "top-level:",
			"partial-success errors must not carry the top-level: prefix")
	}
}
