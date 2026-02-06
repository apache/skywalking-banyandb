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

package property

import (
	"context"
	"hash/crc32"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
)

var syncInterval = 5 * time.Second

func TestSchemaRegistry_HandleDeletion(t *testing.T) {
	registry, registryErr := NewSchemaRegistryClient(&ClientConfig{GRPCTimeout: 5 * time.Second, SyncInterval: syncInterval})
	require.NoError(t, registryErr)
	defer registry.Close()

	handler := newMockEventHandler()
	registry.addHandler(schema.KindStream, handler)

	entry := cacheEntry{
		latestUpdateAt: 100,
		kind:           schema.KindStream,
		group:          "test-group",
		name:           "test-stream",
	}
	propID := "stream_test-group/test-stream"
	registry.cache.Update(propID, &entry)

	registry.handleDeletion(schema.KindStream, propID, &entry, 100)

	assert.Equal(t, int32(1), handler.deleteCalled.Load())
	handler.mu.Lock()
	assert.Equal(t, "test-stream", handler.lastDeleteMeta.Name)
	assert.Equal(t, "test-group", handler.lastDeleteMeta.Group)
	assert.Equal(t, schema.KindStream, handler.lastDeleteMeta.Kind)
	assert.Equal(t, int64(100), handler.lastDeleteMeta.ModRevision)
	assert.Nil(t, handler.lastDeleteMeta.Spec)
	handler.mu.Unlock()

	allEntries := registry.cache.GetAllEntries()
	assert.NotContains(t, allEntries, propID)
}

func TestSchemaRegistry_HandleDeletion_NotInCache(t *testing.T) {
	registry, registryErr := NewSchemaRegistryClient(&ClientConfig{GRPCTimeout: 5 * time.Second, SyncInterval: syncInterval})
	require.NoError(t, registryErr)
	defer registry.Close()

	handler := newMockEventHandler()
	registry.addHandler(schema.KindStream, handler)

	entry := cacheEntry{
		latestUpdateAt: 100,
		kind:           schema.KindStream,
		group:          "test-group",
		name:           "test-stream",
	}
	propID := "stream_test-group/test-stream"

	registry.handleDeletion(schema.KindStream, propID, &entry, 100)

	assert.Equal(t, int32(0), handler.deleteCalled.Load())
}

func TestSchemaRegistry_HandleDeletion_OlderRevision(t *testing.T) {
	registry, registryErr := NewSchemaRegistryClient(&ClientConfig{GRPCTimeout: 5 * time.Second, SyncInterval: syncInterval})
	require.NoError(t, registryErr)
	defer registry.Close()

	handler := newMockEventHandler()
	registry.addHandler(schema.KindStream, handler)

	entry := cacheEntry{
		latestUpdateAt: 200,
		kind:           schema.KindStream,
		group:          "test-group",
		name:           "test-stream",
	}
	propID := "stream_test-group/test-stream"
	registry.cache.Update(propID, &entry)

	// Try to delete with an older revision — should be rejected
	olderEntry := cacheEntry{
		latestUpdateAt: 50,
		kind:           schema.KindStream,
		group:          "test-group",
		name:           "test-stream",
	}
	registry.handleDeletion(schema.KindStream, propID, &olderEntry, 50)

	assert.Equal(t, int32(0), handler.deleteCalled.Load())
	allEntries := registry.cache.GetAllEntries()
	assert.Contains(t, allEntries, propID)
}

func TestParsePropertyID(t *testing.T) {
	tests := []struct {
		name          string
		propID        string
		expectedGroup string
		expectedName  string
		expectedKind  schema.Kind
	}{
		{
			name:          "stream with group",
			propID:        "stream_mygroup/mystream",
			expectedKind:  schema.KindStream,
			expectedGroup: "mygroup",
			expectedName:  "mystream",
		},
		{
			name:          "measure with group",
			propID:        "measure_metrics/cpu",
			expectedKind:  schema.KindMeasure,
			expectedGroup: "metrics",
			expectedName:  "cpu",
		},
		{
			name:          "group without group field",
			propID:        "group_mygroup",
			expectedKind:  schema.KindGroup,
			expectedGroup: "",
			expectedName:  "mygroup",
		},
		{
			name:          "indexRule with group",
			propID:        "indexRule_default/rule1",
			expectedKind:  schema.KindIndexRule,
			expectedGroup: "default",
			expectedName:  "rule1",
		},
		{
			name:          "invalid - no underscore",
			propID:        "invalidformat",
			expectedKind:  0,
			expectedGroup: "",
			expectedName:  "",
		},
		{
			name:          "invalid - unknown kind",
			propID:        "unknown_group/name",
			expectedKind:  0,
			expectedGroup: "",
			expectedName:  "",
		},
		{
			name:          "empty string",
			propID:        "",
			expectedKind:  0,
			expectedGroup: "",
			expectedName:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kind, group, name := parsePropertyID(tt.propID)
			assert.Equal(t, tt.expectedKind, kind)
			assert.Equal(t, tt.expectedGroup, group)
			assert.Equal(t, tt.expectedName, name)
		})
	}
}

func TestBuildUpdatedAtCriteria(t *testing.T) {
	criteria := buildUpdatedAtCriteria(0)
	assert.Nil(t, criteria, "sinceRevision 0 should return nil")

	criteria = buildUpdatedAtCriteria(-1)
	assert.Nil(t, criteria, "sinceRevision -1 should return nil")

	criteria = buildUpdatedAtCriteria(100)
	require.NotNil(t, criteria)
	condition := criteria.GetCondition()
	require.NotNil(t, condition)
	assert.Equal(t, TagKeyUpdatedAt, condition.Name)
	assert.Equal(t, modelv1.Condition_BINARY_OP_GT, condition.Op)
	assert.Equal(t, int64(100), condition.Value.GetInt().GetValue())

	criteria = buildUpdatedAtCriteria(999999)
	require.NotNil(t, criteria)
	assert.Equal(t, int64(999999), criteria.GetCondition().Value.GetInt().GetValue())
}

func TestGetGroupFromTags(t *testing.T) {
	tags := []*modelv1.Tag{
		{Key: TagKeyGroup, Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "test-group"}}}},
		{Key: TagKeyName, Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "test-name"}}}},
	}
	group := getGroupFromTags(tags)
	assert.Equal(t, "test-group", group)

	tags = []*modelv1.Tag{
		{Key: TagKeyName, Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "test-name"}}}},
	}
	group = getGroupFromTags(tags)
	assert.Equal(t, "", group)

	group = getGroupFromTags(nil)
	assert.Equal(t, "", group)

	group = getGroupFromTags([]*modelv1.Tag{})
	assert.Equal(t, "", group)
}

func TestGetNameFromTags(t *testing.T) {
	tags := []*modelv1.Tag{
		{Key: TagKeyGroup, Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "test-group"}}}},
		{Key: TagKeyName, Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "test-name"}}}},
	}
	name := getNameFromTags(tags)
	assert.Equal(t, "test-name", name)

	tags = []*modelv1.Tag{
		{Key: TagKeyGroup, Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "test-group"}}}},
	}
	name = getNameFromTags(tags)
	assert.Equal(t, "", name)

	name = getNameFromTags(nil)
	assert.Equal(t, "", name)
}

func TestNewSchemaRegistry(t *testing.T) {
	registry, registryErr := NewSchemaRegistryClient(&ClientConfig{GRPCTimeout: 5 * time.Second, SyncInterval: syncInterval})
	require.NoError(t, registryErr)
	require.NotNil(t, registry)
	assert.NotNil(t, registry.nodesClient)
	assert.NotNil(t, registry.handlers)
	assert.NotNil(t, registry.cache)
	assert.NotNil(t, registry.closer)
	assert.Equal(t, syncInterval, registry.syncInterval)

	closeErr := registry.Close()
	assert.NoError(t, closeErr)
}

type mockEventHandler struct {
	initCalled          *atomic.Int32
	addOrUpdateCalled   *atomic.Int32
	deleteCalled        *atomic.Int32
	lastAddOrUpdateMeta schema.Metadata
	lastDeleteMeta      schema.Metadata
	mu                  sync.Mutex
}

func newMockEventHandler() *mockEventHandler {
	return &mockEventHandler{
		initCalled:        &atomic.Int32{},
		addOrUpdateCalled: &atomic.Int32{},
		deleteCalled:      &atomic.Int32{},
	}
}

func (m *mockEventHandler) OnInit(_ []schema.Kind) (bool, []int64) {
	m.initCalled.Add(1)
	return false, nil
}

func (m *mockEventHandler) OnAddOrUpdate(md schema.Metadata) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.addOrUpdateCalled.Add(1)
	m.lastAddOrUpdateMeta = md
}

func (m *mockEventHandler) OnDelete(md schema.Metadata) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deleteCalled.Add(1)
	m.lastDeleteMeta = md
}

func TestSchemaRegistry_RegisterHandler(t *testing.T) {
	registry, registryErr := NewSchemaRegistryClient(&ClientConfig{GRPCTimeout: 5 * time.Second, SyncInterval: syncInterval})
	require.NoError(t, registryErr)
	defer registry.Close()

	handler := newMockEventHandler()

	registry.RegisterHandler("test", schema.KindStream, handler)
	assert.Equal(t, int32(1), handler.initCalled.Load())

	registry.mu.RLock()
	handlers := registry.handlers[schema.KindStream]
	registry.mu.RUnlock()
	assert.Len(t, handlers, 1)
}

func TestSchemaRegistry_RegisterHandler_MultipleKinds(t *testing.T) {
	registry, registryErr := NewSchemaRegistryClient(&ClientConfig{GRPCTimeout: 5 * time.Second, SyncInterval: syncInterval})
	require.NoError(t, registryErr)
	defer registry.Close()

	handler := newMockEventHandler()

	registry.RegisterHandler("test", schema.KindStream|schema.KindMeasure, handler)
	assert.Equal(t, int32(1), handler.initCalled.Load())

	registry.mu.RLock()
	streamHandlers := registry.handlers[schema.KindStream]
	measureHandlers := registry.handlers[schema.KindMeasure]
	registry.mu.RUnlock()
	assert.Len(t, streamHandlers, 1)
	assert.Len(t, measureHandlers, 1)
}

func TestSchemaRegistry_OnInit(t *testing.T) {
	registry, registryErr := NewSchemaRegistryClient(&ClientConfig{GRPCTimeout: 5 * time.Second, SyncInterval: syncInterval})
	require.NoError(t, registryErr)
	defer registry.Close()

	ok, revisions := registry.OnInit([]schema.Kind{schema.KindStream, schema.KindMeasure})
	assert.False(t, ok)
	assert.Nil(t, revisions)
}

func TestSchemaRegistry_AddHandler(t *testing.T) {
	registry, registryErr := NewSchemaRegistryClient(&ClientConfig{GRPCTimeout: 5 * time.Second, SyncInterval: syncInterval})
	require.NoError(t, registryErr)
	defer registry.Close()

	handler1 := newMockEventHandler()
	handler2 := newMockEventHandler()

	registry.addHandler(schema.KindStream, handler1)
	registry.addHandler(schema.KindStream, handler2)

	registry.mu.RLock()
	handlers := registry.handlers[schema.KindStream]
	registry.mu.RUnlock()
	assert.Len(t, handlers, 2)
}

func TestSchemaRegistry_NotifyHandlers(t *testing.T) {
	registry, registryErr := NewSchemaRegistryClient(&ClientConfig{GRPCTimeout: 5 * time.Second, SyncInterval: syncInterval})
	require.NoError(t, registryErr)
	defer registry.Close()

	handler := newMockEventHandler()
	registry.addHandler(schema.KindStream, handler)

	md := schema.Metadata{
		TypeMeta: schema.TypeMeta{
			Kind:  schema.KindStream,
			Name:  "test-stream",
			Group: "test-group",
		},
	}

	registry.notifyHandlers(schema.KindStream, md, false)
	assert.Equal(t, int32(1), handler.addOrUpdateCalled.Load())
	handler.mu.Lock()
	assert.Equal(t, "test-stream", handler.lastAddOrUpdateMeta.Name)
	handler.mu.Unlock()

	registry.notifyHandlers(schema.KindStream, md, true)
	assert.Equal(t, int32(1), handler.deleteCalled.Load())
	handler.mu.Lock()
	assert.Equal(t, "test-stream", handler.lastDeleteMeta.Name)
	handler.mu.Unlock()
}

func TestSchemaRegistry_Close(t *testing.T) {
	registry, registryErr := NewSchemaRegistryClient(&ClientConfig{GRPCTimeout: 5 * time.Second, SyncInterval: syncInterval})
	require.NoError(t, registryErr)

	closeErr := registry.Close()
	assert.NoError(t, closeErr)
}

func TestSchemaRegistry_CreateIndexRule_GeneratesCRC32ID(t *testing.T) {
	registry, registryErr := NewSchemaRegistryClient(&ClientConfig{GRPCTimeout: 5 * time.Second, SyncInterval: syncInterval})
	require.NoError(t, registryErr)
	defer registry.Close()

	indexRule := &databasev1.IndexRule{
		Metadata: &commonv1.Metadata{
			Group: "test-group",
			Name:  "test-rule",
		},
		Tags: []string{"tag1"},
		Type: databasev1.IndexRule_TYPE_INVERTED,
	}
	// CreateIndexRule will fail because no servers are available,
	// but the ID should be generated before validation runs.
	_ = registry.CreateIndexRule(context.Background(), indexRule)

	expectedID := crc32.ChecksumIEEE([]byte("test-grouptest-rule"))
	assert.Equal(t, expectedID, indexRule.Metadata.Id)
}

func TestSchemaRegistry_CreateIndexRule_PreservesExistingID(t *testing.T) {
	registry, registryErr := NewSchemaRegistryClient(&ClientConfig{GRPCTimeout: 5 * time.Second, SyncInterval: syncInterval})
	require.NoError(t, registryErr)
	defer registry.Close()

	existingID := uint32(12345)
	indexRule := &databasev1.IndexRule{
		Metadata: &commonv1.Metadata{
			Group: "test-group",
			Name:  "test-rule",
			Id:    existingID,
		},
		Tags: []string{"tag1"},
		Type: databasev1.IndexRule_TYPE_INVERTED,
	}
	// Will fail due to no servers, but ID should remain unchanged
	_ = registry.CreateIndexRule(context.Background(), indexRule)

	assert.Equal(t, existingID, indexRule.Metadata.Id)
}

func TestSchemaRegistry_CreateStream_ValidationError(t *testing.T) {
	registry, registryErr := NewSchemaRegistryClient(&ClientConfig{GRPCTimeout: 5 * time.Second, SyncInterval: syncInterval})
	require.NoError(t, registryErr)
	defer registry.Close()

	_, createErr := registry.CreateStream(context.Background(), &databasev1.Stream{})
	assert.Error(t, createErr)
	assert.Contains(t, createErr.Error(), "stream metadata is nil")
}

func TestSchemaRegistry_UpdateStream_ValidationError(t *testing.T) {
	registry, registryErr := NewSchemaRegistryClient(&ClientConfig{GRPCTimeout: 5 * time.Second, SyncInterval: syncInterval})
	require.NoError(t, registryErr)
	defer registry.Close()

	_, updateErr := registry.UpdateStream(context.Background(), &databasev1.Stream{
		Metadata: &commonv1.Metadata{Name: "test"},
	})
	assert.Error(t, updateErr)
	assert.Contains(t, updateErr.Error(), "stream group is empty")
}

func TestSchemaRegistry_CreateMeasure_ValidationError(t *testing.T) {
	registry, registryErr := NewSchemaRegistryClient(&ClientConfig{GRPCTimeout: 5 * time.Second, SyncInterval: syncInterval})
	require.NoError(t, registryErr)
	defer registry.Close()

	_, createErr := registry.CreateMeasure(context.Background(), &databasev1.Measure{})
	assert.Error(t, createErr)
	assert.Contains(t, createErr.Error(), "measure metadata is nil")
}

func TestSchemaRegistry_CreateTrace_ValidationError(t *testing.T) {
	registry, registryErr := NewSchemaRegistryClient(&ClientConfig{GRPCTimeout: 5 * time.Second, SyncInterval: syncInterval})
	require.NoError(t, registryErr)
	defer registry.Close()

	_, createErr := registry.CreateTrace(context.Background(), &databasev1.Trace{})
	assert.Error(t, createErr)
	assert.Contains(t, createErr.Error(), "trace metadata is nil")
}

func TestSchemaRegistry_CreateGroup_ValidationError(t *testing.T) {
	registry, registryErr := NewSchemaRegistryClient(&ClientConfig{GRPCTimeout: 5 * time.Second, SyncInterval: syncInterval})
	require.NoError(t, registryErr)
	defer registry.Close()

	createErr := registry.CreateGroup(context.Background(), &commonv1.Group{})
	assert.Error(t, createErr)
	assert.Contains(t, createErr.Error(), "metadata is required")
}

func TestSchemaRegistry_CreateIndexRule_ValidationError(t *testing.T) {
	registry, registryErr := NewSchemaRegistryClient(&ClientConfig{GRPCTimeout: 5 * time.Second, SyncInterval: syncInterval})
	require.NoError(t, registryErr)
	defer registry.Close()

	createErr := registry.CreateIndexRule(context.Background(), &databasev1.IndexRule{})
	assert.Error(t, createErr)
	assert.Contains(t, createErr.Error(), "indexRule metadata is nil")
}

func TestSchemaRegistry_CreateIndexRuleBinding_ValidationError(t *testing.T) {
	registry, registryErr := NewSchemaRegistryClient(&ClientConfig{GRPCTimeout: 5 * time.Second, SyncInterval: syncInterval})
	require.NoError(t, registryErr)
	defer registry.Close()

	createErr := registry.CreateIndexRuleBinding(context.Background(), &databasev1.IndexRuleBinding{})
	assert.Error(t, createErr)
	assert.Contains(t, createErr.Error(), "indexRuleBinding metadata is nil")
}

func TestSchemaRegistry_CreateTopNAggregation_ValidationError(t *testing.T) {
	registry, registryErr := NewSchemaRegistryClient(&ClientConfig{GRPCTimeout: 5 * time.Second, SyncInterval: syncInterval})
	require.NoError(t, registryErr)
	defer registry.Close()

	createErr := registry.CreateTopNAggregation(context.Background(), &databasev1.TopNAggregation{})
	assert.Error(t, createErr)
	assert.Contains(t, createErr.Error(), "topNAggregation metadata is nil")
}
