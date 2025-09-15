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

package storage

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/pkg/meter"
	"github.com/apache/skywalking-banyandb/pkg/run"
	resourceSchema "github.com/apache/skywalking-banyandb/pkg/schema"
)

// MockRetentionService is a mock implementation of RetentionService.
type MockRetentionService struct {
	snapshotError  error
	segmentTimes   map[string]time.Time
	hasSegments    map[string]bool
	deleteResponse map[string]bool
	deleteError    map[string]error
	dataPath       string
	snapshotDir    string
	serviceName    string
	groups         []resourceSchema.Group
}

func NewMockRetentionService() *MockRetentionService {
	return &MockRetentionService{
		dataPath:       "/tmp/test",
		snapshotDir:    "/tmp/test/snapshots",
		serviceName:    "test-service",
		groups:         []resourceSchema.Group{},
		segmentTimes:   make(map[string]time.Time),
		hasSegments:    make(map[string]bool),
		deleteResponse: make(map[string]bool),
		deleteError:    make(map[string]error),
	}
}

func (m *MockRetentionService) GetDataPath() string {
	return m.dataPath
}

func (m *MockRetentionService) GetSnapshotDir() string {
	return m.snapshotDir
}

func (m *MockRetentionService) LoadAllGroups() []resourceSchema.Group {
	return m.groups
}

func (m *MockRetentionService) PeekOldestSegmentEndTimeInGroup(group string) (time.Time, bool) {
	endTime, exists := m.segmentTimes[group]
	hasSegs := m.hasSegments[group]
	return endTime, hasSegs && exists
}

func (m *MockRetentionService) DeleteOldestSegmentInGroup(group string) (bool, error) {
	if err, exists := m.deleteError[group]; exists && err != nil {
		return false, err
	}
	return m.deleteResponse[group], nil
}

func (m *MockRetentionService) CleanupOldSnapshots(_ time.Duration) error {
	return m.snapshotError
}

func (m *MockRetentionService) GetServiceName() string {
	return m.serviceName
}

// Helper methods for setting up mock expectations.
func (m *MockRetentionService) SetGroups(groups []resourceSchema.Group) {
	m.groups = groups
}

func (m *MockRetentionService) SetSegmentTime(group string, endTime time.Time, hasSegments bool) {
	m.segmentTimes[group] = endTime
	m.hasSegments[group] = hasSegments
}

func (m *MockRetentionService) SetDeleteResponse(group string, deleted bool, err error) {
	m.deleteResponse[group] = deleted
	m.deleteError[group] = err
}

func (m *MockRetentionService) SetSnapshotError(err error) {
	m.snapshotError = err
}

// Mock metrics types.
type mockMetricsRegistry struct{}

func (m *mockMetricsRegistry) PreRun(context.Context) error { return nil }
func (m *mockMetricsRegistry) Serve() run.StopNotify        { return nil }
func (m *mockMetricsRegistry) GracefulStop()                {}
func (m *mockMetricsRegistry) Name() string                 { return "mock" }
func (m *mockMetricsRegistry) NativeEnabled() bool          { return false }
func (m *mockMetricsRegistry) With(_ meter.Scope) *observability.Factory {
	return &observability.Factory{}
}

func createMockMetricsRegistry() observability.MetricsRegistry {
	return &mockMetricsRegistry{}
}

// Mock group implementation.
type mockGroup struct {
	name string
}

func (m *mockGroup) GetSchema() *commonv1.Group {
	return &commonv1.Group{
		Metadata: &commonv1.Metadata{
			Name: m.name,
		},
	}
}

func (m *mockGroup) SupplyTSDB() io.Closer {
	return nil
}

func createMockGroup(name string) resourceSchema.Group {
	return &mockGroup{name: name}
}

func TestNewDiskMonitor(t *testing.T) {
	service := NewMockRetentionService()
	config := RetentionConfig{
		HighWatermark: 80.0,
		LowWatermark:  60.0,
		CheckInterval: time.Minute,
		Cooldown:      time.Second * 5,
	}
	registry := createMockMetricsRegistry()

	dm := NewDiskMonitor(service, config, registry)

	assert.NotNil(t, dm)
	assert.Equal(t, service, dm.service)
	assert.Equal(t, config, dm.config)
	assert.NotNil(t, dm.logger)
	assert.NotNil(t, dm.stopCh)
	assert.NotNil(t, dm.metrics)
	assert.Equal(t, 24*time.Hour, dm.snapshotMaxAge)
	assert.False(t, dm.isActive.Load())
}

func TestDiskMonitor_StartStop(t *testing.T) {
	service := NewMockRetentionService()
	config := RetentionConfig{
		HighWatermark: 80.0,
		LowWatermark:  60.0,
		CheckInterval: time.Millisecond * 10, // Short interval for testing
		Cooldown:      time.Millisecond,
	}
	registry := createMockMetricsRegistry()

	dm := NewDiskMonitor(service, config, registry)

	dm.Start()

	// Give it a moment to start
	time.Sleep(time.Millisecond * 20)

	assert.NotNil(t, dm.ticker)

	dm.Stop()

	// Verify cleanup
	assert.False(t, dm.isActive.Load())
}

func TestDiskMonitor_StartWithZeroInterval(t *testing.T) {
	service := NewMockRetentionService()
	config := RetentionConfig{
		HighWatermark: 80.0,
		LowWatermark:  60.0,
		CheckInterval: 0, // Zero interval should disable monitoring
		Cooldown:      time.Second,
	}
	registry := createMockMetricsRegistry()

	dm := NewDiskMonitor(service, config, registry)

	dm.Start()

	// Should not create ticker for zero interval
	assert.Nil(t, dm.ticker)

	dm.Stop()
}

func TestDiskMonitor_findGroupWithOldestSegment(t *testing.T) {
	service := NewMockRetentionService()
	config := RetentionConfig{
		HighWatermark: 80.0,
		LowWatermark:  60.0,
		CheckInterval: time.Minute,
		Cooldown:      time.Second,
	}
	registry := createMockMetricsRegistry()

	dm := NewDiskMonitor(service, config, registry)

	t.Run("no groups", func(t *testing.T) {
		groups := []resourceSchema.Group{}
		result := dm.findGroupWithOldestSegment(groups)
		assert.Empty(t, result)
	})

	t.Run("groups with no segments", func(t *testing.T) {
		group1 := createMockGroup("group1")
		group2 := createMockGroup("group2")
		groups := []resourceSchema.Group{group1, group2}

		service.SetSegmentTime("group1", time.Time{}, false)
		service.SetSegmentTime("group2", time.Time{}, false)

		result := dm.findGroupWithOldestSegment(groups)
		assert.Empty(t, result)
	})

	t.Run("single group with segments", func(t *testing.T) {
		group1 := createMockGroup("group1")
		groups := []resourceSchema.Group{group1}

		endTime := time.Now().Add(-time.Hour)
		service.SetSegmentTime("group1", endTime, true)

		result := dm.findGroupWithOldestSegment(groups)
		assert.Equal(t, "group1", result)
	})

	t.Run("multiple groups with segments - oldest wins", func(t *testing.T) {
		group1 := createMockGroup("group1")
		group2 := createMockGroup("group2")
		group3 := createMockGroup("group3")
		groups := []resourceSchema.Group{group1, group2, group3}

		now := time.Now()
		service.SetSegmentTime("group1", now.Add(-time.Hour), true)      // 1 hour ago
		service.SetSegmentTime("group2", now.Add(-2*time.Hour), true)    // 2 hours ago (oldest)
		service.SetSegmentTime("group3", now.Add(-30*time.Minute), true) // 30 minutes ago

		result := dm.findGroupWithOldestSegment(groups)
		assert.Equal(t, "group2", result)
	})

	t.Run("mixed groups - some with segments, some without", func(t *testing.T) {
		group1 := createMockGroup("group1")
		group2 := createMockGroup("group2")
		group3 := createMockGroup("group3")
		groups := []resourceSchema.Group{group1, group2, group3}

		now := time.Now()
		service.SetSegmentTime("group1", time.Time{}, false)        // No segments
		service.SetSegmentTime("group2", now.Add(-time.Hour), true) // Has segments
		service.SetSegmentTime("group3", time.Time{}, false)        // No segments

		result := dm.findGroupWithOldestSegment(groups)
		assert.Equal(t, "group2", result)
	})
}

func TestDiskMonitor_deleteOldestSegment(t *testing.T) {
	service := NewMockRetentionService()
	config := RetentionConfig{
		HighWatermark: 80.0,
		LowWatermark:  60.0,
		CheckInterval: time.Minute,
		Cooldown:      time.Second,
	}
	registry := createMockMetricsRegistry()

	dm := NewDiskMonitor(service, config, registry)

	t.Run("no groups available", func(t *testing.T) {
		service.SetGroups([]resourceSchema.Group{})

		result := dm.deleteOldestSegment("test-service")
		assert.False(t, result)
	})

	t.Run("successful deletion", func(t *testing.T) {
		group1 := createMockGroup("group1")
		service.SetGroups([]resourceSchema.Group{group1})

		endTime := time.Now().Add(-time.Hour)
		service.SetSegmentTime("group1", endTime, true)
		service.SetDeleteResponse("group1", true, nil)

		result := dm.deleteOldestSegment("test-service")
		assert.True(t, result)
	})

	t.Run("deletion fails with error", func(t *testing.T) {
		group1 := createMockGroup("group1")
		service.SetGroups([]resourceSchema.Group{group1})

		endTime := time.Now().Add(-time.Hour)
		service.SetSegmentTime("group1", endTime, true)
		service.SetDeleteResponse("group1", false, assert.AnError)

		result := dm.deleteOldestSegment("test-service")
		assert.False(t, result)
	})

	t.Run("no segments to delete", func(t *testing.T) {
		group1 := createMockGroup("group1")
		service.SetGroups([]resourceSchema.Group{group1})

		endTime := time.Now().Add(-time.Hour)
		service.SetSegmentTime("group1", endTime, true)
		service.SetDeleteResponse("group1", false, nil) // No error, but nothing deleted

		result := dm.deleteOldestSegment("test-service")
		assert.False(t, result)
	})
}

func TestDiskMonitor_cleanupSnapshots(t *testing.T) {
	service := NewMockRetentionService()
	config := RetentionConfig{
		HighWatermark: 80.0,
		LowWatermark:  60.0,
		CheckInterval: time.Minute,
		Cooldown:      time.Second,
	}
	registry := createMockMetricsRegistry()

	dm := NewDiskMonitor(service, config, registry)

	t.Run("successful snapshot cleanup", func(t *testing.T) {
		service.SetSnapshotError(nil)

		err := dm.cleanupSnapshots("test-service")
		assert.NoError(t, err)
	})

	t.Run("snapshot cleanup fails", func(t *testing.T) {
		service.SetSnapshotError(assert.AnError)

		err := dm.cleanupSnapshots("test-service")
		assert.Error(t, err)
	})
}
