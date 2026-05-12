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

package lifecycle

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	fodcv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/fodc/v1"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

func initTestLogger(t *testing.T) *logger.Logger {
	t.Helper()
	err := logger.Init(logger.Logging{Env: "dev", Level: "debug"})
	require.NoError(t, err)
	return logger.GetLogger("test", "lifecycle")
}

// fakeLifecycleService is a programmable test double for GroupLifecycleService.InspectAll.
// Each registered behavior is consumed in order; the call counter lets tests assert how
// many times Collect actually reached the gRPC server (i.e. whether the cache served the
// request without an RPC).
type fakeLifecycleService struct {
	fodcv1.UnimplementedGroupLifecycleServiceServer
	behaviors []func() (*fodcv1.InspectAllResponse, error)
	mu        sync.Mutex
	callCount int
}

func (f *fakeLifecycleService) InspectAll(_ context.Context, _ *fodcv1.InspectAllRequest) (*fodcv1.InspectAllResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.callCount >= len(f.behaviors) {
		f.callCount++
		return nil, status.Error(codes.Internal, "fakeLifecycleService: behavior list exhausted")
	}
	behavior := f.behaviors[f.callCount]
	f.callCount++
	return behavior()
}

func (f *fakeLifecycleService) calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.callCount
}

func returnGroups(groups []*fodcv1.GroupLifecycleInfo) func() (*fodcv1.InspectAllResponse, error) {
	return func() (*fodcv1.InspectAllResponse, error) {
		return &fodcv1.InspectAllResponse{Groups: groups}, nil
	}
}

func returnError(code codes.Code, msg string) func() (*fodcv1.InspectAllResponse, error) {
	return func() (*fodcv1.InspectAllResponse, error) {
		return nil, status.Error(code, msg)
	}
}

// startLocalServer spins up a real gRPC server on a random localhost port hosting the
// supplied fake and returns the listen address. The server is torn down via t.Cleanup.
func startLocalServer(t *testing.T, fake *fakeLifecycleService) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := grpc.NewServer()
	fodcv1.RegisterGroupLifecycleServiceServer(srv, fake)
	go func() {
		_ = srv.Serve(lis)
	}()
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

// newCollectorForFake constructs a Collector wired to a fresh local gRPC server backed by fake.
func newCollectorForFake(t *testing.T, fake *fakeLifecycleService, cacheTTL time.Duration) *Collector {
	t.Helper()
	addr := startLocalServer(t, fake)
	return NewCollector(initTestLogger(t), addr, t.TempDir(), cacheTTL)
}

func sampleGroup(name string) *fodcv1.GroupLifecycleInfo {
	return &fodcv1.GroupLifecycleInfo{Name: name}
}

func TestNewCollector(t *testing.T) {
	log := initTestLogger(t)
	collector := NewCollector(log, "", "", 10*time.Minute)
	require.NotNil(t, collector)
	assert.Nil(t, collector.currentData)
	assert.Equal(t, 10*time.Minute, collector.cacheTTL)
}

func TestCollector_ReadReportFiles_EmptyDir(t *testing.T) {
	log := initTestLogger(t)
	collector := NewCollector(log, "", t.TempDir(), 0)
	reports := collector.readReportFiles()
	assert.Empty(t, reports)
}

func TestCollector_Collect_NoGRPC(t *testing.T) {
	log := initTestLogger(t)
	collector := NewCollector(log, "", "", 0)
	data, err := collector.Collect(t.Context())
	require.NoError(t, err)
	require.NotNil(t, data)
	assert.NotNil(t, collector.currentData)
}

func TestCollector_CacheTTL(t *testing.T) {
	log := initTestLogger(t)
	collector := NewCollector(log, "", "", 5*time.Minute)
	ctx := t.Context()

	now := time.Now()
	collector.nowFunc = func() time.Time { return now }

	// First call should collect and cache
	data1, err := collector.Collect(ctx)
	require.NoError(t, err)
	require.NotNil(t, data1)

	// Second call within TTL should return cached data
	now = now.Add(2 * time.Minute)
	data2, err := collector.Collect(ctx)
	require.NoError(t, err)
	assert.Same(t, data1, data2)

	// After TTL expires, should re-collect
	now = now.Add(4 * time.Minute) // total 6 minutes > 5 minute TTL
	data3, err := collector.Collect(ctx)
	require.NoError(t, err)
	require.NotNil(t, data3)
	assert.NotSame(t, data1, data3)
}

func TestCollector_ZeroTTL_AlwaysRefreshes(t *testing.T) {
	log := initTestLogger(t)
	collector := NewCollector(log, "", "", 0)
	ctx := t.Context()

	data1, err := collector.Collect(ctx)
	require.NoError(t, err)
	require.NotNil(t, data1)

	data2, err := collector.Collect(ctx)
	require.NoError(t, err)
	require.NotNil(t, data2)
	assert.NotSame(t, data1, data2)
}

func TestCollector_ReadReportFiles_MaxFiles(t *testing.T) {
	log := initTestLogger(t)
	dir := t.TempDir()
	collector := NewCollector(log, "", dir, 0)

	for idx, name := range []string{
		"2026-03-20.json",
		"2026-03-21.json",
		"2026-03-22.json",
		"2026-03-23.json",
		"2026-03-24.json",
		"2026-03-25.json",
		"2026-03-26.json",
	} {
		require.NoError(t, os.WriteFile(
			filepath.Join(dir, name),
			[]byte(`{"idx":`+string(rune('0'+idx))+`}`),
			0o600,
		))
	}

	reports := collector.readReportFiles()
	require.Equal(t, maxReportFiles, len(reports))

	// Should be the 5 most recent (sorted descending by name)
	assert.Equal(t, "2026-03-26.json", reports[0].Filename)
	assert.Equal(t, "2026-03-25.json", reports[1].Filename)
	assert.Equal(t, "2026-03-24.json", reports[2].Filename)
	assert.Equal(t, "2026-03-23.json", reports[3].Filename)
	assert.Equal(t, "2026-03-22.json", reports[4].Filename)
}

func TestCollector_ReadReportFiles_SkipsNonJSON(t *testing.T) {
	log := initTestLogger(t)
	dir := t.TempDir()
	collector := NewCollector(log, "", dir, 0)

	require.NoError(t, os.WriteFile(filepath.Join(dir, "2026-03-26.json"), []byte(`{}`), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "readme.txt"), []byte("text"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "data.csv"), []byte("a,b"), 0o600))

	reports := collector.readReportFiles()
	require.Equal(t, 1, len(reports))
	assert.Equal(t, "2026-03-26.json", reports[0].Filename)
}

func TestCollector_ReadReportFiles_OversizedFile(t *testing.T) {
	log := initTestLogger(t)
	dir := t.TempDir()
	collector := NewCollector(log, "", dir, 0)

	require.NoError(t, os.WriteFile(filepath.Join(dir, "2026-03-26.json"), []byte(`{"ok":true}`), 0o600))
	oversized := make([]byte, maxReportFileSize+1)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "2026-03-25.json"), oversized, 0o600))

	reports := collector.readReportFiles()
	require.Equal(t, 1, len(reports))
	assert.Equal(t, "2026-03-26.json", reports[0].Filename)
}

func TestCollectGroups_NoGRPCAddrShortCircuits(t *testing.T) {
	log := initTestLogger(t)
	collector := NewCollector(log, "", "", 0)
	groups, err := collector.collectGroups(t.Context())
	require.NoError(t, err)
	assert.Nil(t, groups)
}

func TestCollectGroups_UnimplementedSetsFlagAndCachesNil(t *testing.T) {
	fake := &fakeLifecycleService{
		behaviors: []func() (*fodcv1.InspectAllResponse, error){
			returnError(codes.Unimplemented, "not supported"),
		},
	}
	collector := newCollectorForFake(t, fake, time.Minute)

	groups, err := collector.collectGroups(t.Context())
	require.NoError(t, err)
	assert.Nil(t, groups)
	assert.True(t, collector.grpcUnimplemented.Load())

	// Subsequent direct calls short-circuit without touching the server.
	groups2, err := collector.collectGroups(t.Context())
	require.NoError(t, err)
	assert.Nil(t, groups2)
	assert.Equal(t, 1, fake.calls())
}

func TestCollectGroups_NilResponseGroupsBecomesEmptySlice(t *testing.T) {
	fake := &fakeLifecycleService{
		behaviors: []func() (*fodcv1.InspectAllResponse, error){
			returnGroups(nil),
		},
	}
	collector := newCollectorForFake(t, fake, time.Minute)

	groups, err := collector.collectGroups(t.Context())
	require.NoError(t, err)
	require.NotNil(t, groups, "successful InspectAll with empty groups must return non-nil empty slice, not nil")
	assert.Empty(t, groups)
}

func TestCollect_CachesSuccessfulResult(t *testing.T) {
	fake := &fakeLifecycleService{
		behaviors: []func() (*fodcv1.InspectAllResponse, error){
			returnGroups([]*fodcv1.GroupLifecycleInfo{sampleGroup("g1"), sampleGroup("g2")}),
		},
	}
	collector := newCollectorForFake(t, fake, time.Minute)
	ctx := t.Context()

	data1, err := collector.Collect(ctx)
	require.NoError(t, err)
	require.Len(t, data1.Groups, 2)

	// With zero TTL, every call should re-collect
	data2, err := collector.Collect(ctx)
	require.NoError(t, err)
	assert.Same(t, data1, data2, "second Collect within TTL must return the cached pointer")
	assert.Equal(t, 1, fake.calls(), "InspectAll must not be invoked on cache hit")
}

func TestCollect_DoesNotCacheTransientFailure(t *testing.T) {
	fake := &fakeLifecycleService{
		behaviors: []func() (*fodcv1.InspectAllResponse, error){
			returnError(codes.DeadlineExceeded, "simulated timeout"),
			returnGroups([]*fodcv1.GroupLifecycleInfo{sampleGroup("g1")}),
		},
	}
	collector := newCollectorForFake(t, fake, 10*time.Minute)
	ctx := t.Context()

	data1, err := collector.Collect(ctx)
	require.Error(t, err, "transient failure must propagate to caller")
	assert.Nil(t, data1, "no LifecycleData on failure")

	collector.mu.RLock()
	cachedData := collector.currentData
	cachedTime := collector.lastCollectTime
	collector.mu.RUnlock()
	assert.Nil(t, cachedData, "transient failure must not write to cache")
	assert.True(t, cachedTime.IsZero(), "lastCollectTime must remain zero after transient failure")

	data2, err := collector.Collect(ctx)
	require.NoError(t, err)
	require.Len(t, data2.Groups, 1, "next call must retry InspectAll, not return stale empty")
	assert.Equal(t, "g1", data2.Groups[0].Name)
	assert.Equal(t, 2, fake.calls(), "InspectAll must be invoked twice")
}

func TestCollect_FailurePropagatesError(t *testing.T) {
	successGroups := []*fodcv1.GroupLifecycleInfo{sampleGroup("g1"), sampleGroup("g2"), sampleGroup("g3")}
	fake := &fakeLifecycleService{
		behaviors: []func() (*fodcv1.InspectAllResponse, error){
			returnGroups(successGroups),
			returnError(codes.DeadlineExceeded, "simulated timeout"),
		},
	}
	collector := newCollectorForFake(t, fake, 0) // zero TTL forces both calls to hit the server
	ctx := t.Context()

	data1, err := collector.Collect(ctx)
	require.NoError(t, err)
	require.Len(t, data1.Groups, 3)

	data2, err := collector.Collect(ctx)
	require.Error(t, err, "transient failure after a successful collect must still surface as an error")
	assert.Nil(t, data2, "caller must not receive a LifecycleData payload on failure")
	assert.Equal(t, 2, fake.calls())
}

func TestCollect_TransientFailureDoesNotAdvanceLastCollectTime(t *testing.T) {
	fake := &fakeLifecycleService{
		behaviors: []func() (*fodcv1.InspectAllResponse, error){
			returnGroups([]*fodcv1.GroupLifecycleInfo{sampleGroup("g1")}),
			returnError(codes.Unavailable, "simulated outage"),
			returnGroups([]*fodcv1.GroupLifecycleInfo{sampleGroup("g1"), sampleGroup("g2")}),
		},
	}
	collector := newCollectorForFake(t, fake, time.Hour)
	ctx := t.Context()

	now := time.Unix(1_000_000_000, 0)
	collector.nowFunc = func() time.Time { return now }

	_, err := collector.Collect(ctx)
	require.NoError(t, err)
	collector.mu.RLock()
	successTime := collector.lastCollectTime
	collector.mu.RUnlock()
	assert.Equal(t, now, successTime)

	now = now.Add(2 * time.Hour) // expire the cache to force a new RPC
	_, err = collector.Collect(ctx)
	require.Error(t, err, "transient failure must surface as an error to the caller")
	collector.mu.RLock()
	failureTime := collector.lastCollectTime
	collector.mu.RUnlock()
	assert.Equal(t, successTime, failureTime, "transient failure must not advance lastCollectTime")

	data, err := collector.Collect(ctx)
	require.NoError(t, err)
	require.Len(t, data.Groups, 2, "after recovery, fresh groups must be cached")
	assert.Equal(t, 3, fake.calls())
}

func TestCollect_UnimplementedCachesAndStops(t *testing.T) {
	fake := &fakeLifecycleService{
		behaviors: []func() (*fodcv1.InspectAllResponse, error){
			returnError(codes.Unimplemented, "not supported"),
		},
	}
	collector := newCollectorForFake(t, fake, 10*time.Minute)
	ctx := t.Context()

	data1, err := collector.Collect(ctx)
	require.NoError(t, err)
	assert.Nil(t, data1.Groups)
	assert.True(t, collector.grpcUnimplemented.Load())

	collector.mu.RLock()
	require.NotNil(t, collector.currentData, "Unimplemented is a known terminal state and must be cached")
	collector.mu.RUnlock()

	data2, err := collector.Collect(ctx)
	require.NoError(t, err)
	assert.Same(t, data1, data2, "subsequent calls within TTL must hit cache")
	assert.Equal(t, 1, fake.calls(), "Unimplemented must short-circuit subsequent RPCs")
}
