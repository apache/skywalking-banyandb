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
	"runtime"
	"sync"
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

// coldTierRepo is a metadata.Repo stub modeled on cold-tier data nodes.
// CollectDataInfo simulates the post-fix output of schemaRepo.collectSeriesIndexInfo
// for idle-closed segments: returns DataInfo whose SegmentInfo entries each carry
// an empty SeriesIndexInfo (DataCount=0, DataSizeBytes=0). The hook lets tests
// inject failures, latency, or per-call mutation to mimic the cold-tier
// "InspectAll while closeIdleSegments is firing" race window.
type coldTierRepo struct {
	metadata.Repo
	groupRegistry *mockGroupRegistry
	hook          func(group string, callIdx int) ([]*databasev1.DataInfo, []string, error)
	callsByGroup  sync.Map // map[string]*atomic.Int64
	totalCalls    atomic.Int64
	concurrentNow atomic.Int32
	concurrentMax atomic.Int32
	panicCount    atomic.Int32
	// expectOverlap, when > 1, makes CollectDataInfo yield until
	// concurrentMax has reached the configured value. The C subtest sets
	// this to 2 so the "at least two simultaneous calls" assertion is
	// observed deterministically rather than depending on the Go
	// scheduler interleaving short-lived goroutines.
	expectOverlap int32
}

func (r *coldTierRepo) GroupRegistry() schema.Group {
	return r.groupRegistry
}

func (r *coldTierRepo) CollectDataInfo(_ context.Context, group string) (out []*databasev1.DataInfo, collectionErrs []string, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			r.panicCount.Add(1)
			err = fmt.Errorf("panic in CollectDataInfo: %v", rec)
		}
	}()
	r.totalCalls.Add(1)
	current := r.concurrentNow.Add(1)
	defer r.concurrentNow.Add(-1)
	for {
		hi := r.concurrentMax.Load()
		if current <= hi || r.concurrentMax.CompareAndSwap(hi, current) {
			break
		}
	}
	cntAny, _ := r.callsByGroup.LoadOrStore(group, new(atomic.Int64))
	idx := cntAny.(*atomic.Int64).Add(1)
	if r.hook != nil {
		return r.hook(group, int(idx-1))
	}
	// Eventually-style overlap barrier: yield cooperatively until
	// concurrentMax has reached expectOverlap, so the C_ConcurrentInspectAll
	// assertion deterministically observes concurrent fan-out even when
	// the runtime serializes short-lived goroutines (e.g. GOMAXPROCS=1).
	// This replaces an arbitrary time.Sleep -- both wasteful when overlap
	// happens fast and racy when the scheduler serializes calls. Polling
	// concurrentMax (which only grows) lets every goroutine return as
	// soon as overlap was observed at any point, not only while it is
	// still happening. The wait is bounded so a regression that genuinely
	// serializes InspectAll fails the assertion rather than hanging.
	if target := atomic.LoadInt32(&r.expectOverlap); target > 1 {
		deadline := time.Now().Add(time.Second)
		for r.concurrentMax.Load() < target && time.Now().Before(deadline) {
			runtime.Gosched()
		}
	}
	return coldTierEmptyDataInfo(), nil, nil
}

// coldTierEmptyDataInfo mirrors the DataInfo shape the fixed schemaRepo
// returns when every selected segment is in the idle-closed residual state
// (s.index = nil): non-nil top-level DataInfo with one SegmentInfo entry whose
// SeriesIndexInfo is the zero value.
func coldTierEmptyDataInfo() []*databasev1.DataInfo {
	return []*databasev1.DataInfo{
		{
			SegmentInfo: []*databasev1.SegmentInfo{
				{
					SegmentId:       "seg-cold-tier-idle",
					SeriesIndexInfo: &databasev1.SeriesIndexInfo{},
					ShardInfo:       []*databasev1.ShardInfo{},
				},
			},
			DataSizeBytes: 0,
		},
	}
}

func newColdTierGroups(catalogs ...commonv1.Catalog) []*commonv1.Group {
	groups := make([]*commonv1.Group, 0, len(catalogs))
	for i, cat := range catalogs {
		groups = append(groups, &commonv1.Group{
			Metadata: &commonv1.Metadata{Name: fmt.Sprintf("cold_group_%d", i)},
			Catalog:  cat,
			ResourceOpts: &commonv1.ResourceOpts{
				ShardNum: 2,
				SegmentInterval: &commonv1.IntervalRule{
					Unit: commonv1.IntervalRule_UNIT_DAY, Num: 1,
				},
				Ttl: &commonv1.IntervalRule{
					Unit: commonv1.IntervalRule_UNIT_DAY, Num: 30,
				},
			},
		})
	}
	return groups
}

// TestInspectAll_ColdTier_Integration simulates the cold-tier traffic
// pattern that triggered the production nil-pointer panic on
// demo-banyandb-data-cold-0. FODC InspectAll fan-out plus
// closeIdleSegments leave segments in the "refCount=0, index=nil"
// residual state. With the typed-nil and refcount fixes applied,
// schemaRepo.collectSeriesIndexInfo short-circuits and returns an empty
// SeriesIndexInfo rather than panicking.
//
// This test wires the real InspectAll RPC handler against a stub
// metadata.Repo that mirrors the post-fix output of the storage path,
// then drives four cold-tier scenarios end-to-end:
//
//	A. single InspectAll after idle-close
//	B. repeated InspectAll cycles (cold-tier reinspect)
//	C. concurrent InspectAll calls
//	D. concurrent InspectAll with simulated idle-close churn between calls
func TestInspectAll_ColdTier_Integration(t *testing.T) {
	groups := newColdTierGroups(
		commonv1.Catalog_CATALOG_MEASURE,
		commonv1.Catalog_CATALOG_STREAM,
		commonv1.Catalog_CATALOG_TRACE,
	)

	t.Run("A_SingleInspectAfterIdleClose", func(t *testing.T) {
		repo := &coldTierRepo{groupRegistry: &mockGroupRegistry{groups: groups}}
		s := &server{log: logger.GetLogger("cold-tier-A"), metadataRepo: repo}

		resp, err := s.InspectAll(context.Background(), &fodcv1.InspectAllRequest{})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Len(t, resp.Groups, len(groups))

		for _, g := range resp.Groups {
			require.NotEmpty(t, g.DataInfo, "group %s should expose post-fix DataInfo", g.Name)
			for _, di := range g.DataInfo {
				require.NotNil(t, di)
				for _, seg := range di.SegmentInfo {
					require.NotNil(t, seg.SeriesIndexInfo,
						"every segment must carry a non-nil (possibly empty) SeriesIndexInfo")
					assert.Equal(t, int64(0), seg.SeriesIndexInfo.DataCount)
					assert.Equal(t, int64(0), seg.SeriesIndexInfo.DataSizeBytes)
				}
			}
		}
		assert.Equal(t, int32(0), repo.panicCount.Load(), "no goroutine should panic")
	})

	t.Run("B_RepeatedInspectAll_ColdTierReinspect", func(t *testing.T) {
		repo := &coldTierRepo{groupRegistry: &mockGroupRegistry{groups: groups}}
		s := &server{log: logger.GetLogger("cold-tier-B"), metadataRepo: repo}

		const cycles = 25
		for i := 0; i < cycles; i++ {
			resp, err := s.InspectAll(context.Background(), &fodcv1.InspectAllRequest{})
			require.NoError(t, err, "cycle %d", i)
			require.Len(t, resp.Groups, len(groups), "cycle %d returned wrong group count", i)
		}

		// Every group must have been visited exactly `cycles` times -- no state
		// leaks across repeated InspectAll calls (this is the "再次获取" check).
		for _, g := range groups {
			cntAny, ok := repo.callsByGroup.Load(g.Metadata.Name)
			require.True(t, ok, "group %s missing call counter", g.Metadata.Name)
			assert.Equal(t, int64(cycles), cntAny.(*atomic.Int64).Load(),
				"group %s expected exactly %d calls", g.Metadata.Name, cycles)
		}
		assert.Equal(t, int64(cycles*len(groups)), repo.totalCalls.Load())
		assert.Equal(t, int32(0), repo.panicCount.Load())
	})

	t.Run("C_ConcurrentInspectAll", func(t *testing.T) {
		repo := &coldTierRepo{
			groupRegistry: &mockGroupRegistry{groups: groups},
			expectOverlap: 2,
		}
		s := &server{log: logger.GetLogger("cold-tier-C"), metadataRepo: repo}

		const callers = 8
		var (
			wg       sync.WaitGroup
			start    = make(chan struct{})
			panicCnt int64
		)
		for i := 0; i < callers; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				defer func() {
					if r := recover(); r != nil {
						atomic.AddInt64(&panicCnt, 1)
						t.Errorf("caller-%d panicked: %v", id, r)
					}
				}()
				<-start
				resp, err := s.InspectAll(context.Background(), &fodcv1.InspectAllRequest{})
				if !assert.NoError(t, err, "caller-%d", id) {
					return
				}
				if !assert.NotNil(t, resp, "caller-%d", id) {
					return
				}
				assert.Len(t, resp.Groups, len(groups), "caller-%d", id)
			}(i)
		}
		close(start)
		wg.Wait()

		assert.Equal(t, int64(0), panicCnt, "no caller may panic")
		assert.Equal(t, int32(0), repo.panicCount.Load())
		assert.Equal(t, int64(callers*len(groups)), repo.totalCalls.Load())
		// Per-group fan-out cap is 32, so at least two groups should have
		// overlapped within at least one InspectAll invocation.
		assert.GreaterOrEqual(t, repo.concurrentMax.Load(), int32(2),
			"concurrent InspectAll should overlap at least two CollectDataInfo calls")
	})

	t.Run("D_ConcurrentInspectAll_WithIdleCloseChurn", func(t *testing.T) {
		// hook simulates the cold-tier race window: every other call returns
		// a "fresh idle close just happened" payload (still empty), and a few
		// calls return a benign error to mimic transient disk pressure.
		brittleHook := func(group string, callIdx int) ([]*databasev1.DataInfo, []string, error) {
			if callIdx%17 == 0 && callIdx > 0 {
				return nil, nil, fmt.Errorf("simulated transient close error on %s call#%d", group, callIdx)
			}
			// Yield to amplify scheduling jitter, then return empty info.
			time.Sleep(50 * time.Microsecond)
			return coldTierEmptyDataInfo(), nil, nil
		}
		repo := &coldTierRepo{
			groupRegistry: &mockGroupRegistry{groups: groups},
			hook:          brittleHook,
		}
		s := &server{log: logger.GetLogger("cold-tier-D"), metadataRepo: repo}

		const callers = 6
		const cycles = 20
		var (
			wg       sync.WaitGroup
			start    = make(chan struct{})
			panicCnt int64
		)
		for i := 0; i < callers; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				defer func() {
					if r := recover(); r != nil {
						atomic.AddInt64(&panicCnt, 1)
						t.Errorf("caller-%d panicked: %v", id, r)
					}
				}()
				<-start
				for c := 0; c < cycles; c++ {
					resp, err := s.InspectAll(context.Background(), &fodcv1.InspectAllRequest{})
					// require.* would FailNow inside this goroutine, which
					// is forbidden by the testing package; use assert and
					// bail out early on the first failure.
					if !assert.NoError(t, err, "caller-%d cycle-%d", id, c) {
						return
					}
					if !assert.NotNil(t, resp, "caller-%d cycle-%d", id, c) {
						return
					}
					if !assert.Len(t, resp.Groups, len(groups), "caller-%d cycle-%d", id, c) {
						return
					}
				}
			}(i)
		}
		close(start)
		wg.Wait()

		require.Equal(t, int64(0), panicCnt, "no caller may panic")
		require.Equal(t, int32(0), repo.panicCount.Load(),
			"CollectDataInfo must not panic under the cold-tier churn")
		assert.GreaterOrEqual(t, repo.concurrentMax.Load(), int32(2),
			"concurrent InspectAll under churn should overlap at least two CollectDataInfo calls")
	})
}
