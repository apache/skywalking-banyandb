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

package querybench

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/onsi/gomega"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/measure"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// The measure soak (scan path v2, step 4d) drives the vectorized measure scan with a
// mixed workload for a bounded time while a writer keeps adding data, so flushes and
// merges change the parts a scan reads. It runs in process, against the same
// in-process cluster the trace soak uses, and gates on what the scan holds:
// block cursors, owner pins, scan reservations, decode tasks and workers, goroutines,
// heap, and on the answers the bounded-window queries must give.
//
// Run it (short preset, about two minutes):
//
//	DQB_SOAK=1 go test ./test/integration/distributed/querybench -run TestMeasureVecSoak -count=1 -v -timeout 20m
//
// Longer, with the bounds raised:
//
//	DQB_SOAK=1 DQB_MEASURE_SOAK_ITERATIONS=1000000 DQB_MEASURE_SOAK_DURATION=2h \
//	  go test ./test/integration/distributed/querybench -run TestMeasureVecSoak -count=1 -v -timeout 3h
//
// In the container, set DQB_SOAK=1 and use run-docker.sh with the same variables.

const (
	// soakEntities series, each with soakPoints points one second apart, are written
	// before the soak starts; the queries with an exact expectation read only them.
	soakEntities = 200
	soakPoints   = 100
	// soakBackgroundGap keeps the background writer's timestamps after the bounded window.
	soakBackgroundGap = 120
	soakWriteTick     = 100 * time.Millisecond
	soakWriteBatch    = 25
	soakSampleEvery   = 100 * time.Millisecond
	soakHeapEvery     = 10 * time.Second
	soakWorkers       = 8
	// soakGoroutineAllowance is the number of goroutines the process may hold above
	// the baseline once the load stops: gRPC transport goroutines come and go.
	soakGoroutineAllowance = 40
	soakMaxReportedErrors  = 20
	// soakWarmupQueries run concurrently before the baseline.
	soakWarmupQueries = 600
)

// soakWindow is the time range of a query. The bounded window holds only the initial
// fixture, which never changes, so exact answers can be checked; the live window also
// holds what the writer adds.
type soakWindow bool

const (
	windowBounded soakWindow = true
	windowLive    soakWindow = false
)

// soakShape is one query shape. build returns the request for a window and verify
// checks a response: exact answers when the window is bounded, only structure
// (order, group shape) when it is live.
type soakShape struct {
	build  func(base time.Time, win soakWindow) *measurev1.QueryRequest
	verify func(base time.Time, resp *measurev1.QueryResponse, win soakWindow) error
	name   string
}

// measureSoakResult is the machine-readable artifact of the measure soak.
type measureSoakResult struct {
	ShapeCounts            map[string]int64 `json:"shape_counts"`
	HeapTimeline           []uint64         `json:"heap_inuse_timeline_bytes"`
	DurationSeconds        float64          `json:"duration_seconds"`
	HeapGrowthPct          float64          `json:"heap_growth_pct"`
	HeapGrowthMaxPct       int              `json:"heap_growth_max_pct"`
	Iterations             int64            `json:"iterations"`
	Verified               int64            `json:"verified_queries"`
	Canceled               int64            `json:"canceled_queries"`
	QueriesStartedDelta    int64            `json:"queries_started_delta"`
	PeakCursorRefs         int              `json:"peak_cursor_refs"`
	PeakReservedBytes      int64            `json:"peak_reserved_bytes"`
	PeakDecodeTasks        int64            `json:"peak_decode_tasks"`
	PeakOwnerPins          int64            `json:"peak_owner_pins"`
	GoroutineBaseline      int              `json:"goroutine_baseline"`
	GoroutineEnd           int              `json:"goroutine_end"`
	EndCursorRefs          int              `json:"end_cursor_refs"`
	EndReservedBytes       int64            `json:"end_reserved_bytes"`
	EndDecodeTasks         int64            `json:"end_decode_tasks"`
	EndOwnerPins           int64            `json:"end_owner_pins"`
	DecodeWorkersDelta     int64            `json:"decode_workers_delta"`
	BackgroundPointsWriten int64            `json:"background_points_written"`
	QueryCountMonotonic    bool             `json:"query_count_monotonic"`
	Pass                   bool             `json:"pass"`
}

// TestMeasureVecSoak is the sustained in-process soak of the vectorized measure
// scan. It proves, after a bounded run of mixed queries under background writes:
//
//  1. no leak: block cursor pool refs, owner pins, scan reservations and decode
//     tasks return to their idle baseline, decode workers do not change, and the
//     goroutine count returns near its baseline;
//  2. no heap growth beyond SOAK_HEAP_GROWTH_MAX_PCT from the post-warmup baseline;
//  3. liveness: the started-query count never decreases and covers every query that
//     completed, and the scan was engaged (it held reservations at some point);
//  4. correctness: every bounded-window query matches an independent expectation
//     computed from the deterministic fixture.
//
// It skips unless DQB_SOAK=1 and DQB_ENGINE is measure (the default).
//
//nolint:gocyclo // one sequential scenario: cluster, fixture, load, gates; splitting it would scatter the shared state.
func TestMeasureVecSoak(t *testing.T) {
	cfg := LoadConfig()
	if !cfg.Soak || cfg.Engine != engineMeasure {
		t.Skipf("set %s=1 (and leave %s unset or measure) to run the measure vectorized soak", envSoak, envEngine)
	}
	if validateErr := cfg.ValidateSoak(); validateErr != nil {
		t.Fatalf("invalid soak config: %v", validateErr)
	}
	if initErr := logger.Init(logger.Logging{Env: "dev", Level: "warn"}); initErr != nil {
		t.Fatalf("initialize logger: %v", initErr)
	}
	gomega.RegisterTestingT(t)

	cluster, clusterBase, clusterErr := startBenchCluster(t, cfg)
	if clusterErr != nil {
		t.Fatalf("start cluster: %v", clusterErr)
	}
	defer cluster.close()
	// Start well in the past so the background writer's timestamps stay behind now for
	// the whole run.
	base := clusterBase.Add(-30 * time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), cfg.SoakMeasureDuration+10*time.Minute)
	defer cancel()
	client := measurev1.NewMeasureServiceClient(cluster.conn)

	t.Logf("[soak] seeding %d series x %d points", soakEntities, soakPoints)
	if writeErr := writeEntityRange(ctx, client, 0, soakEntities, soakPoints, base); writeErr != nil {
		t.Fatalf("seed fixture: %v", writeErr)
	}
	if visErr := waitForSoakFixture(ctx, client, base); visErr != nil {
		t.Fatalf("fixture not visible: %v", visErr)
	}

	shapes := soakShapes()
	// Warm up: every shape once on both windows, so pools, caches and the decode pool
	// are in their steady state before the baseline.
	for _, shape := range shapes {
		for _, win := range []soakWindow{windowBounded, windowLive} {
			if _, warmErr := soakQuery(ctx, client, shape.build(base, win)); warmErr != nil {
				t.Fatalf("warmup %s: %v", shape.name, warmErr)
			}
		}
	}
	var (
		verified, canceled, completed atomic.Int64
		shapeCounts                   sync.Map
		errMu                         sync.Mutex
		failures                      []string
	)
	fail := func(format string, args ...any) {
		errMu.Lock()
		defer errMu.Unlock()
		if len(failures) < soakMaxReportedErrors {
			failures = append(failures, fmt.Sprintf(format, args...))
		}
	}

	// runLoad runs soakWorkers workers until limit queries were issued or ctx ends. The
	// warmup uses it too, with count off, so the pools and the decode pool reach the
	// size the concurrent workload needs before the baseline is taken.
	runLoad := func(loadCtx context.Context, limit int64, count bool) {
		var seq atomic.Int64
		var workerWG sync.WaitGroup
		for workerIdx := 0; workerIdx < soakWorkers; workerIdx++ {
			workerWG.Add(1)
			go func(worker int) {
				defer workerWG.Done()
				rng := rand.New(rand.NewSource(int64(worker) + 1)) //nolint:gosec // a workload shuffle, not a secret
				for loadCtx.Err() == nil {
					n := seq.Add(1)
					if n > limit {
						return
					}
					shape := shapes[int(n)%len(shapes)]
					win := windowBounded
					if (int(n)/len(shapes))%2 == 1 {
						win = windowLive
					}
					req := shape.build(base, win)
					// Every sixth query is canceled mid-flight after a random delay.
					if n%6 == 0 {
						delay := time.Duration(rng.Intn(20)) * time.Millisecond
						cancelCtx, cancelQuery := context.WithTimeout(ctx, delay)
						_, qErr := soakQuery(cancelCtx, client, req)
						cancelQuery()
						if count {
							canceled.Add(1)
						}
						if qErr == nil {
							if count {
								completed.Add(1)
							}
						} else if !isCancellation(qErr) {
							fail("%s (cancel run): %v", shape.name, qErr)
						}
						continue
					}
					resp, qErr := soakQuery(ctx, client, req)
					if qErr != nil {
						fail("%s window=%v: %v", shape.name, win, qErr)
						continue
					}
					if vErr := shape.verify(base, resp, win); vErr != nil {
						fail("%s window=%v: %v", shape.name, win, vErr)
						continue
					}
					if !count {
						continue
					}
					completed.Add(1)
					if win == windowBounded {
						verified.Add(1)
					}
					counter, _ := shapeCounts.LoadOrStore(shape.name, new(atomic.Int64))
					counter.(*atomic.Int64).Add(1)
				}
			}(workerIdx)
		}
		workerWG.Wait()
	}

	// Concurrent warmup, not counted.
	runLoad(ctx, soakWarmupQueries, false)
	idleBaseline := waitForIdleScan(t, 10*time.Second)
	runtime.GC()
	var baseMem runtime.MemStats
	runtime.ReadMemStats(&baseMem)
	goroutineBaseline := runtime.NumGoroutine()
	startedBaseline := measure.QueriesStarted()
	t.Logf("[soak] baseline: heap_inuse=%d goroutines=%d scan=%+v queries_started=%d",
		baseMem.HeapInuse, goroutineBaseline, idleBaseline, startedBaseline)

	loadCtx, stopLoad := context.WithTimeout(ctx, cfg.SoakMeasureDuration)
	defer stopLoad()
	// The background writer: new points after the bounded window, so parts keep
	// flushing and merging under the scans.
	var bgWritten atomic.Int64
	var writerWG sync.WaitGroup
	// DQB_MEASURE_SOAK_WRITES=0 turns the writer off, to tell growth caused by the
	// data it adds from growth caused by the queries.
	writesOn := os.Getenv("DQB_MEASURE_SOAK_WRITES") != "0"
	if writesOn {
		writerWG.Add(1)
	}
	go func() {
		if !writesOn {
			return
		}
		defer writerWG.Done()
		if bgErr := soakBackgroundWriter(loadCtx, client, base, &bgWritten); bgErr != nil {
			fail("background writer: %v", bgErr)
		}
	}()

	// The sampler records the peak of what the scan holds and checks the started-query
	// count never goes backwards.
	var peak soakPeaks
	var heapTimeline []uint64
	monotonic := true
	var samplerWG sync.WaitGroup
	samplerWG.Add(1)
	go func() {
		defer samplerWG.Done()
		prevStarted := startedBaseline
		ticker := time.NewTicker(soakSampleEvery)
		defer ticker.Stop()
		nextHeap := time.Now().Add(soakHeapEvery)
		for {
			if time.Now().After(nextHeap) {
				// A post-GC heap reading shows whether memory plateaus or keeps climbing.
				runtime.GC()
				var mem runtime.MemStats
				runtime.ReadMemStats(&mem)
				heapTimeline = append(heapTimeline, mem.HeapInuse)
				nextHeap = time.Now().Add(soakHeapEvery)
			}
			peak.observe(measure.ScanResourceSnapshot(), runtime.NumGoroutine())
			if now := measure.QueriesStarted(); now < prevStarted {
				monotonic = false
				fail("QueriesStarted went backwards: %d -> %d", prevStarted, now)
			} else {
				prevStarted = now
			}
			select {
			case <-loadCtx.Done():
				return
			case <-ticker.C:
			}
		}
	}()

	started := time.Now()
	t.Logf("[soak] running up to %d iterations or %s with %d workers", cfg.SoakMeasureIterations, cfg.SoakMeasureDuration, soakWorkers)
	runLoad(loadCtx, int64(cfg.SoakMeasureIterations), true)
	stopLoad()
	writerWG.Wait()
	samplerWG.Wait()
	elapsed := time.Since(started)

	// Gates.
	endScan := waitForIdleScan(t, 15*time.Second)
	time.Sleep(2 * time.Second) // let the last flushes land and gRPC goroutines exit
	runtime.GC()
	var endMem runtime.MemStats
	runtime.ReadMemStats(&endMem)
	goroutineEnd := runtime.NumGoroutine()
	startedDelta := measure.QueriesStarted() - startedBaseline

	result := measureSoakResult{
		ShapeCounts:            map[string]int64{},
		DurationSeconds:        elapsed.Seconds(),
		HeapTimeline:           heapTimeline,
		HeapGrowthMaxPct:       cfg.SoakHeapGrowthMaxPct,
		Iterations:             completed.Load(),
		Verified:               verified.Load(),
		Canceled:               canceled.Load(),
		QueriesStartedDelta:    startedDelta,
		PeakCursorRefs:         peak.cursorRefs,
		PeakReservedBytes:      peak.reservedBytes,
		PeakDecodeTasks:        peak.decodeTasks,
		PeakOwnerPins:          peak.ownerPins,
		GoroutineBaseline:      goroutineBaseline,
		GoroutineEnd:           goroutineEnd,
		EndCursorRefs:          endScan.CursorRefs,
		EndReservedBytes:       endScan.ReservedBytes,
		EndDecodeTasks:         endScan.DecodeTasks,
		EndOwnerPins:           endScan.OwnerPins,
		DecodeWorkersDelta:     endScan.DecodeWorkers - idleBaseline.DecodeWorkers,
		BackgroundPointsWriten: bgWritten.Load(),
		QueryCountMonotonic:    monotonic,
	}
	shapeCounts.Range(func(key, value any) bool {
		result.ShapeCounts[key.(string)] = value.(*atomic.Int64).Load()
		return true
	})
	if baseMem.HeapInuse > 0 && endMem.HeapInuse > baseMem.HeapInuse {
		result.HeapGrowthPct = float64(endMem.HeapInuse-baseMem.HeapInuse) / float64(baseMem.HeapInuse) * 100
	}

	for _, failure := range failures {
		t.Errorf("[soak] %s", failure)
	}
	gate := func(ok bool, format string, args ...any) {
		if !ok {
			t.Errorf("[soak] GATE FAIL: "+format, args...)
		}
	}
	gate(endScan.CursorRefs == idleBaseline.CursorRefs, "block cursor refs did not return to baseline: baseline=%d end=%d",
		idleBaseline.CursorRefs, endScan.CursorRefs)
	gate(endScan.OwnerPins == idleBaseline.OwnerPins, "owner pins did not return to baseline: baseline=%d end=%d",
		idleBaseline.OwnerPins, endScan.OwnerPins)
	gate(endScan.ReservedBytes == 0, "scan reservations did not return to 0: %d bytes", endScan.ReservedBytes)
	gate(endScan.DecodeTasks == 0, "decode tasks still alive: %d", endScan.DecodeTasks)
	gate(endScan.QueryResults == idleBaseline.QueryResults, "query results not released: baseline=%d end=%d",
		idleBaseline.QueryResults, endScan.QueryResults)
	gate(result.DecodeWorkersDelta == 0, "decode workers changed: baseline=%d end=%d", idleBaseline.DecodeWorkers, endScan.DecodeWorkers)
	gate(goroutineEnd-goroutineBaseline <= soakGoroutineAllowance, "goroutines did not return near baseline: baseline=%d end=%d allowance=%d",
		goroutineBaseline, goroutineEnd, soakGoroutineAllowance)
	gate(result.HeapGrowthPct <= float64(cfg.SoakHeapGrowthMaxPct), "heap growth %.2f%% > max %d%% (baseline=%d end=%d)",
		result.HeapGrowthPct, cfg.SoakHeapGrowthMaxPct, baseMem.HeapInuse, endMem.HeapInuse)
	gate(monotonic, "started-query count went backwards")
	gate(startedDelta >= completed.Load(), "started-query count %d is below the %d completed queries", startedDelta, completed.Load())
	gate(peak.reservedBytes > 0, "the scan never held a reservation: the vectorized scan was not engaged")
	gate(result.Verified > 0, "no bounded-window query was verified")
	gate(!writesOn || bgWritten.Load() > 0, "the background writer wrote nothing")
	gate(len(failures) == 0, "%d query or verification failures (see above)", len(failures))
	result.Pass = !t.Failed()

	t.Logf("[soak] results: iterations=%d verified=%d canceled=%d duration=%.1fs queries_started_delta=%d bg_points=%d",
		result.Iterations, result.Verified, result.Canceled, result.DurationSeconds, startedDelta, result.BackgroundPointsWriten)
	t.Logf("[soak] peaks: cursor_refs=%d owner_pins=%d reserved_bytes=%d decode_tasks=%d goroutines_max=%d",
		peak.cursorRefs, peak.ownerPins, peak.reservedBytes, peak.decodeTasks, peak.goroutines)
	t.Logf("[soak] heap_inuse after GC every %s: %v", soakHeapEvery, heapTimeline)
	t.Logf("[soak] end: scan=%+v goroutines=%d heap_growth=%.2f%% (max %d%%) shapes=%v",
		endScan, goroutineEnd, result.HeapGrowthPct, cfg.SoakHeapGrowthMaxPct, result.ShapeCounts)
	emitMeasureSoakResult(t, cfg, result)
}

// soakPeaks tracks the highest value of each resource the sampler saw. Only the
// sampler goroutine writes it; the test reads it after the sampler has stopped.
type soakPeaks struct {
	cursorRefs    int
	goroutines    int
	reservedBytes int64
	decodeTasks   int64
	ownerPins     int64
}

func (p *soakPeaks) observe(res measure.ScanResources, goroutines int) {
	p.cursorRefs = max(p.cursorRefs, res.CursorRefs)
	p.reservedBytes = max(p.reservedBytes, res.ReservedBytes)
	p.decodeTasks = max(p.decodeTasks, res.DecodeTasks)
	p.ownerPins = max(p.ownerPins, res.OwnerPins)
	p.goroutines = max(p.goroutines, goroutines)
}

// waitForIdleScan waits for the scan to hold nothing, which it does once the queries
// in flight have finished, and returns the snapshot. It does not fail: a scan that
// never goes idle is reported by the gates with the numbers.
func waitForIdleScan(t *testing.T, limit time.Duration) measure.ScanResources {
	t.Helper()
	deadline := time.Now().Add(limit)
	for {
		res := measure.ScanResourceSnapshot()
		if res.CursorRefs == 0 && res.OwnerPins == 0 && res.ReservedBytes == 0 && res.DecodeTasks == 0 {
			return res
		}
		if time.Now().After(deadline) {
			return res
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func isCancellation(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	switch status.Code(err) {
	case codes.Canceled, codes.DeadlineExceeded:
		return true
	default:
		return false
	}
}

func soakQuery(ctx context.Context, client measurev1.MeasureServiceClient, req *measurev1.QueryRequest) (*measurev1.QueryResponse, error) {
	queryCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	return client.Query(queryCtx, proto.Clone(req).(*measurev1.QueryRequest))
}

// waitForSoakFixture waits until every seeded point is queryable.
func waitForSoakFixture(ctx context.Context, client measurev1.MeasureServiceClient, base time.Time) error {
	deadline := time.Now().Add(2 * time.Minute)
	req := soakBaseRequest(base, windowBounded)
	req.TagProjection = tagProjection(benchTagID, benchTagEntityID)
	req.FieldProjection = &measurev1.QueryRequest_FieldProjection{Names: []string{benchFieldValue}}
	req.Limit = soakEntities * soakPoints
	got := 0
	for time.Now().Before(deadline) {
		resp, err := soakQuery(ctx, client, req)
		if err == nil {
			if got = len(resp.GetDataPoints()); got == soakEntities*soakPoints {
				return nil
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for %d points, last saw %d", soakEntities*soakPoints, got)
}

// soakBackgroundWriter keeps writing new points, strictly after the bounded window,
// until ctx ends. Each tick writes one new timestamp for a rotating slice of series.
func soakBackgroundWriter(ctx context.Context, client measurev1.MeasureServiceClient, base time.Time, written *atomic.Int64) error {
	stream, err := client.Write(ctx)
	if err != nil {
		return fmt.Errorf("open write stream: %w", err)
	}
	recvDone := make(chan error, 1)
	go func() {
		for {
			resp, recvErr := stream.Recv()
			if recvErr != nil {
				if errors.Is(recvErr, io.EOF) || isCancellation(recvErr) {
					recvDone <- nil
				} else {
					recvDone <- recvErr
				}
				return
			}
			if resp.GetStatus() != modelv1.Status_STATUS_SUCCEED.String() {
				recvDone <- fmt.Errorf("write rejected: message_id=%d status=%s", resp.GetMessageId(), resp.GetStatus())
				return
			}
		}
	}()
	metadata := &commonv1.Metadata{Name: benchMeasureName, Group: benchGroupName}
	spec := benchmarkDataPointSpec()
	ticker := time.NewTicker(soakWriteTick)
	defer ticker.Stop()
	var messageID uint64 = 1 << 32
	for tick := 0; ; tick++ {
		select {
		case <-ctx.Done():
			if closeErr := stream.CloseSend(); closeErr != nil && !isCancellation(closeErr) {
				return closeErr
			}
			select {
			case recvErr := <-recvDone:
				return recvErr
			case <-time.After(5 * time.Second):
				return nil
			}
		case recvErr := <-recvDone:
			return recvErr
		case <-ticker.C:
		}
		pointIdx := soakPoints + soakBackgroundGap + tick
		for i := 0; i < soakWriteBatch; i++ {
			entityIdx := (tick*soakWriteBatch + i) % soakEntities
			messageID++
			req := &measurev1.WriteRequest{
				Metadata:      metadata,
				DataPointSpec: spec,
				DataPoint: benchmarkDataPoint(base, fmt.Sprintf("svc%d", entityIdx%benchServiceMod), fmt.Sprintf("entity_%d", entityIdx),
					entityIdx, pointIdx),
				MessageId: messageID,
			}
			if sendErr := stream.Send(req); sendErr != nil {
				if ctx.Err() != nil {
					return nil
				}
				return fmt.Errorf("send write request: %w", sendErr)
			}
			metadata, spec = nil, nil
			written.Add(1)
		}
	}
}

// soakExpectedValue is the field value the fixture writes for entity e at point p.
func soakExpectedValue(entity, point int) int64 {
	return int64((entity%benchServiceMod+1)*1000 + point%1000)
}

func soakBaseRequest(base time.Time, win soakWindow) *measurev1.QueryRequest {
	end := base.Add(time.Duration(soakPoints)*time.Second + 30*time.Second)
	if win == windowLive {
		end = base.Add(2 * time.Hour)
	}
	return &measurev1.QueryRequest{
		Groups:    []string{benchGroupName},
		Name:      benchMeasureName,
		TimeRange: &modelv1.TimeRange{Begin: timestamppb.New(base.Add(-time.Minute)), End: timestamppb.New(end)},
	}
}

// soakRow is one response row.
type soakRow struct {
	ts      time.Time
	id      string
	entity  string
	value   int64
	entityN int
	hasVal  bool
}

func soakParse(resp *measurev1.QueryResponse) []soakRow {
	rows := make([]soakRow, 0, len(resp.GetDataPoints()))
	for _, dp := range resp.GetDataPoints() {
		row := soakRow{ts: dp.GetTimestamp().AsTime(), entityN: -1}
		for _, fam := range dp.GetTagFamilies() {
			for _, tag := range fam.GetTags() {
				switch tag.GetKey() {
				case benchTagID:
					row.id = tag.GetValue().GetStr().GetValue()
				case benchTagEntityID:
					row.entity = tag.GetValue().GetStr().GetValue()
					if _, scanErr := fmt.Sscanf(row.entity, "entity_%d", &row.entityN); scanErr != nil {
						row.entityN = -1
					}
				}
			}
		}
		for _, field := range dp.GetFields() {
			if field.GetName() != benchFieldValue {
				continue
			}
			row.hasVal = true
			switch v := field.GetValue().GetValue().(type) {
			case *modelv1.FieldValue_Int:
				row.value = v.Int.GetValue()
			case *modelv1.FieldValue_Float:
				row.value = int64(v.Float.GetValue())
			default:
				row.hasVal = false
			}
		}
		rows = append(rows, row)
	}
	return rows
}

func soakTimeOrdered(rows []soakRow, asc bool) error {
	for i := 1; i < len(rows); i++ {
		if asc && rows[i].ts.Before(rows[i-1].ts) || !asc && rows[i].ts.After(rows[i-1].ts) {
			return fmt.Errorf("row %d at %s is out of time order after %s (asc=%v)", i, rows[i].ts, rows[i-1].ts, asc)
		}
	}
	return nil
}

// soakRowsAreFixtureRows checks every row of the initial fixture carries the value the
// fixture wrote for its (entity, timestamp).
func soakRowsAreFixtureRows(base time.Time, rows []soakRow) error {
	for i, row := range rows {
		point := int(row.ts.Sub(base) / time.Second)
		if row.entityN < 0 || point < 0 {
			return fmt.Errorf("row %d has entity %q at %s", i, row.entity, row.ts)
		}
		if point >= soakPoints {
			continue // a live-window row written by the background writer
		}
		if !row.hasVal || row.value != soakExpectedValue(row.entityN, point) {
			return fmt.Errorf("row %d (%s at point %d): value %d, want %d", i, row.entity, point, row.value, soakExpectedValue(row.entityN, point))
		}
	}
	return nil
}

func soakProjection(req *measurev1.QueryRequest) *measurev1.QueryRequest {
	req.TagProjection = tagProjection(benchTagID, benchTagEntityID)
	req.FieldProjection = &measurev1.QueryRequest_FieldProjection{Names: []string{benchFieldTotal, benchFieldValue}}
	return req
}

func soakEntityFilter(entity int) *modelv1.Criteria {
	return &modelv1.Criteria{Exp: &modelv1.Criteria_Condition{Condition: &modelv1.Condition{
		Name: benchTagEntityID, Op: modelv1.Condition_BINARY_OP_EQ, Value: strTagValue(fmt.Sprintf("entity_%d", entity)),
	}}}
}

func soakGroupBy(req *measurev1.QueryRequest, bucket string) *measurev1.QueryRequest {
	req.TagProjection = tagProjection(benchTagID)
	req.FieldProjection = &measurev1.QueryRequest_FieldProjection{Names: []string{benchFieldValue}}
	req.GroupBy = &measurev1.QueryRequest_GroupBy{TagProjection: tagProjection(benchTagID), FieldName: benchFieldValue}
	if bucket != "" {
		req.GroupBy.TimeBucket = &measurev1.QueryRequest_GroupBy_TimeBucket{Width: bucket}
	}
	req.Agg = &measurev1.QueryRequest_Aggregation{Function: modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM, FieldName: benchFieldValue}
	req.Limit = soakEntities * soakPoints
	return req
}

// soakShapes lists the mixed workload: every shape of the vectorized scan.
//
//nolint:gocyclo // a table of query shapes; each closure builds a request or checks one response and shares nothing with the others.
func soakShapes() []soakShape {
	const allRows = soakEntities * soakPoints
	return []soakShape{
		{
			name: "single_series_asc",
			build: func(base time.Time, win soakWindow) *measurev1.QueryRequest {
				req := soakProjection(soakBaseRequest(base, win))
				req.Criteria = soakEntityFilter(7)
				req.Limit = soakPoints
				return req
			},
			verify: func(base time.Time, resp *measurev1.QueryResponse, win soakWindow) error {
				rows := soakParse(resp)
				if err := soakTimeOrdered(rows, true); err != nil {
					return err
				}
				if win == windowBounded {
					if len(rows) != soakPoints {
						return fmt.Errorf("got %d rows, want %d", len(rows), soakPoints)
					}
					for i, row := range rows {
						if row.entityN != 7 || !row.ts.Equal(base.Add(time.Duration(i)*time.Second)) || row.value != soakExpectedValue(7, i) {
							return fmt.Errorf("row %d = %+v, want entity_7 at +%ds value %d", i, row, i, soakExpectedValue(7, i))
						}
					}
				}
				return soakRowsAreFixtureRows(base, rows)
			},
		},
		{
			name: "single_series_desc_limit",
			build: func(base time.Time, win soakWindow) *measurev1.QueryRequest {
				req := soakProjection(soakBaseRequest(base, win))
				req.Criteria = soakEntityFilter(11)
				req.OrderBy = &modelv1.QueryOrder{Sort: modelv1.Sort_SORT_DESC}
				req.Limit = 30
				return req
			},
			verify: func(base time.Time, resp *measurev1.QueryResponse, win soakWindow) error {
				rows := soakParse(resp)
				if err := soakTimeOrdered(rows, false); err != nil {
					return err
				}
				if len(rows) != 30 {
					return fmt.Errorf("got %d rows, want 30", len(rows))
				}
				if win == windowBounded {
					for i, row := range rows {
						point := soakPoints - 1 - i
						if row.entityN != 11 || !row.ts.Equal(base.Add(time.Duration(point)*time.Second)) || row.value != soakExpectedValue(11, point) {
							return fmt.Errorf("row %d = %+v, want entity_11 at +%ds", i, row, point)
						}
					}
				}
				return soakRowsAreFixtureRows(base, rows)
			},
		},
		{
			name: "oap_multi_series_no_order",
			build: func(base time.Time, win soakWindow) *measurev1.QueryRequest {
				req := soakProjection(soakBaseRequest(base, win))
				req.Limit = allRows
				return req
			},
			verify: func(base time.Time, resp *measurev1.QueryResponse, win soakWindow) error {
				rows := soakParse(resp)
				if err := soakTimeOrdered(rows, true); err != nil {
					return err
				}
				if win == windowBounded {
					if len(rows) != allRows {
						return fmt.Errorf("got %d rows, want %d", len(rows), allRows)
					}
					var sum, want int64
					perPoint := map[int64]int{}
					for _, row := range rows {
						sum += row.value
						perPoint[row.ts.UnixNano()]++
					}
					for e := 0; e < soakEntities; e++ {
						for p := 0; p < soakPoints; p++ {
							want += soakExpectedValue(e, p)
						}
					}
					if sum != want {
						return fmt.Errorf("sum of values %d, want %d", sum, want)
					}
					for ts, n := range perPoint {
						if n != soakEntities {
							return fmt.Errorf("timestamp %d has %d rows, want %d", ts, n, soakEntities)
						}
					}
				}
				return soakRowsAreFixtureRows(base, rows)
			},
		},
		{
			name: "limit_early_close",
			build: func(base time.Time, win soakWindow) *measurev1.QueryRequest {
				req := soakProjection(soakBaseRequest(base, win))
				req.Limit = 10
				return req
			},
			verify: func(base time.Time, resp *measurev1.QueryResponse, win soakWindow) error {
				rows := soakParse(resp)
				if len(rows) != 10 {
					return fmt.Errorf("got %d rows, want 10", len(rows))
				}
				if err := soakTimeOrdered(rows, true); err != nil {
					return err
				}
				if win == windowBounded {
					for i, row := range rows {
						if !row.ts.Equal(base) {
							return fmt.Errorf("row %d at %s, want the first timestamp %s", i, row.ts, base)
						}
					}
				}
				return soakRowsAreFixtureRows(base, rows)
			},
		},
		{
			name: "time_desc_multi_series",
			build: func(base time.Time, win soakWindow) *measurev1.QueryRequest {
				req := soakProjection(soakBaseRequest(base, win))
				req.OrderBy = &modelv1.QueryOrder{Sort: modelv1.Sort_SORT_DESC}
				req.Limit = 5000
				return req
			},
			verify: func(base time.Time, resp *measurev1.QueryResponse, win soakWindow) error {
				rows := soakParse(resp)
				if len(rows) != 5000 {
					return fmt.Errorf("got %d rows, want 5000", len(rows))
				}
				if err := soakTimeOrdered(rows, false); err != nil {
					return err
				}
				if win == windowBounded {
					// 5,000 rows are the 25 latest timestamps of 200 series each.
					for i, row := range rows {
						point := soakPoints - 1 - i/soakEntities
						if !row.ts.Equal(base.Add(time.Duration(point) * time.Second)) {
							return fmt.Errorf("row %d at %s, want point %d", i, row.ts, point)
						}
					}
				}
				return soakRowsAreFixtureRows(base, rows)
			},
		},
		soakSeriesOrderShape("series_order_asc", modelv1.Sort_SORT_ASC),
		soakSeriesOrderShape("series_order_desc", modelv1.Sort_SORT_DESC),
		{
			name: "group_by_sum",
			build: func(base time.Time, win soakWindow) *measurev1.QueryRequest {
				return soakGroupBy(soakBaseRequest(base, win), "")
			},
			verify: func(_ time.Time, resp *measurev1.QueryResponse, win soakWindow) error {
				rows := soakParse(resp)
				if len(rows) != benchServiceMod {
					return fmt.Errorf("got %d groups, want %d", len(rows), benchServiceMod)
				}
				if win == windowBounded {
					for _, row := range rows {
						var key int
						if _, scanErr := fmt.Sscanf(row.id, "svc%d", &key); scanErr != nil {
							return fmt.Errorf("group id %q", row.id)
						}
						var want int64
						for e := key; e < soakEntities; e += benchServiceMod {
							for p := 0; p < soakPoints; p++ {
								want += soakExpectedValue(e, p)
							}
						}
						if row.value != want {
							return fmt.Errorf("group %s sum %d, want %d", row.id, row.value, want)
						}
					}
				}
				return nil
			},
		},
		{
			name: "group_by_time_bucket_sum",
			build: func(base time.Time, win soakWindow) *measurev1.QueryRequest {
				return soakGroupBy(soakBaseRequest(base, win), "10s")
			},
			verify: func(base time.Time, resp *measurev1.QueryResponse, win soakWindow) error {
				rows := soakParse(resp)
				if win == windowLive {
					if len(rows) < benchServiceMod*soakPoints/10 {
						return fmt.Errorf("got %d bucket rows, want at least %d", len(rows), benchServiceMod*soakPoints/10)
					}
					return nil
				}
				if len(rows) != benchServiceMod*soakPoints/10 {
					return fmt.Errorf("got %d bucket rows, want %d", len(rows), benchServiceMod*soakPoints/10)
				}
				for _, row := range rows {
					var key int
					if _, scanErr := fmt.Sscanf(row.id, "svc%d", &key); scanErr != nil {
						return fmt.Errorf("group id %q", row.id)
					}
					offset := row.ts.Sub(base)
					if offset < 0 || offset%(10*time.Second) != 0 || offset >= soakPoints*time.Second {
						return fmt.Errorf("bucket start %s is not a 10s bucket of the window", row.ts)
					}
					first := int(offset / time.Second)
					var want int64
					for e := key; e < soakEntities; e += benchServiceMod {
						for p := first; p < first+10; p++ {
							want += soakExpectedValue(e, p)
						}
					}
					if row.value != want {
						return fmt.Errorf("group %s bucket +%ds sum %d, want %d", row.id, first, row.value, want)
					}
				}
				return nil
			},
		},
	}
}

func soakSeriesOrderShape(name string, sort modelv1.Sort) soakShape {
	asc := sort == modelv1.Sort_SORT_ASC
	return soakShape{
		name: name,
		build: func(base time.Time, win soakWindow) *measurev1.QueryRequest {
			req := soakProjection(soakBaseRequest(base, win))
			req.OrderBy = &modelv1.QueryOrder{IndexRuleName: benchTagID, Sort: sort}
			req.Limit = soakEntities * soakPoints
			return req
		},
		verify: func(base time.Time, resp *measurev1.QueryResponse, win soakWindow) error {
			rows := soakParse(resp)
			for i := 1; i < len(rows); i++ {
				if asc && rows[i].id < rows[i-1].id || !asc && rows[i].id > rows[i-1].id {
					return fmt.Errorf("row %d id %q is out of series order after %q (asc=%v)", i, rows[i].id, rows[i-1].id, asc)
				}
			}
			if win == windowBounded {
				if len(rows) != soakEntities*soakPoints {
					return fmt.Errorf("got %d rows, want %d", len(rows), soakEntities*soakPoints)
				}
				seen := map[string]int{}
				for _, row := range rows {
					seen[row.entity]++
				}
				for e := 0; e < soakEntities; e++ {
					if n := seen[fmt.Sprintf("entity_%d", e)]; n != soakPoints {
						return fmt.Errorf("entity_%d has %d rows, want %d", e, n, soakPoints)
					}
				}
			}
			return soakRowsAreFixtureRows(base, rows)
		},
	}
}

func emitMeasureSoakResult(t *testing.T, cfg Config, result measureSoakResult) {
	t.Helper()
	reportDir := cfg.ReportDir
	if reportDir == "" {
		reportDir = defaultReportDir
	}
	if mkdirErr := os.MkdirAll(reportDir, 0o755); mkdirErr != nil {
		t.Logf("[soak] could not create report dir %s: %v", reportDir, mkdirErr)
		return
	}
	body, marshalErr := json.MarshalIndent(result, "", "  ")
	if marshalErr != nil {
		t.Logf("[soak] could not marshal soak result: %v", marshalErr)
		return
	}
	outPath := filepath.Join(reportDir, "soak-measure-vec-result.json")
	if writeErr := os.WriteFile(outPath, body, 0o600); writeErr != nil {
		t.Logf("[soak] could not write soak result to %s: %v", outPath, writeErr)
		return
	}
	t.Logf("[soak] result written to %s", outPath)
}
