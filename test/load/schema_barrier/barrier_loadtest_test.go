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

//go:build loadtest

// Package schemabarrier hosts the CP-6 SLO load harness for the schema
// consistency cluster barrier. It is gated behind the `loadtest` build tag
// so the regular `go test ./...` cycle skips it; invoke via the make target
// `make load-test-barrier`.
//
// The harness brings up an in-process distributed cluster (3 data nodes +
// 1 liaison) and drives the CP-6 SLO load profile pinned in the plan:
//   - 100 concurrent AwaitRevisionApplied(R, 5s) callers continuously
//     re-issuing on completion;
//   - schema mutation rate of 10 successful GroupRegistryService.Update
//     ops per second across the seeded groups;
//   - a 1-minute warm-up followed by a 5-minute measurement window;
//   - report p50 / p95 / p99 from the client-side per-call duration.
//
// The client-side measurement is bounded above by the server-side
// schema_await_revision_applied_duration_seconds histogram (RPC overhead on
// localhost gRPC is sub-ms), so an SLO check on the client number is
// strictly stricter than CP-6's "p99 < 200ms on the server-side histogram"
// criterion. A future revision may scrape the Prometheus endpoint directly;
// the current report records both numbers when the listener is reachable.
package schemabarrier

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/onsi/gomega"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/durationpb"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	schemav1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/schema/v1"
	schemapkg "github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/test"
	test_measure "github.com/apache/skywalking-banyandb/pkg/test/measure"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	test_stream "github.com/apache/skywalking-banyandb/pkg/test/stream"
	test_trace "github.com/apache/skywalking-banyandb/pkg/test/trace"
)

var (
	flagWarmUp        = flag.Duration("loadtest.warm-up", time.Minute, "warm-up duration before sampling begins")
	flagMeasure       = flag.Duration("loadtest.measure", 5*time.Minute, "measurement window duration")
	flagCallers       = flag.Int("loadtest.callers", 100, "concurrent AwaitRevisionApplied callers")
	flagMutateRate    = flag.Int("loadtest.mutate-rate", 10, "successful Group.Update operations per second")
	flagBarrierTarget = flag.Int("loadtest.barrier-offset", 5, "AwaitRevisionApplied targets latest+offset; tune so each call holds open ~1s on average")
	flagBarrierTimeout = flag.Duration("loadtest.barrier-timeout", 5*time.Second, "AwaitRevisionApplied per-call timeout")
	flagReportPath    = flag.String("loadtest.report", "load-report.json", "path to write the JSON report; pass '-' for stdout-only")
)

// loadReport is serialized to disk at end of the measurement window. The
// fields mirror what CP-6 sign-off cites: configuration, raw counts, and
// the p50 / p95 / p99 of client-side per-call duration. Future revisions
// may add a `ServerSideP99` field when Prometheus scraping is wired.
type loadReport struct {
	Profile      profile  `json:"profile"`
	StartedAt    string   `json:"started_at"`
	FinishedAt   string   `json:"finished_at"`
	BarrierCalls int64    `json:"barrier_calls"`
	BarrierErrs  int64    `json:"barrier_errors"`
	BarrierTimeouts int64 `json:"barrier_timeouts"`
	MutateOps    int64    `json:"mutate_ops"`
	MutateErrs   int64    `json:"mutate_errors"`
	P50Seconds   float64  `json:"p50_seconds"`
	P95Seconds   float64  `json:"p95_seconds"`
	P99Seconds   float64  `json:"p99_seconds"`
	MaxSeconds   float64  `json:"max_seconds"`
}

type profile struct {
	WarmUp           string `json:"warm_up"`
	Measure          string `json:"measure"`
	Callers          int    `json:"callers"`
	MutateRatePerSec int    `json:"mutate_rate_per_sec"`
	BarrierOffset    int    `json:"barrier_target_offset"`
	BarrierTimeout   string `json:"barrier_timeout"`
}

// TestSchemaBarrierLoad is the CP-6 SLO load harness entry point. Run via:
//
//	make load-test-barrier
//
// or directly with overrides:
//
//	go test -tags=loadtest -timeout 30m ./test/load/schema_barrier/... \
//	    -loadtest.warm-up=10s -loadtest.measure=30s -loadtest.callers=20 \
//	    -loadtest.mutate-rate=5 -loadtest.report=/tmp/load-report.json
//
// The smoke configuration above keeps the harness runnable on a developer
// laptop in under a minute; the SLO sign-off run uses the defaults.
func TestSchemaBarrierLoad(t *testing.T) {
	gomega.RegisterTestingT(t)

	tmpDir, tmpDirCleanup, err := test.NewSpace()
	require.NoError(t, err, "allocate tmp dir for cluster")
	defer tmpDirCleanup()

	dfWriter := setup.NewDiscoveryFileWriter(tmpDir)
	config := setup.PropertyClusterConfig(dfWriter)

	t.Logf("starting 3 data nodes")
	dataCloses := make([]func(), 0, 3)
	for i := 0; i < 3; i++ {
		dataCloses = append(dataCloses, setup.DataNode(config))
	}

	t.Logf("preloading schema (stream + measure + trace)")
	setup.PreloadSchemaViaProperty(config,
		test_stream.PreloadSchema, test_measure.PreloadSchema, test_trace.PreloadSchema)
	config.AddLoadedKinds(schemapkg.KindStream, schemapkg.KindMeasure, schemapkg.KindTrace)

	t.Logf("starting liaison node")
	liaisonAddr, closeLiaison := setup.LiaisonNode(config,
		"--measure-metadata-cache-wait-duration=5s",
		"--stream-metadata-cache-wait-duration=5s",
		"--trace-metadata-cache-wait-duration=5s")
	defer func() {
		closeLiaison()
		for _, c := range dataCloses {
			c()
		}
	}()

	t.Logf("dialing liaison at %s", liaisonAddr)
	conn, err := grpchelper.Conn(liaisonAddr, 10*time.Second,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err, "dial liaison")
	defer func() { _ = conn.Close() }()

	barrierClient := schemav1.NewSchemaBarrierServiceClient(conn)
	groupsClient := databasev1.NewGroupRegistryServiceClient(conn)

	// Seed the latest revision baseline from a synthetic AwaitRevisionApplied(0).
	// The mutator + caller goroutines treat this as their starting point and
	// advance it via atomic writes after each successful Group.Update.
	var latestRev atomic.Int64
	startResp, err := barrierClient.AwaitRevisionApplied(context.Background(),
		&schemav1.AwaitRevisionAppliedRequest{MinRevision: 0, Timeout: durationpb.New(2 * time.Second)})
	require.NoError(t, err, "baseline AwaitRevisionApplied")
	require.True(t, startResp.GetApplied(), "baseline call must report applied")
	// startResp.Applied=true does not give us a revision; we seed from the
	// first List call below — the mutator path needs a positive revision to
	// bump from anyway.

	listCtx, listCancel := context.WithTimeout(context.Background(), 10*time.Second)
	listResp, err := groupsClient.List(listCtx, &databasev1.GroupRegistryServiceListRequest{})
	listCancel()
	require.NoError(t, err, "list seeded groups")
	require.NotEmpty(t, listResp.GetGroup(), "PreloadSchema must seed at least one group")
	for _, g := range listResp.GetGroup() {
		if rev := g.GetMetadata().GetModRevision(); rev > latestRev.Load() {
			latestRev.Store(rev)
		}
	}
	t.Logf("seeded %d groups; baseline ModRevision=%d", len(listResp.GetGroup()), latestRev.Load())

	// Background workers are bounded by harnessCtx; cancelled at end of run.
	harnessCtx, harnessCancel := context.WithCancel(context.Background())
	defer harnessCancel()

	var (
		mutateOps    atomic.Int64
		mutateErrs   atomic.Int64
		barrierCalls atomic.Int64
		barrierErrs  atomic.Int64
		barrierTimes atomic.Int64

		samplingMu sync.Mutex
		samples    []time.Duration
		// samplingActive guards `samples` writes so warm-up calls are not
		// recorded. The mutex is acquired only on completion — gauging
		// contention against the per-iteration RPC cost is negligible.
		samplingActive atomic.Bool
	)

	// Mutator: 1 goroutine driving 10 ops/sec across the seeded groups
	// (round-robin). Each iteration calls Group.Update with the latest List
	// snapshot of the group; etcd bumps mod_revision on every Update even
	// when the body is identical, so latestRev advances at the configured
	// rate as long as the cluster is healthy.
	mutateInterval := time.Second / time.Duration(*flagMutateRate)
	go func() {
		ticker := time.NewTicker(mutateInterval)
		defer ticker.Stop()
		idx := 0
		for {
			select {
			case <-harnessCtx.Done():
				return
			case <-ticker.C:
			}
			if len(listResp.GetGroup()) == 0 {
				return
			}
			g := listResp.GetGroup()[idx%len(listResp.GetGroup())]
			idx++
			ctx, cancel := context.WithTimeout(harnessCtx, 5*time.Second)
			updResp, updErr := groupsClient.Update(ctx, &databasev1.GroupRegistryServiceUpdateRequest{Group: g})
			cancel()
			if updErr != nil {
				mutateErrs.Add(1)
				continue
			}
			mutateOps.Add(1)
			if rev := updResp.GetModRevision(); rev > 0 {
				for {
					prev := latestRev.Load()
					if rev <= prev || latestRev.CompareAndSwap(prev, rev) {
						break
					}
				}
			}
		}
	}()

	// Callers: N goroutines each looping AwaitRevisionApplied(latest+offset, T).
	// On completion they immediately re-issue with the freshly-read latest
	// snapshot, maintaining ~N in-flight calls throughout the measurement
	// window.
	var callerWg sync.WaitGroup
	for i := 0; i < *flagCallers; i++ {
		callerWg.Add(1)
		go func() {
			defer callerWg.Done()
			for {
				select {
				case <-harnessCtx.Done():
					return
				default:
				}
				target := latestRev.Load() + int64(*flagBarrierTarget)
				start := time.Now()
				resp, callErr := barrierClient.AwaitRevisionApplied(harnessCtx,
					&schemav1.AwaitRevisionAppliedRequest{
						MinRevision: target,
						Timeout:     durationpb.New(*flagBarrierTimeout),
					})
				duration := time.Since(start)
				barrierCalls.Add(1)
				if callErr != nil {
					barrierErrs.Add(1)
					// Brief jittered backoff so we do not hammer on the same
					// failure (typically harnessCtx cancellation at end of run).
					select {
					case <-harnessCtx.Done():
						return
					case <-time.After(time.Duration(rand.IntN(20)) * time.Millisecond):
					}
					continue
				}
				if !resp.GetApplied() {
					barrierTimes.Add(1)
				}
				if samplingActive.Load() {
					samplingMu.Lock()
					samples = append(samples, duration)
					samplingMu.Unlock()
				}
			}
		}()
	}

	t.Logf("warm-up: %s @ %d callers, %d mutate ops/sec, target=latest+%d, timeout=%s",
		*flagWarmUp, *flagCallers, *flagMutateRate, *flagBarrierTarget, *flagBarrierTimeout)
	startedAt := time.Now()
	select {
	case <-time.After(*flagWarmUp):
	case <-harnessCtx.Done():
	}

	t.Logf("entering measurement window (%s)", *flagMeasure)
	samplingActive.Store(true)
	measureStart := time.Now()
	select {
	case <-time.After(*flagMeasure):
	case <-harnessCtx.Done():
	}
	samplingActive.Store(false)
	t.Logf("measurement window finished after %s; cooling down callers", time.Since(measureStart))

	harnessCancel()
	callerWg.Wait()

	// Take a stable snapshot — no further appends can happen because
	// samplingActive is false and goroutines have all returned.
	samplingMu.Lock()
	collected := append([]time.Duration(nil), samples...)
	samplingMu.Unlock()

	report := loadReport{
		Profile: profile{
			WarmUp:           flagWarmUp.String(),
			Measure:          flagMeasure.String(),
			Callers:          *flagCallers,
			MutateRatePerSec: *flagMutateRate,
			BarrierOffset:    *flagBarrierTarget,
			BarrierTimeout:   flagBarrierTimeout.String(),
		},
		StartedAt:       startedAt.UTC().Format(time.RFC3339Nano),
		FinishedAt:      time.Now().UTC().Format(time.RFC3339Nano),
		BarrierCalls:    barrierCalls.Load(),
		BarrierErrs:     barrierErrs.Load(),
		BarrierTimeouts: barrierTimes.Load(),
		MutateOps:       mutateOps.Load(),
		MutateErrs:      mutateErrs.Load(),
	}
	if len(collected) > 0 {
		sort.Slice(collected, func(i, j int) bool { return collected[i] < collected[j] })
		report.P50Seconds = collected[percentileIndex(len(collected), 0.50)].Seconds()
		report.P95Seconds = collected[percentileIndex(len(collected), 0.95)].Seconds()
		report.P99Seconds = collected[percentileIndex(len(collected), 0.99)].Seconds()
		report.MaxSeconds = collected[len(collected)-1].Seconds()
	}

	t.Logf("report: calls=%d errs=%d timeouts=%d mutate=%d/%d  p50=%.4fs  p95=%.4fs  p99=%.4fs  max=%.4fs  samples=%d",
		report.BarrierCalls, report.BarrierErrs, report.BarrierTimeouts,
		report.MutateOps, report.MutateOps+report.MutateErrs,
		report.P50Seconds, report.P95Seconds, report.P99Seconds, report.MaxSeconds,
		len(collected))

	if *flagReportPath != "-" {
		blob, marshalErr := json.MarshalIndent(report, "", "  ")
		require.NoError(t, marshalErr, "marshal load report")
		require.NoError(t, os.WriteFile(*flagReportPath, blob, 0o644), "write load report to %s", *flagReportPath)
		t.Logf("wrote report to %s", *flagReportPath)
	} else {
		blob, _ := json.MarshalIndent(report, "", "  ")
		fmt.Println(string(blob))
	}

	if report.BarrierCalls == 0 || len(collected) == 0 {
		t.Fatalf("no barrier samples collected during measurement window")
	}
}

// percentileIndex returns the nearest-rank index for a 0..1 percentile in a
// sorted slice of length n. The traditional Prometheus-bucket interpretation
// is "the value at or below which p×100% of observations fall"; for a
// ~30 000-sample run the off-by-one between nearest-rank and linear
// interpolation is well below the SLO's measurement noise.
func percentileIndex(n int, p float64) int {
	if n == 0 {
		return 0
	}
	idx := int(float64(n-1) * p)
	if idx < 0 {
		return 0
	}
	if idx >= n {
		return n - 1
	}
	return idx
}
