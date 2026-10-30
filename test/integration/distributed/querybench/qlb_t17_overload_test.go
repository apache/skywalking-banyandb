//go:build querymetrics

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
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/onsi/gomega"
	"google.golang.org/grpc/status"

	bydbqlv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/bydbql/v1"
	liaisongrpc "github.com/apache/skywalking-banyandb/banyand/liaison/grpc"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/pool"
	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/query/ql"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	vmeasure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"
)

// TestQLBT17Overload separates capacity protection and RPC recovery from successful-query timing.
func TestQLBT17Overload(t *testing.T) {
	if os.Getenv("DQB_T17_OVERLOAD") != "1" {
		t.Skip("set DQB_T17_OVERLOAD=1 for fixed 32-caller overload and recovery verification")
	}
	cfg := LoadConfig()
	if !cfg.RunBench || !cfg.InContainer || cfg.Engine != engineMeasure {
		t.Fatal("T17 overload requires container measure engine")
	}
	cfg.Cardinality, cfg.QueryMemoryMiB, cfg.Scenario, cfg.Mode = 1024, 256, ScenarioQLBT17, modeVec
	e := qlbT17OverloadEvidence{Contract: qlbT17OverloadRevision, Cardinality: 1024, QueryMemoryMiB: 256, Workers: 32, ProcessID: os.Getpid()}
	if err := qlbT17OverloadLimits(&e); err != nil {
		t.Fatal(err)
	}
	if err := logger.Init(logger.Logging{Env: "dev", Level: "info"}); err != nil {
		t.Fatal(err)
	}
	gomega.RegisterTestingT(t)
	defer func() {
		body, err := json.MarshalIndent(e, "", "  ")
		if err == nil {
			err = os.MkdirAll(cfg.ReportDir, 0o755)
		}
		path := filepath.Join(cfg.ReportDir, "t17-overload-1024.json")
		if err == nil {
			err = os.WriteFile(path, append(body, '\n'), 0o600)
		}
		if err != nil {
			t.Error(err)
		}
		t.Logf("T17 overload evidence: %s", path)
	}()
	var err error
	e.Probe, err = qlbT17RunCapacityProbe()
	if err != nil {
		t.Fatal(err)
	}
	cluster, base, err := startBenchCluster(t, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.close()
	e.FixtureBaseUnixNanos = base.UnixNano()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Minute)
	defer cancel()
	if _, err = writeQLBT17Data(ctx, cluster.conn, cfg, base); err != nil {
		t.Fatal(err)
	}
	client := bydbqlv1.NewBydbQLServiceClient(cluster.conn)
	if err = waitForQLBT17Visibility(ctx, client, 1024, base); err != nil {
		t.Fatal(err)
	}
	for _, full := range []bool{false, true} {
		cohort, cohortErr := qlbT17RunOverloadCohort(ctx, client, base, full)
		e.Cohorts = append(e.Cohorts, cohort)
		if cohortErr != nil {
			t.Fatal(cohortErr)
		}
	}
	e.Overload, e.Recovery = "pass", "pass"
	if err = validateQLBT17OverloadEvidence(e); err != nil {
		e.Overload, e.Recovery = "invalid", "invalid"
		t.Fatal(err)
	}
}

func qlbT17RunOverloadCohort(ctx context.Context, client bydbqlv1.BydbQLServiceClient, base time.Time, full bool) (qlbT17OverloadCohort, error) {
	c := qlbT17OverloadCohort{Mode: "topk", Attempts: 96, Calls: make([]qlbT17OverloadCall, 96)}
	if full {
		c.Mode = "fullsort"
	}
	restore := qlbT17SetFullSort(full)
	defer restore()
	oracles := qlbT17Oracles(qlbT17Spec{composed: true}, 1024, base)
	// Finish every submitted call, including after rejection. A barrier starts each
	// cohort of 32 callers together; no retry can become a throughput sample.
	var inFlight, peak atomic.Int64
	for wave := 0; wave < 3; wave++ {
		start := make(chan struct{})
		var ready, done sync.WaitGroup
		ready.Add(32)
		for worker := 0; worker < 32; worker++ {
			index := wave*32 + worker
			done.Go(func() {
				ready.Done()
				<-start
				current := inFlight.Add(1)
				for previous := peak.Load(); current > previous; previous = peak.Load() {
					if peak.CompareAndSwap(previous, current) {
						break
					}
				}
				c.Calls[index] = qlbT17OverloadRPC(ctx, client, oracles, base, index)
				inFlight.Add(-1)
			})
		}
		ready.Wait()
		close(start)
		done.Wait()
	}
	c.PeakInFlight = int(peak.Load())
	var err error
	c.Cleanup, err = qlbT17OverloadCleanup()
	if err != nil {
		return c, err
	}
	for index, call := range c.Calls {
		if err = validateQLBT17OverloadCall(call, index, oracles, true); err != nil {
			return c, err
		}
		if call.Status == "OK" {
			c.Successful++
		} else {
			c.Rejected++
		}
	}
	// The cluster, bindings, budget and hooks are unchanged for post-drain recovery.
	for binding := range oracles {
		c.Recovery = append(c.Recovery, qlbT17OverloadRPC(ctx, client, oracles, base, binding))
	}
	c.RecoveryCleanup, err = qlbT17OverloadCleanup()
	if err != nil {
		return c, err
	}
	for index, call := range c.Recovery {
		if err = validateQLBT17OverloadCall(call, index, oracles, false); err != nil {
			return c, err
		}
	}
	return c, nil
}

func qlbT17OverloadRPC(ctx context.Context, client bydbqlv1.BydbQLServiceClient, oracles []*qlbT17Oracle, base time.Time, index int) qlbT17OverloadCall {
	call := qlbT17OverloadCall{Index: index, Binding: index % 3}
	queryCtx, cancel := context.WithTimeout(ctx, queryTimeout(1024))
	defer cancel()
	response, err := client.Query(queryCtx, oracles[call.Binding].query(base))
	call.Status = status.Code(err).String()
	if err != nil {
		call.Error = status.Convert(err).Message()
		if response.GetMeasureResult() != nil {
			call.Rows = len(response.GetMeasureResult().DataPoints)
			call.Hash = qlbT17OrderedHash(response.GetMeasureResult())
		}
		return call
	}
	if response.GetMeasureResult() == nil {
		call.Status = "MissingResult"
		return call
	}
	call.Rows = len(response.GetMeasureResult().DataPoints)
	call.Hash = qlbT17OrderedHash(response.GetMeasureResult())
	if err = oracles[call.Binding].check(response.GetMeasureResult()); err != nil {
		call.Error = err.Error()
		return call
	}
	call.Exact = true
	return call
}

func qlbT17OverloadCleanup() (map[string]int64, error) {
	waitForQLBT02ResourceRelease(0, 0, 0, 0)
	cache := liaisongrpc.BydbQLPlanCacheStats()
	values := map[string]int64{
		"native_results": vmeasure.BatchExecutionStats().LiveResults, "native_reserved": vectorized.QueryMemoryStats().LiveReservedBytes,
		"owner_pins": vectorized.OutstandingOwnerPins(), "flatten_refs": int64(pool.AllRefsCount()["native-query-codec-flatten"]),
		"sort": ql.SortExecutionStats().LiveBytes, "aggregate": ql.AggregateExecutionStats().LiveBytes,
		"native_aggregate": ql.NativeAggregateExecutionStats().LiveBytes, "filter": ql.FilterExecutionStats().LiveBytes,
		"having": ql.HavingExecutionStats().LiveBytes, "group_expression": ql.GroupExpressionExecutionStats().LiveBytes,
		"cache_inflight": cache.InFlight, "cache_compile": cache.CompileReservedBytes, "cache_waiters": cache.Waiters, "cache_uncached": cache.UncachedBytes,
	}
	return values, validateQLBT17OverloadCleanup(values)
}

// Read actual cgroup limits; environment labels cannot prove the fixed container.
func qlbT17OverloadLimits(e *qlbT17OverloadEvidence) error {
	read := func(path string) (int64, error) {
		body, err := os.ReadFile(path)
		if err != nil {
			return 0, err
		}
		return strconv.ParseInt(strings.TrimSpace(string(body)), 10, 64)
	}
	body, err := os.ReadFile("/sys/fs/cgroup/cpu.max")
	if err != nil {
		return fmt.Errorf("T17 overload requires readable cgroup v2 limits: %w", err)
	}
	fields := strings.Fields(string(body))
	if len(fields) != 2 {
		return fmt.Errorf("invalid cpu.max")
	}
	if e.CPUQuota, err = strconv.ParseInt(fields[0], 10, 64); err != nil {
		return err
	}
	if e.CPUPeriod, err = strconv.ParseInt(fields[1], 10, 64); err != nil {
		return err
	}
	if e.MemoryLimit, err = read("/sys/fs/cgroup/memory.max"); err != nil {
		return err
	}
	if e.SwapLimit, err = read("/sys/fs/cgroup/memory.swap.max"); err != nil {
		return err
	}
	if e.CPUPeriod <= 0 || e.CPUQuota != 4*e.CPUPeriod || e.MemoryLimit != 8<<30 || e.SwapLimit != 0 {
		return fmt.Errorf("T17 requires 4 CPUs, 8 GiB RAM and zero swap")
	}
	return nil
}

type qlbT17StaticMemory struct{ protector.Nop }

func (qlbT17StaticMemory) GetLimit() uint64      { return 6 << 30 }
func (qlbT17StaticMemory) AvailableBytes() int64 { return 6 << 30 }

type qlbT17HeldReceive struct {
	release   func()
	transport *query.TransportScope
	scope     *vectorized.NativeReceiveScope
	tracker   *vectorized.MemoryTracker
}

func qlbT17RunCapacityProbe() (qlbT17CapacityProbe, error) {
	budget := protector.NewQueryBudget(qlbT17StaticMemory{})
	p := qlbT17CapacityProbe{ProtectorLimit: 6 << 30, PoolLimit: budget.Limit(), QueryLimit: budget.MaxQuery()}
	held := make([]qlbT17HeldReceive, 0, 64)
	defer func() {
		for _, q := range held {
			q.scope.Close()
			q.release()
			q.transport.Close()
		}
	}()
	// Bound the diagnostic without freezing the current exact rejection ordinal.
	for attempt := 0; attempt < 64; attempt++ {
		ctx, transport := query.NewTransportScope(context.Background())
		ctx, release, err := budget.AdmitMeasureContext(ctx)
		if err != nil {
			transport.Close()
			return p, fmt.Errorf("probe initial admission: %w", err)
		}
		ctx = vectorized.WithQueryMemoryTracker(ctx, 256<<20)
		tracker := vectorized.QueryMemoryTracker(ctx, 256<<20)
		_, scope := vectorized.WithNativeReceiveScope(ctx, tracker)
		held = append(held, qlbT17HeldReceive{release, transport, scope, tracker})
		before := budget.Reserved()
		_, err = scope.ReserveFanout(2, 64<<10, 32<<20)
		if err != nil {
			if !errors.Is(err, protector.ErrQueryResourceExhausted) || tracker.Used() != 0 || budget.Reserved() != before {
				return p, fmt.Errorf("probe failed reservation did not reject and roll back: %w", err)
			}
			p.Rejected, p.Rollback = err.Error(), "pass"
			break
		}
		p.Admitted++
	}
	if p.Rejected == "" {
		return p, fmt.Errorf("probe did not reach bounded shared-pool rejection")
	}
	p.HeldBytes = budget.Reserved()
	for _, q := range held {
		q.scope.Close()
		q.release()
	}
	p.AfterLocalClose = budget.Reserved()
	for _, q := range held {
		q.transport.Close()
		if q.tracker.Used() != 0 {
			return p, fmt.Errorf("probe tracker leak")
		}
	}
	after := budget.Reserved()
	p.AfterTransportClose = &after
	ctx, transport := query.NewTransportScope(context.Background())
	defer transport.Close()
	ctx, release, err := budget.AdmitMeasureContext(ctx)
	if err != nil {
		return p, err
	}
	defer release()
	ctx = vectorized.WithQueryMemoryTracker(ctx, 256<<20)
	tracker := vectorized.QueryMemoryTracker(ctx, 256<<20)
	_, scope := vectorized.WithNativeReceiveScope(ctx, tracker)
	defer scope.Close()
	if _, err = scope.ReserveFanout(2, 64<<10, 32<<20); err != nil {
		return p, err
	}
	scope.Close()
	release()
	transport.Close()
	final := budget.Reserved()
	p.FinalBytes = &final
	if after != 0 || final != 0 || tracker.Used() != 0 || p.AfterLocalClose != p.HeldBytes {
		return p, fmt.Errorf("probe ownership or recovery leak")
	}
	p.Recovery = "pass"
	return p, nil
}

func TestQLBT17HeldCapacityContract(t *testing.T) {
	p, err := qlbT17RunCapacityProbe()
	if err != nil {
		t.Fatal(err)
	}
	if p.Admitted == 0 || p.Rejected == "" || p.Recovery != "pass" {
		t.Fatalf("incomplete capacity evidence: %+v", p)
	}
}
