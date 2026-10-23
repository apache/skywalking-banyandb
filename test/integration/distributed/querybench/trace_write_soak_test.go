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
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/onsi/gomega"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// The trace write soak holds long-lived trace Write streams open under a steady
// rate and measures what a client sees: when each row is acknowledged, how the
// process heap and RSS move, and whether the written traces become queryable. It
// uses only the public trace Write and Query APIs, so the same file runs against
// any build of the liaison.
//
//	DQB_WRITE_SOAK=1 DQB_WRITE_SOAK_DURATION=30s DQB_WRITE_SOAK_LABEL=smoke \
//	  go test ./test/integration/distributed/querybench -run TestTraceWriteSoak -count=1 -v -timeout 20m
//
// In the container, run-docker.sh passes the variables through and orchestrate.sh
// runs the test when DQB_WRITE_SOAK=1.

const (
	envWriteSoak         = "DQB_WRITE_SOAK"
	envWriteSoakWriters  = "DQB_WRITE_SOAK_WRITERS"
	envWriteSoakDuration = "DQB_WRITE_SOAK_DURATION"
	envWriteSoakRate     = "DQB_WRITE_SOAK_RATE"
	envWriteSoakLabel    = "DQB_WRITE_SOAK_LABEL"
	envWriteSoakEngine   = "DQB_WRITE_SOAK_ENGINE"
	envWriteSoakFullMax  = "DQB_WRITE_SOAK_FULLCHECK_MAX"

	defaultWriteSoakWriters  = 4
	defaultWriteSoakDuration = 5 * time.Minute
	defaultWriteSoakRate     = 2000
	defaultWriteSoakFullMax  = 200000

	writeSoakHeapEvery      = 10 * time.Second
	writeSoakVisibilityGap  = 30 * time.Second
	writeSoakVisibilityAge  = 5 * time.Second
	writeSoakVisibilitySize = 20
	writeSoakCheckBatch     = 100
	writeSoakCheckWorkers   = 4
	writeSoakCheckRetryFor  = 2 * time.Minute
	writeSoakQueryTimeout   = time.Minute
	writeSoakDrainTimeout   = 10 * time.Minute
	writeSoakMaxErrors      = 20
	writeSoakVersionStride  = uint64(1) << 40
)

// writeSoakConfig is the workload shape read from the environment.
type writeSoakConfig struct {
	Label         string
	Duration      time.Duration
	Writers       int
	Rate          int
	FullCheckMax  int
	SpansPerTrace int
}

// latencySummary is a percentile summary of acknowledgement latencies in milliseconds.
type latencySummary struct {
	Count int     `json:"count"`
	P50   float64 `json:"p50_ms"`
	P95   float64 `json:"p95_ms"`
	P99   float64 `json:"p99_ms"`
	Max   float64 `json:"max_ms"`
}

// writeSoakHeapSample is one heap and memory reading.
type writeSoakHeapSample struct {
	ElapsedSeconds float64 `json:"elapsed_seconds"`
	HeapAlloc      uint64  `json:"heap_alloc_settled_bytes"`
	HeapInuse      uint64  `json:"heap_inuse_settled_bytes"`
	RSSBeforeGC    uint64  `json:"rss_before_settle_bytes"`
	RSSAfterGC     uint64  `json:"rss_after_settle_bytes"`
	CgroupMemory   uint64  `json:"cgroup_memory_bytes"`
	Goroutines     int     `json:"goroutines"`
}

// writeSoakVisibilitySample is one in-run visibility query.
type writeSoakVisibilitySample struct {
	Error          string  `json:"error,omitempty"`
	ElapsedSeconds float64 `json:"elapsed_seconds"`
	QueryMillis    float64 `json:"query_ms"`
	Asked          int     `json:"asked"`
	Found          int     `json:"found"`
	Complete       int     `json:"complete"`
}

// writeSoakFinalCheck is the post-drain check of the written traces.
type writeSoakFinalCheck struct {
	Mode             string  `json:"mode"`
	Note             string  `json:"note,omitempty"`
	TracesTotal      int     `json:"traces_written"`
	TracesChecked    int     `json:"traces_checked"`
	TracesComplete   int     `json:"traces_complete"`
	TracesMissing    int     `json:"traces_missing"`
	TracesShort      int     `json:"traces_short"`
	TracesDuplicated int     `json:"traces_with_duplicate_spans_or_entries"`
	SpansLost        int     `json:"spans_lost"`
	SpansDuplicated  int     `json:"spans_duplicated"`
	Seconds          float64 `json:"seconds"`
}

// writeSoakReport is the machine-readable artifact of the write soak.
type writeSoakReport struct {
	Final                 any                         `json:"final_check"`
	StatusCounts          map[string]int64            `json:"ack_status_counts"`
	Label                 string                      `json:"label"`
	GoVersion             string                      `json:"go_version"`
	Errors                []string                    `json:"errors,omitempty"`
	Heap                  []writeSoakHeapSample       `json:"heap_series"`
	Visibility            []writeSoakVisibilitySample `json:"visibility_samples"`
	AckAll                latencySummary              `json:"ack_latency_all"`
	AckBeforeClose        latencySummary              `json:"ack_latency_before_close"`
	AckAfterClose         latencySummary              `json:"ack_latency_after_close"`
	Writers               int                         `json:"writers"`
	RatePerWriter         int                         `json:"rate_per_writer"`
	SpansPerTrace         int                         `json:"spans_per_trace"`
	GOMAXPROCS            int                         `json:"gomaxprocs"`
	SendSeconds           float64                     `json:"send_seconds"`
	DrainSeconds          float64                     `json:"drain_seconds"`
	RowsSent              int64                       `json:"rows_sent"`
	RowsAckedSucceed      int64                       `json:"rows_acked_succeed"`
	RowsAckedNonSuccess   int64                       `json:"rows_acked_non_success"`
	AcksBeforeClose       int64                       `json:"acks_before_close"`
	AcksAfterClose        int64                       `json:"acks_after_close"`
	AcksDuplicate         int64                       `json:"acks_duplicate_version"`
	AcksUnmatched         int64                       `json:"acks_unmatched_version"`
	SentRowsPerSecond     float64                     `json:"sent_rows_per_second"`
	AckedRowsPerSecond    float64                     `json:"acked_rows_per_second_over_send_window"`
	AckedRowsPerSecondAll float64                     `json:"acked_rows_per_second_until_last_ack"`
	HeapStart             uint64                      `json:"heap_alloc_start_bytes"`
	HeapPeak              uint64                      `json:"heap_alloc_peak_bytes"`
	HeapEnd               uint64                      `json:"heap_alloc_end_bytes"`
	HeapInuseStart        uint64                      `json:"heap_inuse_start_bytes"`
	HeapInusePeak         uint64                      `json:"heap_inuse_peak_bytes"`
	HeapInuseEnd          uint64                      `json:"heap_inuse_end_bytes"`
	RSSStart              uint64                      `json:"rss_start_bytes"`
	RSSPeak               uint64                      `json:"rss_peak_bytes"`
	RSSEnd                uint64                      `json:"rss_end_bytes"`
	DurationSeconds       float64                     `json:"configured_duration_seconds"`
}

// soakTraceRecord is a written trace and when its last span was sent.
type soakTraceRecord struct {
	sentAt time.Time
	id     string
	spans  int
}

// writeSoakWriter is the state of one write stream.
type writeSoakWriter struct {
	// sendNanos[i] is the monotonic send time of the row with version offset+i+1, and
	// ackNanos[i] the latency plus one once it was acknowledged.
	statusCounts  map[string]int64
	sendNanos     []int64
	ackNanos      []int64
	traces        []soakTraceRecord
	offset        uint64
	sent          int64
	closedAt      atomic.Int64
	lastAckNanos  atomic.Int64
	beforeClose   atomic.Int64
	afterClose    atomic.Int64
	succeed       atomic.Int64
	nonSuccess    atomic.Int64
	duplicate     atomic.Int64
	unmatched     atomic.Int64
	mu            sync.Mutex
	statusMu      sync.Mutex
	closeSendDone atomic.Bool
}

func loadWriteSoakConfig() writeSoakConfig {
	return writeSoakConfig{
		Label:         getString(envWriteSoakLabel, "run"),
		Duration:      getDuration(envWriteSoakDuration, defaultWriteSoakDuration),
		Writers:       getInt(envWriteSoakWriters, defaultWriteSoakWriters),
		Rate:          getInt(envWriteSoakRate, defaultWriteSoakRate),
		FullCheckMax:  getInt(envWriteSoakFullMax, defaultWriteSoakFullMax),
		SpansPerTrace: getInt(envSpansPerTrace, defaultSpansPerTrace),
	}
}

// TestTraceWriteSoak drives long-lived trace Write streams at a steady rate against the
// in-process cluster and reports acknowledgement latency, heap, RSS and visibility. It
// has no performance gates: it fails only on harness errors and on rows that are lost or
// duplicated after the streams drain. It skips unless DQB_WRITE_SOAK=1.
//
//nolint:gocyclo // one sequential scenario: cluster, writers, samplers, drain, final check, report.
func TestTraceWriteSoak(t *testing.T) {
	if !getBool(envWriteSoak) {
		t.Skipf("set %s=1 to run the trace write soak", envWriteSoak)
	}
	if engine := getString(envWriteSoakEngine, engineTrace); engine != engineTrace {
		t.Skipf("%s=%s selects another write soak", envWriteSoakEngine, engine)
	}
	soakCfg := loadWriteSoakConfig()
	if soakCfg.Writers <= 0 || soakCfg.Rate <= 0 || soakCfg.Duration <= 0 || soakCfg.SpansPerTrace <= 0 {
		t.Fatalf("invalid write soak config: %+v", soakCfg)
	}
	if initErr := logger.Init(logger.Logging{Env: "dev", Level: "warn"}); initErr != nil {
		t.Fatalf("initialize logger: %v", initErr)
	}
	gomega.RegisterTestingT(t)
	cfg := LoadConfig()
	cfg.Engine = engineTrace
	cfg.Soak = false
	cluster, _, clusterErr := startSoakCluster(t, cfg)
	if clusterErr != nil {
		t.Fatalf("start cluster: %v", clusterErr)
	}
	defer cluster.close()

	ctx, cancel := context.WithTimeout(context.Background(), soakCfg.Duration+writeSoakDrainTimeout+30*time.Minute)
	defer cancel()
	client := tracev1.NewTraceServiceClient(cluster.conn)

	runStarted := time.Now()
	rowsPerWriter := int(soakCfg.Duration.Seconds()*float64(soakCfg.Rate)) + soakCfg.Rate
	writers := make([]*writeSoakWriter, soakCfg.Writers)
	for idx := range writers {
		writers[idx] = &writeSoakWriter{
			offset:       uint64(idx+1) * writeSoakVersionStride,
			sendNanos:    make([]int64, rowsPerWriter),
			ackNanos:     make([]int64, rowsPerWriter),
			statusCounts: map[string]int64{},
		}
	}
	var (
		errMu         sync.Mutex
		harnessErrors []string
	)
	addError := func(format string, args ...any) {
		errMu.Lock()
		defer errMu.Unlock()
		if len(harnessErrors) < writeSoakMaxErrors {
			harnessErrors = append(harnessErrors, fmt.Sprintf(format, args...))
		}
	}
	report := &writeSoakReport{
		Label:           soakCfg.Label,
		GoVersion:       runtime.Version(),
		GOMAXPROCS:      runtime.GOMAXPROCS(0),
		Writers:         soakCfg.Writers,
		RatePerWriter:   soakCfg.Rate,
		SpansPerTrace:   soakCfg.SpansPerTrace,
		DurationSeconds: soakCfg.Duration.Seconds(),
		StatusCounts:    map[string]int64{},
	}

	// Heap and visibility samplers run for the send window.
	runCtx, stopRun := context.WithCancel(ctx)
	defer stopRun()
	var samplerWG sync.WaitGroup
	var sampleMu sync.Mutex
	samplerWG.Add(2)
	go func() {
		defer samplerWG.Done()
		sampleHeap := func() {
			sample := takeWriteSoakHeapSample(runStarted)
			sampleMu.Lock()
			report.Heap = append(report.Heap, sample)
			sampleMu.Unlock()
			t.Logf("[write-soak] t=%.0fs heap_alloc=%d heap_inuse=%d rss=%d/%d cgroup=%d goroutines=%d", sample.ElapsedSeconds,
				sample.HeapAlloc, sample.HeapInuse, sample.RSSBeforeGC, sample.RSSAfterGC, sample.CgroupMemory, sample.Goroutines)
		}
		sampleHeap()
		ticker := time.NewTicker(writeSoakHeapEvery)
		defer ticker.Stop()
		for {
			select {
			case <-runCtx.Done():
				return
			case <-ticker.C:
				sampleHeap()
			}
		}
	}()
	go func() {
		defer samplerWG.Done()
		rng := rand.New(rand.NewSource(7)) //nolint:gosec // a sampling shuffle, not a secret
		ticker := time.NewTicker(writeSoakVisibilityGap)
		defer ticker.Stop()
		for {
			select {
			case <-runCtx.Done():
				return
			case <-ticker.C:
				sample, ok := sampleVisibility(runCtx, client, writers, rng, runStarted)
				if !ok {
					continue
				}
				sampleMu.Lock()
				report.Visibility = append(report.Visibility, sample)
				sampleMu.Unlock()
				t.Logf("[write-soak] visibility t=%.0fs asked=%d found=%d complete=%d query=%.0fms err=%s", sample.ElapsedSeconds,
					sample.Asked, sample.Found, sample.Complete, sample.QueryMillis, sample.Error)
			}
		}
	}()

	// Writers.
	sendStart := time.Now()
	sendDeadline := sendStart.Add(soakCfg.Duration)
	var writerWG sync.WaitGroup
	recvDone := make([]chan struct{}, len(writers))
	for idx, writer := range writers {
		recvDone[idx] = make(chan struct{})
		stream, streamErr := client.Write(ctx)
		if streamErr != nil {
			t.Fatalf("open write stream %d: %v", idx, streamErr)
		}
		go func() {
			defer close(recvDone[idx])
			receiveWriteSoakAcks(stream, writer, sendStart, addError)
		}()
		writerWG.Add(1)
		go func() {
			defer writerWG.Done()
			sendWriteSoakRows(stream, writer, idx, soakCfg, sendStart, sendDeadline, addError)
		}()
	}
	writerWG.Wait()
	sendEnd := time.Now()
	stopRun()
	samplerWG.Wait()

	// Drain: every stream was half-closed by its sender; wait for the receivers.
	drainStart := time.Now()
	drainTimer := time.NewTimer(writeSoakDrainTimeout)
	defer drainTimer.Stop()
	for idx := range writers {
		select {
		case <-recvDone[idx]:
		case <-drainTimer.C:
			addError("drain timed out after %s waiting for writer %d acknowledgements", writeSoakDrainTimeout, idx)
			cancel()
			<-recvDone[idx]
		}
	}
	drainEnd := time.Now()

	summarizeWriteSoakAcks(report, writers)
	report.DrainSeconds = drainEnd.Sub(drainStart).Seconds()
	report.SendSeconds = sendEnd.Sub(sendStart).Seconds()
	if report.SendSeconds > 0 {
		report.SentRowsPerSecond = float64(report.RowsSent) / report.SendSeconds
		report.AckedRowsPerSecond = float64(report.AcksBeforeClose) / report.SendSeconds
	}
	t.Logf("[write-soak] sent=%d succeed=%d before_close=%d after_close=%d non_success=%v drain=%.1fs", report.RowsSent,
		report.RowsAckedSucceed, report.AcksBeforeClose, report.AcksAfterClose, report.StatusCounts, report.DrainSeconds)

	// Final settled heap after drain, then the visibility check.
	endSample := takeWriteSoakHeapSample(runStarted)
	report.Heap = append(report.Heap, endSample)

	checkCtx, checkCancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer checkCancel()
	final := finalWriteSoakCheck(checkCtx, client, writers, soakCfg, addError)
	report.Final = final
	t.Logf("[write-soak] final check: %+v", final)

	finishWriteSoakReport(report)
	errMu.Lock()
	report.Errors = append([]string(nil), harnessErrors...)
	errMu.Unlock()
	writeWriteSoakReport(t, report, soakCfg)

	for _, msg := range report.Errors {
		t.Errorf("[write-soak] %s", msg)
	}
	if final.SpansLost > 0 || final.SpansDuplicated > 0 || final.TracesMissing > 0 || final.TracesDuplicated > 0 {
		t.Errorf("[write-soak] rows lost or duplicated after drain: %+v", final)
	}
}

// sendWriteSoakRows sends rows at the configured rate until the deadline, then half-closes.
func sendWriteSoakRows(
	stream tracev1.TraceService_WriteClient, writer *writeSoakWriter, writerIdx int,
	soakCfg writeSoakConfig, start, deadline time.Time, addError func(string, ...any),
) {
	metadata := &commonv1.Metadata{Name: benchTraceName, Group: benchTraceGroupName}
	interval := time.Second / time.Duration(soakCfg.Rate)
	traceIdx, spanIdx := 0, 0
	traceID := ""
	for {
		due := start.Add(time.Duration(writer.sent) * interval)
		if due.After(deadline) || int(writer.sent) >= len(writer.sendNanos) {
			break
		}
		if wait := time.Until(due); wait > 0 {
			time.Sleep(wait)
		}
		if spanIdx == 0 {
			traceID = fmt.Sprintf("wsoak-w%d-%09d", writerIdx, traceIdx)
		}
		version := writer.offset + uint64(writer.sent) + 1
		nowTS := time.Now().Truncate(time.Millisecond)
		tags := traceTags(nowTS, version, traceIdx, spanIdx, traceID, fmt.Sprintf("svc-%d", traceIdx%8), 1000)
		tags[len(tags)-1] = &modelv1.TagValue{Value: &modelv1.TagValue_Timestamp{Timestamp: timestamppb.New(nowTS)}}
		req := &tracev1.WriteRequest{
			Tags:    tags,
			Span:    traceSpanPayload(defaultSpanBytes, traceID, spanIdx),
			Version: version,
		}
		if writer.sent == 0 {
			req.Metadata = metadata
		}
		atomic.StoreInt64(&writer.sendNanos[writer.sent], int64(time.Since(start))+1)
		if sendErr := stream.Send(req); sendErr != nil {
			if !errors.Is(sendErr, io.EOF) {
				addError("writer %d send: %v", writerIdx, sendErr)
			} else {
				addError("writer %d send: stream ended early (EOF); see the receive error", writerIdx)
			}
			break
		}
		writer.sent++
		spanIdx++
		if spanIdx == soakCfg.SpansPerTrace {
			writer.mu.Lock()
			writer.traces = append(writer.traces, soakTraceRecord{id: traceID, spans: spanIdx, sentAt: time.Now()})
			writer.mu.Unlock()
			traceIdx++
			spanIdx = 0
		}
	}
	// A trace cut short by the deadline is still written; record it with its real count.
	if spanIdx > 0 {
		writer.mu.Lock()
		writer.traces = append(writer.traces, soakTraceRecord{id: traceID, spans: spanIdx, sentAt: time.Now()})
		writer.mu.Unlock()
	}
	writer.closedAt.Store(int64(time.Since(start)) + 1)
	writer.closeSendDone.Store(true)
	if closeErr := stream.CloseSend(); closeErr != nil {
		addError("writer %d close send: %v", writerIdx, closeErr)
	}
}

// receiveWriteSoakAcks reads responses concurrently with the sender.
func receiveWriteSoakAcks(stream tracev1.TraceService_WriteClient, writer *writeSoakWriter, start time.Time, addError func(string, ...any)) {
	for {
		resp, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			return
		}
		if recvErr != nil {
			addError("receive trace write response: %v", recvErr)
			return
		}
		now := int64(time.Since(start))
		before := !writer.closeSendDone.Load()
		writer.statusMu.Lock()
		writer.statusCounts[resp.GetStatus()]++
		writer.statusMu.Unlock()
		if resp.GetStatus() != modelv1.Status_STATUS_SUCCEED.String() {
			writer.nonSuccess.Add(1)
			continue
		}
		writer.succeed.Add(1)
		slot := int64(resp.GetVersion()) - int64(writer.offset) - 1
		if slot < 0 || slot >= int64(len(writer.sendNanos)) || atomic.LoadInt64(&writer.sendNanos[slot]) == 0 {
			writer.unmatched.Add(1)
			continue
		}
		latency := now - (atomic.LoadInt64(&writer.sendNanos[slot]) - 1)
		if !atomic.CompareAndSwapInt64(&writer.ackNanos[slot], 0, latency+1) {
			writer.duplicate.Add(1)
			continue
		}
		if before {
			writer.beforeClose.Add(1)
		} else {
			writer.afterClose.Add(1)
		}
		writer.lastAckNanos.Store(now)
	}
}

func summarizeWriteSoakAcks(report *writeSoakReport, writers []*writeSoakWriter) {
	var all, before, after []float64
	var lastAck int64
	for _, writer := range writers {
		report.RowsSent += writer.sent
		report.RowsAckedSucceed += writer.succeed.Load()
		report.RowsAckedNonSuccess += writer.nonSuccess.Load()
		report.AcksBeforeClose += writer.beforeClose.Load()
		report.AcksAfterClose += writer.afterClose.Load()
		report.AcksDuplicate += writer.duplicate.Load()
		report.AcksUnmatched += writer.unmatched.Load()
		for status, count := range writer.statusCounts {
			report.StatusCounts[status] += count
		}
		lastAck = max(lastAck, writer.lastAckNanos.Load())
		closed := writer.closedAt.Load() - 1
		for slot := int64(0); slot < writer.sent; slot++ {
			ack := writer.ackNanos[slot]
			if ack == 0 {
				continue
			}
			latency := ack - 1
			ms := float64(latency) / float64(time.Millisecond)
			all = append(all, ms)
			// Acknowledged before the half-close: send time plus latency precedes it.
			if (writer.sendNanos[slot] - 1 + latency) < closed {
				before = append(before, ms)
			} else {
				after = append(after, ms)
			}
		}
	}
	report.AckAll = summarizeAckLatencies(all)
	report.AckBeforeClose = summarizeAckLatencies(before)
	report.AckAfterClose = summarizeAckLatencies(after)
	if lastAck > 0 {
		report.AckedRowsPerSecondAll = float64(report.RowsAckedSucceed) / (float64(lastAck) / float64(time.Second))
	}
}

func summarizeAckLatencies(values []float64) latencySummary {
	if len(values) == 0 {
		return latencySummary{}
	}
	sort.Float64s(values)
	pick := func(q float64) float64 {
		idx := int(q * float64(len(values)-1))
		return values[idx]
	}
	return latencySummary{Count: len(values), P50: pick(0.50), P95: pick(0.95), P99: pick(0.99), Max: values[len(values)-1]}
}

func takeWriteSoakHeapSample(started time.Time) writeSoakHeapSample {
	rssBefore := readProcessRSS()
	stats := settledHeapLive()
	return writeSoakHeapSample{
		ElapsedSeconds: time.Since(started).Seconds(),
		HeapAlloc:      stats.HeapAlloc,
		HeapInuse:      stats.HeapInuse,
		RSSBeforeGC:    rssBefore,
		RSSAfterGC:     readProcessRSS(),
		CgroupMemory:   readCgroupMemory(),
		Goroutines:     runtime.NumGoroutine(),
	}
}

func finishWriteSoakReport(report *writeSoakReport) {
	for idx, sample := range report.Heap {
		if idx == 0 {
			report.HeapStart, report.HeapInuseStart, report.RSSStart = sample.HeapAlloc, sample.HeapInuse, sample.RSSBeforeGC
		}
		report.HeapPeak = max(report.HeapPeak, sample.HeapAlloc)
		report.HeapInusePeak = max(report.HeapInusePeak, sample.HeapInuse)
		report.RSSPeak = max(report.RSSPeak, sample.RSSBeforeGC)
		report.HeapEnd, report.HeapInuseEnd, report.RSSEnd = sample.HeapAlloc, sample.HeapInuse, sample.RSSAfterGC
	}
}

// readProcessRSS returns the resident set size of this process, or 0 when unavailable.
func readProcessRSS() uint64 {
	body, readErr := os.ReadFile("/proc/self/status")
	if readErr != nil {
		return 0
	}
	for _, line := range strings.Split(string(body), "\n") {
		if !strings.HasPrefix(line, "VmRSS:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			return 0
		}
		kib, parseErr := strconv.ParseUint(fields[1], 10, 64)
		if parseErr != nil {
			return 0
		}
		return kib * 1024
	}
	return 0
}

// readCgroupMemory returns the container cgroup's current memory usage, or 0 when unavailable.
func readCgroupMemory() uint64 {
	for _, path := range []string{"/sys/fs/cgroup/memory.current", "/sys/fs/cgroup/memory/memory.usage_in_bytes"} {
		body, readErr := os.ReadFile(path)
		if readErr != nil {
			continue
		}
		value, parseErr := strconv.ParseUint(strings.TrimSpace(string(body)), 10, 64)
		if parseErr == nil {
			return value
		}
	}
	return 0
}

// traceIDsQuery builds a by-ID query for the given trace IDs.
func traceIDsQuery(ids []string, from, to time.Time) *tracev1.QueryRequest {
	return &tracev1.QueryRequest{
		Groups: []string{benchTraceGroupName},
		Name:   benchTraceName,
		TimeRange: &modelv1.TimeRange{
			Begin: timestamppb.New(from),
			End:   timestamppb.New(to),
		},
		TagProjection: []string{traceTagTraceID, traceTagSpanID},
		Criteria:      traceCondition(traceTagTraceID, modelv1.Condition_BINARY_OP_IN, strArrayTagValue(ids)),
		Limit:         uint32(len(ids)),
	}
}

// queryTraceIDs queries the IDs and returns the number of spans seen per trace ID and
// the number of trace entries returned per ID.
func queryTraceIDs(
	ctx context.Context, client tracev1.TraceServiceClient, ids []string, from time.Time,
) (spans map[string]int, entries map[string]int, queryErr error) {
	queryCtx, cancel := context.WithTimeout(ctx, writeSoakQueryTimeout)
	defer cancel()
	// The time range must be millisecond aligned.
	req := traceIDsQuery(ids, from.Truncate(time.Millisecond), time.Now().Add(time.Hour).Truncate(time.Millisecond))
	resp, err := client.Query(queryCtx, req, grpc.MaxCallRecvMsgSize(512<<20))
	if err != nil {
		return nil, nil, err
	}
	spans = make(map[string]int, len(ids))
	entries = make(map[string]int, len(ids))
	for _, trace := range resp.GetTraces() {
		spans[trace.GetTraceId()] += len(trace.GetSpans())
		entries[trace.GetTraceId()]++
	}
	return spans, entries, nil
}

// sampleVisibility queries a sample of traces written at least writeSoakVisibilityAge ago.
func sampleVisibility(
	ctx context.Context, client tracev1.TraceServiceClient, writers []*writeSoakWriter, rng *rand.Rand, started time.Time,
) (writeSoakVisibilitySample, bool) {
	cutoff := time.Now().Add(-writeSoakVisibilityAge)
	var eligible []soakTraceRecord
	for _, writer := range writers {
		writer.mu.Lock()
		eligible = append(eligible, writer.traces[:sort.Search(len(writer.traces), func(i int) bool { return writer.traces[i].sentAt.After(cutoff) })]...)
		writer.mu.Unlock()
	}
	if len(eligible) == 0 {
		return writeSoakVisibilitySample{}, false
	}
	rng.Shuffle(len(eligible), func(i, j int) { eligible[i], eligible[j] = eligible[j], eligible[i] })
	if len(eligible) > writeSoakVisibilitySize {
		eligible = eligible[:writeSoakVisibilitySize]
	}
	ids := make([]string, len(eligible))
	for idx, rec := range eligible {
		ids[idx] = rec.id
	}
	sample := writeSoakVisibilitySample{ElapsedSeconds: time.Since(started).Seconds(), Asked: len(ids)}
	queryStart := time.Now()
	spans, _, queryErr := queryTraceIDs(ctx, client, ids, started.Add(-time.Hour))
	sample.QueryMillis = float64(time.Since(queryStart)) / float64(time.Millisecond)
	if queryErr != nil {
		if ctx.Err() != nil {
			return writeSoakVisibilitySample{}, false
		}
		sample.Error = queryErr.Error()
		return sample, true
	}
	for _, rec := range eligible {
		if spans[rec.id] > 0 {
			sample.Found++
		}
		if spans[rec.id] == rec.spans {
			sample.Complete++
		}
	}
	return sample, true
}

// finalWriteSoakCheck verifies after drain that each written trace is queryable exactly
// once with the right span count. It checks every trace when the total is within
// FullCheckMax and a random sample of that size otherwise, retrying incomplete traces for
// up to writeSoakCheckRetryFor so asynchronous flushes can land.
func finalWriteSoakCheck(
	ctx context.Context, client tracev1.TraceServiceClient, writers []*writeSoakWriter, soakCfg writeSoakConfig, addError func(string, ...any),
) writeSoakFinalCheck {
	started := time.Now()
	var records []soakTraceRecord
	for _, writer := range writers {
		writer.mu.Lock()
		records = append(records, writer.traces...)
		writer.mu.Unlock()
	}
	check := writeSoakFinalCheck{Mode: "full", TracesTotal: len(records)}
	if len(records) > soakCfg.FullCheckMax {
		rng := rand.New(rand.NewSource(11)) //nolint:gosec // a sampling shuffle, not a secret
		rng.Shuffle(len(records), func(i, j int) { records[i], records[j] = records[j], records[i] })
		records = records[:soakCfg.FullCheckMax]
		check.Mode = "sample"
		check.Note = fmt.Sprintf("random sample of %d of %d traces: a full check exceeds %s=%d", len(records), check.TracesTotal, envWriteSoakFullMax, soakCfg.FullCheckMax)
	}
	check.TracesChecked = len(records)
	from := time.Now().Add(-24 * time.Hour)
	type verdict struct {
		spans   int
		entries int
	}
	var verdictMu sync.Mutex
	verdicts := make(map[string]verdict, len(records))
	pending := records
	deadline := time.Now().Add(writeSoakCheckRetryFor)
	for {
		var stillPending []soakTraceRecord
		var pendingMu sync.Mutex
		group, groupCtx := errgroup.WithContext(ctx)
		group.SetLimit(writeSoakCheckWorkers)
		for begin := 0; begin < len(pending); begin += writeSoakCheckBatch {
			batch := pending[begin:min(begin+writeSoakCheckBatch, len(pending))]
			group.Go(func() error {
				ids := make([]string, len(batch))
				for idx, rec := range batch {
					ids[idx] = rec.id
				}
				spans, entries, queryErr := queryTraceIDs(groupCtx, client, ids, from)
				if queryErr != nil {
					return fmt.Errorf("final check query: %w", queryErr)
				}
				for _, rec := range batch {
					verdictMu.Lock()
					verdicts[rec.id] = verdict{spans: spans[rec.id], entries: entries[rec.id]}
					verdictMu.Unlock()
					if spans[rec.id] != rec.spans || entries[rec.id] != 1 {
						pendingMu.Lock()
						stillPending = append(stillPending, rec)
						pendingMu.Unlock()
					}
				}
				return nil
			})
		}
		if waitErr := group.Wait(); waitErr != nil {
			addError("%v", waitErr)
			break
		}
		pending = stillPending
		if len(pending) == 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(2 * time.Second)
	}
	for _, rec := range records {
		got := verdicts[rec.id]
		switch {
		case got.spans == 0:
			check.TracesMissing++
			check.SpansLost += rec.spans
		case got.spans < rec.spans:
			check.TracesShort++
			check.SpansLost += rec.spans - got.spans
		case got.spans > rec.spans || got.entries > 1:
			check.TracesDuplicated++
			check.SpansDuplicated += max(0, got.spans-rec.spans)
		}
		if got.spans == rec.spans && got.entries == 1 {
			check.TracesComplete++
		}
	}
	check.Seconds = time.Since(started).Seconds()
	return check
}

func writeWriteSoakReport(t *testing.T, report *writeSoakReport, soakCfg writeSoakConfig) {
	t.Helper()
	reportDir := getString(envReportDir, defaultReportDir)
	if mkdirErr := os.MkdirAll(reportDir, 0o755); mkdirErr != nil {
		t.Errorf("create report dir %s: %v", reportDir, mkdirErr)
		return
	}
	body, marshalErr := json.MarshalIndent(report, "", "  ")
	if marshalErr != nil {
		t.Errorf("marshal write soak report: %v", marshalErr)
		return
	}
	outPath := filepath.Join(reportDir, "write-soak-"+soakCfg.Label+".json")
	if writeErr := os.WriteFile(outPath, body, 0o600); writeErr != nil {
		t.Errorf("write write soak report %s: %v", outPath, writeErr)
		return
	}
	t.Logf("[write-soak] report written to %s", outPath)
}
