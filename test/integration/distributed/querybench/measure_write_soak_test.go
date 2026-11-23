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
	"errors"
	"fmt"
	"io"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/onsi/gomega"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	testmeasure "github.com/apache/skywalking-banyandb/pkg/test/measure"
)

// The measure write soak is the measure engine of the long-lived write soak
// (DQB_WRITE_SOAK_ENGINE=measure). It holds one measure Write stream open per writer
// under a steady rate and measures what a client sees: when each data point is
// acknowledged (keyed by message_id), how the process heap and RSS move, and whether
// the written data points become queryable. It uses only the public measure
// Write and Query APIs, so the same file runs against any build of the liaison.
// A measure is a versioned upsert, so a data point written twice is stored once; the
// final check therefore reports lost data points and counts more than one stored copy
// of a data point as a duplicate.
//
//	DQB_WRITE_SOAK=1 DQB_WRITE_SOAK_ENGINE=measure DQB_WRITE_SOAK_DURATION=30s DQB_WRITE_SOAK_LABEL=smoke \
//	  go test ./test/integration/distributed/querybench -run TestMeasureWriteSoak -count=1 -v -timeout 20m

const (
	measureSoakGroup         = "sw_metric"
	measureSoakName          = "service_cpm_minute"
	measureSoakTagID         = "id"
	measureSoakTagEntity     = "entity_id"
	measureSoakFieldSlot     = "total"
	measureSoakFieldWriter   = "value"
	measureSoakFamily        = "default"
	measureSoakCheckBatch    = 200
	measureSoakQueryLimit    = 1024
	measureSoakQueryLimitMax = 65536
	measureSoakFullMaxUnset  = 100000000
	measureSoakVisibleAge    = writeSoakVisibilityAge
)

func measureSoakRowKey(writerIdx int, slot int64) string {
	return fmt.Sprintf("msoak-w%d-%09d", writerIdx, slot)
}

// measureSoakEntity is the series of one writer, so a writer's data points sit in adjacent blocks of one series.
func measureSoakEntity(writerIdx int) string {
	return fmt.Sprintf("msoak-w%d", writerIdx)
}

// measureWriteSoakFinalCheck is the post-drain check of the written data points.
type measureWriteSoakFinalCheck struct {
	Mode             string  `json:"mode"`
	Note             string  `json:"note,omitempty"`
	PointsWritten    int     `json:"points_written"`
	PointsChecked    int     `json:"points_checked"`
	PointsStoredOnce int     `json:"points_stored_once"`
	PointsMissing    int     `json:"points_missing"`
	PointsDuplicated int     `json:"points_duplicated"`
	Seconds          float64 `json:"seconds"`
}

// measureSoakWriter adds the published send count and the data point timestamps to the shared per-writer ack state.
// Every query narrows its time range to the timestamps of the keys it asks for, so a query touches the few blocks
// that hold them instead of every block the soak has written.
type measureSoakWriter struct {
	*writeSoakWriter
	// sentMillis[i] is the Unix millisecond timestamp of the data point in slot i, stored once it is sent.
	sentMillis []int64
	published  atomic.Int64
}

// TestMeasureWriteSoak drives long-lived measure Write streams at a steady rate against
// the in-process cluster and reports acknowledgement latency, heap, RSS and visibility.
// It has no performance gates: it fails only on harness errors and on data points that are
// lost or duplicated after the streams drain. It skips unless DQB_WRITE_SOAK=1 and
// DQB_WRITE_SOAK_ENGINE=measure.
//
//nolint:gocyclo // one sequential scenario: cluster, writers, samplers, drain, final check, report.
func TestMeasureWriteSoak(t *testing.T) {
	if !getBool(envWriteSoak) {
		t.Skipf("set %s=1 to run the measure write soak", envWriteSoak)
	}
	if engine := getString(envWriteSoakEngine, engineTrace); engine != engineMeasure {
		t.Skipf("set %s=%s to run the measure write soak", envWriteSoakEngine, engineMeasure)
	}
	soakCfg := loadWriteSoakConfig()
	if soakCfg.Writers <= 0 || soakCfg.Rate <= 0 || soakCfg.Duration <= 0 {
		t.Fatalf("invalid write soak config: %+v", soakCfg)
	}
	soakCfg.FullCheckMax = getInt(envWriteSoakFullMax, measureSoakFullMaxUnset)
	if initErr := logger.Init(logger.Logging{Env: "dev", Level: "warn"}); initErr != nil {
		t.Fatalf("initialize logger: %v", initErr)
	}
	gomega.RegisterTestingT(t)
	cfg := LoadConfig()
	cfg.Engine = engineTrace
	cfg.Soak = false
	cluster, _, clusterErr := startSoakClusterWith(t, cfg, nil, testmeasure.PreloadSchema, schema.KindMeasure)
	if clusterErr != nil {
		t.Fatalf("start cluster: %v", clusterErr)
	}
	defer cluster.close()

	ctx, cancel := context.WithTimeout(context.Background(), soakCfg.Duration+writeSoakDrainTimeout+30*time.Minute)
	defer cancel()
	client := measurev1.NewMeasureServiceClient(cluster.conn)

	runStarted := time.Now()
	rowsPerWriter := int(soakCfg.Duration.Seconds()*float64(soakCfg.Rate)) + soakCfg.Rate
	writers := make([]*measureSoakWriter, soakCfg.Writers)
	ackWriters := make([]*writeSoakWriter, soakCfg.Writers)
	for idx := range writers {
		writers[idx] = &measureSoakWriter{
			writeSoakWriter: &writeSoakWriter{
				offset:       uint64(idx+1) * writeSoakVersionStride,
				sendNanos:    make([]int64, rowsPerWriter),
				ackNanos:     make([]int64, rowsPerWriter),
				statusCounts: map[string]int64{},
			},
			sentMillis: make([]int64, rowsPerWriter),
		}
		ackWriters[idx] = writers[idx].writeSoakWriter
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
		SpansPerTrace:   1,
		DurationSeconds: soakCfg.Duration.Seconds(),
		StatusCounts:    map[string]int64{},
	}

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
				sample, ok := sampleMeasureVisibility(runCtx, client, writers, rng, runStarted)
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
			receiveMeasureSoakAcks(stream, writer.writeSoakWriter, sendStart, addError)
		}()
		writerWG.Add(1)
		go func() {
			defer writerWG.Done()
			sendMeasureSoakRows(stream, writer, idx, soakCfg, sendStart, sendDeadline, addError)
		}()
	}
	writerWG.Wait()
	sendEnd := time.Now()
	stopRun()
	samplerWG.Wait()

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

	summarizeWriteSoakAcks(report, ackWriters)
	report.DrainSeconds = drainEnd.Sub(drainStart).Seconds()
	report.SendSeconds = sendEnd.Sub(sendStart).Seconds()
	if report.SendSeconds > 0 {
		report.SentRowsPerSecond = float64(report.RowsSent) / report.SendSeconds
		report.AckedRowsPerSecond = float64(report.AcksBeforeClose) / report.SendSeconds
	}
	t.Logf("[write-soak] sent=%d succeed=%d before_close=%d after_close=%d non_success=%v drain=%.1fs", report.RowsSent,
		report.RowsAckedSucceed, report.AcksBeforeClose, report.AcksAfterClose, report.StatusCounts, report.DrainSeconds)

	endSample := takeWriteSoakHeapSample(runStarted)
	report.Heap = append(report.Heap, endSample)

	checkCtx, checkCancel := context.WithTimeout(context.Background(), 60*time.Minute)
	defer checkCancel()
	final := finalMeasureWriteSoakCheck(checkCtx, client, writers, soakCfg, addError)
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
	if final.PointsMissing > 0 || final.PointsDuplicated > 0 {
		t.Errorf("[write-soak] data points lost or duplicated after drain: %+v", final)
	}
}

// sendMeasureSoakRows sends data points at the configured rate until the deadline, then half-closes. The data point
// timestamps of a writer strictly increase by at least one millisecond, because two data points of one series
// at the same timestamp and version would overwrite each other.
func sendMeasureSoakRows(
	stream measurev1.MeasureService_WriteClient, writer *measureSoakWriter, writerIdx int,
	soakCfg writeSoakConfig, start, deadline time.Time, addError func(string, ...any),
) {
	metadata := &commonv1.Metadata{Name: measureSoakName, Group: measureSoakGroup}
	interval := time.Second / time.Duration(soakCfg.Rate)
	var lastMillis int64
	for {
		due := start.Add(time.Duration(writer.sent) * interval)
		if due.After(deadline) || int(writer.sent) >= len(writer.sendNanos) {
			break
		}
		if wait := time.Until(due); wait > 0 {
			time.Sleep(wait)
		}
		messageID := writer.offset + uint64(writer.sent) + 1
		rowKey := measureSoakRowKey(writerIdx, writer.sent)
		pointMillis := max(time.Now().UnixMilli(), lastMillis+1)
		lastMillis = pointMillis
		req := &measurev1.WriteRequest{
			MessageId: messageID,
			DataPoint: &measurev1.DataPointValue{
				Timestamp: timestamppb.New(time.UnixMilli(pointMillis)),
				TagFamilies: []*modelv1.TagFamilyForWrite{{
					Tags: []*modelv1.TagValue{strTagValue(rowKey), strTagValue(measureSoakEntity(writerIdx))},
				}},
				Fields: []*modelv1.FieldValue{intFieldValue(writer.sent), intFieldValue(int64(writerIdx))},
			},
		}
		if writer.sent == 0 {
			req.Metadata = metadata
		}
		atomic.StoreInt64(&writer.sentMillis[writer.sent], pointMillis)
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
		writer.published.Store(writer.sent)
	}
	writer.closedAt.Store(int64(time.Since(start)) + 1)
	writer.closeSendDone.Store(true)
	if closeErr := stream.CloseSend(); closeErr != nil {
		addError("writer %d close send: %v", writerIdx, closeErr)
	}
}

// receiveMeasureSoakAcks reads responses concurrently with the sender and matches them by message_id.
func receiveMeasureSoakAcks(stream measurev1.MeasureService_WriteClient, writer *writeSoakWriter, start time.Time, addError func(string, ...any)) {
	for {
		resp, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			return
		}
		if recvErr != nil {
			addError("receive measure write response: %v", recvErr)
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
		slot := int64(resp.GetMessageId()) - int64(writer.offset) - 1
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

// queryMeasureRowKeys queries the data points of one writer within [from, to] and returns how many came back per
// slot (the "total" field). A query's limit also sizes its memory allowance, so a query refused with
// RESOURCE_EXHAUSTED is retried with a larger limit, which only enlarges the allowance and never the result.
func queryMeasureRowKeys(ctx context.Context, client measurev1.MeasureServiceClient, writerIdx, count int, from, to time.Time) (map[int64]int, error) {
	var lastErr error
	for limit := max(measureSoakQueryLimit, 2*count); limit <= measureSoakQueryLimitMax; limit *= 4 {
		seen, err := queryMeasureRowKeysWithLimit(ctx, client, writerIdx, from, to, limit)
		if err == nil {
			return seen, nil
		}
		lastErr = err
		if status.Code(err) != codes.ResourceExhausted {
			break
		}
	}
	return nil, lastErr
}

func queryMeasureRowKeysWithLimit(
	ctx context.Context, client measurev1.MeasureServiceClient, writerIdx int, from, to time.Time, limit int,
) (map[int64]int, error) {
	queryCtx, cancel := context.WithTimeout(ctx, writeSoakQueryTimeout)
	defer cancel()
	req := &measurev1.QueryRequest{
		Groups: []string{measureSoakGroup},
		Name:   measureSoakName,
		// The time range must be millisecond aligned.
		TimeRange: &modelv1.TimeRange{
			Begin: timestamppb.New(from.Truncate(time.Millisecond)),
			End:   timestamppb.New(to.Truncate(time.Millisecond)),
		},
		TagProjection: &modelv1.TagProjection{TagFamilies: []*modelv1.TagProjection_TagFamily{
			{Name: measureSoakFamily, Tags: []string{measureSoakTagEntity}},
		}},
		FieldProjection: &measurev1.QueryRequest_FieldProjection{Names: []string{measureSoakFieldSlot}},
		Criteria:        traceCondition(measureSoakTagEntity, modelv1.Condition_BINARY_OP_EQ, strTagValue(measureSoakEntity(writerIdx))),
		Limit:           uint32(limit),
	}
	resp, err := client.Query(queryCtx, req, grpc.MaxCallRecvMsgSize(512<<20))
	if err != nil {
		return nil, err
	}
	seen := make(map[int64]int, len(resp.GetDataPoints()))
	for _, point := range resp.GetDataPoints() {
		for _, field := range point.GetFields() {
			if field.GetName() == measureSoakFieldSlot {
				seen[field.GetValue().GetInt().GetValue()]++
			}
		}
	}
	return seen, nil
}

// sampleMeasureVisibility queries a sample of data points sent at least writeSoakVisibilityAge ago, one narrow query each.
func sampleMeasureVisibility(
	ctx context.Context, client measurev1.MeasureServiceClient, writers []*measureSoakWriter, rng *rand.Rand, started time.Time,
) (writeSoakVisibilitySample, bool) {
	cutoff := int64(time.Since(started) - measureSoakVisibleAge)
	type pick struct {
		writer int
		slot   int64
		millis int64
	}
	picks := make([]pick, 0, writeSoakVisibilitySize)
	for attempt := 0; attempt < 20*writeSoakVisibilitySize && len(picks) < writeSoakVisibilitySize; attempt++ {
		idx := rng.Intn(len(writers))
		published := writers[idx].published.Load()
		if published == 0 {
			continue
		}
		slot := rng.Int63n(published)
		sentNanos := atomic.LoadInt64(&writers[idx].sendNanos[slot])
		if sentNanos == 0 || sentNanos-1 > cutoff {
			continue
		}
		picks = append(picks, pick{writer: idx, slot: slot, millis: atomic.LoadInt64(&writers[idx].sentMillis[slot])})
	}
	if len(picks) == 0 {
		return writeSoakVisibilitySample{}, false
	}
	sample := writeSoakVisibilitySample{ElapsedSeconds: time.Since(started).Seconds(), Asked: len(picks)}
	queryStart := time.Now()
	for _, p := range picks {
		at := time.UnixMilli(p.millis)
		seen, queryErr := queryMeasureRowKeys(ctx, client, p.writer, 1, at, at.Add(time.Millisecond))
		if queryErr != nil {
			if ctx.Err() != nil {
				return writeSoakVisibilitySample{}, false
			}
			sample.Error = queryErr.Error()
			break
		}
		if seen[p.slot] > 0 {
			sample.Found++
		}
		if seen[p.slot] == 1 {
			sample.Complete++
		}
	}
	sample.QueryMillis = float64(time.Since(queryStart)) / float64(time.Millisecond)
	return sample, true
}

// measureSoakBatch is a run of consecutive slots of one writer, checked with one narrow query.
type measureSoakBatch struct {
	writer int
	first  int64
	count  int64
}

// finalMeasureWriteSoakCheck verifies after drain that each written data point is stored exactly once. It checks
// every data point when the total is within FullCheckMax and a random sample of batches of that size otherwise,
// retrying batches that are not yet stored once for up to writeSoakCheckRetryFor so asynchronous flushes can land.
func finalMeasureWriteSoakCheck(
	ctx context.Context, client measurev1.MeasureServiceClient, writers []*measureSoakWriter, soakCfg writeSoakConfig, addError func(string, ...any),
) measureWriteSoakFinalCheck {
	started := time.Now()
	var batches []measureSoakBatch
	total := 0
	for idx, writer := range writers {
		for first := int64(0); first < writer.sent; first += measureSoakCheckBatch {
			batches = append(batches, measureSoakBatch{writer: idx, first: first, count: min(measureSoakCheckBatch, writer.sent-first)})
		}
		total += int(writer.sent)
	}
	check := measureWriteSoakFinalCheck{Mode: "full", PointsWritten: total}
	if total > soakCfg.FullCheckMax {
		rng := rand.New(rand.NewSource(11)) //nolint:gosec // a sampling shuffle, not a secret
		rng.Shuffle(len(batches), func(i, j int) { batches[i], batches[j] = batches[j], batches[i] })
		keep := max(1, soakCfg.FullCheckMax/measureSoakCheckBatch)
		batches = batches[:min(keep, len(batches))]
		check.Mode = "sample"
	}
	for _, batch := range batches {
		check.PointsChecked += int(batch.count)
	}
	if check.Mode == "sample" {
		check.Note = fmt.Sprintf("random sample of %d of %d data points: a full check exceeds %s=%d",
			check.PointsChecked, check.PointsWritten, envWriteSoakFullMax, soakCfg.FullCheckMax)
	}
	// Per batch: how many of its data points were stored exactly once, missing, or more than once.
	type verdict struct{ once, missing, duplicated int }
	var verdictMu sync.Mutex
	verdicts := make(map[measureSoakBatch]verdict, len(batches))
	pending := batches
	deadline := time.Now().Add(writeSoakCheckRetryFor)
	for {
		var stillPending []measureSoakBatch
		var pendingMu sync.Mutex
		group, groupCtx := errgroup.WithContext(ctx)
		group.SetLimit(writeSoakCheckWorkers)
		for _, batch := range pending {
			group.Go(func() error {
				writer := writers[batch.writer]
				begin := time.UnixMilli(atomic.LoadInt64(&writer.sentMillis[batch.first]))
				end := time.UnixMilli(atomic.LoadInt64(&writer.sentMillis[batch.first+batch.count-1]))
				seen, queryErr := queryMeasureRowKeys(groupCtx, client, batch.writer, int(batch.count), begin, end.Add(time.Millisecond))
				if queryErr != nil {
					return fmt.Errorf("final check query writer=%d first=%d count=%d range=[%s,%s]: %w", batch.writer, batch.first, batch.count,
						begin.Format(time.RFC3339Nano), end.Format(time.RFC3339Nano), queryErr)
				}
				var got verdict
				for slot := batch.first; slot < batch.first+batch.count; slot++ {
					switch n := seen[slot]; {
					case n == 0:
						got.missing++
					case n > 1:
						got.duplicated++
					default:
						got.once++
					}
				}
				verdictMu.Lock()
				verdicts[batch] = got
				verdictMu.Unlock()
				if got.once != int(batch.count) {
					pendingMu.Lock()
					stillPending = append(stillPending, batch)
					pendingMu.Unlock()
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
	for _, batch := range batches {
		got, checked := verdicts[batch]
		if !checked {
			check.PointsMissing += int(batch.count)
			continue
		}
		check.PointsStoredOnce += got.once
		check.PointsMissing += got.missing
		check.PointsDuplicated += got.duplicated
	}
	check.Seconds = time.Since(started).Seconds()
	return check
}
