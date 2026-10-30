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
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// The stream write soak is the stream engine of the long-lived write soak
// (DQB_WRITE_SOAK_ENGINE=stream). It holds one stream Write stream open per writer
// under a steady rate and measures what a client sees: when each element is
// acknowledged (keyed by message_id), how the process heap and RSS move, and whether
// the written elements become queryable exactly once. It uses only the public stream
// Write and Query APIs, so the same file runs against any build of the liaison.
//
//	DQB_WRITE_SOAK=1 DQB_WRITE_SOAK_ENGINE=stream DQB_WRITE_SOAK_DURATION=30s DQB_WRITE_SOAK_LABEL=smoke \
//	  go test ./test/integration/distributed/querybench -run TestStreamWriteSoak -count=1 -v -timeout 20m

const (
	engineStream = "stream"

	benchStreamGroupName = "bench-stream-group"
	benchStreamName      = "sw"

	streamSoakTagPayload = "payload"
	streamSoakTagRowKey  = "row_key"
	streamSoakTagService = "service_id"
	streamSoakTagState   = "state"
	streamSoakTagLatency = "duration"
	streamSoakFamilyData = "data"
	streamSoakFamilyTags = "searchable"
	streamSoakIndexRule  = "bench-row-key"

	streamSoakPayloadBytes  = 256
	streamSoakCheckBatch    = 200
	streamSoakQueryLimit    = 1024
	streamSoakQueryLimitMax = 65536
	streamSoakFullMaxUnset  = 100000000
	streamSoakVisibleCutoff = writeSoakVisibilityAge
)

func streamSoakRowKey(writerIdx int, slot int64) string {
	return fmt.Sprintf("ssoak-w%d-%09d", writerIdx, slot)
}

func preloadStreamBenchSchema(shardNum int) func(context.Context, schema.Registry) error {
	if shardNum <= 0 {
		shardNum = defaultShardNum
	}
	return func(ctx context.Context, registry schema.Registry) error {
		updatedAt := timestamppb.New(time.Date(2021, 4, 15, 1, 30, 15, 10*int(time.Millisecond), time.UTC))
		group := &commonv1.Group{
			Metadata: &commonv1.Metadata{Name: benchStreamGroupName},
			Catalog:  commonv1.Catalog_CATALOG_STREAM,
			ResourceOpts: &commonv1.ResourceOpts{
				ShardNum:        uint32(shardNum),
				SegmentInterval: &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 1},
				Ttl:             &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 30},
			},
			UpdatedAt: updatedAt,
		}
		if _, createErr := registry.CreateGroup(ctx, group); createErr != nil && !errors.Is(createErr, schema.ErrGRPCAlreadyExists) {
			return fmt.Errorf("create stream benchmark group: %w", createErr)
		}
		stream := &databasev1.Stream{
			Metadata: &commonv1.Metadata{Name: benchStreamName, Group: benchStreamGroupName},
			TagFamilies: []*databasev1.TagFamilySpec{
				{Name: streamSoakFamilyData, Tags: []*databasev1.TagSpec{{Name: streamSoakTagPayload, Type: databasev1.TagType_TAG_TYPE_DATA_BINARY}}},
				{Name: streamSoakFamilyTags, Tags: []*databasev1.TagSpec{
					{Name: streamSoakTagRowKey, Type: databasev1.TagType_TAG_TYPE_STRING},
					{Name: streamSoakTagService, Type: databasev1.TagType_TAG_TYPE_STRING},
					{Name: streamSoakTagState, Type: databasev1.TagType_TAG_TYPE_INT},
					{Name: streamSoakTagLatency, Type: databasev1.TagType_TAG_TYPE_INT},
				}},
			},
			Entity:    &databasev1.Entity{TagNames: []string{streamSoakTagService}},
			UpdatedAt: updatedAt,
		}
		if _, createErr := registry.CreateStream(ctx, stream); createErr != nil && !errors.Is(createErr, schema.ErrGRPCAlreadyExists) {
			return fmt.Errorf("create stream benchmark resource: %w", createErr)
		}
		rule := &databasev1.IndexRule{
			Metadata:  &commonv1.Metadata{Name: streamSoakIndexRule, Group: benchStreamGroupName},
			Tags:      []string{streamSoakTagRowKey},
			Type:      databasev1.IndexRule_TYPE_INVERTED,
			UpdatedAt: updatedAt,
		}
		if _, createErr := registry.CreateIndexRule(ctx, rule); createErr != nil && !errors.Is(createErr, schema.ErrGRPCAlreadyExists) {
			return fmt.Errorf("create stream benchmark index rule: %w", createErr)
		}
		binding := &databasev1.IndexRuleBinding{
			Metadata:  &commonv1.Metadata{Name: "bench-sw-index-rule-binding", Group: benchStreamGroupName},
			Rules:     []string{streamSoakIndexRule},
			Subject:   &databasev1.Subject{Catalog: commonv1.Catalog_CATALOG_STREAM, Name: benchStreamName},
			BeginAt:   timestamppb.New(time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)),
			ExpireAt:  timestamppb.New(time.Date(2121, 1, 1, 0, 0, 0, 0, time.UTC)),
			UpdatedAt: updatedAt,
		}
		if _, createErr := registry.CreateIndexRuleBinding(ctx, binding); createErr != nil && !errors.Is(createErr, schema.ErrGRPCAlreadyExists) {
			return fmt.Errorf("create stream benchmark index rule binding: %w", createErr)
		}
		return nil
	}
}

// streamWriteSoakFinalCheck is the post-drain check of the written elements.
type streamWriteSoakFinalCheck struct {
	Mode               string  `json:"mode"`
	Note               string  `json:"note,omitempty"`
	ElementsWritten    int     `json:"elements_written"`
	ElementsChecked    int     `json:"elements_checked"`
	ElementsStoredOnce int     `json:"elements_stored_once"`
	ElementsMissing    int     `json:"elements_missing"`
	ElementsDuplicated int     `json:"elements_duplicated"`
	Seconds            float64 `json:"seconds"`
}

// Each writer is its own series (entity service_id = svc-<writer>), so the elements of a key batch sit in a few
// adjacent blocks of one series and a narrow query loads few of them.

// streamSoakWriter adds the published send count and the element timestamps to the shared per-writer ack state.
// Every query narrows its time range to the timestamps of the keys it asks for, so a query touches the few blocks
// that hold them instead of every block the soak has written.
type streamSoakWriter struct {
	*writeSoakWriter
	// sentMillis[i] is the Unix millisecond timestamp of the element in slot i, stored once it is sent.
	sentMillis []int64
	published  atomic.Int64
}

// TestStreamWriteSoak drives long-lived stream Write streams at a steady rate against
// the in-process cluster and reports acknowledgement latency, heap, RSS and visibility.
// It has no performance gates: it fails only on harness errors and on elements that are
// lost or duplicated after the streams drain. It skips unless DQB_WRITE_SOAK=1 and
// DQB_WRITE_SOAK_ENGINE=stream.
//
//nolint:gocyclo // one sequential scenario: cluster, writers, samplers, drain, final check, report.
func TestStreamWriteSoak(t *testing.T) {
	if !getBool(envWriteSoak) {
		t.Skipf("set %s=1 to run the stream write soak", envWriteSoak)
	}
	if engine := getString(envWriteSoakEngine, engineTrace); engine != engineStream {
		t.Skipf("set %s=%s to run the stream write soak", envWriteSoakEngine, engineStream)
	}
	soakCfg := loadWriteSoakConfig()
	if soakCfg.Writers <= 0 || soakCfg.Rate <= 0 || soakCfg.Duration <= 0 {
		t.Fatalf("invalid write soak config: %+v", soakCfg)
	}
	soakCfg.FullCheckMax = getInt(envWriteSoakFullMax, streamSoakFullMaxUnset)
	if initErr := logger.Init(logger.Logging{Env: "dev", Level: "warn"}); initErr != nil {
		t.Fatalf("initialize logger: %v", initErr)
	}
	gomega.RegisterTestingT(t)
	cfg := LoadConfig()
	cfg.Engine = engineTrace
	cfg.Soak = false
	cluster, _, clusterErr := startSoakClusterWith(t, cfg, nil, preloadStreamBenchSchema(cfg.ShardNum), schema.KindStream)
	if clusterErr != nil {
		t.Fatalf("start cluster: %v", clusterErr)
	}
	defer cluster.close()

	ctx, cancel := context.WithTimeout(context.Background(), soakCfg.Duration+writeSoakDrainTimeout+30*time.Minute)
	defer cancel()
	client := streamv1.NewStreamServiceClient(cluster.conn)

	runStarted := time.Now()
	rowsPerWriter := int(soakCfg.Duration.Seconds()*float64(soakCfg.Rate)) + soakCfg.Rate
	writers := make([]*streamSoakWriter, soakCfg.Writers)
	ackWriters := make([]*writeSoakWriter, soakCfg.Writers)
	for idx := range writers {
		writers[idx] = &streamSoakWriter{
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
				sample, ok := sampleStreamVisibility(runCtx, client, writers, rng, runStarted)
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
			receiveStreamSoakAcks(stream, writer.writeSoakWriter, sendStart, addError)
		}()
		writerWG.Add(1)
		go func() {
			defer writerWG.Done()
			sendStreamSoakRows(stream, writer, idx, soakCfg, sendStart, sendDeadline, addError)
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
	final := finalStreamWriteSoakCheck(checkCtx, client, writers, soakCfg, addError)
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
	if final.ElementsMissing > 0 || final.ElementsDuplicated > 0 {
		t.Errorf("[write-soak] elements lost or duplicated after drain: %+v", final)
	}
}

// sendStreamSoakRows sends elements at the configured rate until the deadline, then half-closes.
func sendStreamSoakRows(
	stream streamv1.StreamService_WriteClient, writer *streamSoakWriter, writerIdx int,
	soakCfg writeSoakConfig, start, deadline time.Time, addError func(string, ...any),
) {
	metadata := &commonv1.Metadata{Name: benchStreamName, Group: benchStreamGroupName}
	interval := time.Second / time.Duration(soakCfg.Rate)
	payload := make([]byte, streamSoakPayloadBytes)
	for {
		due := start.Add(time.Duration(writer.sent) * interval)
		if due.After(deadline) || int(writer.sent) >= len(writer.sendNanos) {
			break
		}
		if wait := time.Until(due); wait > 0 {
			time.Sleep(wait)
		}
		messageID := writer.offset + uint64(writer.sent) + 1
		rowKey := streamSoakRowKey(writerIdx, writer.sent)
		elementTime := time.Now().Truncate(time.Millisecond)
		req := &streamv1.WriteRequest{
			MessageId: messageID,
			Element: &streamv1.ElementValue{
				ElementId: rowKey,
				Timestamp: timestamppb.New(elementTime),
				TagFamilies: []*modelv1.TagFamilyForWrite{
					{Tags: []*modelv1.TagValue{{Value: &modelv1.TagValue_BinaryData{BinaryData: payload}}}},
					{Tags: []*modelv1.TagValue{
						{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: rowKey}}},
						{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: fmt.Sprintf("svc-%d", writerIdx)}}},
						intTagValue(1),
						intTagValue(1000),
					}},
				},
			},
		}
		if writer.sent == 0 {
			req.Metadata = metadata
		}
		atomic.StoreInt64(&writer.sentMillis[writer.sent], elementTime.UnixMilli())
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

// receiveStreamSoakAcks reads responses concurrently with the sender and matches them by message_id.
func receiveStreamSoakAcks(stream streamv1.StreamService_WriteClient, writer *writeSoakWriter, start time.Time, addError func(string, ...any)) {
	for {
		resp, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			return
		}
		if recvErr != nil {
			addError("receive stream write response: %v", recvErr)
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

// queryStreamRowKeys queries the row keys within [from, to] and returns how many elements came back per key.
// A query's limit also sizes its memory allowance, which must cover every block the query loads at once, and a
// stored block can be large even when only one of its elements matches. A query refused with RESOURCE_EXHAUSTED is
// therefore retried with a larger limit, which only enlarges the allowance and never the result.
func queryStreamRowKeys(ctx context.Context, client streamv1.StreamServiceClient, keys []string, from, to time.Time) (map[string]int, error) {
	var lastErr error
	for limit := max(streamSoakQueryLimit, 2*len(keys)); limit <= streamSoakQueryLimitMax; limit *= 4 {
		seen, err := queryStreamRowKeysWithLimit(ctx, client, keys, from, to, limit)
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

func queryStreamRowKeysWithLimit(
	ctx context.Context, client streamv1.StreamServiceClient, keys []string, from, to time.Time, limit int,
) (map[string]int, error) {
	queryCtx, cancel := context.WithTimeout(ctx, writeSoakQueryTimeout)
	defer cancel()
	req := &streamv1.QueryRequest{
		Groups: []string{benchStreamGroupName},
		Name:   benchStreamName,
		// The time range must be millisecond aligned.
		TimeRange: &modelv1.TimeRange{
			Begin: timestamppb.New(from.Truncate(time.Millisecond)),
			End:   timestamppb.New(to.Truncate(time.Millisecond)),
		},
		Projection: &modelv1.TagProjection{TagFamilies: []*modelv1.TagProjection_TagFamily{
			{Name: streamSoakFamilyTags, Tags: []string{streamSoakTagRowKey}},
		}},
		Criteria: traceCondition(streamSoakTagRowKey, modelv1.Condition_BINARY_OP_IN, strArrayTagValue(keys)),
		Limit:    uint32(limit),
	}
	resp, err := client.Query(queryCtx, req, grpc.MaxCallRecvMsgSize(512<<20))
	if err != nil {
		return nil, err
	}
	seen := make(map[string]int, len(keys))
	for _, element := range resp.GetElements() {
		for _, family := range element.GetTagFamilies() {
			for _, tag := range family.GetTags() {
				if tag.GetKey() == streamSoakTagRowKey {
					seen[tag.GetValue().GetStr().GetValue()]++
				}
			}
		}
	}
	return seen, nil
}

// sampleStreamVisibility queries a sample of elements sent at least writeSoakVisibilityAge ago, one narrow query each.
func sampleStreamVisibility(
	ctx context.Context, client streamv1.StreamServiceClient, writers []*streamSoakWriter, rng *rand.Rand, started time.Time,
) (writeSoakVisibilitySample, bool) {
	cutoff := int64(time.Since(started) - streamSoakVisibleCutoff)
	type pick struct {
		key    string
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
		picks = append(picks, pick{key: streamSoakRowKey(idx, slot), millis: atomic.LoadInt64(&writers[idx].sentMillis[slot])})
	}
	if len(picks) == 0 {
		return writeSoakVisibilitySample{}, false
	}
	sample := writeSoakVisibilitySample{ElapsedSeconds: time.Since(started).Seconds(), Asked: len(picks)}
	queryStart := time.Now()
	for _, p := range picks {
		at := time.UnixMilli(p.millis)
		seen, queryErr := queryStreamRowKeys(ctx, client, []string{p.key}, at.Add(-time.Millisecond), at.Add(time.Millisecond))
		if queryErr != nil {
			if ctx.Err() != nil {
				return writeSoakVisibilitySample{}, false
			}
			sample.Error = queryErr.Error()
			break
		}
		if seen[p.key] > 0 {
			sample.Found++
		}
		if seen[p.key] == 1 {
			sample.Complete++
		}
	}
	sample.QueryMillis = float64(time.Since(queryStart)) / float64(time.Millisecond)
	return sample, true
}

// streamSoakBatch is a run of consecutive slots of one writer, checked with one narrow query.
type streamSoakBatch struct {
	writer int
	first  int64
	count  int64
}

// finalStreamWriteSoakCheck verifies after drain that each written element is stored exactly once. It checks
// every element when the total is within FullCheckMax and a random sample of batches of that size otherwise,
// retrying batches that are not yet stored once for up to writeSoakCheckRetryFor so asynchronous flushes can land.
func finalStreamWriteSoakCheck(
	ctx context.Context, client streamv1.StreamServiceClient, writers []*streamSoakWriter, soakCfg writeSoakConfig, addError func(string, ...any),
) streamWriteSoakFinalCheck {
	started := time.Now()
	var batches []streamSoakBatch
	total := 0
	for idx, writer := range writers {
		for first := int64(0); first < writer.sent; first += streamSoakCheckBatch {
			batches = append(batches, streamSoakBatch{writer: idx, first: first, count: min(streamSoakCheckBatch, writer.sent-first)})
		}
		total += int(writer.sent)
	}
	check := streamWriteSoakFinalCheck{Mode: "full", ElementsWritten: total}
	if total > soakCfg.FullCheckMax {
		rng := rand.New(rand.NewSource(11)) //nolint:gosec // a sampling shuffle, not a secret
		rng.Shuffle(len(batches), func(i, j int) { batches[i], batches[j] = batches[j], batches[i] })
		keep := max(1, soakCfg.FullCheckMax/streamSoakCheckBatch)
		batches = batches[:min(keep, len(batches))]
		check.Mode = "sample"
	}
	for _, batch := range batches {
		check.ElementsChecked += int(batch.count)
	}
	if check.Mode == "sample" {
		check.Note = fmt.Sprintf("random sample of %d of %d elements: a full check exceeds %s=%d",
			check.ElementsChecked, check.ElementsWritten, envWriteSoakFullMax, soakCfg.FullCheckMax)
	}
	// Per batch: how many of its elements were stored exactly once, missing, or more than once.
	type verdict struct{ once, missing, duplicated int }
	var verdictMu sync.Mutex
	verdicts := make(map[streamSoakBatch]verdict, len(batches))
	pending := batches
	deadline := time.Now().Add(writeSoakCheckRetryFor)
	for {
		var stillPending []streamSoakBatch
		var pendingMu sync.Mutex
		group, groupCtx := errgroup.WithContext(ctx)
		group.SetLimit(writeSoakCheckWorkers)
		for _, batch := range pending {
			group.Go(func() error {
				writer := writers[batch.writer]
				keys := make([]string, batch.count)
				for i := range keys {
					keys[i] = streamSoakRowKey(batch.writer, batch.first+int64(i))
				}
				begin := time.UnixMilli(atomic.LoadInt64(&writer.sentMillis[batch.first]))
				end := time.UnixMilli(atomic.LoadInt64(&writer.sentMillis[batch.first+batch.count-1]))
				seen, queryErr := queryStreamRowKeys(groupCtx, client, keys, begin.Add(-time.Millisecond), end.Add(time.Millisecond))
				if queryErr != nil {
					return fmt.Errorf("final check query writer=%d first=%d count=%d range=[%s,%s]: %w", batch.writer, batch.first, batch.count,
						begin.Format(time.RFC3339Nano), end.Format(time.RFC3339Nano), queryErr)
				}
				var got verdict
				for _, key := range keys {
					switch n := seen[key]; {
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
			check.ElementsMissing += int(batch.count)
			continue
		}
		check.ElementsStoredOnce += got.once
		check.ElementsMissing += got.missing
		check.ElementsDuplicated += got.duplicated
	}
	check.Seconds = time.Since(started).Seconds()
	return check
}
