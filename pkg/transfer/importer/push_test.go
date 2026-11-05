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

package importer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/prototext"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer"
	"github.com/apache/skywalking-banyandb/pkg/transfer/cluster"
	"github.com/apache/skywalking-banyandb/pkg/transfer/config"
	"github.com/apache/skywalking-banyandb/pkg/transfer/exitcode"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/csvfmt"
)

// pushFixture is a target already holding the fixture's schema, and the decided entries.
type pushFixture struct {
	f       *fakeLiaison
	li      *cluster.Liaison
	art     *Artifact
	dec     *decision
	entries []Entry
}

func newPushFixture(t *testing.T) *pushFixture {
	t.Helper()
	f := newFakeLiaison()
	mirrorFake(f, dataSchemas())
	li := f.serve(t)
	art, dec := entriesFor(t, li, f, writeDataArtifact(t))
	require.Len(t, dec.plan.Entries, 3)
	return &pushFixture{f: f, li: li, art: art, dec: dec, entries: dec.plan.Entries}
}

func pushOpts(log *bytes.Buffer) PushOptions {
	return PushOptions{
		Log: log, Import: config.Import{
			WriteBatchSize: 1000, WriteBatchBytes: 4 << 20, WriteInflightBatches: 2, SendRetryTimeout: time.Second, Parallelism: math.MaxInt,
		},
	}
}

func (p *pushFixture) push(t *testing.T, liaisons []*cluster.Liaison, opts PushOptions) (*PushResult, error) {
	t.Helper()
	if liaisons == nil {
		liaisons = []*cluster.Liaison{p.li}
	}
	return Push(context.Background(), liaisons, p.art, &p.dec.state.Schemas, p.entries, opts)
}

func batchSizes(f *fakeLiaison) []int {
	var out []int
	for _, b := range f.receivedBatches() {
		out = append(out, batchRows(b))
	}
	return out
}

func TestWorkerCount(t *testing.T) {
	entries := []Entry{{TargetNodes: []string{"a", "b"}}, {TargetNodes: []string{"b", "c"}}, {}, {}}
	n, reason := WorkerCount(math.MaxInt, entries)
	assert.Equal(t, 3, n)
	assert.Contains(t, reason, "3 target data node(s)")
	n, _ = WorkerCount(math.MaxInt, entries[:2])
	assert.Equal(t, 2, n, "clamped to the work items")
	n, _ = WorkerCount(1, entries)
	assert.Equal(t, 1, n)
	n, reason = WorkerCount(16, entries)
	assert.Equal(t, 4, n)
	assert.Contains(t, reason, "clamped")
	n, _ = WorkerCount(math.MaxInt, nil)
	assert.Equal(t, 0, n)
}

func TestPushCutsBatchesByRows(t *testing.T) {
	p := newPushFixture(t)
	var log bytes.Buffer
	opts := pushOpts(&log)
	opts.WriteBatchSize = 2
	opts.Parallelism = 1
	var events []string
	opts.Hook = func(e string) { events = append(events, e) }
	res, err := p.push(t, nil, opts)
	require.NoError(t, err)
	// measure 2 rows, property 2 rows, stream 3 rows; batches never span work items.
	assert.Equal(t, []int{2, 2, 2, 1}, batchSizes(p.f))
	assert.Len(t, events, 4)
	assert.Contains(t, events, "batch stream/sw_stream/hot/20261016/shard-0 seq=1 rows=1")
	assert.Equal(t, uint64(3), res.Rows[pushKey(commonv1.Catalog_CATALOG_STREAM, "sw_stream", "hot", ShardItem)])
	assert.Len(t, res.Done, 3)
	var seqs []uint64
	for _, b := range p.f.receivedBatches() {
		seqs = append(seqs, b.GetBatchSeq())
	}
	assert.Equal(t, []uint64{0, 0, 0, 1}, seqs, "one stream per (catalog, group, stage), batch_seq from 0")
	assert.Len(t, p.f.inits, 3)
}

func TestPushCutsBatchesByBytesAndSendsAnOversizedRowAlone(t *testing.T) {
	p := newPushFixture(t)
	var log bytes.Buffer
	opts := pushOpts(&log)
	opts.Parallelism = 1
	opts.WriteBatchBytes = 1
	_, err := p.push(t, nil, opts)
	require.NoError(t, err)
	assert.Equal(t, []int{1, 1, 1, 1, 1, 1, 1}, batchSizes(p.f))
	assert.Contains(t, log.String(), "more than --write-batch-bytes 1; it is sent in a batch of its own")

	q := newPushFixture(t)
	opts.WriteBatchBytes = 0
	// Room for exactly two stream rows: each costs its encoding plus its length prefix and tag.
	_, rows := streamRows(t, q)
	var rowSize int
	for _, r := range rows {
		rowSize = max(rowSize, 1+csvfmt.RowSize(r.msg))
	}
	q.entries = slices.DeleteFunc(q.entries, func(e Entry) bool { return e.Item.Catalog != commonv1.Catalog_CATALOG_STREAM })
	opts.WriteBatchBytes = uint64(2 * rowSize)
	_, err = q.push(t, nil, opts)
	require.NoError(t, err)
	assert.Equal(t, []int{2, 1}, batchSizes(q.f))
}

func TestPushRetriesARefusedFirstBatchOnTheNextLiaison(t *testing.T) {
	fastPolls(t)
	p := newPushFixture(t)
	busy := newFakeLiaison()
	mirrorFake(busy, dataSchemas())
	busy.importErr = func(context.Context, *transferv1.ImportRowsRequest_Init, uint64) error {
		return status.Error(codes.ResourceExhausted, "server is under memory pressure, please retry later")
	}
	busyLi := busy.serve(t)
	var log bytes.Buffer
	opts := pushOpts(&log)
	opts.Parallelism = 1
	_, err := p.push(t, []*cluster.Liaison{busyLi, p.li}, opts)
	require.NoError(t, err)
	assert.Equal(t, 7, sum(batchSizes(p.f)))
	assert.Empty(t, busy.receivedBatches())
	assert.Contains(t, log.String(), "retrying via "+p.li.Addr)
}

func TestPushWaitsForASchemaNotLiveYet(t *testing.T) {
	fastPolls(t)
	p := newPushFixture(t)
	var refused atomic.Int32
	p.f.importErr = func(_ context.Context, _ *transferv1.ImportRowsRequest_Init, seq uint64) error {
		if seq == 0 && refused.Add(1) <= 2 {
			return importStatus(codes.NotFound, transfer.ImportReasonSchemaNotFound, map[string]string{transfer.ImportMetadataBatchSeq: "0"},
				"batch_seq=0: stream sw_stream/log not found")
		}
		return nil
	}
	var log bytes.Buffer
	opts := pushOpts(&log)
	opts.Parallelism = 1
	_, err := p.push(t, nil, opts)
	require.NoError(t, err)
	assert.Equal(t, 7, sum(batchSizes(p.f)))
	assert.Contains(t, log.String(), "NotFound")
}

func TestPushFailsOnAMidStreamErrorWithoutRetrying(t *testing.T) {
	fastPolls(t)
	p := newPushFixture(t)
	p.f.importErr = func(_ context.Context, init *transferv1.ImportRowsRequest_Init, seq uint64) error {
		if init.GetCatalog() == commonv1.Catalog_CATALOG_STREAM && seq == 1 {
			return nodeFailed(codes.Unavailable, "1", "data-a", "connection refused")
		}
		return nil
	}
	var log bytes.Buffer
	opts := pushOpts(&log)
	opts.Parallelism = 1
	opts.WriteBatchSize = 1
	_, err := p.push(t, nil, opts)
	require.Error(t, err)
	assert.Equal(t, exitcode.ExitRuntime, exitcode.For(err))
	assert.Equal(t, 1, strings.Count(err.Error(), "stream/sw_stream/hot/20261016/shard-0"), "the work item is named once: %v", err)
	assert.ErrorContains(t, err, "record 2")
	assert.ErrorContains(t, err, "2 of 3 work item(s) done")
	assert.Contains(t, log.String(), "work items not done (1):\n  stream/sw_stream/hot/20261016/shard-0")
	assert.Equal(t, 1, streamInitCount(p.f), "a stream is never reopened once a batch was acknowledged")
}

func TestPushExplainsAResourceExhaustedBatch(t *testing.T) {
	cases := map[string]struct {
		err     error
		notWant string
		want    []string
	}{
		"disk full": {
			err:     nodeFailed(codes.ResourceExhausted, "0", "data-a", "disk full"),
			want:    []string{"data node data-a refused the batch for lack of resources", "free disk space", "MiB"},
			notWant: "--write-batch-bytes",
		},
		"message too large": {
			err:     importStatus(codes.ResourceExhausted, transfer.ImportReasonMessageTooLarge, nil, "grpc: received message larger than max (9 vs 8)"),
			want:    []string{"lower --write-batch-bytes", "MiB"},
			notWant: "disk",
		},
		"load shedding without a detail": {
			err:     status.Error(codes.ResourceExhausted, "server is under memory pressure, please retry later"),
			want:    []string{"refused the import stream under load", "--send-retry-timeout", "MiB"},
			notWant: "--write-batch-bytes",
		},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			p := newPushFixture(t)
			p.f.importErr = func(context.Context, *transferv1.ImportRowsRequest_Init, uint64) error { return c.err }
			var log bytes.Buffer
			opts := pushOpts(&log)
			opts.SendRetryTimeout = 0
			_, err := p.push(t, nil, opts)
			require.Error(t, err)
			for _, w := range c.want {
				assert.ErrorContains(t, err, w)
			}
			assert.NotContains(t, err.Error(), c.notWant)
		})
	}
}

func TestPushRequestsDoNotDependOnParallelism(t *testing.T) {
	requests := func(parallelism int) []string {
		p := newPushFixture(t)
		var log bytes.Buffer
		opts := pushOpts(&log)
		opts.Parallelism = parallelism
		opts.WriteBatchSize = 1
		_, err := p.push(t, nil, opts)
		require.NoError(t, err)
		var out []string
		for _, b := range p.f.receivedBatches() {
			b.BatchSeq = 0
			out = append(out, prototext.Format(b))
		}
		slices.Sort(out)
		return out
	}
	assert.Equal(t, requests(1), requests(math.MaxInt))
}

func TestPushLimitsTheRowRate(t *testing.T) {
	p := newPushFixture(t)
	var log bytes.Buffer
	opts := pushOpts(&log)
	opts.WriteBatchSize = 1
	opts.WriteRateLimit = 5
	opts.Parallelism = 1
	start := time.Now()
	_, err := p.push(t, nil, opts)
	require.NoError(t, err)
	// 7 rows at 5 rows/s with a burst of 5: the last 2 rows wait for about 400ms.
	elapsed := time.Since(start)
	assert.GreaterOrEqual(t, elapsed, 300*time.Millisecond)
	assert.Less(t, elapsed, 5*time.Second)
	assert.Equal(t, 7, sum(batchSizes(p.f)))
	assert.Equal(t, uint64(0), p.f.inits[0].GetLiaisonBytesPerSecond())
}

func TestPushRateLimitBurstCoversABatch(t *testing.T) {
	p := newPushFixture(t)
	var log bytes.Buffer
	opts := pushOpts(&log)
	opts.WriteBatchSize = 7
	opts.WriteRateLimit = 2
	opts.Parallelism = 1
	start := time.Now()
	// A batch of more rows than the rate must not exceed the burst.
	_, err := p.push(t, nil, opts)
	require.NoError(t, err)
	assert.Less(t, time.Since(start), 2*time.Second)
	assert.Equal(t, 7, sum(batchSizes(p.f)))
}

func TestPushSendsTheRatesInInit(t *testing.T) {
	p := newPushFixture(t)
	var log bytes.Buffer
	opts := pushOpts(&log)
	opts.LiaisonRateLimit = 100<<20 + 0.5
	opts.NodeRateLimit = 50<<20 + 0.5
	opts.NodeRowRateLimit = 1000.9
	_, err := p.push(t, nil, opts)
	require.NoError(t, err)
	require.NotEmpty(t, p.f.inits)
	for _, in := range p.f.inits {
		assert.Equal(t, uint64(100<<20), in.GetLiaisonBytesPerSecond())
		assert.Equal(t, uint64(50<<20), in.GetRateLimit().GetBytesPerSecond())
		assert.Equal(t, uint64(1000), in.GetRateLimit().GetRowsPerSecond())
	}
}

func TestPushSendsTheSameNodeRatesToEveryLiaison(t *testing.T) {
	p := newPushFixture(t)
	var log bytes.Buffer
	opts := pushOpts(&log)
	opts.Parallelism = 2
	opts.NodeRateLimit = 90
	opts.NodeRowRateLimit = 61
	// Every liaison enforces the configured rates on its own: they are not split.
	_, err := p.push(t, []*cluster.Liaison{p.li, p.li, p.li}, opts)
	require.NoError(t, err)
	require.NotEmpty(t, p.f.inits)
	for _, in := range p.f.inits {
		assert.Equal(t, uint64(90), in.GetRateLimit().GetBytesPerSecond())
		assert.Equal(t, uint64(61), in.GetRateLimit().GetRowsPerSecond())
	}
}

func streamInitCount(f *fakeLiaison) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, in := range f.inits {
		if in.GetCatalog() == commonv1.Catalog_CATALOG_STREAM {
			n++
		}
	}
	return n
}

func TestPushRetriesOnlyAFirstBatchNoNodeTookPartIn(t *testing.T) {
	fastPolls(t)
	cases := map[string]struct {
		err   error
		retry bool
	}{
		"liaison gone after the batch was sent": {err: status.Error(codes.Unavailable, "the liaison is draining")},
		"liaison under load":                    {err: status.Error(codes.ResourceExhausted, "server is under memory pressure, please retry later"), retry: true},
		"data node unavailable":                 {err: nodeFailed(codes.Unavailable, "0", "data-a", "connection refused")},
		"data node disk full":                   {err: nodeFailed(codes.ResourceExhausted, "0", "data-a", "disk full")},
		"batch larger than accepted": {
			err: importStatus(codes.ResourceExhausted, transfer.ImportReasonMessageTooLarge, nil, "grpc: received message larger than max (9 vs 8)"),
		},
		"invalid row": {
			err: importStatus(codes.InvalidArgument, transfer.ImportReasonRowInvalid,
				map[string]string{transfer.ImportMetadataBatchSeq: "0", transfer.ImportMetadataRow: "0"}, "batch_seq=0: row 0: element is required"),
		},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			p := newPushFixture(t)
			var refused atomic.Int32
			p.f.importErr = func(_ context.Context, init *transferv1.ImportRowsRequest_Init, seq uint64) error {
				if init.GetCatalog() == commonv1.Catalog_CATALOG_STREAM && seq == 0 && refused.Add(1) == 1 {
					return c.err
				}
				return nil
			}
			var log bytes.Buffer
			opts := pushOpts(&log)
			opts.Parallelism = 1
			_, err := p.push(t, nil, opts)
			if c.retry {
				require.NoError(t, err)
				assert.Equal(t, 2, streamInitCount(p.f))
				assert.Contains(t, log.String(), "retrying via")
				return
			}
			require.Error(t, err)
			assert.Equal(t, exitcode.ExitRuntime, exitcode.For(err))
			assert.ErrorContains(t, err, status.Convert(c.err).Message())
			assert.Equal(t, 1, streamInitCount(p.f), "the first batch is not resent")
			assert.NotContains(t, log.String(), "retrying via")
		})
	}
}

func TestPushRetriesAMeasureBatchAfterAnUnavailableWithoutDetail(t *testing.T) {
	fastPolls(t)
	p := newPushFixture(t)
	var refused atomic.Int32
	p.f.importErr = func(_ context.Context, init *transferv1.ImportRowsRequest_Init, seq uint64) error {
		if init.GetCatalog() == commonv1.Catalog_CATALOG_MEASURE && seq == 0 && refused.Add(1) == 1 {
			return status.Error(codes.Unavailable, "the liaison is draining")
		}
		return nil
	}
	var log bytes.Buffer
	opts := pushOpts(&log)
	opts.Parallelism = 1
	_, err := p.push(t, nil, opts)
	require.NoError(t, err, "a measure row written twice is stored once, so the batch is resent")
	assert.Contains(t, log.String(), "retrying via")
}

func TestPushGivesUpWhenSendRetryTimeoutIsUsedUp(t *testing.T) {
	fastPolls(t)
	p := newPushFixture(t)
	p.f.importErr = func(_ context.Context, _ *transferv1.ImportRowsRequest_Init, seq uint64) error {
		if seq == 0 {
			return status.Error(codes.ResourceExhausted, "server is under memory pressure, please retry later")
		}
		return nil
	}
	var log bytes.Buffer
	opts := pushOpts(&log)
	opts.Parallelism = 1
	opts.SendRetryTimeout = 50 * time.Millisecond
	_, err := p.push(t, nil, opts)
	require.Error(t, err)
	assert.Equal(t, exitcode.ExitRuntime, exitcode.For(err))
	assert.ErrorContains(t, err, "server is under memory pressure")
	p.f.mu.Lock()
	inits := len(p.f.inits)
	p.f.mu.Unlock()
	assert.Greater(t, inits, 1)
	assert.Less(t, inits, 20)
	assert.Contains(t, log.String(), "retrying via")
}

func TestPushDoesNotRetryAnOversizedFirstBatch(t *testing.T) {
	fastPolls(t)
	p := newPushFixture(t)
	p.f.importErr = func(_ context.Context, _ *transferv1.ImportRowsRequest_Init, seq uint64) error {
		if seq == 0 {
			return importStatus(codes.ResourceExhausted, transfer.ImportReasonMessageTooLarge, nil, "grpc: received message larger than max (9 vs 8)")
		}
		return nil
	}
	var log bytes.Buffer
	opts := pushOpts(&log)
	opts.Parallelism = 1
	_, err := p.push(t, nil, opts)
	require.Error(t, err)
	assert.ErrorContains(t, err, "--write-batch-bytes")
	assert.Len(t, p.f.inits, 1)
}

// streamRows are the rows of the fixture's stream work item, in sending order.
func streamRows(t *testing.T, p *pushFixture) (*Entry, []row) {
	t.Helper()
	for i := range p.entries {
		e := &p.entries[i]
		if e.Item.Catalog != commonv1.Catalog_CATALOG_STREAM {
			continue
		}
		var rows []row
		b := newRowBuilder(layoutOf(t, &p.art.Schemas, e.Item.Catalog, e.Item.Group), &p.dec.state.Schemas, e.Item.Catalog, e.Item.Group)
		require.NoError(t, b.itemRows(context.Background(), p.art.Dir, &e.Item, func(r row) error {
			rows = append(rows, r)
			return nil
		}))
		return e, rows
	}
	t.Fatal("no stream work item")
	return nil, nil
}

func TestPushNamesTheRowTheLiaisonRejected(t *testing.T) {
	rangeOf := func(rows []row) string {
		last := rows[len(rows)-1]
		return fmt.Sprintf("file %s record %d to file %s record %d", rows[0].file, rows[0].record, last.file, last.record)
	}
	cases := map[string]struct {
		err   error
		where func(rows []row) string
	}{
		"the named row": {
			err: importStatus(codes.InvalidArgument, transfer.ImportReasonRowInvalid,
				map[string]string{transfer.ImportMetadataBatchSeq: "0", transfer.ImportMetadataRow: "1"}, "batch_seq=0: row 1: element is required"),
			where: func(rows []row) string { return fmt.Sprintf("(file %s record %d)", rows[1].file, rows[1].record) },
		},
		"a row named only in the message": {
			err:   status.Error(codes.InvalidArgument, "batch_seq=0: row 1: element is required"),
			where: rangeOf,
		},
		"the batch's range": {
			err:   status.Error(codes.InvalidArgument, "batch_seq=0: the batch is refused"),
			where: rangeOf,
		},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			p := newPushFixture(t)
			e, rows := streamRows(t, p)
			require.Greater(t, len(rows), 1)
			p.f.importErr = func(_ context.Context, init *transferv1.ImportRowsRequest_Init, _ uint64) error {
				if init.GetCatalog() == commonv1.Catalog_CATALOG_STREAM {
					return c.err
				}
				return nil
			}
			var log bytes.Buffer
			opts := pushOpts(&log)
			opts.Parallelism = 1
			_, err := p.push(t, nil, opts)
			require.Error(t, err)
			assert.ErrorContains(t, err, c.where(rows))
			assert.Equal(t, 1, strings.Count(err.Error(), e.Item.Key()), "the work item is named once: %v", err)
		})
	}
}

var workItemsCount = regexp.MustCompile(`work items (?:not )?done \((\d+)\)`)

func TestPushFailureCancelsOtherWorkers(t *testing.T) {
	p := newPushFixture(t)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	var blocked atomic.Int32
	p.f.importErr = func(ctx context.Context, init *transferv1.ImportRowsRequest_Init, _ uint64) error {
		if init.GetCatalog() == commonv1.Catalog_CATALOG_STREAM {
			return status.Error(codes.InvalidArgument, "batch_seq=0: row 0: element is required")
		}
		blocked.Add(1)
		defer blocked.Add(-1)
		select {
		case <-ctx.Done():
		case <-release:
		case <-time.After(10 * time.Second):
		}
		return nil
	}
	var log bytes.Buffer
	opts := pushOpts(&log)
	opts.Parallelism = len(p.entries)
	start := time.Now()
	_, err := p.push(t, nil, opts)
	require.Error(t, err)
	assert.Less(t, time.Since(start), 5*time.Second, "the other workers are canceled, not waited for")
	workers, _ := WorkerCount(opts.Parallelism, p.entries)
	assert.Equal(t, len(p.entries), workers)
	// The blocked batches return once their streams are canceled, and none of them is stored.
	require.Eventually(t, func() bool { return blocked.Load() == 0 }, 5*time.Second, 10*time.Millisecond)
	assert.Empty(t, p.f.receivedBatches(), "no batch is stored after the failure")
	total := 0
	for _, m := range workItemsCount.FindAllStringSubmatch(log.String(), -1) {
		n, convErr := strconv.Atoi(m[1])
		require.NoError(t, convErr)
		total += n
	}
	assert.Equal(t, len(p.entries), total, log.String())
	assert.ErrorContains(t, err, fmt.Sprintf("0 of %d work item(s) done", len(p.entries)))
}

func TestPushReportsAnInterruptionAsTheCause(t *testing.T) {
	p := newPushFixture(t)
	var log bytes.Buffer
	opts := pushOpts(&log)
	opts.Parallelism = 1
	opts.WriteBatchSize = 1
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var once atomic.Bool
	opts.Hook = func(string) {
		if once.CompareAndSwap(false, true) {
			cancel()
		}
	}
	_, err := Push(ctx, []*cluster.Liaison{p.li}, p.art, &p.dec.state.Schemas, p.entries, opts)
	require.Error(t, err)
	assert.True(t, errors.Is(err, context.Canceled), err)
	assert.NotContains(t, err.Error(), "end of the import stream")
	assert.Equal(t, exitcode.ExitRuntime, exitcode.For(err))
	assert.Contains(t, log.String(), "work items done")
	assert.Contains(t, log.String(), "work items not done")
}

func sum(n []int) int {
	var s int
	for _, v := range n {
		s += v
	}
	return s
}

// receivedSeqs records the batch_seq of every stream batch the fake receives, in order.
func receivedSeqs(f *fakeLiaison) func() []uint64 {
	var mu sync.Mutex
	var seqs []uint64
	f.importErr = func(_ context.Context, init *transferv1.ImportRowsRequest_Init, seq uint64) error {
		if init.GetCatalog() == commonv1.Catalog_CATALOG_STREAM {
			mu.Lock()
			seqs = append(seqs, seq)
			mu.Unlock()
		}
		return nil
	}
	return func() []uint64 {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(seqs)
	}
}

func TestPushInflightWindowDoesNotChangeTheRequests(t *testing.T) {
	requests := func(window int) []string {
		p := newPushFixture(t)
		var log bytes.Buffer
		opts := pushOpts(&log)
		opts.Parallelism = 1
		opts.WriteBatchSize = 1
		opts.WriteInflightBatches = window
		res, err := p.push(t, nil, opts)
		require.NoError(t, err)
		assert.Len(t, res.Done, len(p.entries))
		var out []string
		for _, b := range p.f.receivedBatches() {
			out = append(out, prototext.Format(b))
		}
		return out
	}
	one := requests(1)
	assert.Len(t, one, 7)
	assert.Equal(t, one, requests(2))
	assert.Equal(t, one, requests(config.MaxWriteInflightBatches))
}

func TestPushSendsTheNextBatchBeforeTheAck(t *testing.T) {
	for _, c := range []struct {
		window    int
		sentAhead bool
	}{{window: 1}, {window: 2, sentAhead: true}} {
		t.Run(strconv.Itoa(c.window), func(t *testing.T) {
			p := newPushFixture(t)
			seqs := receivedSeqs(p.f)
			var sentAhead atomic.Bool
			wait := 200 * time.Millisecond
			if c.sentAhead {
				wait = 5 * time.Second
			}
			// The fake holds the acknowledgement of stream batch 1 until batch 2 arrives.
			p.f.ackGate = func(init *transferv1.ImportRowsRequest_Init, seq uint64) {
				if init.GetCatalog() != commonv1.Catalog_CATALOG_STREAM || seq != 1 {
					return
				}
				deadline := time.Now().Add(wait)
				for time.Now().Before(deadline) {
					if slices.Contains(seqs(), 2) {
						sentAhead.Store(true)
						return
					}
					time.Sleep(5 * time.Millisecond)
				}
			}
			var log bytes.Buffer
			opts := pushOpts(&log)
			opts.Parallelism = 1
			opts.WriteBatchSize = 1
			opts.WriteInflightBatches = c.window
			res, err := p.push(t, nil, opts)
			require.NoError(t, err)
			assert.Len(t, res.Done, len(p.entries))
			assert.Equal(t, []uint64{0, 1, 2}, seqs())
			assert.Equal(t, c.sentAhead, sentAhead.Load())
		})
	}
}

func TestPushNamesTheFailedInflightBatch(t *testing.T) {
	p := newPushFixture(t)
	e, rows := streamRows(t, p)
	require.Len(t, rows, 3)
	p.f.importErr = func(_ context.Context, init *transferv1.ImportRowsRequest_Init, seq uint64) error {
		if init.GetCatalog() == commonv1.Catalog_CATALOG_STREAM && seq == 2 {
			return nodeFailed(codes.Unavailable, "2", "data-a", "connection refused")
		}
		return nil
	}
	var acked []string
	var log bytes.Buffer
	opts := pushOpts(&log)
	opts.Parallelism = 1
	opts.WriteBatchSize = 1
	opts.Hook = func(ev string) { acked = append(acked, ev) }
	res, err := p.push(t, nil, opts)
	require.Error(t, err)
	assert.ErrorContains(t, err, e.Item.Key()+" via "+p.li.Addr+": batch_seq=2")
	assert.ErrorContains(t, err, fmt.Sprintf("file %s record %d to file %s record %d", rows[2].file, rows[2].record, rows[2].file, rows[2].record))
	assert.ErrorContains(t, err, "2 of 3 work item(s) done")
	assert.Contains(t, log.String(), "work items not done (1):\n  "+e.Item.Key())
	assert.Contains(t, acked, "batch "+e.Item.Key()+" seq=1 rows=1", "the batch before the failed one is acknowledged")
	assert.Equal(t, uint64(2), res.Rows[pushKey(commonv1.Catalog_CATALOG_STREAM, "sw_stream", "hot", ShardItem)], "only acknowledged rows count")
}

func TestFailedBatchPrefersTheNamedBatch(t *testing.T) {
	b1, b2 := &sentBatch{seq: 1}, &sentBatch{seq: 2}
	st := &importStream{inflight: []*sentBatch{b1, b2}}
	w := &worker{}
	assert.Same(t, b2, w.failedBatch(st, nodeFailed(codes.Unavailable, "2", "data-a", "connection refused")))
	assert.Same(t, b1, w.failedBatch(st, status.Error(codes.Unavailable, "gone")))
	assert.Same(t, b1, w.failedBatch(st, nodeFailed(codes.Unavailable, "9", "data-a", "connection refused")))
}

// pushGoroutines lists the stacks of the goroutines of a push: its workers and the client
// side of its import streams.
func pushGoroutines() []string {
	buf := make([]byte, 4<<20)
	var out []string
	for _, g := range strings.Split(string(buf[:runtime.Stack(buf, true)]), "\n\n") {
		if strings.Contains(g, "importer.(*worker)") || strings.Contains(g, "grpc.newClientStreamWithParams") {
			out = append(out, g)
		}
	}
	return out
}

func TestPushLeavesNoGoroutineBehind(t *testing.T) {
	p := newPushFixture(t)
	var log bytes.Buffer
	opts := pushOpts(&log)
	opts.WriteBatchSize = 1
	var seen atomic.Bool
	opts.Hook = func(string) { seen.Store(seen.Load() || len(pushGoroutines()) > 0) }
	for range 3 {
		_, err := p.push(t, nil, opts)
		require.NoError(t, err)
	}
	require.True(t, seen.Load(), "the probe sees the goroutines of a running push")
	opts.Hook = nil
	// A batch in flight fails while the next ones are sent: every stream must still end.
	p.f.importErr = func(_ context.Context, init *transferv1.ImportRowsRequest_Init, seq uint64) error {
		if init.GetCatalog() == commonv1.Catalog_CATALOG_STREAM && seq == 1 {
			return status.Error(codes.Internal, "boom")
		}
		return nil
	}
	_, err := p.push(t, nil, opts)
	require.Error(t, err)
	var left []string
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		if left = pushGoroutines(); len(left) == 0 {
			return
		}
	}
	t.Fatalf("goroutines left behind by Push:\n%s", strings.Join(left, "\n\n"))
}

func TestPushSendsPropertiesInFileOrder(t *testing.T) {
	p := newPushFixture(t)
	var log bytes.Buffer
	opts := pushOpts(&log)
	opts.Parallelism = 1
	opts.WriteBatchSize = 1
	_, err := p.push(t, nil, opts)
	require.NoError(t, err)
	var sent []string
	for _, b := range p.f.receivedBatches() {
		for _, r := range b.GetProperty().GetRows() {
			sent = append(sent, fmt.Sprintf("%s@%d/%d %s", r.GetProperty().GetId(), r.GetProperty().GetMetadata().GetModRevision(),
				r.GetDeleteTime(), r.GetProperty().GetTags()[0].GetValue().GetStr().GetValue()))
		}
	}
	// The artifact lists p1@5 before p1@3 (a tombstone): both are sent as read.
	assert.Equal(t, []string{"p1@5/0 new", fmt.Sprintf("p1@3/%d old", dataTime.UnixNano())}, sent)
}
