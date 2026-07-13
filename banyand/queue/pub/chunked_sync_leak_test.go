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

package pub

import (
	"context"
	"runtime"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// reaperFrame is the gRPC client-side stream cleanup goroutine that
// (*clientStream) spawns per streaming RPC in newClientStreamWithParams. It
// blocks on `select { <-cc.ctx.Done(); <-ctx.Done() }` and only exits when the
// per-call context is cancelled or the ClientConn is closed — NOT when the
// stream is drained or CloseSend() is called (see grpc stream.go). SyncPart is a
// streaming RPC, so each call spawns one; if its context is never cancelled the
// goroutine leaks forever.
const reaperFrame = "newClientStreamWithParams"

// countReaperGoroutines returns the number of live goroutines currently parked
// in the gRPC client-stream reaper. It counts whole goroutine blocks (not raw
// substring hits) so it is robust to grpc's internal closure numbering and to
// unrelated goroutine noise.
func countReaperGoroutines() int {
	buf := make([]byte, 1<<22)
	n := runtime.Stack(buf, true)
	count := 0
	for _, g := range strings.Split(string(buf[:n]), "\n\ngoroutine ") {
		if strings.Contains(g, reaperFrame) {
			count++
		}
	}
	return count
}

// stableReaperCount polls countReaperGoroutines until two consecutive readings
// agree, so reapers that are mid-teardown (the fixed path cancels them
// asynchronously) settle before we assert, keeping the test non-flaky.
func stableReaperCount() int {
	prev := -1
	for i := 0; i < 100; i++ {
		time.Sleep(20 * time.Millisecond)
		cur := countReaperGoroutines()
		if cur == prev {
			return cur
		}
		prev = cur
	}
	return prev
}

// firstReaperStack returns one reaper goroutine's stack, surfaced in the failure
// message so a regression is immediately actionable.
func firstReaperStack() string {
	buf := make([]byte, 1<<22)
	n := runtime.Stack(buf, true)
	for _, g := range strings.Split(string(buf[:n]), "\n\ngoroutine ") {
		if strings.Contains(g, reaperFrame) {
			return "goroutine " + strings.TrimSpace(g)
		}
	}
	return "<none>"
}

func newLeakTestClient(t *testing.T) *chunkedSyncClient {
	t.Helper()
	address := getAddress()
	_, cleanup := setupChunkedSyncServer(address, 0) // 0 mismatches -> success path
	t.Cleanup(cleanup)

	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to dial mock server: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	return &chunkedSyncClient{
		conn:      conn,
		log:       logger.GetLogger("test-chunked-sync-leak"),
		chunkSize: 1024,
	}
}

func leakTestParts() []queue.StreamingPartData {
	return []queue.StreamingPartData{
		{
			ID:      1,
			Group:   "test-group",
			ShardID: 1,
			Topic:   "stream_write",
			Files: []queue.FileInfo{
				createFileInfo("leak-test-file.dat", []byte("payload for chunked sync leak test")),
			},
		},
	}
}

// TestSyncStreamingPartsDoesNotLeakReaperGoroutines is a regression guard for the
// production goroutine leak where a liaison accumulated 347k goroutines parked in
// grpc's newClientStreamWithParams reaper, all from SyncPart streams whose context
// was never cancelled.
//
// SyncStreamingParts opens a SyncPart client stream and only CloseSend()s it; the
// reaper exits solely on context cancellation / ClientConn close. In production
// the caller passes the write-queue syncer's process-lifetime loopCloser.Ctx()
// (context.WithCancel(context.Background()), cancelled only at shutdown), and the
// client breaks out of the Recv loop on the SyncResult frame before reading the
// trailing io.EOF — so the stream is never finished and the reaper leaks.
//
// This test reproduces exactly that condition: a single never-cancelled context
// shared across many syncs. It passes only because SyncStreamingParts derives a
// per-call cancellable context and defer-cancels it (chunked_sync.go). Reverting
// that fix makes reapers accumulate ~1 per sync and this test fails.
func TestSyncStreamingPartsDoesNotLeakReaperGoroutines(t *testing.T) {
	const iterations = 30

	client := newLeakTestClient(t)

	// Warm up the lazy ClientConn/transport so its long-lived background
	// goroutines exist before the baseline (they are not reapers, but this keeps
	// the environment steady).
	warmCtx, warmCancel := context.WithTimeout(context.Background(), 10*time.Second)
	if _, err := client.SyncStreamingParts(warmCtx, leakTestParts()); err != nil {
		warmCancel()
		t.Fatalf("warm-up sync failed: %v", err)
	}
	warmCancel()

	baseline := stableReaperCount()

	// Reproduce the production trigger: the caller's context is NEVER cancelled
	// while the syncs run (mirrors loopCloser.Ctx()).
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for i := 0; i < iterations; i++ {
		result, err := client.SyncStreamingParts(ctx, leakTestParts())
		if err != nil {
			t.Fatalf("sync %d failed: %v", i, err)
		}
		// Asserting success guarantees the SyncPart stream path actually ran, so a
		// clean reaper count reflects proper cleanup rather than a vacuous no-op.
		if !result.Success {
			t.Fatalf("sync %d not successful: %+v", i, result)
		}
	}

	after := stableReaperCount()
	leaked := after - baseline
	t.Logf("reaper goroutines: baseline=%d after=%d leaked=%d over %d syncs (never-cancelled context)",
		baseline, after, leaked, iterations)

	// Each leaked sync would park exactly one reaper; allow a tiny slack for
	// scheduling. A number near `iterations` means the SyncPart stream context is
	// no longer being cancelled — the leak has regressed.
	if leaked > 2 {
		t.Fatalf("SyncStreamingParts leaked %d gRPC reaper goroutines over %d syncs with a never-cancelled "+
			"context — SyncPart's stream context must be cancelled on every return path (chunked_sync.go). "+
			"Sample leaked stack:\n%s", leaked, iterations, firstReaperStack())
	}
}
