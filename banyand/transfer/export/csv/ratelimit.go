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

package csv

import (
	"context"
	"math"
	"time"

	"golang.org/x/time/rate"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// chunkSize is the largest DataChunk an Export stream sends.
	chunkSize = 1 << 20
	// earlyFlush is how long a stream that waits for its row bucket at all holds the rows it
	// has written before it sends them.
	earlyFlush = time.Second
	// earlyFlushWait is the wait for the row bucket before which a stream sends the rows it
	// holds right away, so that a low row rate shows every row as it is written.
	earlyFlushWait = 100 * time.Millisecond
)

// chunkLimit is the largest chunk a stream with the byte bucket l sends: chunkSize, or one
// second's worth of a lower rate (the bucket's burst), so that a throttled stream still sends
// a chunk every second; nil is unlimited.
func chunkLimit(l *rate.Limiter) int {
	if l == nil {
		return chunkSize
	}
	return min(chunkSize, l.Burst())
}

// newByteLimiter returns the per-stream byte bucket for bytesPerSecond, or nil when it is 0
// (unlimited). The burst is one second's worth, and chunkLimit keeps a chunk within it, so
// WaitN never fails for a chunk.
func newByteLimiter(bytesPerSecond uint64) *rate.Limiter {
	if bytesPerSecond == 0 {
		return nil
	}
	return rate.NewLimiter(rate.Limit(float64(bytesPerSecond)), burstOf(bytesPerSecond))
}

// newRowLimiter returns the per-stream row bucket for rowsPerSecond, or nil when it is 0.
func newRowLimiter(rowsPerSecond uint64) *rate.Limiter {
	if rowsPerSecond == 0 {
		return nil
	}
	return rate.NewLimiter(rate.Limit(float64(rowsPerSecond)), burstOf(max(1, rowsPerSecond)))
}

func burstOf(v uint64) int {
	if v > math.MaxInt32 {
		return math.MaxInt32
	}
	return int(v)
}

// rowWait is how long taking one token from l would wait now.
func rowWait(l *rate.Limiter) time.Duration {
	tokens := l.Tokens()
	if tokens >= 1 {
		return 0
	}
	return time.Duration((1 - tokens) / float64(l.Limit()) * float64(time.Second))
}

// waitN takes n tokens from l, blocking until they are available; a nil limiter never
// waits. A canceled stream surfaces as its gRPC status.
func waitN(ctx context.Context, l *rate.Limiter, n int) error {
	if l == nil {
		return nil
	}
	if err := l.WaitN(ctx, n); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return status.FromContextError(ctxErr).Err()
		}
		// Every take fits the burst, so the wait would outlast the context's deadline.
		return status.Error(codes.DeadlineExceeded, err.Error())
	}
	return nil
}
