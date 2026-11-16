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

package snapshot

import (
	"context"
	"errors"
	"io/fs"
	"math"

	"golang.org/x/time/rate"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer/ratelimit"
)

// ChunkSize is the largest DataChunk an Export stream sends: the relay's one-chunk burst, so
// that a relay never splits a chunk.
const ChunkSize = ratelimit.MaxBurst

// ReadError maps a failure while reading the snapshot to the stream status: a file that
// vanished means the session was preempted (FailedPrecondition), a canceled stream keeps
// its context status, gRPC statuses pass through, anything else is Internal.
func ReadError(what string, err error) error {
	if err == nil {
		return nil
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return status.FromContextError(err).Err()
	}
	if errors.Is(err, fs.ErrNotExist) {
		return status.Errorf(codes.FailedPrecondition, "%s: the export session snapshot is gone (preempted or released): %v", what, err)
	}
	return status.Errorf(codes.Internal, "%s: %v", what, err)
}

// ChunkLimit is the largest chunk a stream with the byte bucket l sends: ChunkSize, or one
// second's worth of a lower rate (the bucket's burst), so that a throttled stream still sends
// a chunk every second; nil is unlimited.
func ChunkLimit(l *rate.Limiter) int {
	if l == nil {
		return ChunkSize
	}
	return min(ChunkSize, l.Burst())
}

// NewBucket returns the per-stream bucket of perSecond bytes or rows, its burst one second's
// worth, or nil when perSecond is 0 (unlimited). ChunkLimit keeps a chunk within the burst.
func NewBucket(perSecond uint64) *rate.Limiter { return ratelimit.New(perSecond, math.MaxInt32) }

// Framer frames the files of one shard, or the segment-level files, of an Export stream into
// file_started / chunk / file_finished, buffering chunks of up to ChunkLimit bytes. A chunk
// waits for the byte bucket before it is sent, so a slow client or a low rate stops the
// reading.
type Framer struct {
	Ctx   context.Context
	Bytes *rate.Limiter
	Send  func(*transferv1.ExportResponse) error
	// OnChunk, when set, is called with each chunk right before it is sent.
	OnChunk      func(*transferv1.DataChunk)
	buf          []byte
	ShardID      uint32
	ShardIndex   uint32
	SegmentLevel bool
}

// StartFile sends the file_started frame of a file.
func (f *Framer) StartFile(relativePath string, seq uint32) error {
	return f.Send(&transferv1.ExportResponse{Frame: &transferv1.ExportResponse_FileStarted{FileStarted: &transferv1.FileStarted{
		RelativePath: relativePath,
		ShardId:      f.ShardID,
		SegmentLevel: f.SegmentLevel,
		ShardIndex:   f.ShardIndex,
		Seq:          seq,
	}}})
}

// Write buffers p into chunks and sends every full one; it copies p, which the caller reuses.
func (f *Framer) Write(p []byte) error {
	limit := ChunkLimit(f.Bytes)
	for len(p) > 0 {
		if f.buf == nil {
			f.buf = make([]byte, 0, limit)
		}
		n := min(len(p), limit-len(f.buf))
		f.buf = append(f.buf, p[:n]...)
		p = p[n:]
		if len(f.buf) == limit {
			if err := f.Flush(); err != nil {
				return err
			}
		}
	}
	return nil
}

// WriteOwned sends the buffered bytes, then p in chunks without copying it; the caller must
// not change p afterwards.
func (f *Framer) WriteOwned(p []byte) error {
	if err := f.Flush(); err != nil {
		return err
	}
	limit := ChunkLimit(f.Bytes)
	for len(p) > 0 {
		n := min(len(p), limit)
		if err := f.sendChunk(p[:n:n]); err != nil {
			return err
		}
		p = p[n:]
	}
	return nil
}

// Flush sends the buffered bytes as one chunk, if any.
func (f *Framer) Flush() error {
	if len(f.buf) == 0 {
		return nil
	}
	// The frame owns its bytes: gRPC may still hold the message after Send returns.
	content := f.buf
	f.buf = nil
	return f.sendChunk(content)
}

// Finish sends the buffered bytes, then the file_finished frame finished.
func (f *Framer) Finish(finished *transferv1.FileFinished) error {
	if err := f.Flush(); err != nil {
		return err
	}
	return f.Send(&transferv1.ExportResponse{Frame: &transferv1.ExportResponse_FileFinished{FileFinished: finished}})
}

func (f *Framer) sendChunk(content []byte) error {
	if err := ratelimit.Wait(f.Ctx, f.Bytes, len(content)); err != nil {
		return err
	}
	chunk := &transferv1.DataChunk{Content: content}
	if f.OnChunk != nil {
		f.OnChunk(chunk)
	}
	return f.Send(&transferv1.ExportResponse{Frame: &transferv1.ExportResponse_Chunk{Chunk: chunk}})
}
