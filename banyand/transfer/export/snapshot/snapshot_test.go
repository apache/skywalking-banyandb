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
	"fmt"
	"io/fs"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/apache/skywalking-banyandb/pkg/transfer/ratelimit"
)

func TestReadError(t *testing.T) {
	require.NoError(t, ReadError("x", nil))
	require.Equal(t, codes.FailedPrecondition, status.Code(ReadError("x", fmt.Errorf("open: %w", fs.ErrNotExist))))
	require.Equal(t, codes.Canceled, status.Code(ReadError("x", context.Canceled)))
	require.Equal(t, codes.Unavailable, status.Code(ReadError("x", status.Error(codes.Unavailable, "gone"))))
	require.Equal(t, codes.Internal, status.Code(ReadError("x", errors.New("boom"))))
}

func TestLimiters(t *testing.T) {
	require.Equal(t, ratelimit.MaxBurst, ChunkSize, "a chunk is the relay's one-chunk burst")
	require.Nil(t, NewBucket(0))
	require.Equal(t, 1, NewBucket(1).Burst(), "one second's worth")
	require.Equal(t, 4*ChunkSize, NewBucket(4*ChunkSize).Burst())
	// A chunk always fits the burst: a rate below ChunkSize sends one second's worth at a time.
	require.Equal(t, ChunkSize, ChunkLimit(nil))
	require.Equal(t, 1, ChunkLimit(NewBucket(1)))
	require.Equal(t, 64<<10, ChunkLimit(NewBucket(64<<10)))
	require.Equal(t, ChunkSize, ChunkLimit(NewBucket(4*ChunkSize)))
	require.NoError(t, ratelimit.Wait(context.Background(), NewBucket(1), ChunkLimit(NewBucket(1))))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.Equal(t, codes.Canceled, status.Code(ratelimit.Wait(ctx, NewBucket(1), 1)))
}
