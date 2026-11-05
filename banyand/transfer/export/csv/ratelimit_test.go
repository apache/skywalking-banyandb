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
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/csvfmt"
)

// rateStream is one throttled CSV stream of a single-tag property layout whose frames are
// recorded.
type rateStream struct {
	fw     *fileWriter
	frames *[]*transferv1.ExportResponse
	cells  []string
	tag    int
}

func newRateStream(t *testing.T, bytes, rows *rate.Limiter, gzip bool) *rateStream {
	t.Helper()
	layout, err := csvfmt.PropertyLayout([]*databasev1.Property{{
		Metadata: &commonv1.Metadata{Name: "ui", Group: "g"},
		Tags:     []*databasev1.TagSpec{{Name: "k", Type: databasev1.TagType_TAG_TYPE_STRING}},
	}})
	require.NoError(t, err)
	var frames []*transferv1.ExportResponse
	settings := &streamSettings{bytes: bytes, rows: rows, gzip: gzip, send: func(r *transferv1.ExportResponse) error {
		frames = append(frames, r)
		return nil
	}}
	run := newUnitRun(context.Background(), nil, settings)
	idx, ok := layout.TagIndex("ui", "", "k")
	require.True(t, ok)
	return &rateStream{
		fw:     run.newWriter(layout, "property/g/shard-0", 0, 0, false, map[string]int64{"ui": 1}),
		frames: &frames,
		cells:  layout.NewRow(),
		tag:    idx,
	}
}

// write writes n rows whose tag value is size bytes and closes the file. It returns how
// long it took, the file and the chunks in the order they were sent.
func (s *rateStream) write(t *testing.T, n, size int) (time.Duration, *exportedFile, []*transferv1.DataChunk) {
	t.Helper()
	value := strings.Repeat("x", size)
	start := time.Now()
	for i := 0; i < n; i++ {
		s.cells[s.tag] = value
		require.NoError(t, s.fw.writeRow("ui", s.cells))
	}
	require.NoError(t, s.fw.close())
	elapsed := time.Since(start)
	files := decodeFiles(t, *s.frames)
	require.Len(t, files, 1)
	require.Len(t, csvRows(t, files[0]), n, "the file decodes to every row written")
	var chunks []*transferv1.DataChunk
	for _, f := range *s.frames {
		if c := f.GetChunk(); c != nil {
			chunks = append(chunks, c)
		}
	}
	return elapsed, files[0], chunks
}

// drained returns l with its bucket emptied, so that every take waits for the rate.
func drained(l *rate.Limiter) *rate.Limiter {
	l.AllowN(time.Now(), l.Burst())
	return l
}

// byteRateTime is how long sending total bytes at bytesPerSecond takes from a full bucket.
func byteRateTime(total, bytesPerSecond int) time.Duration {
	burst := min(total, bytesPerSecond)
	return time.Duration(float64(total-burst) / float64(bytesPerSecond) * float64(time.Second))
}

// assertTook checks elapsed against the time a rate needs, tolerating a slow machine.
func assertTook(t *testing.T, elapsed, want time.Duration) {
	t.Helper()
	require.GreaterOrEqual(t, elapsed, want*85/100, "a throttled stream must not finish before its rate allows")
	require.LessOrEqual(t, elapsed, want+4*time.Second, "the stream took far longer than its rate needs")
}

// Below, at and above 1 MiB/s every chunk is at most one second's worth or 1 MiB, every chunk
// but the last is full, the first never exceeds the burst, and a known volume takes the time
// the rate needs beyond the burst.
func TestFrameSink_ByteRateChunksAndPace(t *testing.T) {
	for name, tc := range map[string]struct {
		bytesPerSecond int
		wantChunk      int
		rows           int
	}{
		"below 1 MiB/s": {bytesPerSecond: 256 << 10, wantChunk: 256 << 10, rows: 10},
		"at 1 MiB/s":    {bytesPerSecond: 1 << 20, wantChunk: 1 << 20, rows: 40},
		"above 1 MiB/s": {bytesPerSecond: 4 << 20, wantChunk: 1 << 20, rows: 160},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			limiter := newBucket(uint64(tc.bytesPerSecond))
			require.Equal(t, tc.wantChunk, chunkLimit(limiter))
			// 64 KiB rows: about 1.5s beyond the one-second burst at every rate.
			elapsed, file, chunks := newRateStream(t, limiter, nil, false).write(t, tc.rows, 64<<10)
			require.NotEmpty(t, chunks)
			require.LessOrEqual(t, len(chunks[0].GetContent()), limiter.Burst(), "the first chunk fits the burst")
			for i, c := range chunks {
				if i < len(chunks)-1 {
					require.Len(t, c.GetContent(), tc.wantChunk, "chunk %d of %d is not full", i, len(chunks))
					continue
				}
				require.LessOrEqual(t, len(c.GetContent()), tc.wantChunk)
			}
			require.Len(t, chunks, (len(file.content)+tc.wantChunk-1)/tc.wantChunk)
			assertTook(t, elapsed, byteRateTime(len(file.content), tc.bytesPerSecond))
		})
	}
}

// A row rate whose wait for the next row is long sends each row before that wait: one chunk
// per row, carrying 1, 2, 3... rows, and the file stays valid plain or gzip. The bucket is
// drained first so that every row waits.
func TestFileWriter_LowRowRateSendsEveryRow(t *testing.T) {
	for _, rowsPerSecond := range []uint64{1, 5} {
		for _, gzip := range []bool{false, true} {
			name := "plain"
			if gzip {
				name = "gzip"
			}
			t.Run(fmt.Sprintf("%s/%d rows per second", name, rowsPerSecond), func(t *testing.T) {
				t.Parallel()
				n := 3
				if rowsPerSecond == 5 {
					n = 6
				}
				elapsed, _, chunks := newRateStream(t, nil, drained(newBucket(rowsPerSecond)), gzip).write(t, n, 8)
				var carried []uint64
				for _, c := range chunks {
					carried = append(carried, c.GetRows())
				}
				want := make([]uint64, n)
				for i := range want {
					want[i] = uint64(i + 1)
				}
				require.Equal(t, want, carried, "one chunk per row")
				assertTook(t, elapsed, time.Duration(n)*time.Second/time.Duration(rowsPerSecond))
			})
		}
	}
}

// A high row rate waits only briefly per row: the rows go out about once a second, not one
// chunk per row, and the file stays valid plain or gzip.
func TestFileWriter_HighRowRateDoesNotFlushPerRow(t *testing.T) {
	const rowsPerSecond, n = 50000, 60000
	for name, gzip := range map[string]bool{"plain": false, "gzip": true} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			elapsed, _, chunks := newRateStream(t, nil, drained(newBucket(rowsPerSecond)), gzip).write(t, n, 8)
			require.LessOrEqual(t, len(chunks), 10, "a high row rate must not send a chunk per row")
			require.Equal(t, uint64(n), chunks[len(chunks)-1].GetRows())
			assertTook(t, elapsed, time.Duration(n)*time.Second/rowsPerSecond)
		})
	}
}

// With both rates set, the lower one governs the stream.
func TestFileWriter_LowerOfByteAndRowRateGoverns(t *testing.T) {
	t.Run("row rate", func(t *testing.T) {
		t.Parallel()
		// 4 small rows at 4 rows/s take about a second; 1 MiB/s alone would not wait at all.
		const rowsPerSecond, n = 4, 4
		elapsed, _, chunks := newRateStream(t, newBucket(1<<20), drained(newBucket(rowsPerSecond)), false).write(t, n, 8)
		assertTook(t, elapsed, time.Second)
		var carried []uint64
		for _, c := range chunks {
			carried = append(carried, c.GetRows())
		}
		require.Equal(t, []uint64{1, 2, 3, 4}, carried)
	})
	t.Run("byte rate", func(t *testing.T) {
		t.Parallel()
		// 5 rows of 64 KiB at 128 KiB/s take about 1.5s; 50000 rows/s alone would not wait.
		const bytesPerSecond = 128 << 10
		elapsed, file, chunks := newRateStream(t, newBucket(bytesPerSecond), newBucket(50000), false).write(t, 5, 64<<10)
		for _, c := range chunks {
			require.LessOrEqual(t, len(c.GetContent()), bytesPerSecond)
		}
		require.Len(t, chunks, (len(file.content)+bytesPerSecond-1)/bytesPerSecond, "full chunks, not one per row")
		assertTook(t, elapsed, byteRateTime(len(file.content), bytesPerSecond))
	})
}
