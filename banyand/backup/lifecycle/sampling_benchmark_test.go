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

package lifecycle

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/pipeline"
	"github.com/apache/skywalking-banyandb/banyand/trace"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

func BenchmarkSamplingPreparation(b *testing.B) {
	for _, partCount := range []int{1, 100} {
		for _, spanBytes := range []int{1024, 1 << 20} {
			for _, mode := range []string{"mixed", "keep-all", "drop-all"} {
				b.Run(fmt.Sprintf("parts=%d/body=%d/%s", partCount, spanBytes, mode), func(b *testing.B) {
					shard := filepath.Join(b.TempDir(), "00000000")
					now := time.Now()
					for partID := 1; partID <= partCount; partID++ {
						rows := []trace.PartEncoderRow{
							{TraceID: "a-drop", SpanID: "span", Span: make([]byte, spanBytes), Timestamp: now.UnixNano()},
							{TraceID: "b-keep", SpanID: "span", Span: make([]byte, spanBytes), Timestamp: now.UnixNano()},
						}
						_, release := trace.EncodePart(shard, fs.NewLocalFileSystem(), uint64(partID), rows)
						release()
					}
					group := &GroupConfig{Group: &commonv1.Group{Metadata: &commonv1.Metadata{Name: "benchmark"}}, SourceStage: "hot", TargetStage: "warm", TargetShardNum: 1}
					schema := &databasev1.Trace{Metadata: &commonv1.Metadata{Name: "trace", Group: "benchmark"}}
					segment := &timestamp.TimeRange{Start: now.Add(-24 * time.Hour), End: now.Add(24 * time.Hour)}
					artifactRoot := b.TempDir()
					b.ReportAllocs()
					b.SetBytes(int64(2 * partCount * spanBytes))
					b.ResetTimer()
					for iteration := 0; iteration < b.N; iteration++ {
						sampler := &migrationTestSampler{mode: mode}
						session := pipeline.NewSession([]sdk.Sampler{sampler}, 3)
						policy := &pinnedSamplingPolicy{
							schema: schema, config: &commonv1.TracePipelineConfig{Enabled: true},
							session: session, samplers: []sdk.Sampler{sampler}, host: samplingHostConfig{Timeout: time.Second},
						}
						directory := filepath.Join(artifactRoot, fmt.Sprintf("attempt-%d", iteration))
						prepared, err := prepareTraceMigration(context.Background(), directory, shard, filepath.Dir(shard), segment, 0, group, policy)
						if err != nil {
							b.Fatal(err)
						}
						info, statErr := os.Stat(filepath.Join(directory, "decisions"))
						if statErr != nil {
							b.Fatal(statErr)
						}
						b.ReportMetric(float64(info.Size()), "decision-bytes")
						b.ReportMetric(float64(prepared.manifest.KeptSpans), "retained-spans")
						if err = prepared.Close(); err != nil {
							b.Fatal(err)
						}
						policy.close()
					}
				})
			}
		}
	}
}
