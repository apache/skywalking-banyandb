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

package trace

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/dustin/go-humanize"

	"github.com/apache/skywalking-banyandb/pkg/query"
)

const (
	partMetadataHeader = "MinTimestamp, MaxTimestamp, CompressionSize, UncompressedSize, TotalCount, BlocksCount"
	blockHeader        = "PartID, TraceID, Count, UncompressedSize"
	traceIDSampleLimit = 5
)

func (pm *partMetadata) String() string {
	minTimestamp := time.Unix(0, pm.MinTimestamp).Format(time.Stamp)
	maxTimestamp := time.Unix(0, pm.MaxTimestamp).Format(time.Stamp)

	return fmt.Sprintf("%s, %s, %s, %s, %s, %s",
		minTimestamp, maxTimestamp, humanize.Bytes(pm.CompressedSizeBytes),
		humanize.Bytes(pm.UncompressedSpanSizeBytes), humanize.Comma(int64(pm.TotalCount)),
		humanize.Comma(int64(pm.BlocksCount)))
}

func (bc *blockCursor) String() string {
	return fmt.Sprintf("%d, %s, %d, %s",
		bc.p.partMetadata.ID, bc.bm.traceID, bc.bm.count, humanize.Bytes(bc.bm.uncompressedSpanSizeBytes))
}

func startBlockScanSpan(ctx context.Context, traceIDs []string, parts []*part) (func(*blockCursor), func(error)) {
	tracer := query.GetTracer(ctx)
	if tracer == nil {
		return nil, func(error) {}
	}

	span, _ := tracer.StartSpan(ctx, "scan-blocks")
	span.Tag("trace_id_count", strconv.Itoa(len(traceIDs)))
	if len(traceIDs) > 0 {
		limit := traceIDSampleLimit
		if limit > len(traceIDs) {
			limit = len(traceIDs)
		}
		span.Tag("trace_ids_sample", strings.Join(traceIDs[:limit], ","))
		if len(traceIDs) > limit {
			span.Tagf("trace_ids_omitted", "%d", len(traceIDs)-limit)
		}
	}
	span.Tag("part_header", partMetadataHeader)
	span.Tag("part_count", strconv.Itoa(len(parts)))
	for i := range parts {
		if parts[i] == nil {
			continue
		}
		span.Tag(fmt.Sprintf("part_%d_%s", parts[i].partMetadata.ID, parts[i].path), parts[i].partMetadata.String())
	}

	var (
		totalBytes uint64
		blocks     []string
	)

	return func(bc *blockCursor) {
			if bc == nil {
				return
			}
			blocks = append(blocks, bc.String())
			totalBytes += bc.bm.uncompressedSpanSizeBytes
		}, func(err error) {
			span.Tag("block_header", blockHeader)
			span.Tag("block_total_bytes", humanize.Bytes(totalBytes))
			span.Tag("block_count", strconv.Itoa(len(blocks)))
			for i := range blocks {
				span.Tag(fmt.Sprintf("block_%d", i), blocks[i])
			}
			if err != nil {
				span.Error(err)
			}
			span.Stop()
		}
}
