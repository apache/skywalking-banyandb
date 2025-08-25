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
	"time"

	"github.com/dustin/go-humanize"

	"github.com/apache/skywalking-banyandb/pkg/query"
)

// TODO: check the header.
const (
	partMetadataHeader = "MinTimestamp, MaxTimestamp, CompressionSize, UncompressedSize, TotalCount, BlocksCount"
	blockHeader        = "PartID, TraceID, Count, UncompressedSize"
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

func startBlockScanSpan(ctx context.Context, tid string, parts []*part, qr *queryResult) func() {
	tracer := query.GetTracer(ctx)
	if tracer == nil {
		return func() {}
	}

	span, _ := tracer.StartSpan(ctx, "scan-blocks")
	span.Tag("trace_id", tid)
	span.Tag("part_header", partMetadataHeader)
	span.Tag("part_num", fmt.Sprintf("%d", len(parts)))
	for i := range parts {
		span.Tag(fmt.Sprintf("part_%d_%s", parts[i].partMetadata.ID, parts[i].path),
			parts[i].partMetadata.String())
	}

	return func() {
		span.Tag("block_header", blockHeader)
		for i := range qr.data {
			span.Tag(fmt.Sprintf("block_%d", i), qr.data[i].String())
		}
		span.Stop()
	}
}
