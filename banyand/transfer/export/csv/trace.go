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
	"fmt"
	"path/filepath"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	dumptrace "github.com/apache/skywalking-banyandb/banyand/internal/dump/trace"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/transfer/export/rowsrc"
	"github.com/apache/skywalking-banyandb/banyand/transfer/export/snapshot"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/csvfmt"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/nativefmt"
)

// traceColumns maps, per trace in the order the group lists them, every tag of the trace to
// its layout column, and returns the traces' schema revisions.
func traceColumns(traces []*databasev1.Trace, layout *csvfmt.Layout) ([]map[string]int, map[string]int64) {
	columns := make([]map[string]int, len(traces))
	revisions := make(map[string]int64, len(traces))
	for i, t := range traces {
		name := t.GetMetadata().GetName()
		columns[i] = map[string]int{}
		for _, tag := range t.GetTags() {
			if idx, ok := layout.TagIndex(name, "", tag.GetName()); ok {
				columns[i][tag.GetName()] = idx
			}
		}
		revisions[name] = t.GetMetadata().GetModRevision()
	}
	return columns, revisions
}

func produceTraceCSV(run *unitRun, unit *transferv1.SegmentUnit, segDir string) error {
	group := unit.GetGroup()
	traces, err := run.meta.TraceRegistry().ListTrace(run.ctx, schema.ListOpt{Group: group})
	if err != nil {
		return status.Errorf(codes.Internal, "list traces of group %s: %v", group, err)
	}
	layout, err := csvfmt.TraceLayout(traces)
	if err != nil {
		return status.Errorf(codes.FailedPrecondition, "trace group %s cannot be exported as CSV: %v", group, err)
	}
	resolver, err := rowsrc.NewTraceResolver(group, traces)
	if err != nil {
		return err
	}
	columns, revisions := traceColumns(traces, layout)
	fixed := fixedColumns(layout)
	for i, shard := range unit.GetShardIds() {
		fw := run.newWriter(layout, shardPrefix(unit, shard), shard, uint32(i), false, revisions)
		shardDir := filepath.Join(segDir, nativefmt.ShardDir(shard))
		if err = writeTraceShard(run, fw, shardDir, layout, fixed, resolver, columns); err != nil {
			return err
		}
		if err = fw.close(); err != nil {
			return snapshot.ReadError("finish "+shardDir, err)
		}
	}
	return nil
}

func writeTraceShard(run *unitRun, fw *fileWriter, shardDir string, layout *csvfmt.Layout, fixed map[csvfmt.ColumnKind]int,
	resolver *rowsrc.TraceResolver, columns []map[string]int,
) error {
	partIDs, err := snapshot.SessionPartIDs(shardDir)
	if err != nil {
		return snapshot.ReadError("list parts of "+shardDir, err)
	}
	cells := layout.NewRow()
	for _, id := range partIDs {
		rofs := rowsrc.NewReadOnlyFS()
		part, openErr := dumptrace.OpenPart(id, shardDir, rofs)
		if openErr == nil && rofs.Err() != nil {
			_ = part.Close()
			openErr = rofs.Err()
		}
		if openErr != nil {
			return snapshot.ReadError(fmt.Sprintf("open part %016x of %s", id, shardDir), openErr)
		}
		it := part.Iterator()
		for it.Next() {
			row := it.Row()
			res := resolver.Resolve(row.Tags)
			if res == nil {
				run.DropRow("trace rows whose tag set matches no trace of the group")
				continue
			}
			resetRow(cells)
			cells[fixed[csvfmt.KindTraceID]] = csvfmt.EncodeString(row.TraceID)
			cells[fixed[csvfmt.KindSpanID]] = csvfmt.EncodeString(row.SpanID)
			cells[fixed[csvfmt.KindSpan]] = csvfmt.EncodeBinary(row.Span)
			setStoredTags(cells, layout, row.Tags, row.TagTypes, columns[res.Index], run, res.Name)
			if err = fw.writeRow(res.Name, cells); err != nil {
				_ = it.Close()
				_ = part.Close()
				return snapshot.ReadError("write "+res.Name+" row", err)
			}
		}
		err = it.Err()
		_ = it.Close()
		_ = part.Close()
		if err != nil {
			return snapshot.ReadError(fmt.Sprintf("read part %016x of %s", id, shardDir), err)
		}
	}
	return nil
}
