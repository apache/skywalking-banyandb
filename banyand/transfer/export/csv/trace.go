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
	"slices"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	dumptrace "github.com/apache/skywalking-banyandb/banyand/internal/dump/trace"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/csvfmt"
)

type traceResource struct {
	tags map[string]int
	// stored is the tag set a part of the trace stores: its tags without the id tags.
	stored map[string]struct{}
	name   string
}

// traceResolver tells the trace of a row from the row's tag set.
type traceResolver struct {
	bySignature map[string]*traceResource
	// fallback caches the superset match of signatures that match no trace exactly.
	fallback map[string]*traceResource
	// declared holds every tag some trace of the group stores; other stored tags were
	// deleted from the schema and do not tell traces apart.
	declared  map[string]struct{}
	only      *traceResource
	resources []*traceResource
	scratch   []string
}

// resolve returns the trace of a row, or nil. A row whose tag set is no trace's exact set
// was written before tags were added to its trace and comes from an old unmerged part: it
// belongs to the only trace whose stored tag set contains all of the row's tags; with no
// or several such traces the row is ambiguous.
func (r *traceResolver) resolve(tags map[string][]byte) *traceResource {
	if r.only != nil {
		return r.only
	}
	var sig string
	sig, r.scratch = rowSignature(tags, r.declared, r.scratch)
	if res, ok := r.bySignature[sig]; ok {
		return res
	}
	if res, ok := r.fallback[sig]; ok {
		return res
	}
	var match *traceResource
	for _, res := range r.resources {
		if !containsAll(res.stored, r.scratch) {
			continue
		}
		if match != nil {
			match = nil
			break
		}
		match = res
	}
	r.fallback[sig] = match
	return match
}

func containsAll(set map[string]struct{}, names []string) bool {
	for _, n := range names {
		if _, ok := set[n]; !ok {
			return false
		}
	}
	return true
}

// traceSignature is the sorted tag names of a trace without its trace-id and span-id tags:
// exactly the tag columns a part of that trace stores.
func traceSignature(t *databasev1.Trace) string {
	names := make([]string, 0, len(t.GetTags()))
	for _, tag := range t.GetTags() {
		if tag.GetName() == t.GetTraceIdTagName() || tag.GetName() == t.GetSpanIdTagName() {
			continue
		}
		names = append(names, tag.GetName())
	}
	slices.Sort(names)
	return strings.Join(names, "\x00")
}

// rowSignature is the signature of the tags a trace row's columns hold, by tag name and
// limited to the tags some trace of the group declares.
func rowSignature(tags map[string][]byte, declared map[string]struct{}, scratch []string) (string, []string) {
	scratch = scratch[:0]
	for column := range tags {
		name := storedTagName(column)
		if _, ok := declared[name]; ok {
			scratch = append(scratch, name)
		}
	}
	slices.Sort(scratch)
	scratch = slices.Compact(scratch)
	return strings.Join(scratch, "\x00"), scratch
}

// newTraceResolver indexes the traces of a group by tag set and returns their schema
// revisions. Two traces with the same tag set cannot be told apart in CSV.
func newTraceResolver(group string, traces []*databasev1.Trace, layout *csvfmt.Layout) (*traceResolver, map[string]int64, error) {
	resolver := &traceResolver{
		bySignature: make(map[string]*traceResource, len(traces)),
		fallback:    map[string]*traceResource{},
		declared:    map[string]struct{}{},
	}
	revisions := make(map[string]int64, len(traces))
	for _, t := range traces {
		name := t.GetMetadata().GetName()
		res := &traceResource{name: name, tags: map[string]int{}, stored: map[string]struct{}{}}
		for _, tag := range t.GetTags() {
			if idx, ok := layout.TagIndex(name, "", tag.GetName()); ok {
				res.tags[tag.GetName()] = idx
			}
			if tag.GetName() != t.GetTraceIdTagName() && tag.GetName() != t.GetSpanIdTagName() {
				res.stored[tag.GetName()] = struct{}{}
				resolver.declared[tag.GetName()] = struct{}{}
			}
		}
		revisions[name] = t.GetMetadata().GetModRevision()
		sig := traceSignature(t)
		if other, dup := resolver.bySignature[sig]; dup {
			first, second := min(other.name, name), max(other.name, name)
			return nil, nil, status.Errorf(codes.FailedPrecondition,
				"traces %s and %s in group %s have the same tag set; export it in native format", first, second, group)
		}
		resolver.bySignature[sig] = res
		resolver.resources = append(resolver.resources, res)
	}
	if len(traces) == 1 {
		resolver.only = resolver.resources[0]
	}
	return resolver, revisions, nil
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
	resolver, revisions, err := newTraceResolver(group, traces, layout)
	if err != nil {
		return err
	}
	fixed := fixedColumns(layout)
	for i, shard := range unit.GetShardIds() {
		fw := run.newWriter(layout, shardPrefix(unit, shard), shard, uint32(i), false, revisions)
		shardDir := filepath.Join(segDir, fmt.Sprintf("%s%d", shardDirPrefix, shard))
		if err = writeTraceShard(run, fw, shardDir, layout, fixed, resolver); err != nil {
			return err
		}
		if err = fw.close(); err != nil {
			return readError("finish "+shardDir, err)
		}
	}
	return nil
}

func writeTraceShard(run *unitRun, fw *fileWriter, shardDir string, layout *csvfmt.Layout, fixed map[csvfmt.ColumnKind]int,
	resolver *traceResolver,
) error {
	partIDs, err := SessionPartIDs(shardDir)
	if err != nil {
		return readError("list parts of "+shardDir, err)
	}
	cells := layout.NewRow()
	for _, id := range partIDs {
		rofs := newReadOnlyFS()
		part, openErr := dumptrace.OpenPart(id, shardDir, rofs)
		if openErr == nil && rofs.err != nil {
			_ = part.Close()
			openErr = rofs.err
		}
		if openErr != nil {
			return readError(fmt.Sprintf("open part %016x of %s", id, shardDir), openErr)
		}
		it := part.Iterator()
		for it.Next() {
			row := it.Row()
			res := resolver.resolve(row.Tags)
			if res == nil {
				run.dropRow("trace rows whose tag set matches no trace of the group")
				continue
			}
			resetRow(cells)
			cells[fixed[csvfmt.KindTraceID]] = csvfmt.EncodeString(row.TraceID)
			cells[fixed[csvfmt.KindSpanID]] = csvfmt.EncodeString(row.SpanID)
			cells[fixed[csvfmt.KindSpan]] = csvfmt.EncodeBinary(row.Span)
			setStoredTags(cells, layout, row.Tags, row.TagTypes, res.tags, run, res.name)
			if err = fw.writeRow(res.name, cells); err != nil {
				_ = it.Close()
				_ = part.Close()
				return readError("write "+res.name+" row", err)
			}
		}
		err = it.Err()
		_ = it.Close()
		_ = part.Close()
		if err != nil {
			return readError(fmt.Sprintf("read part %016x of %s", id, shardDir), err)
		}
	}
	return nil
}
