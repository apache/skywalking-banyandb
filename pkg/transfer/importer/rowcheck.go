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

package importer

import (
	"cmp"
	"context"
	"fmt"
	"runtime"
	"slices"
	"sync"

	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
	"github.com/apache/skywalking-banyandb/pkg/transfer"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/csvfmt"
)

// liaisonMaxRecvBytes is the default --max-recv-msg-size of a liaison: a batch of one row
// larger than it is refused; tests lower it.
var liaisonMaxRecvBytes = 16 << 20

// dataNodeMaxRecvBytes is the default --max-recv-msg-size of a data node's queue server: the
// liaison hands each row to a data node as a message of its own, so a row larger than it is
// refused there; tests lower it.
var dataNodeMaxRecvBytes = 10 << 20

const (
	// batchFrameOverhead bounds what a batch request adds around its rows.
	batchFrameOverhead = 32
	// nodeFrameOverhead bounds what the internal write request and its send frame add around
	// a row on its way to a data node.
	nodeFrameOverhead = 64
	// rowCheckExamples is how many failing rows the plan lists.
	rowCheckExamples = 10
	// rowCheckLimit bounds the work items checked at once.
	rowCheckLimit = 8
)

// RowPos is a row of the artifact and its size as sent.
type RowPos struct {
	File   string `json:"file"`
	Record uint64 `json:"record"`
	Bytes  int    `json:"bytes"`
}

// BadRow is a row the liaison would refuse.
type BadRow struct {
	Item   string `json:"item"`
	File   string `json:"file"`
	Reason string `json:"reason"`
	Record uint64 `json:"record"`
}

// RowCheck is the result of the read-only row precheck of the planned work items.
type RowCheck struct {
	// Bad lists the first rowCheckExamples failing rows by file and record.
	Bad []BadRow `json:"bad"`
	// BadItems counts the failing rows per work item key.
	BadItems map[string]uint64 `json:"badItems"`
	// Largest is the largest row.
	Largest RowPos `json:"largest"`
	// LargestOverBatch is the largest row over --write-batch-bytes, sent in a batch of its own.
	LargestOverBatch RowPos `json:"largestOverBatch"`
	Rows             uint64 `json:"rows"`
	BadRows          uint64 `json:"badRows"`
	OverBatchRows    uint64 `json:"overBatchRows"`
	Items            int    `json:"items"`
}

func (rc *RowCheck) addBad(b BadRow) {
	rc.BadRows++
	rc.BadItems[b.Item]++
	rc.Bad = append(rc.Bad, b)
	if len(rc.Bad) > 2*rowCheckExamples {
		rc.trimBad()
	}
}

func (rc *RowCheck) trimBad() {
	slices.SortFunc(rc.Bad, func(a, b BadRow) int {
		return cmp.Or(cmp.Compare(a.File, b.File), cmp.Compare(a.Record, b.Record), cmp.Compare(a.Reason, b.Reason))
	})
	rc.Bad = rc.Bad[:min(len(rc.Bad), rowCheckExamples)]
}

func (rc *RowCheck) merge(o *RowCheck) {
	rc.Rows += o.Rows
	rc.BadRows += o.BadRows
	rc.OverBatchRows += o.OverBatchRows
	for k, n := range o.BadItems {
		rc.BadItems[k] += n
	}
	rc.Bad = append(rc.Bad, o.Bad...)
	rc.trimBad()
	if larger(o.Largest, rc.Largest) {
		rc.Largest = o.Largest
	}
	if larger(o.LargestOverBatch, rc.LargestOverBatch) {
		rc.LargestOverBatch = o.LargestOverBatch
	}
}

// larger orders rows by size, then by position, so the result does not depend on scheduling.
func larger(a, b RowPos) bool {
	if a.Bytes != b.Bytes {
		return a.Bytes > b.Bytes
	}
	return a.Bytes > 0 && cmp.Or(cmp.Compare(a.File, b.File), cmp.Compare(a.Record, b.Record)) < 0
}

// CheckRows reads every row of every entry, read-only, and assembles it with the row builder
// of the push for the target schema (the predicted one before the schema write), then applies
// the checks the liaison applies before writing: a row the liaison would refuse, or too large
// for its default receive limit, is bad. It records the largest row and the rows over
// batchBytes. Work items are checked several at a time. An artifact that cannot be laid out
// is an error; a context error is returned as is.
func CheckRows(ctx context.Context, art *Artifact, target *Schemas, entries []Entry, batchBytes uint64) (*RowCheck, error) {
	builders, err := newBuilders(art, target, entries)
	if err != nil {
		return nil, err
	}
	total := &RowCheck{BadItems: map[string]uint64{}, Bad: []BadRow{}, Items: len(entries)}
	var mu sync.Mutex
	eg, egCtx := errgroup.WithContext(ctx)
	eg.SetLimit(min(runtime.GOMAXPROCS(0), rowCheckLimit))
	for i := range entries {
		e := &entries[i]
		b := builders[builderKey{catalog: e.Item.Catalog, group: e.Item.Group}]
		eg.Go(func() error {
			rc, checkErr := b.checkItem(egCtx, art.Dir, &e.Item, batchBytes)
			if checkErr != nil {
				return checkErr
			}
			mu.Lock()
			defer mu.Unlock()
			total.merge(rc)
			return nil
		})
	}
	if err = eg.Wait(); err != nil {
		return nil, err
	}
	return total, nil
}

// checkItem checks every row of every source of an item, as read; an unreadable file is one
// bad row and ends the check of the item.
func (b *rowBuilder) checkItem(ctx context.Context, dir string, item *WorkItem, batchBytes uint64) (*RowCheck, error) {
	rc := &RowCheck{BadItems: map[string]uint64{}}
	key := item.Key()
	err := b.scanSources(ctx, dir, item, func(file string, record uint64, cells []csvfmt.Value) error {
		rc.Rows++
		msg, err := b.build(cells)
		if err == nil {
			err = b.checkRow(msg)
		}
		if err != nil {
			rc.addBad(BadRow{Item: key, File: file, Record: record, Reason: err.Error()})
			return nil
		}
		pos := RowPos{File: file, Record: record, Bytes: 1 + csvfmt.RowSize(msg)}
		if larger(pos, rc.Largest) {
			rc.Largest = pos
		}
		switch {
		case pos.Bytes+batchFrameOverhead > liaisonMaxRecvBytes:
			rc.addBad(BadRow{Item: key, File: file, Record: record, Reason: fmt.Sprintf(
				"the row is %d bytes; a batch of it alone exceeds the %d bytes a liaison receives by default (--max-recv-msg-size): raise it on every liaison of the target",
				pos.Bytes, liaisonMaxRecvBytes)})
		case pos.Bytes+nodeFrameOverhead > dataNodeMaxRecvBytes:
			rc.addBad(BadRow{Item: key, File: file, Record: record, Reason: fmt.Sprintf(
				"the row is %d bytes; it exceeds the %d bytes a data node receives by default (--max-recv-msg-size of its queue server): raise it on every data node of the target",
				pos.Bytes, dataNodeMaxRecvBytes)})
		case uint64(pos.Bytes) > batchBytes:
			rc.OverBatchRows++
			if larger(pos, rc.LargestOverBatch) {
				rc.LargestOverBatch = pos
			}
		}
		return nil
	})
	if err != nil {
		if ctx.Err() != nil {
			return nil, context.Cause(ctx)
		}
		rc.addBad(BadRow{Item: key, Reason: err.Error()})
	}
	return rc, nil
}

// checkRow applies to an assembled row the checks the liaison applies before writing it.
func (b *rowBuilder) checkRow(msg proto.Message) error {
	switch m := msg.(type) {
	case *transferv1.ImportRowsRequest_StreamRow:
		el := m.GetElement()
		if el == nil {
			return fmt.Errorf("element is required")
		}
		if err := timestamp.CheckPb(el.GetTimestamp()); err != nil {
			return fmt.Errorf("invalid timestamp: %w", err)
		}
		return b.checkEntity(m.GetName(), el.GetTagFamilies())
	case *transferv1.ImportRowsRequest_MeasureRow:
		point := m.GetPoint()
		if point == nil {
			return fmt.Errorf("point is required")
		}
		if point.GetVersion() == 0 {
			return fmt.Errorf("the version of measure %s must not be 0", m.GetName())
		}
		if err := timestamp.CheckPb(point.GetTimestamp()); err != nil {
			return fmt.Errorf("invalid timestamp: %w", err)
		}
		return b.checkEntity(m.GetName(), point.GetTagFamilies())
	case *transferv1.ImportRowsRequest_TraceRow:
		return b.checkTrace(m)
	case *transferv1.ImportRowsRequest_PropertyRow:
		md := m.GetProperty().GetMetadata()
		switch {
		case md == nil:
			return fmt.Errorf("property metadata is required")
		case md.GetGroup() != b.group:
			return fmt.Errorf("property group %s is not the stream's group %s", md.GetGroup(), b.group)
		case md.GetName() == "" || m.GetProperty().GetId() == "":
			return fmt.Errorf("property name and id are required")
		case m.GetDeleteTime() < 0:
			return fmt.Errorf("delete time must not be negative")
		}
	}
	return nil
}

// checkEntity checks that every entity tag of the resource has a slot in the row and a column
// in the artifact: without one, every row would be written to one series.
func (b *rowBuilder) checkEntity(name string, families []*modelv1.TagFamilyForWrite) error {
	rm := b.resources[name]
	for _, pos := range rm.entity {
		tag := rm.names[pos[0]][pos[1]]
		if pos[0] >= len(families) || pos[1] >= len(families[pos[0]].GetTags()) {
			return fmt.Errorf("entity tag %s of %s is missing", tag, name)
		}
		if rm.families[pos[0]][pos[1]] < 0 {
			return fmt.Errorf("entity tag %s of %s has no column in the artifact, so its rows would share one series", tag, name)
		}
	}
	return nil
}

func (b *rowBuilder) checkTrace(m *transferv1.ImportRowsRequest_TraceRow) error {
	rm := b.resources[m.GetName()]
	tr := rm.trace
	tags := m.GetTags()
	if len(tags) != len(tr.GetTags()) {
		return fmt.Errorf("trace %s has %d tags, the row carries %d", m.GetName(), len(tr.GetTags()), len(tags))
	}
	if rm.traceIdx < 0 || rm.tsIdx < 0 {
		return fmt.Errorf("trace %s does not declare its trace id or timestamp tag", m.GetName())
	}
	if tags[rm.traceIdx].GetStr().GetValue() == "" {
		return fmt.Errorf("trace id tag %s must be a non-empty string", tr.GetTraceIdTagName())
	}
	if rm.spanIdx >= 0 && tags[rm.spanIdx].GetStr().GetValue() == "" {
		return fmt.Errorf("span id tag %s must be a non-empty string", tr.GetSpanIdTagName())
	}
	ts := tags[rm.tsIdx].GetTimestamp()
	if ts == nil {
		return fmt.Errorf("timestamp tag %s must not be null", tr.GetTimestampTagName())
	}
	if err := timestamp.CheckPb(ts); err != nil {
		return fmt.Errorf("invalid timestamp: %w", err)
	}
	return nil
}

// ApplyRowCheck records rc in the plan: the work items with failing rows move to Rejected
// (gate ROWS) with the first failing rows and the totals as gate details, rows larger than
// --write-batch-bytes give one warning, and so does a --write-batch-bytes whose full batches
// the liaison's default receive limit refuses.
func ApplyRowCheck(p *Plan, rc *RowCheck, batchBytes uint64) {
	p.RowCheck = rc
	if rc.BadRows > 0 {
		kept := p.Entries[:0]
		for _, e := range p.Entries {
			n := rc.BadItems[e.Item.Key()]
			if n == 0 {
				kept = append(kept, e)
				continue
			}
			p.Rejected = append(p.Rejected, Rejection{
				Item: e.Item, TargetStage: e.TargetStage, TargetNodes: e.TargetNodes, Gate: GateRows,
				Reason: fmt.Sprintf("%d row(s) the liaison would refuse", n),
			})
		}
		p.Entries = kept
		slices.SortFunc(p.Rejected, func(a, b Rejection) int {
			return cmp.Or(compareItems(a.Item, b.Item), cmp.Compare(a.Gate, b.Gate))
		})
		for _, b := range rc.Bad {
			where := b.Item
			if b.File != "" {
				where = fmt.Sprintf("file %s record %d", b.File, b.Record)
			}
			p.GateDetails = append(p.GateDetails, fmt.Sprintf("row check: %s: %s", where, b.Reason))
		}
		p.GateDetails = append(p.GateDetails, fmt.Sprintf("row check: %d row(s) of %d work item(s) fail; up to %d are listed",
			rc.BadRows, len(rc.BadItems), rowCheckExamples))
		p.GateDetails = transfer.UniqueSorted(p.GateDetails)
	}
	warnings := len(p.Warnings)
	if rc.OverBatchRows > 0 {
		l := rc.LargestOverBatch
		p.Warnings = append(p.Warnings, fmt.Sprintf(
			"%d row(s) are larger than --write-batch-bytes %d (largest %d bytes, file %s record %d); each is sent in a batch of its own",
			rc.OverBatchRows, batchBytes, l.Bytes, l.File, l.Record))
	}
	if batchBytes+batchFrameOverhead > uint64(liaisonMaxRecvBytes) {
		p.Warnings = append(p.Warnings, fmt.Sprintf(
			"--write-batch-bytes %d exceeds the %d bytes a liaison receives by default (--max-recv-msg-size): "+
				"raise it on every liaison of the target, or lower --write-batch-bytes",
			batchBytes, liaisonMaxRecvBytes))
	}
	if len(p.Warnings) > warnings {
		p.Warnings = transfer.UniqueSorted(p.Warnings)
	}
}
