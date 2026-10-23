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

package query

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/query"
	logical_stream "github.com/apache/skywalking-banyandb/pkg/query/logical/stream"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	vstream "github.com/apache/skywalking-banyandb/pkg/query/vectorized/stream"
)

const (
	mergeFamily  = "f"
	mergeNameTag = "name"
	mergeKeyTag  = "key"
	mergeExtra   = "extra"
)

// mergeRow is one element of a group. key is the sort-tag value and ts the timestamp.
type mergeRow struct {
	tags map[string]string
	id   uint64
	ts   int64
	key  byte
}

// mergeScan is a fake group scan returning pre-built batches.
type mergeScan struct {
	err     error
	schema  *vectorized.BatchSchema
	hidden  string
	batches []*vectorized.RecordBatch
	proj    []model.TagProjection
}

func (s *mergeScan) ExecuteVectorized(context.Context) ([]*vectorized.RecordBatch, *vectorized.BatchSchema, error) {
	if s.err != nil {
		return nil, nil, s.err
	}
	return s.batches, s.schema, nil
}

func (s *mergeScan) ProjectionTags() []model.TagProjection { return s.proj }

func (s *mergeScan) HiddenOrderTag() string { return s.hidden }

// countingOwner records how often its references were taken and dropped.
type countingOwner struct {
	retains, releases int
}

func (o *countingOwner) Retain()  { o.retains++ }
func (o *countingOwner) Release() { o.releases++ }

// newMergeBatch builds a batch on schema holding rows, in order. The schema's tag columns
// are filled from row.tags, and the order key from row.key when the schema carries one.
func newMergeBatch(schema *vectorized.BatchSchema, rows []mergeRow) *vectorized.RecordBatch {
	batch := vectorized.NewRecordBatch(schema, len(rows))
	for _, row := range rows {
		batch.Columns[schema.TimestampIndex()].(*vectorized.TypedColumn[int64]).Append(row.ts)
		batch.Columns[schema.ElementIDIndex()].(*vectorized.TypedColumn[int64]).Append(vstream.ElementIDToColumn(row.id))
		batch.Columns[schema.SeriesIDIndex()].(*vectorized.TypedColumn[int64]).Append(vstream.SeriesIDToColumn(1))
		if schema.OrderKeyIndex() >= 0 {
			batch.Columns[schema.OrderKeyIndex()].(*vectorized.TypedColumn[[]byte]).Append([]byte{row.key})
		}
		for _, family := range schema.TagFamilyGroups {
			for _, colIdx := range family.Columns {
				name := schema.Columns[colIdx].Name
				value := row.tags[name]
				if name == mergeKeyTag {
					value = fmt.Sprintf("%d", row.key)
				}
				batch.Columns[colIdx].(*vectorized.TypedColumn[*modelv1.TagValue]).Append(oracleStr(value))
			}
		}
		batch.Len++
	}
	return batch
}

// newMergeScan builds a group scan over rows. tagNames are the executed tag columns, orderTag is
// "" for a timestamp-ordered scan, and hidden names the order tag the client did not project.
// split spreads the rows over two batches.
func newMergeScan(tagNames []string, orderTag, hidden string, rows []mergeRow, split bool) *mergeScan {
	family := ""
	if orderTag != "" {
		family = mergeFamily
	}
	schema := vstream.BuildStreamBatchSchema([]model.TagProjection{{Family: mergeFamily, Names: tagNames}}, family, orderTag)
	var batches []*vectorized.RecordBatch
	if split && len(rows) > 1 {
		batches = []*vectorized.RecordBatch{newMergeBatch(schema, rows[:1]), newMergeBatch(schema, rows[1:])}
	} else {
		batches = []*vectorized.RecordBatch{newMergeBatch(schema, rows)}
	}
	proj := make([]string, 0, len(tagNames))
	for _, name := range tagNames {
		if name != hidden {
			proj = append(proj, name)
		}
	}
	return &mergeScan{
		schema:  schema,
		batches: batches,
		hidden:  hidden,
		proj:    []model.TagProjection{{Family: mergeFamily, Names: proj}},
	}
}

func mergeNamed(name string, id uint64, key byte, ts int64) mergeRow {
	return mergeRow{id: id, key: key, ts: ts, tags: map[string]string{mergeNameTag: name}}
}

// describeMergeElements renders each element as name[family:tag=value,...] so a test asserts the
// exact tags an element carries and not only its position.
func describeMergeElements(elements []*streamv1.Element) []string {
	described := make([]string, 0, len(elements))
	for _, element := range elements {
		var name string
		var parts []string
		for _, family := range element.GetTagFamilies() {
			for _, tag := range family.GetTags() {
				value := tag.GetValue().GetStr().GetValue()
				if tag.GetKey() == mergeNameTag {
					name = value
				}
				parts = append(parts, fmt.Sprintf("%s:%s=%s", family.GetName(), tag.GetKey(), value))
			}
		}
		described = append(described, fmt.Sprintf("%s[%s]", name, strings.Join(parts, ",")))
	}
	return described
}

// runMergeDispatch runs the dispatch and returns the elements of its proto response. A panic in
// the merge fails the test as an assertion rather than aborting the run.
func runMergeDispatch(t *testing.T, merge *logical_stream.VecMerge) []*streamv1.Element {
	t.Helper()
	p := newOracleStandaloneProcessor(t)
	var handled bool
	var msg bus.Message
	require.NotPanics(t, func() {
		handled, msg = p.tryVecMergeDispatch(context.Background(), merge, &streamv1.QueryRequest{Name: "merge-stream"}, 1)
	})
	require.True(t, handled)
	resp, ok := msg.Data().(*streamv1.QueryResponse)
	require.True(t, ok, "the merge dispatch returned %T, not a proto response", msg.Data())
	return resp.GetElements()
}

func mergeNames(elements []*streamv1.Element) []string {
	var names []string
	for _, element := range elements {
		for _, family := range element.GetTagFamilies() {
			for _, tag := range family.GetTags() {
				if tag.GetKey() == mergeNameTag {
					names = append(names, tag.GetValue().GetStr().GetValue())
				}
			}
		}
	}
	return names
}

// TestStreamVecDispatch_MultiGroupBatchMerge pins the cross-group merge of the multi-group vec
// dispatch against the apache/skywalking#14070 oracle, hand-written in the issue:
//
//	Group 1: A(key=1), B(key=3)
//	Group 2: A(key=1), C(key=2)
//
// The two copies of A share an ElementID. Ascending by key with offset 1, limit 2 gives C, B;
// descending with offset 0, limit 2 gives B, C. By timestamp (A=1, C=2, B=3) the answers are the
// same. Splitting a group over two batches and leaving the sort tag out of the projection must
// not change them.
func TestStreamVecDispatch_MultiGroupBatchMerge(t *testing.T) {
	const idA, idB, idC = 1, 2, 3
	tests := []struct {
		name         string
		want         []string
		offset       uint32
		limit        uint32
		byTime       bool
		desc         bool
		split        bool
		hideSortTag  bool
		wantNoKeyTag bool
	}{
		{name: "key asc offset 1 limit 2", want: []string{"C", "B"}, offset: 1, limit: 2},
		{name: "key desc offset 0 limit 2", want: []string{"B", "C"}, limit: 2, desc: true},
		{name: "key asc split batches", want: []string{"C", "B"}, offset: 1, limit: 2, split: true},
		{name: "key desc split batches", want: []string{"B", "C"}, limit: 2, desc: true, split: true},
		{name: "key asc unprojected sort tag", want: []string{"C", "B"}, offset: 1, limit: 2, hideSortTag: true, wantNoKeyTag: true},
		{name: "key desc unprojected sort tag split", want: []string{"B", "C"}, limit: 2, desc: true, split: true, hideSortTag: true, wantNoKeyTag: true},
		{name: "key asc whole result", want: []string{"A", "C", "B"}, limit: 10},
		{name: "time asc offset 1 limit 2", want: []string{"C", "B"}, offset: 1, limit: 2, byTime: true},
		{name: "time desc offset 0 limit 2", want: []string{"B", "C"}, limit: 2, desc: true, byTime: true},
		{name: "time asc split batches", want: []string{"C", "B"}, offset: 1, limit: 2, byTime: true, split: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// A(key=1, ts=1), B(key=3, ts=3), C(key=2, ts=2).
			groupOne := []mergeRow{mergeNamed("A", idA, 1, 1), mergeNamed("B", idB, 3, 3)}
			groupTwo := []mergeRow{mergeNamed("A", idA, 1, 1), mergeNamed("C", idC, 2, 2)}
			var scans []*mergeScan
			for _, rows := range [][]mergeRow{groupOne, groupTwo} {
				switch {
				case tt.byTime:
					scans = append(scans, newMergeScan([]string{mergeNameTag}, "", "", rows, tt.split))
				case tt.hideSortTag:
					scans = append(scans, newMergeScan([]string{mergeNameTag, mergeKeyTag}, mergeKeyTag, mergeKeyTag, rows, tt.split))
				default:
					scans = append(scans, newMergeScan([]string{mergeNameTag, mergeKeyTag}, mergeKeyTag, "", rows, tt.split))
				}
			}
			merge := &logical_stream.VecMerge{
				Groups:     []logical_stream.VecMergeGroup{{Scan: scans[0]}, {Scan: scans[1]}},
				Offset:     tt.offset,
				Limit:      tt.limit,
				SortByTime: tt.byTime,
				Desc:       tt.desc,
			}
			elements := runMergeDispatch(t, merge)
			require.Equal(t, tt.want, mergeNames(elements))
			if tt.wantNoKeyTag {
				for _, element := range elements {
					for _, family := range element.GetTagFamilies() {
						for _, tag := range family.GetTags() {
							require.NotEqual(t, mergeKeyTag, tag.GetKey(), "the unprojected sort tag reached the client")
						}
					}
				}
			}
		})
	}

	// Groups define different tags. group1 projects name and extra, group2 projects name only.
	// The recorded output of main for this fixture carries the extra tag on group1's elements and
	// omits it from group2's, and the batch merge must keep that shape.
	t.Run("schema-evolved groups keep each group's tags", func(t *testing.T) {
		groupOne := []mergeRow{
			{id: 1, ts: 1, tags: map[string]string{mergeNameTag: "A", mergeExtra: "ea"}},
			{id: 2, ts: 3, tags: map[string]string{mergeNameTag: "B", mergeExtra: "eb"}},
		}
		groupTwo := []mergeRow{mergeNamed("C", 3, 0, 2), mergeNamed("D", 4, 0, 4)}
		merge := &logical_stream.VecMerge{
			Groups: []logical_stream.VecMergeGroup{
				{Scan: newMergeScan([]string{mergeNameTag, mergeExtra}, "", "", groupOne, false)},
				{Scan: newMergeScan([]string{mergeNameTag}, "", "", groupTwo, false)},
			},
			Limit:      10,
			SortByTime: true,
		}
		elements := runMergeDispatch(t, merge)
		require.Equal(t, []string{
			"A[f:name=A,f:extra=ea]",
			"C[f:name=C]",
			"B[f:name=B,f:extra=eb]",
			"D[f:name=D]",
		}, describeMergeElements(elements))
	})

	// A failing second group must hand back the batches the first group already produced and
	// keep the existing error message.
	t.Run("a failing group releases the executed groups' batches", func(t *testing.T) {
		owner := &countingOwner{}
		first := newMergeScan([]string{mergeNameTag}, "", "", []mergeRow{mergeNamed("A", 1, 0, 1)}, false)
		for _, batch := range first.batches {
			batch.Owners.Pin(owner)
		}
		require.Equal(t, 1, owner.retains)
		second := &mergeScan{err: errors.New("boom")}
		merge := &logical_stream.VecMerge{
			Groups:     []logical_stream.VecMergeGroup{{Scan: first}, {Scan: second}},
			Limit:      10,
			SortByTime: true,
		}
		p := newOracleStandaloneProcessor(t)
		handled, msg := p.tryVecMergeDispatch(context.Background(), merge, &streamv1.QueryRequest{Name: "merge-stream"}, 1)
		require.True(t, handled)
		failure, ok := msg.Data().(*common.Error)
		require.True(t, ok, "the failure returned %T, not an error", msg.Data())
		require.Contains(t, failure.Error(), "execute the vectorized query plan for stream merge-stream: boom")
		require.Equal(t, 1, owner.releases, "the executed group's batches were not released")
	})
}

// failOnChargeLease is a budget lease that rejects its failOn-th charge (1-based) and accepts the others.
type failOnChargeLease struct {
	charges int
	failOn  int
}

func (l *failOnChargeLease) Charge(uint64) error {
	l.charges++
	if l.charges == l.failOn {
		return errors.New("budget exhausted")
	}
	return nil
}
func (*failOnChargeLease) Refund(uint64) {}
func (*failOnChargeLease) Limit() uint64 { return 1 << 20 }
func (*failOnChargeLease) Used() uint64  { return 0 }
func (*failOnChargeLease) Owner() any    { return "merge-test" }

// dispatchMergeWith runs the dispatch under ctx and returns the response data.
func dispatchMergeWith(ctx context.Context, t *testing.T, merge *logical_stream.VecMerge) any {
	t.Helper()
	p := newOracleStandaloneProcessor(t)
	var handled bool
	var msg bus.Message
	require.NotPanics(t, func() {
		handled, msg = p.tryVecMergeDispatch(ctx, merge, &streamv1.QueryRequest{Name: "merge-stream"}, 1)
	})
	require.True(t, handled)
	return msg.Data()
}

// TestStreamVecDispatch_MultiGroupBatchMergeBoundaries covers the merge stage's budget and
// cancellation failures, a result larger than one output batch, and the order of equal keys.
func TestStreamVecDispatch_MultiGroupBatchMergeBoundaries(t *testing.T) {
	pinnedGroups := func(owner *countingOwner) []*mergeScan {
		scans := []*mergeScan{
			newMergeScan([]string{mergeNameTag}, "", "", []mergeRow{mergeNamed("A", 1, 0, 1)}, false),
			newMergeScan([]string{mergeNameTag}, "", "", []mergeRow{mergeNamed("B", 2, 0, 2)}, false),
		}
		for _, scan := range scans {
			for _, batch := range scan.batches {
				batch.Owners.Pin(owner)
			}
		}
		return scans
	}
	timeMerge := func(scans []*mergeScan) *logical_stream.VecMerge {
		return &logical_stream.VecMerge{
			Groups:     []logical_stream.VecMergeGroup{{Scan: scans[0]}, {Scan: scans[1]}},
			Limit:      10,
			SortByTime: true,
		}
	}

	// failOn 1 and 2 reject the aligned copy of the first and second group, 3 rejects SortedMerge's
	// first charge, so every one of them must return the merge error and release the raw batches.
	for failOn := 1; failOn <= 3; failOn++ {
		t.Run(fmt.Sprintf("budget rejection on charge %d releases the batches", failOn), func(t *testing.T) {
			owner := &countingOwner{}
			ctx := query.WithBudgetLease(context.Background(), &failOnChargeLease{failOn: failOn})
			data := dispatchMergeWith(ctx, t, timeMerge(pinnedGroups(owner)))
			failure, ok := data.(*common.Error)
			require.True(t, ok, "the dispatch returned %T, not an error", data)
			require.Contains(t, failure.Error(), "merge the vectorized stream batches for stream merge-stream: ")
			require.Contains(t, failure.Error(), "budget exhausted")
			require.Equal(t, 2, owner.releases, "the executed groups' batches were not released")
		})
	}

	t.Run("a canceled context fails the merge and releases the batches", func(t *testing.T) {
		owner := &countingOwner{}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		data := dispatchMergeWith(ctx, t, timeMerge(pinnedGroups(owner)))
		failure, ok := data.(*common.Error)
		require.True(t, ok, "the dispatch returned %T, not an error", data)
		require.Contains(t, failure.Error(), "context canceled")
		require.Equal(t, 2, owner.releases, "the executed groups' batches were not released")
	})

	// 1500 rows in each group, 3000 in all, exceed the 1024-row output batch of the merge.
	// Group one holds the odd timestamps 1,3,5,... and group two the even ones 2,4,6,..., so the
	// merged order interleaves them: g1-0, g2-0, g1-1, g2-1, ...
	t.Run("a result spanning several output batches stays ordered", func(t *testing.T) {
		const perGroup = 1500
		var groupOne, groupTwo []mergeRow
		var want []string
		for i := range perGroup {
			groupOne = append(groupOne, mergeNamed(fmt.Sprintf("g1-%d", i), uint64(2*i+1), 0, int64(2*i+1)))
			groupTwo = append(groupTwo, mergeNamed(fmt.Sprintf("g2-%d", i), uint64(2*i+2), 0, int64(2*i+2)))
			want = append(want, fmt.Sprintf("g1-%d", i), fmt.Sprintf("g2-%d", i))
		}
		merge := &logical_stream.VecMerge{
			Groups: []logical_stream.VecMergeGroup{
				{Scan: newMergeScan([]string{mergeNameTag}, "", "", groupOne, false)},
				{Scan: newMergeScan([]string{mergeNameTag}, "", "", groupTwo, false)},
			},
			Limit:      2 * perGroup,
			SortByTime: true,
		}
		require.Equal(t, want, mergeNames(runMergeDispatch(t, merge)))
		merge = &logical_stream.VecMerge{
			Groups: []logical_stream.VecMergeGroup{
				{Scan: newMergeScan([]string{mergeNameTag}, "", "", groupOne, false)},
				{Scan: newMergeScan([]string{mergeNameTag}, "", "", groupTwo, false)},
			},
			Offset:     1100,
			Limit:      3,
			SortByTime: true,
		}
		require.Equal(t, want[1100:1103], mergeNames(runMergeDispatch(t, merge)))
	})

	// Four distinct elements share one key. Equal keys keep the order the groups were scanned in,
	// so groups [A,B] and [C,D] page as A,B then C,D, in both directions.
	t.Run("equal keys keep group scan order", func(t *testing.T) {
		for _, desc := range []bool{false, true} {
			build := func(offset uint32) *logical_stream.VecMerge {
				groupOne := []mergeRow{mergeNamed("A", 1, 5, 1), mergeNamed("B", 2, 5, 2)}
				groupTwo := []mergeRow{mergeNamed("C", 3, 5, 3), mergeNamed("D", 4, 5, 4)}
				return &logical_stream.VecMerge{
					Groups: []logical_stream.VecMergeGroup{
						{Scan: newMergeScan([]string{mergeNameTag, mergeKeyTag}, mergeKeyTag, "", groupOne, false)},
						{Scan: newMergeScan([]string{mergeNameTag, mergeKeyTag}, mergeKeyTag, "", groupTwo, false)},
					},
					Offset: offset,
					Limit:  2,
					Desc:   desc,
				}
			}
			require.Equal(t, []string{"A", "B"}, mergeNames(runMergeDispatch(t, build(0))), "desc=%t", desc)
			require.Equal(t, []string{"C", "D"}, mergeNames(runMergeDispatch(t, build(2))), "desc=%t", desc)
		}
	})
}
