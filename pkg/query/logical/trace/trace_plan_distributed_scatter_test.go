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
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/iter"
	"github.com/apache/skywalking-banyandb/pkg/iter/sort"
	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
)

var errFakeIdle = errors.New("fake scan idle timeout")

type fakeNodeStream struct {
	err    error
	name   string
	chunks []any
	next   int
	closed atomic.Bool
	// block makes Next wait for the context once the chunks are exhausted.
	block bool
}

func (f *fakeNodeStream) Node() string { return f.name }

func (f *fakeNodeStream) Next(ctx context.Context) (bus.Message, error) {
	if f.next < len(f.chunks) {
		chunk := f.chunks[f.next]
		f.next++
		return bus.NewMessageWithNode(1, f.name, chunk), nil
	}
	if f.err != nil {
		return bus.Message{}, f.err
	}
	if f.block {
		<-ctx.Done()
		return bus.Message{}, ctx.Err()
	}
	return bus.Message{}, io.EOF
}

func (f *fakeNodeStream) Close() { f.closed.Store(true) }

type fakeDistributedContext struct {
	openErr error
	streams []*fakeNodeStream
}

func (f *fakeDistributedContext) Broadcast(time.Duration, bus.Topic, bus.Message) ([]bus.Future, error) {
	return nil, errors.New("the trace plan must scatter, not broadcast")
}

func (f *fakeDistributedContext) Scatter(context.Context, bus.Topic, bus.Message, bus.ScatterOptions) ([]bus.NodeStream, error) {
	if f.openErr != nil {
		return nil, f.openErr
	}
	out := make([]bus.NodeStream, len(f.streams))
	for i, s := range f.streams {
		out[i] = s
	}
	return out, nil
}

func (f *fakeDistributedContext) TimeRange() *modelv1.TimeRange {
	return &modelv1.TimeRange{Begin: timestamppb.New(time.Unix(1, 0)), End: timestamppb.New(time.Unix(2, 0))}
}

func (f *fakeDistributedContext) NodeSelectors() map[string][]string { return nil }

func scatterTestResult(tid, spanID string) model.TraceResult {
	return model.TraceResult{
		TID:     tid,
		Spans:   [][]byte{[]byte("span-" + spanID)},
		SpanIDs: []string{spanID},
		Tags:    []model.Tag{{Name: "svc", Values: []*modelv1.TagValue{strTagValueForDistributedTest("svc-" + spanID)}}},
	}
}

// oneTracePerChunk encodes every result as its own frame chunk.
func oneTracePerChunk(results []model.TraceResult) []any {
	chunks := make([]any, 0, len(results))
	for i := range results {
		chunks = append(chunks, EncodeTraceResultFrame(results[i:i+1]))
	}
	return chunks
}

// referenceMerge is the whole-result algorithm the plan ran before it streamed:
// a k-way merge of the nodes' complete results with a global trace-ID map.
func referenceMerge(nodes [][]model.TraceResult, sortByTraceID, desc bool) []model.TraceResult {
	var st []sort.Iterator[*comparableTraceResult]
	for _, n := range nodes {
		st = append(st, newSortableTraceResults(iter.FromSlice(n), sortByTraceID))
	}
	merged := sort.NewItemIter(st, desc)
	var result []model.TraceResult
	seen := make(map[string]int)
	for merged.Next() {
		tr := merged.Val().result
		if idx, ok := seen[tr.TID]; !ok {
			seen[tr.TID] = len(result)
			result = append(result, tr)
		} else {
			mergeTraceResultSpans(&result[idx], &tr)
		}
	}
	return result
}

func collect(t *testing.T, it iter.Iterator[model.TraceResult]) []model.TraceResult {
	t.Helper()
	var out []model.TraceResult
	for {
		r, ok := it.Next()
		require.NoError(t, r.Error)
		if !ok {
			return out
		}
		out = append(out, r)
	}
}

func newScatterTestPlan() *distributedPlan {
	return &distributedPlan{queryTemplate: &tracev1.QueryRequest{Name: "t"}, sortByTraceID: true}
}

func TestDistributedPlanScatterMatchesWholeResultMerge(t *testing.T) {
	var nodeA, nodeB, nodeC []model.TraceResult
	for i := 0; i < 40; i++ {
		nodeA = append(nodeA, scatterTestResult(fmt.Sprintf("a-%03d", i), fmt.Sprintf("a%d", i)))
		nodeB = append(nodeB, scatterTestResult(fmt.Sprintf("b-%03d", i), fmt.Sprintf("b%d", i)))
		nodeC = append(nodeC, scatterTestResult(fmt.Sprintf("c-%03d", i), fmt.Sprintf("c%d", i)))
	}
	// One trace is split across the three nodes, with span s2 held by two of them.
	nodeA = append(nodeA, scatterTestResult("split", "s1"))
	nodeB = append(nodeB, scatterTestResult("split", "s2"))
	nodeC = append(nodeC, scatterTestResult("split", "s2"))
	nodes := [][]model.TraceResult{nodeA, nodeB, nodeC}
	for i := range nodes {
		sortResultsByTID(nodes[i])
	}
	fake := &fakeDistributedContext{}
	for i, n := range nodes {
		fake.streams = append(fake.streams, &fakeNodeStream{name: fmt.Sprintf("node-%d", i), chunks: oneTracePerChunk(n)})
	}
	want := referenceMerge(nodes, true, false)
	got, err := newScatterTestPlan().Execute(executor.WithDistributedExecutionContext(context.Background(), fake))
	require.NoError(t, err)
	gotResults := collect(t, got)
	require.Equal(t, describeResults(want), describeResults(gotResults))
	require.Len(t, gotResults, 121)
	for _, r := range gotResults {
		if r.TID == "split" {
			require.ElementsMatch(t, []string{"s1", "s2"}, r.SpanIDs)
		}
	}
	for _, s := range fake.streams {
		require.True(t, s.closed.Load(), "stream %s must be closed", s.name)
	}
}

// describeResults renders the observable content of the results: order, IDs, spans and tag values.
func describeResults(results []model.TraceResult) []string {
	out := make([]string, 0, len(results))
	for i := range results {
		r := &results[i]
		line := fmt.Sprintf("%s key=%d spans=%q ids=%v", r.TID, r.Key, r.Spans, r.SpanIDs)
		for _, tag := range r.Tags {
			line += " " + tag.Name + "="
			for _, v := range tag.Values {
				line += v.GetStr().GetValue() + ","
			}
		}
		out = append(out, line)
	}
	return out
}

func sortResultsByTID(results []model.TraceResult) {
	for i := 1; i < len(results); i++ {
		for j := i; j > 0 && results[j].TID < results[j-1].TID; j-- {
			results[j], results[j-1] = results[j-1], results[j]
		}
	}
}

func TestDistributedPlanScatterNodeFailureFailsQuery(t *testing.T) {
	good := []model.TraceResult{scatterTestResult("g1", "g1"), scatterTestResult("g2", "g2")}
	for name, nodeErr := range map[string]error{
		"transport": errors.New("scan on node broken failed: boom"),
		"idle":      fmt.Errorf("node broken sent nothing for 15s: %w", errFakeIdle),
	} {
		t.Run(name, func(t *testing.T) {
			fake := &fakeDistributedContext{streams: []*fakeNodeStream{
				{name: "good", chunks: oneTracePerChunk(good)},
				{name: "broken", chunks: oneTracePerChunk(good[:1]), err: nodeErr},
			}}
			_, err := newScatterTestPlan().Execute(executor.WithDistributedExecutionContext(context.Background(), fake))
			require.Error(t, err)
			require.ErrorContains(t, err, "broken")
			require.ErrorIs(t, err, nodeErr)
			for _, s := range fake.streams {
				require.True(t, s.closed.Load(), "stream %s must be closed", s.name)
			}
		})
	}
}

func TestDistributedPlanScatterOpenFailure(t *testing.T) {
	openErr := errors.New("no active nodes")
	fake := &fakeDistributedContext{openErr: openErr}
	_, err := newScatterTestPlan().Execute(executor.WithDistributedExecutionContext(context.Background(), fake))
	require.ErrorIs(t, err, openErr)
}

func TestDistributedPlanScatterStopsOnCallerCancellation(t *testing.T) {
	fake := &fakeDistributedContext{streams: []*fakeNodeStream{
		{name: "slow", chunks: oneTracePerChunk([]model.TraceResult{scatterTestResult("t", "t")}), block: true},
	}}
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(50*time.Millisecond, cancel)
	_, err := newScatterTestPlan().Execute(executor.WithDistributedExecutionContext(ctx, fake))
	require.ErrorIs(t, err, context.Canceled)
	require.True(t, fake.streams[0].closed.Load())
}

func TestDistributedPlanScatterAttachesNodeSubTraces(t *testing.T) {
	tracer, ctx := query.NewTracer(context.Background(), "root")
	subTrace := &commonv1.Trace{TraceId: "data-node", Spans: []*commonv1.Span{{Message: "data-span"}}}
	first := &tracev1.InternalQueryResponse{InternalTraces: []*tracev1.InternalTrace{
		{TraceId: "t1", Spans: []*tracev1.Span{{Span: []byte("x"), SpanId: "x"}}},
	}}
	last := &tracev1.InternalQueryResponse{TraceQueryResult: subTrace}
	fake := &fakeDistributedContext{streams: []*fakeNodeStream{{name: "n", chunks: []any{first, last}}}}
	it, err := newScatterTestPlan().Execute(executor.WithDistributedExecutionContext(ctx, fake))
	require.NoError(t, err)
	require.Len(t, collect(t, it), 1)
	found := false
	var walk func(spans []*commonv1.Span)
	walk = func(spans []*commonv1.Span) {
		for _, s := range spans {
			if s.Message == "data-span" {
				found = true
			}
			walk(s.Children)
		}
	}
	walk(tracer.ToProto().Spans)
	require.True(t, found, "the data node's trace must be attached under the liaison's span")
}
