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

package stream

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
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/iter/sort"
	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
)

var errFakeExhausted = errors.New("fake query resource exhausted")

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
	return nil, errors.New("the stream plan must scatter, not broadcast")
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

func scatterElement(ts int64, id uint64) *streamv1.Element {
	return &streamv1.Element{ElementId: hexElementID(id), Timestamp: timestamppb.New(time.Unix(0, ts))}
}

// oneElementPerChunk makes every element its own proto chunk.
func oneElementPerChunk(elements []*streamv1.Element) []any {
	chunks := make([]any, 0, len(elements))
	for _, e := range elements {
		chunks = append(chunks, &streamv1.QueryResponse{Elements: []*streamv1.Element{e}})
	}
	return chunks
}

func newScatterTestPlan() *distributedPlan {
	return &distributedPlan{queryTemplate: &streamv1.QueryRequest{Name: "s"}, sortByTime: true}
}

// referenceMerge is the whole-result algorithm the plan ran before it streamed:
// a k-way merge of every node's complete result with a first-seen element-ID map.
func referenceMerge(nodes [][]*streamv1.Element, plan *distributedPlan) []*streamv1.Element {
	var see []sort.Iterator[*comparableElement]
	for _, n := range nodes {
		see = append(see, newSortableElements(n, plan.sortByTime, plan.sortTagSpec))
	}
	merged := sort.NewItemIter(see, plan.desc)
	var result []*streamv1.Element
	seen := make(map[string]bool)
	for merged.Next() {
		element := merged.Val().Element
		if !seen[element.ElementId] {
			seen[element.ElementId] = true
			result = append(result, element)
		}
	}
	return result
}

func TestDistributedPlanScatterMatchesWholeResultMerge(t *testing.T) {
	var nodeA, nodeB, nodeC []*streamv1.Element
	for i := int64(0); i < 40; i++ {
		nodeA = append(nodeA, scatterElement(i*3, uint64(1000+i)))
		nodeB = append(nodeB, scatterElement(i*3+1, uint64(2000+i)))
		nodeC = append(nodeC, scatterElement(i*3+2, uint64(3000+i)))
	}
	// Replicas: elements 7 and 20 of node A also live on the other nodes, and
	// element 1000 is the same timestamp and ID on every node.
	nodeB = append(nodeB, scatterElement(7*3, 1007), scatterElement(0, 1000))
	nodeC = append(nodeC, scatterElement(20*3, 1020), scatterElement(0, 1000))
	nodes := [][]*streamv1.Element{nodeA, nodeB, nodeC}
	for i := range nodes {
		sortElementsByTime(nodes[i])
	}
	plan := newScatterTestPlan()
	fake := &fakeDistributedContext{}
	for i, n := range nodes {
		fake.streams = append(fake.streams, &fakeNodeStream{name: fmt.Sprintf("node-%d", i), chunks: oneElementPerChunk(n)})
	}
	want := referenceMerge(nodes, plan)
	got, err := plan.Execute(executor.WithDistributedExecutionContext(context.Background(), fake))
	require.NoError(t, err)
	require.Equal(t, elementIDSeq(want), elementIDSeq(got))
	require.Len(t, got, 120)
	for _, s := range fake.streams {
		require.True(t, s.closed.Load(), "stream %s must be closed", s.name)
	}
}

func sortElementsByTime(elements []*streamv1.Element) {
	for i := 1; i < len(elements); i++ {
		for j := i; j > 0 && elements[j].Timestamp.AsTime().Before(elements[j-1].Timestamp.AsTime()); j-- {
			elements[j], elements[j-1] = elements[j-1], elements[j]
		}
	}
}

func TestDistributedPlanScatterNodeFailureFailsQuery(t *testing.T) {
	good := []*streamv1.Element{scatterElement(1, 1), scatterElement(2, 2)}
	for name, nodeErr := range map[string]error{
		"transport":  errors.New("scan on node broken failed: boom"),
		"exhausted":  fmt.Errorf("scan on node broken failed: %w", errFakeExhausted),
		"idle":       fmt.Errorf("node broken sent nothing for 15s: %w", context.DeadlineExceeded),
		"mid-stream": fmt.Errorf("stream broke: %w", io.ErrUnexpectedEOF),
	} {
		t.Run(name, func(t *testing.T) {
			fake := &fakeDistributedContext{streams: []*fakeNodeStream{
				{name: "good", chunks: oneElementPerChunk(good)},
				{name: "broken", chunks: oneElementPerChunk(good[:1]), err: nodeErr},
			}}
			_, err := newScatterTestPlan().Execute(executor.WithDistributedExecutionContext(context.Background(), fake))
			require.Error(t, err)
			require.ErrorContains(t, err, "broken")
			// A typed failure keeps its identity, so the liaison can still report RESOURCE_EXHAUSTED.
			require.ErrorIs(t, err, nodeErr)
			for _, s := range fake.streams {
				require.True(t, s.closed.Load(), "stream %s must be closed", s.name)
			}
		})
	}
}

func TestDistributedPlanScatterOpenFailure(t *testing.T) {
	openErr := errors.New("no active nodes")
	_, err := newScatterTestPlan().Execute(executor.WithDistributedExecutionContext(context.Background(), &fakeDistributedContext{openErr: openErr}))
	require.ErrorIs(t, err, openErr)
}

func TestDistributedPlanScatterStopsOnCallerCancellation(t *testing.T) {
	fake := &fakeDistributedContext{streams: []*fakeNodeStream{
		{name: "slow", chunks: oneElementPerChunk([]*streamv1.Element{scatterElement(1, 1)}), block: true},
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
	first := &streamv1.QueryResponse{Elements: []*streamv1.Element{scatterElement(1, 1)}}
	last := &streamv1.QueryResponse{Trace: subTrace}
	fake := &fakeDistributedContext{streams: []*fakeNodeStream{{name: "n", chunks: []any{first, last}}}}
	got, err := newScatterTestPlan().Execute(executor.WithDistributedExecutionContext(ctx, fake))
	require.NoError(t, err)
	require.Len(t, got, 1)
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
