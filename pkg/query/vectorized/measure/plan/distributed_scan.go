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

package plan

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"sync/atomic"

	"google.golang.org/protobuf/proto"

	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/tracelabels"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	vmeasure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure/frame"
)

// Receiving a chunk charges the tracker for its decoded cells, the merge descriptors that refer to them and the
// copies of the encoded body in flight; the charge is returned when the last batch that borrows the chunk lets go.
const (
	chunkCellBytes     = 128
	chunkBodyCopies    = int64(4)
	chunkRowOverhead   = 128
	chunkSchemaWorkset = 1024
)

// reservedOwner returns a chunk's reservation to the tracker once nothing borrows the chunk any more.
type reservedOwner struct {
	tracker *vectorized.MemoryTracker
	amount  int64
	refs    atomic.Int32
}

func (o *reservedOwner) Retain() { o.refs.Add(1) }

func (o *reservedOwner) Release() {
	if o.refs.Add(-1) == 0 {
		o.tracker.Release(o.amount)
	}
}

// scanSet owns what one distributed query holds while it reads its data nodes: the node streams, the scan context
// that cancels them, the memory reserved for the output workspace and the trace spans of the scans.
type scanSet struct {
	tracker    *vectorized.MemoryTracker
	cancel     context.CancelFunc
	workspaces map[int]int64
	sources    []*chunkSource
	scans      []*scanTrace
	batchSize  int
	closed     bool
}

// scanTrace is the trace span of one scatter, closed with the set.
type scanTrace struct {
	span    *query.Span
	ctx     context.Context
	sources []*chunkSource
}

func newScanSet(tracker *vectorized.MemoryTracker, cancel context.CancelFunc, batchSize int) *scanSet {
	return &scanSet{tracker: tracker, cancel: cancel, batchSize: batchSize, workspaces: make(map[int]int64)}
}

// close cancels the scans, closes the node streams and returns what the set reserved. It is safe to call more than
// once, and it must run on the goroutine that reads the streams or after it is done.
func (s *scanSet) close() {
	if s.closed {
		return
	}
	s.closed = true
	s.cancel()
	for _, src := range s.sources {
		src.stream.Close()
	}
	for _, scan := range s.scans {
		scan.finish()
	}
	for group, amount := range s.workspaces {
		s.tracker.Release(amount)
		delete(s.workspaces, group)
	}
}

// finish attaches the data nodes' traces to the span and stops it.
func (t *scanTrace) finish() {
	if t.span == nil {
		return
	}
	nodes := make([]nodeInfo, 0, len(t.sources))
	var frameBytes int64
	for _, src := range t.sources {
		info := nodeInfo{
			trace:     src.trace,
			bytes:     src.bytes,
			latencyNS: extractNodeLatency(src.trace),
			rows:      src.rows,
			hasError:  src.failed || (src.trace != nil && src.trace.GetError()),
		}
		if src.trace != nil {
			info.rows = max(info.rows, extractNodeTagInt64(src.trace, tracelabels.TagRespCount))
		}
		frameBytes += src.bytes
		nodes = append(nodes, info)
	}
	applyFanoutCap(t.ctx, t.span, nodes)
	addTraceTagf(t.span, tracelabels.TagResponseCount, "%d", len(nodes))
	addTraceTagf(t.span, tracelabels.TagFrameBytesTotal, "%d", frameBytes)
	stopTraceSpan(t.span)
	t.span = nil
}

// workspace reserves, once per group, the pool and current batch the merge of that group's rows needs.
func (s *scanSet) workspace(group int, numCols uint64) error {
	if _, ok := s.workspaces[group]; ok {
		return nil
	}
	if numCols > (math.MaxInt64-chunkRowOverhead)/chunkCellBytes {
		return vectorized.ErrMemoryBudgetExceeded
	}
	rowWidth := numCols*chunkCellBytes + chunkRowOverhead
	if uint64(s.batchSize) > math.MaxInt64/rowWidth {
		return vectorized.ErrMemoryBudgetExceeded
	}
	amount := int64(uint64(s.batchSize) * rowWidth)
	if err := s.tracker.Reserve(amount); err != nil {
		return err
	}
	s.workspaces[group] = amount
	return nil
}

// chunkSource is one data node's stream of frames of one group's request.
type chunkSource struct {
	stream bus.NodeStream
	set    *scanSet
	trace  *commonv1.Trace
	bytes  int64
	rows   int64
	group  int
	index  int
	done   bool
	failed bool
}

// frameOf extracts the frame and the data node's trace from a chunk. A chunk is a raw frame, or the response
// that carries the trace of a traced query beside the last frame.
func frameOf(payload any) ([]byte, *commonv1.Trace, error) {
	switch response := payload.(type) {
	case nil:
		return nil, nil, nil
	case []byte:
		return response, nil, nil
	case *measurev1.InternalQueryResponse:
		if len(response.GetDataPoints()) > 0 && len(response.GetRawFrameBody()) == 0 {
			return nil, nil, fmt.Errorf("vec distributed plan: got data_points proto response under raw wire mode")
		}
		return response.GetRawFrameBody(), response.GetTrace(), nil
	default:
		return nil, nil, fmt.Errorf("vec distributed plan: unexpected response %T", payload)
	}
}

// next returns the next non-empty chunk decoded into a batch that the caller releases, or nil once the node has
// sent everything. A failed stream is an error, never an end.
func (c *chunkSource) next(ctx context.Context) (*vectorized.RecordBatch, error) {
	for !c.done {
		message, err := c.stream.Next(ctx)
		if errors.Is(err, io.EOF) {
			c.done = true
			return nil, nil
		}
		if err != nil {
			c.failed = true
			return nil, fmt.Errorf("data node %s: %w", c.stream.Node(), err)
		}
		body, trace, frameErr := frameOf(message.Data())
		if frameErr != nil {
			c.failed = true
			return nil, frameErr
		}
		if trace != nil {
			c.trace = trace
		}
		if len(body) == 0 {
			continue
		}
		batch, decodeErr := c.set.decode(c.group, body)
		if decodeErr != nil {
			c.failed = true
			return nil, fmt.Errorf("data node %s: %w", c.stream.Node(), decodeErr)
		}
		if batch.ActiveLen() == 0 {
			batch.ReleaseOwners()
			continue
		}
		c.bytes += int64(len(body))
		c.rows += int64(batch.ActiveLen())
		return batch, nil
	}
	return nil, nil
}

// decode reserves what a chunk needs, decodes it and ties the reservation to the lifetime of the batch.
func (s *scanSet) decode(group int, body []byte) (*vectorized.RecordBatch, error) {
	header, _, headerErr := frame.ValidateHeader(body)
	if headerErr != nil {
		return nil, headerErr
	}
	vmeasure.RecordNativeFrameRows(header.NumRows)
	if workspaceErr := s.workspace(group, header.NumCols); workspaceErr != nil {
		return nil, workspaceErr
	}
	if header.NumCols > (math.MaxInt64-chunkRowOverhead)/chunkCellBytes {
		return nil, vectorized.ErrMemoryBudgetExceeded
	}
	rowWidth := header.NumCols*chunkCellBytes + chunkRowOverhead
	if header.NumRows > uint64(math.MaxInt64)/rowWidth {
		return nil, vectorized.ErrMemoryBudgetExceeded
	}
	amount := int64(header.NumRows * rowWidth)
	if int64(len(body)) > (math.MaxInt64-amount-chunkSchemaWorkset)/chunkBodyCopies {
		return nil, vectorized.ErrMemoryBudgetExceeded
	}
	amount += int64(len(body))*chunkBodyCopies + chunkSchemaWorkset
	if reserveErr := s.tracker.Reserve(amount); reserveErr != nil {
		return nil, reserveErr
	}
	batch, decodeErr := frame.Decode(body)
	if decodeErr != nil {
		s.tracker.Release(amount)
		return nil, fmt.Errorf("decode frame: %w", decodeErr)
	}
	owner := &reservedOwner{tracker: s.tracker, amount: amount}
	owner.refs.Store(1)
	batch.Owners.Pin(owner)
	owner.Release()
	return batch, nil
}

// scanMessage builds the topic and the message of one internal query, encoding the QL-only patterns when there are
// any. release returns the memory reserved for the encoding and may be called more than once.
func (p *DistributedPlan) scanMessage(ctx context.Context, dctx executor.DistributedExecutionContext, request *measurev1.InternalQueryRequest,
) (bus.Topic, bus.Message, func(), error) {
	release := func() {}
	topic := data.TopicInternalMeasureQuery
	var payload any = request
	if len(p.patterns) > 0 {
		encodeBytes := vmeasure.PatternEncodeReservationBytes(request, p.patterns)
		tracker := vectorized.QueryMemoryTracker(ctx, int64(p.cfg.QueryMemoryMiB)*1024*1024)
		if err := tracker.Reserve(encodeBytes); err != nil {
			return topic, bus.Message{}, release, err
		}
		release = onceFunc(func() { tracker.Release(encodeBytes) })
		body, err := vmeasure.EncodeMeasurePatternRequest(request, p.patterns)
		if err != nil {
			return topic, bus.Message{}, release, err
		}
		topic = data.TopicInternalMeasurePatternQuery
		payload = body
	}
	group := ""
	if groups := request.GetRequest().GetGroups(); len(groups) > 0 {
		group = groups[0]
	}
	message := bus.NewMessageWithSelectorsAndGroup(bus.MessageID(dctx.TimeRange().Begin.Nanos), dctx.NodeSelectors(), dctx.TimeRange(), group, payload)
	return topic, message, release, nil
}

// openScan opens one scan stream per data node for the group's request and adds them to the set.
func (p *DistributedPlan) openScan(ctx, scanCtx context.Context, set *scanSet, dctx executor.DistributedExecutionContext,
	request *measurev1.InternalQueryRequest, group int, spanName string, spanGroup string,
) error {
	span, spanCtx := startTraceSpan(ctx, spanName)
	if spanGroup != "" {
		addTraceTag(span, tracelabels.TagGroupName, spanGroup)
	}
	addTraceTagf(span, tracelabels.TagNodeCount, "%d", nodeSelectorCount(dctx.NodeSelectors()))
	addTraceTag(span, tracelabels.TagTimeRange, dctx.TimeRange().String())
	topic, message, release, messageErr := p.scanMessage(ctx, dctx, request)
	defer release()
	var streams []bus.NodeStream
	var scatterErr error
	if messageErr == nil {
		// The scan streams live as long as the scan context, which the result cancels when it is closed; each
		// node is bounded by the scatter idle timeout and its heartbeats rather than a fixed deadline.
		streams, scatterErr = dctx.Scatter(scanCtx, topic, message, bus.ScatterOptions{})
	}
	if err := errors.Join(messageErr, scatterErr); err != nil {
		wrapped := fmt.Errorf("vec distributed plan: scan: %w", err)
		if span != nil {
			span.Error(wrapped)
			stopTraceSpan(span)
		}
		return wrapped
	}
	scan := &scanTrace{span: span, ctx: spanCtx}
	for _, stream := range streams {
		src := &chunkSource{stream: stream, set: set, group: group, index: len(set.sources)}
		set.sources = append(set.sources, src)
		scan.sources = append(scan.sources, src)
	}
	set.scans = append(set.scans, scan)
	return nil
}

func onceFunc(fn func()) func() {
	var done atomic.Bool
	return func() {
		if done.CompareAndSwap(false, true) {
			fn()
		}
	}
}

// cloneNodeRequest returns the internal request a data node gets for the group.
func (p *DistributedPlan) cloneNodeRequest(dctx executor.DistributedExecutionContext) *measurev1.QueryRequest {
	nodeRequest := proto.Clone(p.nodeTemplate).(*measurev1.QueryRequest)
	nodeRequest.TimeRange = dctx.TimeRange()
	return nodeRequest
}
