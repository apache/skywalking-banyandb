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

package grpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/banyand/liaison/grpc/route"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	propertydb "github.com/apache/skywalking-banyandb/banyand/property/db"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/partition"
	"github.com/apache/skywalking-banyandb/pkg/run"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
	"github.com/apache/skywalking-banyandb/pkg/transfer"
	"github.com/apache/skywalking-banyandb/pkg/transfer/ratelimit"
)

// importDeps are the liaison components the ImportService routes and writes through.
type importDeps struct {
	groups func(ctx context.Context, name string) (*commonv1.Group, error)
	// listGroups lists every group, which a lifecycle stage's selector is laid out by.
	listGroups func(ctx context.Context) ([]*commonv1.Group, error)
	properties func(ctx context.Context, md *commonv1.Metadata) (*databasev1.Property, error)
	// selfNode names this process, the only data node of a standalone process.
	selfNode func() string
	// writeConfig carries the write pressure timeout and backoff a busy data node is retried
	// with; nil selects the defaults.
	writeConfig *writeSessionConfig
	tire2       queue.Client
	routeTable  route.TableProvider
	// dataNodes locate, per catalog, the data nodes the cluster writes online data to: the
	// nodes the liaison's write queue syncs a stream, measure or trace to, and the property
	// node registry.
	dataNodes map[commonv1.Catalog]NodeRegistry
	groupRepo *groupRepo
	streams   *discoveryService
	measures  *discoveryService
	traces    *discoveryService
}

// importService is the liaison-side ImportService. It writes the rows of a CSV artifact
// straight to the data nodes of the target stage over the import topics, fanning every row out
// to its replicas; the data nodes enforce the import's rate limit. A standalone process has no
// tire2 route table: it is its own only data node and writes through its local pipeline.
type importService struct {
	transferv1.UnimplementedImportServiceServer
	importDeps
	l    *logger.Logger
	rate *ratelimit.Relay
}

func newImportService(deps importDeps, l *logger.Logger) *importService {
	return &importService{importDeps: deps, l: l, rate: ratelimit.NewRelay()}
}

// pressureConfig is the pressure timeout and backoff a busy data node is retried with.
func (s *importService) pressureConfig() writeSessionConfig {
	var cfg writeSessionConfig
	if s.writeConfig != nil {
		cfg = *s.writeConfig
	}
	return cfg.withDefaults()
}

// group returns the named group, NotFound when it does not exist. A group the server keeps
// for itself is refused: it is not user data.
func (s *importService) group(ctx context.Context, name string) (*commonv1.Group, error) {
	if common.IsInternalGroup(name) {
		return nil, status.Errorf(codes.InvalidArgument, "group %s is internal to the server and cannot be imported", name)
	}
	g, err := s.groups(ctx, name)
	if err != nil {
		if isGroupNotFound(err) {
			return nil, importError(codes.NotFound, transfer.ImportReasonSchemaNotFound, nil, fmt.Sprintf("group %s not found", name))
		}
		return nil, schemaReadError(err, "get group %s", name)
	}
	if g.GetResourceOpts() == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "group %s has no resource options", name)
	}
	return g, nil
}

// routeFor computes the placement snapshot of a group's stage; it caches nothing.
func (s *importService) routeFor(ctx context.Context, g *commonv1.Group, stage string) (*importRoute, error) {
	name := g.GetMetadata().GetName()
	ro := g.GetResourceOpts()
	if g.GetCatalog() == commonv1.Catalog_CATALOG_PROPERTY {
		return propertyRoute(name, ro, s.dataNodes[commonv1.Catalog_CATALOG_PROPERTY])
	}
	if s.routeTable == nil {
		return standaloneRoute(name, ro, s.selfNode())
	}
	return s.stageRoute(ctx, g, stage)
}

// Targets reports where each shard of the requested (group, stage) is written, routing the
// stored group through the same route functions ImportRows uses. It is read-only.
func (s *importService) Targets(ctx context.Context, req *transferv1.TargetsRequest) (*transferv1.TargetsResponse, error) {
	if err := req.ValidateAll(); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	name := req.GetGroup()
	g, err := s.group(ctx, name)
	if err != nil {
		return nil, err
	}
	r, err := s.routeFor(ctx, g, req.GetStage())
	if err != nil {
		return nil, err
	}
	out := r.toProto()
	out.InactiveNodeIds = s.inactiveNodes(r)
	out.GroupDeleting = s.groupRepo.pendingDeletion(name)
	return out, nil
}

// inactiveNodes returns the sorted nodes of the route this liaison is not connected to. A
// standalone process is its own only node, always active.
func (s *importService) inactiveNodes(r *importRoute) []string {
	if s.routeTable == nil {
		return nil
	}
	active := make(map[string]struct{})
	for _, n := range s.routeTable.GetRouteTable().GetActive() {
		active[n] = struct{}{}
	}
	var inactive []string
	for _, n := range r.nodes() {
		if _, ok := active[n]; !ok {
			inactive = append(inactive, n)
		}
	}
	return inactive
}

// ImportRows replays the rows of one (catalog, group, stage). Each batch is paid to the
// import rate limiter, written to every target node and acknowledged once all of them
// accepted it. Up to importPipelineDepth batches are in flight at once, acknowledged in the
// order they arrived; the first failure in that order ends the stream.
func (s *importService) ImportRows(stream transferv1.ImportService_ImportRowsServer) error {
	first, err := stream.Recv()
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err != nil {
		return err
	}
	if err = first.ValidateAll(); err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	init := first.GetInit()
	if init == nil {
		return status.Error(codes.InvalidArgument, "the first frame of an import stream must be init")
	}
	ctx := stream.Context()
	g, err := s.group(ctx, init.GetGroup())
	if err != nil {
		return err
	}
	if g.GetCatalog() != init.GetCatalog() {
		return status.Errorf(codes.InvalidArgument, "group %s is of catalog %s, not %s", init.GetGroup(), g.GetCatalog(), init.GetCatalog())
	}
	r, err := s.routeFor(ctx, g, init.GetStage())
	if err != nil {
		return err
	}
	s.l.Info().Str("peer", peerAddr(ctx)).Stringer("catalog", init.GetCatalog()).Str("group", init.GetGroup()).
		Str("requested_stage", init.GetStage()).Str("stage", r.stage).Int("target_nodes", len(r.nodes())).
		Msg("import stream opened")
	defer s.rate.Open(init.GetLiaisonBytesPerSecond())()

	depth := importPipelineDepth
	if init.GetCatalog() == commonv1.Catalog_CATALOG_PROPERTY {
		depth = 1
	}
	return newImportPipeline(ctx, depth).run(stream, &importWriter{s: s, init: init, route: r})
}

// importPipelineDepth is how many batches an import stream writes at once; acknowledgements
// stay in batch order. Property streams write one batch at a time, so an entity's revisions
// arrive in order.
const importPipelineDepth = 4

// importStep is one received batch, or the error that ended the receiving, in stream order.
type importStep struct {
	err  error
	done chan struct{}
	seq  uint64
}

// importPipeline keeps the batches of one import stream in flight. A receiver goroutine reads,
// pays and starts the write of each batch; the handler goroutine acknowledges them in order.
type importPipeline struct {
	ctx    context.Context
	cancel context.CancelFunc
	// steps carries the batches in stream order; slots holds one token per batch in flight.
	steps chan *importStep
	slots chan struct{}
	// failed is closed once a write fails, so that no further batch is started.
	failed     chan struct{}
	workers    sync.WaitGroup
	workersMu  sync.Mutex
	failOnce   sync.Once
	workersEnd bool
}

func newImportPipeline(parent context.Context, depth int) *importPipeline {
	ctx, cancel := context.WithCancel(parent)
	return &importPipeline{
		ctx:    ctx,
		cancel: cancel,
		steps:  make(chan *importStep, depth),
		slots:  make(chan struct{}, depth),
		failed: make(chan struct{}),
	}
}

// run acknowledges the batches in stream order and returns the first failure. Before it
// returns, every write it started has ended; the receiver may still be blocked in Recv, which
// returns once the handler has returned.
func (p *importPipeline) run(stream transferv1.ImportService_ImportRowsServer, w *importWriter) error {
	run.Go(p.ctx, "liaison.grpc.import.receive", w.s.l, func(context.Context) { p.receive(stream, w) })
	var err error
	for st := range p.steps {
		<-st.done
		err = st.err
		if err == nil {
			err = stream.Send(&transferv1.ImportRowsResponse{BatchSeq: st.seq})
		}
		// Every batch holds a slot; only a receiver that panicked may push a step without one.
		select {
		case <-p.slots:
		default:
		}
		if err != nil {
			break
		}
	}
	// The later batches still in flight are canceled and waited for.
	p.cancel()
	p.workersMu.Lock()
	p.workersEnd = true
	p.workersMu.Unlock()
	p.workers.Wait()
	return err
}

// receive reads the batches of the stream until it ends, a frame is invalid or a write fails.
// A receiver that panicked ends the stream with an internal error rather than as complete.
func (p *importPipeline) receive(stream transferv1.ImportService_ImportRowsServer, w *importWriter) {
	ended := false
	defer func() {
		if !ended {
			p.pushFailure(status.Error(codes.Internal, "the import stream receiver failed"))
		}
		close(p.steps)
	}()
	p.receiveBatches(stream, w)
	ended = true
}

// receiveBatches takes a slot before each Recv, so at most depth batches are held at once.
func (p *importPipeline) receiveBatches(stream transferv1.ImportService_ImportRowsServer, w *importWriter) {
	for p.acquireSlot() {
		batch, err := p.next(stream, w.s)
		if errors.Is(err, io.EOF) {
			p.releaseSlot()
			return
		}
		if err == nil {
			st := &importStep{done: make(chan struct{})}
			if p.start(st, w, batch) {
				if !p.push(st) {
					return
				}
				continue
			}
			// start refuses only once run canceled the pipeline or an earlier write failed.
			err = errEarlierBatchFailed
			if ctxErr := p.ctx.Err(); ctxErr != nil {
				err = status.FromContextError(ctxErr).Err()
			}
		}
		p.pushFailure(err)
		return
	}
}

// pushFailure hands run an already-ended step carrying err.
func (p *importPipeline) pushFailure(err error) {
	st := &importStep{done: make(chan struct{}), err: err}
	close(st.done)
	p.push(st)
}

// acquireSlot waits for a free slot and tells whether another batch may be received: not once
// the stream is canceled or a write failed, including while the slot was awaited.
func (p *importPipeline) acquireSlot() bool {
	select {
	case p.slots <- struct{}{}:
	case <-p.ctx.Done():
		return false
	case <-p.failed:
		return false
	}
	if p.hasFailed() {
		p.releaseSlot()
		return false
	}
	return true
}

// hasFailed tells whether a write of the stream failed.
func (p *importPipeline) hasFailed() bool {
	select {
	case <-p.failed:
		return true
	default:
		return false
	}
}

// releaseSlot gives back a slot taken for a batch that is not received.
func (p *importPipeline) releaseSlot() { <-p.slots }

// next receives, validates and pays one batch.
func (p *importPipeline) next(stream transferv1.ImportService_ImportRowsServer, s *importService) (*transferv1.ImportRowsRequest_RowBatch, error) {
	req, err := stream.Recv()
	if errors.Is(err, io.EOF) {
		return nil, err
	}
	if err != nil {
		return nil, withTooLargeDetail(err)
	}
	batch := req.GetBatch()
	// Validate stops at the first error: a batch holds up to 10000 rows.
	if err = req.Validate(); err != nil {
		if batch != nil {
			return nil, batchError(codes.InvalidArgument, batch.GetBatchSeq(), "%v", err)
		}
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if batch == nil {
		return nil, status.Error(codes.InvalidArgument, "init is allowed only as the first frame of an import stream")
	}
	if err = p.ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if !s.rate.Unlimited() {
		if err = s.rate.Pay(p.ctx, proto.Size(batch)); err != nil {
			return nil, batchStatus(batch.GetBatchSeq(), err)
		}
	}
	return batch, nil
}

// errEarlierBatchFailed ends a batch that was not written because an earlier one failed.
var errEarlierBatchFailed = status.Error(codes.Aborted, "not written: an earlier batch failed")

// start writes a batch in its own goroutine; it refuses once run stopped waiting for writes or
// a write failed, so no batch begins writing after a failed one.
func (p *importPipeline) start(st *importStep, w *importWriter, batch *transferv1.ImportRowsRequest_RowBatch) bool {
	p.workersMu.Lock()
	defer p.workersMu.Unlock()
	if p.workersEnd || p.hasFailed() {
		return false
	}
	st.seq = batch.GetBatchSeq()
	p.workers.Add(1)
	// A write that panics leaves this error, so its batch is never acknowledged.
	st.err = batchError(codes.Internal, st.seq, "the write failed")
	run.Go(p.ctx, "liaison.grpc.import.write", w.s.l, func(ctx context.Context) {
		defer p.workers.Done()
		defer close(st.done)
		defer func() {
			if st.err != nil {
				p.failOnce.Do(func() { close(p.failed) })
			}
		}()
		if err := ctx.Err(); err != nil {
			st.err = status.FromContextError(err).Err()
			return
		}
		if p.hasFailed() {
			st.err = errEarlierBatchFailed
			return
		}
		st.err = w.write(ctx, batch)
	})
	return true
}

// push hands a step to run, unless run has stopped reading them.
func (p *importPipeline) push(st *importStep) bool {
	select {
	case p.steps <- st:
		return true
	case <-p.ctx.Done():
		return false
	}
}

// importBatchTimeout bounds how long the data nodes may take to acknowledge one sealed batch,
// beyond the time the import's rate limit holds it on a node.
const importBatchTimeout = defaultWriteTimeout

// importWriter turns the batches of one import stream into internal write requests.
type importWriter struct {
	s     *importService
	init  *transferv1.ImportRowsRequest_Init
	route *importRoute
	// propertySchemas caches, per stream, the property schemas its batches named; a property
	// stream writes one batch at a time.
	propertySchemas map[string]*databasev1.Property
}

// shardMessage is one row of an import topic and the shard it belongs to.
type shardMessage struct {
	payload *transferv1.ImportWrite
	shard   uint32
}

// write turns the rows of a batch into write requests and writes them to their nodes; an empty
// batch writes nothing.
func (w *importWriter) write(ctx context.Context, batch *transferv1.ImportRowsRequest_RowBatch) error {
	seq := batch.GetBatchSeq()
	var (
		got   commonv1.Catalog
		rows  int
		topic bus.Topic
		// build turns the rows into write requests; nil for property, which is repaired instead.
		build func() ([]shardMessage, error)
	)
	switch r := batch.GetRows().(type) {
	case *transferv1.ImportRowsRequest_RowBatch_Stream:
		got, rows, topic = commonv1.Catalog_CATALOG_STREAM, len(r.Stream.GetRows()), data.TopicStreamImport
		build = func() ([]shardMessage, error) { return w.streamMessages(seq, r.Stream.GetRows()) }
	case *transferv1.ImportRowsRequest_RowBatch_Measure:
		got, rows, topic = commonv1.Catalog_CATALOG_MEASURE, len(r.Measure.GetRows()), data.TopicMeasureImport
		build = func() ([]shardMessage, error) { return w.measureMessages(seq, r.Measure.GetRows()) }
	case *transferv1.ImportRowsRequest_RowBatch_Trace:
		got, rows, topic = commonv1.Catalog_CATALOG_TRACE, len(r.Trace.GetRows()), data.TopicTraceImport
		build = func() ([]shardMessage, error) { return w.traceMessages(seq, r.Trace.GetRows()) }
	case *transferv1.ImportRowsRequest_RowBatch_Property:
		got, rows = commonv1.Catalog_CATALOG_PROPERTY, len(r.Property.GetRows())
	}
	if rows == 0 {
		return nil
	}
	if catalog := w.init.GetCatalog(); got != catalog {
		return batchError(codes.InvalidArgument, seq, "%d rows of catalog %s in a %s import stream", rows, got, catalog)
	}
	group := w.init.GetGroup()
	if err := w.s.groupRepo.acquireRequest(group); err != nil {
		return batchError(codes.FailedPrecondition, seq, "group %s is pending deletion", group)
	}
	defer w.s.groupRepo.releaseRequest(group)
	if build == nil {
		return w.writeProperties(ctx, seq, batch.GetProperty().GetRows())
	}
	msgs, err := build()
	if err != nil {
		return err
	}
	return w.publish(ctx, seq, topic, msgs)
}

// metadataCache returns the lookup of the Metadata of the stream group's resources, one per
// name, that the rows of a batch share.
func (w *importWriter) metadataCache() func(name string) *commonv1.Metadata {
	cache := make(map[string]*commonv1.Metadata)
	return func(name string) *commonv1.Metadata {
		md, ok := cache[name]
		if !ok {
			md = &commonv1.Metadata{Group: w.init.GetGroup(), Name: name}
			cache[name] = md
		}
		return md
	}
}

func (w *importWriter) streamMessages(seq uint64, rows []*transferv1.ImportRowsRequest_StreamRow) ([]shardMessage, error) {
	msgs := make([]shardMessage, 0, len(rows))
	metadataOf := w.metadataCache()
	for i, row := range rows {
		md := metadataOf(row.GetName())
		el := row.GetElement()
		if el == nil {
			return nil, rowError(seq, i, "element is required")
		}
		if err := timestamp.CheckPb(el.GetTimestamp()); err != nil {
			return nil, rowError(seq, i, "invalid timestamp: %v", err)
		}
		values, shard, err := w.s.streams.navigateByLocatorWithShardNum(md, el.GetTagFamilies(), nil, nil, w.route.shardNum)
		if errors.Is(err, errNotExist) {
			return nil, schemaNotFound(seq, "stream %s/%s not found", md.GetGroup(), md.GetName())
		}
		if err != nil {
			return nil, rowError(seq, i, "locate the shard of stream %s: %v", md.GetName(), err)
		}
		msgs = append(msgs, shardMessage{shard: uint32(shard), payload: &transferv1.ImportWrite{Row: &transferv1.ImportWrite_Stream{
			Stream: &streamv1.InternalWriteRequest{
				Request:      &streamv1.WriteRequest{Metadata: md, Element: el},
				ShardId:      uint32(shard),
				EntityValues: values[1:].Encode(),
				RawElementId: row.GetRawElementId(),
			},
		}}})
	}
	return msgs, nil
}

func (w *importWriter) measureMessages(seq uint64, rows []*transferv1.ImportRowsRequest_MeasureRow) ([]shardMessage, error) {
	msgs := make([]shardMessage, 0, len(rows))
	metadataOf := w.metadataCache()
	for i, row := range rows {
		md := metadataOf(row.GetName())
		point := row.GetPoint()
		if point == nil {
			return nil, rowError(seq, i, "point is required")
		}
		if point.GetVersion() == 0 {
			return nil, rowError(seq, i, "the version of measure %s must not be 0", md.GetName())
		}
		if err := timestamp.CheckPb(point.GetTimestamp()); err != nil {
			return nil, rowError(seq, i, "invalid timestamp: %v", err)
		}
		values, shard, err := w.s.measures.navigateByLocatorWithShardNum(md, point.GetTagFamilies(), nil, nil, w.route.shardNum)
		if errors.Is(err, errNotExist) {
			return nil, schemaNotFound(seq, "measure %s/%s not found", md.GetGroup(), md.GetName())
		}
		if err != nil {
			return nil, rowError(seq, i, "locate the shard of measure %s: %v", md.GetName(), err)
		}
		msgs = append(msgs, shardMessage{shard: uint32(shard), payload: &transferv1.ImportWrite{Row: &transferv1.ImportWrite_Measure{
			Measure: &measurev1.InternalWriteRequest{
				Request:      &measurev1.WriteRequest{Metadata: md, DataPoint: point},
				ShardId:      uint32(shard),
				EntityValues: values[1:].Encode(),
			},
		}}})
	}
	return msgs, nil
}

func nonEmptyStr(tag *modelv1.TagValue) (string, bool) {
	v := tag.GetStr().GetValue()
	return v, v != ""
}

// traceResource is what the rows of one trace in a batch share: its metadata, its schema and
// the positions of its trace id, span id and timestamp tags, all taken from one schema snapshot.
type traceResource struct {
	md       *commonv1.Metadata
	schema   *databasev1.Trace
	traceIdx int
	spanIdx  int
	tsIdx    int
}

func (w *importWriter) traceResource(seq uint64, name string) (*traceResource, error) {
	md := &commonv1.Metadata{Group: w.init.GetGroup(), Name: name}
	tr, ok := w.s.traces.entityRepo.getTrace(getID(md))
	if !ok {
		return nil, schemaNotFound(seq, "trace %s/%s not found", md.GetGroup(), md.GetName())
	}
	traceIdx, spanIdx, tsIdx := -1, -1, -1
	for i, t := range tr.GetTags() {
		switch t.GetName() {
		case tr.GetTraceIdTagName():
			traceIdx = i
		case tr.GetSpanIdTagName():
			spanIdx = i
		case tr.GetTimestampTagName():
			tsIdx = i
		}
	}
	if traceIdx < 0 || tsIdx < 0 {
		return nil, batchError(codes.FailedPrecondition, seq, "trace %s does not declare its trace id or timestamp tag", md.GetName())
	}
	return &traceResource{md: md, schema: tr, traceIdx: traceIdx, spanIdx: spanIdx, tsIdx: tsIdx}, nil
}

func (w *importWriter) traceMessages(seq uint64, rows []*transferv1.ImportRowsRequest_TraceRow) ([]shardMessage, error) {
	msgs := make([]shardMessage, 0, len(rows))
	resources := make(map[string]*traceResource)
	for i, row := range rows {
		res, ok := resources[row.GetName()]
		if !ok {
			var err error
			if res, err = w.traceResource(seq, row.GetName()); err != nil {
				return nil, err
			}
			resources[row.GetName()] = res
		}
		tr := res.schema
		tags := row.GetTags()
		if len(tags) != len(tr.GetTags()) {
			return nil, rowError(seq, i, "trace %s has %d tags, the row carries %d", res.md.GetName(), len(tr.GetTags()), len(tags))
		}
		// The import is stricter than an online write, which also takes binary trace ids.
		traceID, valid := nonEmptyStr(tags[res.traceIdx])
		if !valid {
			return nil, rowError(seq, i, "trace id tag %s must be a non-empty string", tr.GetTraceIdTagName())
		}
		if res.spanIdx >= 0 {
			if _, valid = nonEmptyStr(tags[res.spanIdx]); !valid {
				return nil, rowError(seq, i, "span id tag %s must be a non-empty string", tr.GetSpanIdTagName())
			}
		}
		ts := tags[res.tsIdx].GetTimestamp()
		if ts == nil {
			return nil, rowError(seq, i, "timestamp tag %s must not be null", tr.GetTimestampTagName())
		}
		if err := timestamp.CheckPb(ts); err != nil {
			return nil, rowError(seq, i, "invalid timestamp: %v", err)
		}
		shard := uint32(partition.TraceShardID(traceID, w.route.shardNum))
		msgs = append(msgs, shardMessage{shard: shard, payload: &transferv1.ImportWrite{Row: &transferv1.ImportWrite_Trace{
			Trace: &tracev1.InternalWriteRequest{
				ShardId: shard,
				Request: &tracev1.WriteRequest{Metadata: res.md, Tags: tags, Span: row.GetSpan(), Version: 1},
			},
		}}})
	}
	return msgs, nil
}

// publish fans the messages out to the targets of their shards, one batch publisher per
// node so that a failure names its node, and waits until every node answered.
func (w *importWriter) publish(ctx context.Context, seq uint64, topic bus.Topic, msgs []shardMessage) error {
	byNode := make(map[string][]*transferv1.ImportWrite)
	for _, m := range msgs {
		for _, n := range w.route.targets(m.shard) {
			byNode[n] = append(byNode[n], m.payload)
		}
	}
	nodes := make([]string, 0, len(byNode))
	for n := range byNode {
		nodes = append(nodes, n)
	}
	sort.Strings(nodes)
	// A plain group: a failed node does not cancel the others, every node is waited for.
	var eg errgroup.Group
	for _, node := range nodes {
		eg.Go(func() error {
			return w.publishToNode(ctx, seq, topic, node, byNode[node])
		})
	}
	return eg.Wait()
}

// publishToNode writes the payloads to the node. A node busy under memory pressure refuses the
// whole batch before writing any of it, so the batch is sent to it again with backoff until the
// write pressure timeout ends; it then fails as ResourceExhausted.
func (w *importWriter) publishToNode(ctx context.Context, seq uint64, topic bus.Topic, node string, payloads []*transferv1.ImportWrite) error {
	cfg := w.s.pressureConfig()
	timeout := w.batchTimeout(payloads)
	var firstBusy time.Time
	for attempt := 0; ; attempt++ {
		busy, err := w.publishToNodeOnce(ctx, seq, topic, node, payloads, timeout)
		if busy == notBusy {
			return err
		}
		wait := grpchelper.JitteredBackoff(cfg.retryBackoff, cfg.retryBackoffMax, attempt, grpchelper.DefaultJitterFactor)
		// The import's own rate limit is expected to hold a batch back for as long as the
		// imports sharing the node need: only memory pressure is a failure once it lasts.
		if busy == pressureBusy {
			now := time.Now()
			if firstBusy.IsZero() {
				firstBusy = now
			}
			elapsed := now.Sub(firstBusy)
			if elapsed >= cfg.pressureTimeout {
				w.s.l.Warn().Str("node", node).Uint64("batch_seq", seq).Dur("elapsed", elapsed).
					Msg("the data node stayed busy; failing the import stream")
				return importError(codes.ResourceExhausted, transfer.ImportReasonNodeFailed,
					map[string]string{transfer.ImportMetadataBatchSeq: strconv.FormatUint(seq, 10), transfer.ImportMetadataNode: node},
					fmt.Sprintf("batch_seq=%d node=%s: the data node stayed busy under transient load (memory pressure) for %s", seq, node, elapsed))
			}
			wait = min(wait, cfg.pressureTimeout-elapsed)
		}
		timer := time.NewTimer(wait)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return status.FromContextError(ctx.Err()).Err()
		}
	}
}

// nodeBusy is why a node refused a whole batch before writing any of it.
type nodeBusy int

const (
	notBusy nodeBusy = iota
	// pressureBusy is a node under memory pressure.
	pressureBusy
	// rateBusy is a node whose import rate limit cannot admit the batch in time.
	rateBusy
)

// busyOf tells why a busy answer refused a batch; a reply relayed from a remote node carries
// the node's message inside its own.
func busyOf(ce *common.Error) nodeBusy {
	if strings.Contains(ce.Error(), transfer.ImportRateBusy) {
		return rateBusy
	}
	return pressureBusy
}

// publishToNodeOnce sends the init frame and the payloads through a fresh batch publisher that
// waits timeout for the node's answer. busy tells whether, and why, the node refused the whole
// batch before writing any of it.
func (w *importWriter) publishToNodeOnce(ctx context.Context, seq uint64, topic bus.Topic, node string, payloads []*transferv1.ImportWrite,
	timeout time.Duration,
) (busy nodeBusy, err error) {
	publisher := w.s.tire2.NewBatchPublisher(timeout)
	frames := append([]*transferv1.ImportWrite{w.initFrame()}, payloads...)
	for i, p := range frames {
		if _, err = publisher.Publish(ctx, topic, bus.NewBatchMessageWithNode(bus.MessageID(time.Now().UnixNano()), node, p)); err != nil {
			_, _ = publisher.Close()
			if i == 0 && isServerBusy(err) {
				return pressureBusy, nil
			}
			return notBusy, nodeFailure(seq, node, err, false)
		}
	}
	cee, err := publisher.Close()
	for _, ce := range cee {
		if ce != nil && ce.Status() != modelv1.Status_STATUS_SUCCEED {
			if ce.Status() == modelv1.Status_STATUS_SERVER_BUSY {
				return busyOf(ce), nil
			}
			return notBusy, nodeFailure(seq, node, ce, true)
		}
	}
	if err != nil {
		return notBusy, nodeFailure(seq, node, err, true)
	}
	return notBusy, nil
}

// initFrame is the frame a stream to a data node opens with: the rate limit the node admits
// the rows at.
func (w *importWriter) initFrame() *transferv1.ImportWrite {
	return &transferv1.ImportWrite{Init: &transferv1.ImportWriteInit{RateLimit: w.init.GetRateLimit()}}
}

// batchTimeout is how long a node may take to answer a batch: importBatchTimeout to write it,
// plus, under a rate limit, the payloads' own share of the rate and the ratelimit.NodeSlack the
// node may queue them behind other imports. A node that cannot admit them within that answers
// busy at once instead of holding them.
func (w *importWriter) batchTimeout(payloads []*transferv1.ImportWrite) time.Duration {
	limit := w.init.GetRateLimit()
	var share time.Duration
	if bps := limit.GetBytesPerSecond(); bps > 0 {
		n := 0
		for _, p := range payloads {
			n += proto.Size(p)
		}
		share = time.Duration(float64(n) / float64(bps) * float64(time.Second))
	}
	if rps := limit.GetRowsPerSecond(); rps > 0 {
		share = max(share, time.Duration(float64(len(payloads))/float64(rps)*float64(time.Second)))
	}
	if share == 0 {
		return importBatchTimeout
	}
	return importBatchTimeout + share + ratelimit.NodeSlack
}

// writeProperties repairs every property revision on the nodes of its shard, through the same
// batch path as the other catalogs. A node repairs the rows of its batch one after another in
// batch order, the revisions of one entity grouped, so a later revision never overtakes an
// earlier one.
func (w *importWriter) writeProperties(ctx context.Context, seq uint64, rows []*transferv1.ImportRowsRequest_PropertyRow) error {
	var order []string
	byEntity := make(map[string][]*transferv1.ImportRowsRequest_PropertyRow)
	for i, row := range rows {
		prop := row.GetProperty()
		md := prop.GetMetadata()
		switch {
		case md == nil:
			return rowError(seq, i, "property metadata is required")
		case md.GetGroup() != w.init.GetGroup():
			return rowError(seq, i, "property group %s is not the stream's group %s", md.GetGroup(), w.init.GetGroup())
		case md.GetName() == "" || prop.GetId() == "":
			return rowError(seq, i, "property name and id are required")
		case row.GetDeleteTime() < 0:
			return rowError(seq, i, "delete time must not be negative")
		}
		entity := propertydb.GetEntity(prop)
		if _, ok := byEntity[entity]; !ok {
			order = append(order, entity)
		}
		byEntity[entity] = append(byEntity[entity], row)
	}
	if err := w.checkPropertySchemas(ctx, seq, rows); err != nil {
		return err
	}
	msgs := make([]shardMessage, 0, len(rows))
	for _, entity := range order {
		shard, err := partition.ShardID(convert.StringToBytes(entity), w.route.shardNum)
		if err != nil {
			return batchError(codes.FailedPrecondition, seq, "locate the shard of property %s: %v", entity, err)
		}
		for _, row := range byEntity[entity] {
			msgs = append(msgs, shardMessage{shard: uint32(shard), payload: &transferv1.ImportWrite{
				Row: &transferv1.ImportWrite_Property{Property: &propertyv1.InternalRepairRequest{
					ShardId:    uint64(shard),
					Id:         propertydb.GetPropertyID(row.GetProperty()),
					Property:   row.GetProperty(),
					DeleteTime: row.GetDeleteTime(),
				}},
			}})
		}
	}
	return w.publish(ctx, seq, data.TopicPropertyImport, msgs)
}

func (w *importWriter) checkPropertySchemas(ctx context.Context, seq uint64, rows []*transferv1.ImportRowsRequest_PropertyRow) error {
	if w.propertySchemas == nil {
		w.propertySchemas = make(map[string]*databasev1.Property)
	}
	for i, row := range rows {
		prop := row.GetProperty()
		md := prop.GetMetadata()
		ps, ok := w.propertySchemas[md.GetName()]
		if !ok {
			var err error
			ps, err = w.s.properties(ctx, &commonv1.Metadata{Group: md.GetGroup(), Name: md.GetName()})
			if isGroupNotFound(err) {
				return schemaNotFound(seq, "property %s/%s not found", md.GetGroup(), md.GetName())
			}
			if err != nil {
				return batchStatus(seq, schemaReadError(err, "get property %s/%s", md.GetGroup(), md.GetName()))
			}
			w.propertySchemas[md.GetName()] = ps
		}
		if row.GetDeleteTime() > 0 {
			continue
		}
		if err := checkPropertyTags(ps, prop); err != nil {
			return rowError(seq, i, "%v", err)
		}
	}
	return nil
}

func isGroupNotFound(err error) bool {
	return errors.Is(err, schema.ErrGRPCResourceNotFound) || status.Code(err) == codes.NotFound
}

// schemaReadError is the status of a failed schema read that is not a missing resource: the
// status of a done context, otherwise Unavailable, since the schema servers are read over the
// network and the client backs off and retries such a failure. Every schema read comes before
// the write, so the failure says nothing was written.
func schemaReadError(err error, format string, args ...any) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return status.FromContextError(err).Err()
	}
	return importError(codes.Unavailable, transfer.ImportReasonNotWritten, nil, fmt.Sprintf("%s: %v", fmt.Sprintf(format, args...), err))
}

// batchStatus re-states a status error as a batch failure of batch seq, keeping its code and
// details.
func batchStatus(seq uint64, err error) error {
	st := status.Convert(err).Proto()
	st.Message = fmt.Sprintf("batch_seq=%d: %s", seq, st.GetMessage())
	return status.ErrorProto(st)
}

func batchError(code codes.Code, seq uint64, format string, args ...any) error {
	return status.Errorf(code, "batch_seq=%d: %s", seq, fmt.Sprintf(format, args...))
}

// importError is a status carrying the ErrorInfo detail the import client classifies the
// failure by; the message stays the human-readable text.
func importError(code codes.Code, reason string, metadata map[string]string, msg string) error {
	st := status.New(code, msg)
	detailed, err := st.WithDetails(&errdetails.ErrorInfo{Domain: transfer.ImportErrorDomain, Reason: reason, Metadata: metadata})
	if err != nil {
		return st.Err()
	}
	return detailed.Err()
}

// rowError rejects row i of a batch as invalid.
func rowError(seq uint64, i int, format string, args ...any) error {
	return importError(codes.InvalidArgument, transfer.ImportReasonRowInvalid,
		map[string]string{transfer.ImportMetadataBatchSeq: strconv.FormatUint(seq, 10), transfer.ImportMetadataRow: strconv.Itoa(i)},
		fmt.Sprintf("batch_seq=%d: row %d: %s", seq, i, fmt.Sprintf(format, args...)))
}

// schemaNotFound rejects a batch naming a resource that does not exist.
func schemaNotFound(seq uint64, format string, args ...any) error {
	return importError(codes.NotFound, transfer.ImportReasonSchemaNotFound,
		map[string]string{transfer.ImportMetadataBatchSeq: strconv.FormatUint(seq, 10)},
		fmt.Sprintf("batch_seq=%d: %s", seq, fmt.Sprintf(format, args...)))
}

// withTooLargeDetail marks the receive error of a frame over the server's message size limit.
func withTooLargeDetail(err error) error {
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.ResourceExhausted || !strings.Contains(st.Message(), "larger than max") {
		return err
	}
	return importError(codes.ResourceExhausted, transfer.ImportReasonMessageTooLarge, nil, st.Message())
}

// nodeFailure is the stream status of a batch a node did not accept: disk full is
// ResourceExhausted; a node that could not be handed the batch (no client, failed health check,
// failed send) or whose connection broke while it answered is Unavailable; anything else the
// node reported is Internal. A cluster property repair failure is Internal even on a full disk,
// since the repair reply carries only an error text. Other nodes may already hold part of the batch,
// which the NODE_FAILED detail tells the client.
func nodeFailure(seq uint64, node string, err error, sent bool) error {
	code := codes.Internal
	var ce *common.Error
	matched := errors.As(err, &ce)
	switch {
	case matched && ce.Status() == modelv1.Status_STATUS_DISK_FULL:
		code = codes.ResourceExhausted
	case !sent:
		code = codes.Unavailable
	case matched && ce.Status() == modelv1.Status_STATUS_INTERNAL_ERROR && brokenConnection(ce.Error()):
		code = codes.Unavailable
	}
	return importError(code, transfer.ImportReasonNodeFailed,
		map[string]string{transfer.ImportMetadataBatchSeq: strconv.FormatUint(seq, 10), transfer.ImportMetadataNode: node},
		fmt.Sprintf("batch_seq=%d node=%s: %v", seq, node, err))
}

// brokenConnection tells whether a batch event's message is the text of a gRPC status that
// fails a node over: pub reports a broken response stream as an internal error carrying it.
func brokenConnection(msg string) bool {
	for _, c := range []codes.Code{codes.Unavailable, codes.DeadlineExceeded, codes.Canceled} {
		if strings.Contains(msg, "rpc error: code = "+c.String()+" ") {
			return true
		}
	}
	return false
}

func isServerBusy(err error) bool {
	var ce *common.Error
	return errors.As(err, &ce) && ce.Status() == modelv1.Status_STATUS_SERVER_BUSY
}
