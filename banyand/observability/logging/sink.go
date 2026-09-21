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

// Package logging stores BanyanDB's own log events in BanyanDB, in the
// _monitoring_log stream group, alongside the measures native observability
// already writes into _monitoring.
package logging

import (
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// Drop reasons. The set is closed: every path that loses an event names one of
// these, so a count is always attributable to a stage.
const (
	reasonBufferFull    = "buffer_full"
	reasonMemoryReserve = "memory_pressure"
	reasonOversizeEvent = "oversize_event"
	reasonEncodeFailed  = "encode_failed"
	reasonPublishFailed = "publish_failed"
	reasonSchemaMissing = "schema_unavailable"
	// reasonSchemaIncompatible is separate from schema_unavailable because the
	// two call for opposite responses: an unavailable schema is a wait, an
	// incompatible one is a stream that has to be dropped by hand and will
	// never resolve on its own.
	reasonSchemaIncompatible = "schema_incompatible"
	reasonShutdown           = "shutdown_deadline"
)

// queueDepth bounds the ring by count as well as by bytes. The byte budget is
// the limit that matters; this one keeps a flood of very small events from
// growing the channel without bound before the budget notices.
const queueDepth = 8192

// NodeInfo identifies the process whose logs these are. It is late-bound:
// the node's identity is not known when the sink is constructed.
type NodeInfo struct {
	NodeID      string
	NodeType    string
	GRPCAddress string
	HTTPAddress string
}

// entry pairs a request with the bytes reserved for it, so the accounting
// settled at flush is exactly the accounting taken at admission.
type entry struct {
	req *streamv1.WriteRequest
	// seq is assigned at admission, so the identity stamped later at flush
	// still reflects the order events arrived in.
	seq  uint64
	size int64
}

// Sink buffers admitted log events and hands them to a consumer for writing.
//
// It is constructed before logger.Init, so the buffer exists for the lines a
// process emits while it is still starting, and activated once the services it
// publishes through are running.
type Sink struct {
	queue    chan entry
	dropped  map[string]*atomic.Uint64
	budget   atomic.Pointer[func() int64]
	node     atomic.Pointer[NodeInfo]
	pool     sync.Pool
	cfg      *logger.NativeLogging
	queued   atomic.Int64
	inFlight atomic.Int64
	seq      atomic.Uint64
	epoch    int64
	written  atomic.Uint64
}

// NewSink allocates the buffer. It performs no I/O and reaches no service, so
// it is safe to call while the command tree is still being built -- which is
// where it is called, before the flags are parsed. It keeps the configuration
// by reference for that reason: the values arrive later, and nothing reads
// them until Init has admitted the first event.
func NewSink(cfg *logger.NativeLogging) *Sink {
	s := &Sink{
		cfg:     cfg,
		queue:   make(chan entry, queueDepth),
		epoch:   time.Now().UnixNano(),
		dropped: make(map[string]*atomic.Uint64),
	}
	for _, r := range allReasons {
		s.dropped[r] = &atomic.Uint64{}
	}
	s.pool.New = func() any { return &streamv1.WriteRequest{} }
	initial := func() int64 { return cfg.MaxBytes }
	s.budget.Store(&initial)
	return s
}

// SetNode publishes the identity stamped onto every event from here on.
func (s *Sink) SetNode(n NodeInfo) {
	s.node.Store(&n)
}

// SetBudget replaces the byte budget with one that tracks memory availability.
// Until it is called the configured cap is the only bound, which is also the
// steady state wherever no memory protector is registered.
func (s *Sink) SetBudget(f func() int64) {
	if f != nil {
		// Published atomically: Admit reads this from every goroutine that
		// logs, and Serve installs it while those are already running.
		s.budget.Store(&f)
	}
}

// budgetBytes is the byte budget in force right now.
func (s *Sink) budgetBytes() int64 {
	if f := s.budget.Load(); f != nil {
		return (*f)()
	}
	return 0
}

// Dropped reports how many events were lost for one reason.
func (s *Sink) Dropped(reason string) uint64 {
	if c, ok := s.dropped[reason]; ok {
		return c.Load()
	}
	return 0
}

// Written reports how many events reached a destination.
func (s *Sink) Written() uint64 { return s.written.Load() }

// QueuedBytes reports the bytes currently held in the buffer.
func (s *Sink) QueuedBytes() int64 { return s.queued.Load() }

func (s *Sink) drop(reason string) {
	if c, ok := s.dropped[reason]; ok {
		c.Add(1)
	}
}

func (s *Sink) dropN(reason string, n int) {
	if c, ok := s.dropped[reason]; ok {
		c.Add(uint64(n))
	}
}

// Admit implements logger.NativeSink. It runs on the goroutine that emitted the
// line, so it never blocks and never waits for capacity: over budget it drops
// the newest event and counts it, leaving what is already queued intact so a
// burst keeps the head that explains it.
func (s *Sink) Admit(level zerolog.Level, module string, line []byte) {
	size := int64(len(line))
	if s.cfg.MaxEventBytes > 0 && size > s.cfg.MaxEventBytes {
		// Truncating a JSON body would leave an unparseable record, so an
		// oversized event is dropped whole.
		s.drop(reasonOversizeEvent)
		return
	}
	budget := s.budgetBytes()
	if budget <= 0 {
		// Zero is what the adaptive term reports when nothing is available. It
		// means no budget, not an absent limit.
		s.drop(reasonMemoryReserve)
		return
	}
	// Reserve before building, so two goroutines cannot both read the same
	// total and both enqueue. Every failure path below returns the reservation.
	if s.queued.Add(size)+s.inFlight.Load() > budget {
		s.queued.Add(-size)
		s.drop(reasonBufferFull)
		return
	}
	req, err := s.build(level, module, line)
	if err != nil {
		s.queued.Add(-size)
		s.drop(reasonEncodeFailed)
		return
	}
	select {
	case s.queue <- entry{req: req, size: size, seq: s.seq.Add(1)}:
	default:
		s.queued.Add(-size)
		s.release(req)
		s.drop(reasonBufferFull)
	}
}

func (s *Sink) release(req *streamv1.WriteRequest) {
	if req == nil {
		return
	}
	// Clear what the request retains before it goes back, so a pooled entry
	// cannot keep an event's bytes alive after it has been written.
	req.Element = nil
	req.Metadata = nil
	req.MessageId = 0
	s.pool.Put(req)
}

// build turns one encoded event into a write request. The known keys become
// tags and only what is left goes into the data family, so nothing is stored
// twice and the original line is not retained.
func (s *Sink) build(level zerolog.Level, module string, line []byte) (*streamv1.WriteRequest, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(line, &raw); err != nil {
		return nil, err
	}

	// Taken here rather than parsed back out of the line. zerolog's default
	// TimeFieldFormat is RFC3339, which carries whole seconds, so the encoded
	// string cannot order two events from the same second -- and with no index
	// rules the timestamp is the only orderable key this stream has.
	//
	// build runs on the goroutine that emitted the line, microseconds after
	// zerolog encoded it, so this is still the event's own time rather than the
	// flush time. Truncated because both the liaison validator and the data node
	// reject a sub-millisecond remainder.
	eventTime := time.Now().Truncate(time.Millisecond)

	var message string
	if m, ok := raw[zerolog.MessageFieldName]; ok {
		_ = json.Unmarshal(m, &message)
	}

	// Whatever has no tag of its own is kept together, so a call site that adds
	// a field does not need a schema change to keep it.
	for _, known := range []string{
		zerolog.LevelFieldName, zerolog.TimestampFieldName,
		zerolog.MessageFieldName, moduleFieldName,
	} {
		delete(raw, known)
	}
	var fields []byte
	if len(raw) > 0 {
		encoded, err := json.Marshal(raw)
		if err != nil {
			return nil, err
		}
		fields = encoded
	}

	req, _ := s.pool.Get().(*streamv1.WriteRequest)
	if req == nil {
		req = &streamv1.WriteRequest{}
	}
	req.Metadata = &commonv1.Metadata{Group: GroupName, Name: StreamName}
	req.MessageId = uint64(time.Now().UnixNano())
	// The identity tags are left blank here and filled by stamp at flush. The
	// node's own id is not known until its services start, while admission
	// begins as soon as logging is initialized, so stamping them now would
	// store the whole startup window under an empty node id -- half the series
	// key, and an input to the shard.
	req.Element = &streamv1.ElementValue{
		Timestamp: timestamppb.New(eventTime),
		TagFamilies: []*modelv1.TagFamilyForWrite{
			{Tags: []*modelv1.TagValue{
				strTag(""), // node_id, filled by stamp
				strTag(""), // node_type, filled by stamp
				strTag(module),
				strTag(level.String()),
				strTag(""), // grpc_address, filled by stamp
				strTag(""), // http_address, filled by stamp
				strTag(message),
				strTag(""), // log_id, filled by stamp
			}},
			{Tags: []*modelv1.TagValue{binaryTag(fields)}},
		},
	}
	return req, nil
}

// stamp fills the identity a request could not carry at admission. It runs on
// the consumer, by which point the node has started and published its own
// identity, so a line buffered during startup is stored under the same node as
// one emitted an hour later.
func (s *Sink) stamp(e entry) {
	node := s.node.Load()
	if node == nil {
		node = &NodeInfo{}
	}
	elementID := fmt.Sprintf("%s-%d-%d", node.NodeID, s.epoch, e.seq)
	tags := e.req.GetElement().GetTagFamilies()[0].GetTags()
	setStr(tags[0], node.NodeID)
	setStr(tags[1], node.NodeType)
	setStr(tags[4], node.GRPCAddress)
	setStr(tags[5], node.HTTPAddress)
	setStr(tags[7], elementID)
	e.req.Element.ElementId = elementID
}

func setStr(t *modelv1.TagValue, v string) {
	if str, ok := t.GetValue().(*modelv1.TagValue_Str); ok {
		str.Str.Value = v
	}
}

func strTag(v string) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: v}}}
}

func binaryTag(v []byte) *modelv1.TagValue {
	if len(v) == 0 {
		return &modelv1.TagValue{Value: &modelv1.TagValue_Null{}}
	}
	return &modelv1.TagValue{Value: &modelv1.TagValue_BinaryData{BinaryData: v}}
}

// moduleFieldName is the key zerolog stamps the module under. It has a tag of
// its own, so it never reaches the data family.
const moduleFieldName = "module"
