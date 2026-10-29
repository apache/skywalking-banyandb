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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// Drop reasons. The set is closed: every path that loses an event names one of
// these, so a count is always attributable to a stage.
const (
	// reasonBufferFull counts an event dropped because the buffer was at its
	// byte or event bound: the consumer fell behind what the node logs.
	reasonBufferFull = "buffer_full"
	// reasonMemoryPressure counts an event dropped because the memory
	// protector reported high pressure.
	reasonMemoryPressure = "memory_pressure"
	reasonOversizeEvent  = "oversize_event"
	reasonEncodeFailed   = "encode_failed"
	reasonPublishFailed  = "publish_failed"
	reasonSchemaMissing  = "schema_unavailable"
	// reasonSchemaIncompatible is separate from schema_unavailable because the
	// two call for opposite responses: an unavailable schema is a wait, an
	// incompatible one is a stream that has to be dropped by hand and will
	// never resolve on its own.
	reasonSchemaIncompatible = "schema_incompatible"
	// reasonDestinationUnready counts a batch dropped because the local write
	// topic still had no subscriber after the bounded retries. The stream
	// service registers that topic and can start after the first flush.
	reasonDestinationUnready = "destination_unready"
	// reasonLocateFailed counts a batch a liaison dropped because no data node
	// could be located for it, after the bounded retries.
	reasonLocateFailed = "locate_failed"
	reasonShutdown     = "shutdown_deadline"
)

// maxBufferBytes bounds what the buffer holds, queued and in flight. It is a
// safety bound, not a tuning knob: the memory protector decides when memory
// is short, and the buffer only has to absorb a burst or a slow write.
const maxBufferBytes = 8 << 20

// maxEventBytes is the largest event kept. A larger one is dropped whole:
// truncating it would leave a JSON body that cannot be parsed.
const maxEventBytes = 64 << 10

// queueDepth bounds the buffer by count as well as by bytes. The byte bound is
// the one that matters, so the count sits above what it admits: at
// entryCost per typical event, 8MiB holds about 3800 events.
const queueDepth = 8192

// entryOverheadBytes is what a queued event holds beyond its own line and its
// fields buffer: the pooled request with its element and tag values. Measured
// at 2064 bytes held per entry for a 151-byte line with a 64-byte fields
// buffer, and 5044 for a 52-byte line in a request that kept a 3072-byte
// buffer, so the line alone under-counts the heap by more than ten times, and
// a bound charged on the line alone would let the buffer grow far past it.
// TestQueuedEntryFitsItsCharge and TestReusedEntryFitsItsCharge hold the
// constant to what an entry really costs.
const entryOverheadBytes = 1984

// entryCost is what one event charges against the byte bound: its line, the
// fixed structure of its request, and the capacity of the request's fields
// buffer. A request recycled from a larger event keeps that buffer, up to
// maxRetainedFieldsBytes, even when the event it now carries has no fields.
func entryCost(size int64, r *request) int64 {
	return size + entryOverheadBytes + int64(r.fields.Cap())
}

// NodeInfo identifies the process whose logs these are. It is late-bound:
// the node's identity is not known when the sink is constructed.
type NodeInfo struct {
	NodeID      string
	NodeType    string
	GRPCAddress string
	HTTPAddress string
}

// searchableTagCount is the number of tags in the searchable family, len(searchableTags).
const searchableTagCount = 8

// maxRetainedFieldsBytes bounds the fields buffer a pooled request keeps. A
// request that encoded a larger event gives its buffer up on release, so one
// large event cannot pin that much memory in the pool.
const maxRetainedFieldsBytes = 4 << 10

// request is a write request pooled with every message nested in it: the
// metadata, the element and its timestamp, the tag families, the tag values
// and their wrappers, the buffer the leftover fields are encoded into, and
// the request it is offered to the publisher as. release clears only what one
// event owns.
type request struct {
	*streamv1.WriteRequest
	data     *modelv1.TagValue
	binary   *modelv1.TagValue_BinaryData
	null     *modelv1.TagValue_Null
	str      [searchableTagCount]*modelv1.Str
	buffered queue.BufferedRequest
	fields   bytes.Buffer
	// seq is assigned at admission, so the identity stamped later at delivery
	// still reflects the order events arrived in.
	seq uint64
	// encodeFailed is set at delivery when the request could not be turned
	// into a write, so its outcome is counted as encode_failed. It is atomic
	// because Done can run while a stuck delivery still holds the request.
	encodeFailed atomic.Bool
}

func newRequest() *request {
	r := &request{
		binary: &modelv1.TagValue_BinaryData{},
		null:   &modelv1.TagValue_Null{},
	}
	// The data tag is null until an event with fields sets it, and release
	// puts it back to null.
	r.data = &modelv1.TagValue{Value: r.null}
	searchable := make([]*modelv1.TagValue, searchableTagCount)
	for i := range searchable {
		r.str[i] = &modelv1.Str{}
		searchable[i] = &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: r.str[i]}}
	}
	r.WriteRequest = &streamv1.WriteRequest{
		Metadata: &commonv1.Metadata{},
		Element: &streamv1.ElementValue{
			Timestamp: &timestamppb.Timestamp{},
			TagFamilies: []*modelv1.TagFamilyForWrite{
				{Tags: searchable},
				{Tags: []*modelv1.TagValue{r.data}},
			},
		},
	}
	return r
}

// Sink encodes admitted log events and offers them to a buffered publisher,
// which owns the buffer, the batching and the delivery.
//
// It is constructed before logger.Init and attached right after it, so the
// buffer exists for the lines a process emits while it is still starting. The
// publisher starts delivering once the services it publishes through run.
type Sink struct {
	pub      queue.BufferedPublisher
	dropped  map[string]*atomic.Uint64
	reported map[string]*atomic.Int64
	pressure atomic.Pointer[func() bool]
	node     atomic.Pointer[NodeInfo]
	detach   atomic.Pointer[func()]
	pool     sync.Pool
	// maxEventBytes is maxEventBytes, a field so a test can reach it.
	maxEventBytes int64
	seq           atomic.Uint64
	epoch         int64
	written       atomic.Uint64
}

// NewSink allocates the buffer. It performs no I/O, reaches no service and
// touches no logger, so it is safe to call while the command tree is still
// being built, which is where it is called.
func NewSink() *Sink {
	return newSink(nil)
}

// newSink is NewSink with the buffer's bounds adjusted, for a test.
func newSink(adjust func(*queue.BufferedConfig)) *Sink {
	s := &Sink{
		epoch:         time.Now().UnixNano(),
		dropped:       make(map[string]*atomic.Uint64),
		reported:      make(map[string]*atomic.Int64),
		maxEventBytes: maxEventBytes,
	}
	for _, r := range allReasons {
		s.dropped[r] = &atomic.Uint64{}
		s.reported[r] = &atomic.Int64{}
	}
	s.pool.New = func() any {
		r := newRequest()
		r.buffered = queue.BufferedRequest{
			Data:    r,
			Done:    func(o queue.Outcome) { s.settle(r, o) },
			Release: func() { s.release(r) },
		}
		return r
	}
	cfg := queue.BufferedConfig{
		Pressure:      s.underPressure,
		Name:          logger.NativeWriteModule,
		MaxBytes:      maxBufferBytes,
		MaxCount:      queueDepth,
		BatchCount:    flushSize,
		FlushInterval: flushInterval,
		MaxRetries:    destinationRetries,
		DrainTimeout:  drainTimeout,
	}
	if adjust != nil {
		adjust(&cfg)
	}
	// No logger yet: logging is not initialized when the sink is built. The
	// delivery marks each batch as a native write instead.
	s.pub = queue.NewBufferedPublisher(context.Background(), cfg)
	return s
}

// Attach makes the sink the destination of native logging. Every line
// admitted from here on is buffered for the consumer.
func (s *Sink) Attach() {
	detach := logger.AttachNativeSink(s)
	s.detach.Store(&detach)
}

// Detach stops admission. It returns once no later line can be admitted.
func (s *Sink) Detach() {
	if detach := s.detach.Swap(nil); detach != nil {
		(*detach)()
	}
}

// SetNode publishes the identity stamped onto every event from here on.
func (s *Sink) SetNode(n NodeInfo) {
	s.node.Store(&n)
}

// SetPressure installs the check that reports high memory pressure. It must
// not block: it runs on every goroutine that logs. Until it is set, and
// wherever no memory protector runs, the buffer's own bounds are the only
// limit.
func (s *Sink) SetPressure(f func() bool) {
	if f != nil {
		s.pressure.Store(&f)
	}
}

func (s *Sink) underPressure() bool {
	if f := s.pressure.Load(); f != nil {
		return (*f)()
	}
	return false
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

// QueuedBytes reports the bytes currently held in the buffer, including the
// batch being published.
func (s *Sink) QueuedBytes() int64 {
	charged, _ := s.pub.Bytes()
	return charged
}

// InFlightBytes reports the bytes of the batch being published.
func (s *Sink) InFlightBytes() int64 {
	_, inFlight := s.pub.Bytes()
	return inFlight
}

func (s *Sink) drop(reason string) {
	if c, ok := s.dropped[reason]; ok {
		c.Add(1)
	}
}

// Admit implements logger.NativeSink. It runs on the goroutine that emitted the
// line, so it never blocks and never waits for memory: the publisher refuses
// under pressure or at its bound, and the newest event is dropped and counted,
// leaving what is already queued intact so a burst keeps the head that
// explains it.
func (s *Sink) Admit(level zerolog.Level, module string, line []byte) {
	size := int64(len(line))
	if size > s.maxEventBytes {
		s.drop(reasonOversizeEvent)
		return
	}
	req := s.get()
	if err := s.fill(req, level, module, line); err != nil {
		s.release(req)
		s.drop(reasonEncodeFailed)
		return
	}
	req.seq = s.seq.Add(1)
	// Charged once built, so a fields buffer the encoding grew is counted.
	req.buffered.Bytes = entryCost(size, req)
	switch s.pub.Offer(&req.buffered) {
	case queue.Accepted:
		return
	case queue.RefusedPressure:
		s.drop(reasonMemoryPressure)
	case queue.RefusedClosed:
		s.drop(reasonShutdown)
	default:
		s.drop(reasonBufferFull)
	}
	s.release(req)
}

// settle counts how an accepted event ended. It is the request's Done.
func (s *Sink) settle(r *request, o queue.Outcome) {
	switch {
	case r.encodeFailed.Load():
		s.drop(reasonEncodeFailed)
	case o.Delivered:
		s.written.Add(1)
	case o.Reason == queue.OutcomeShutdownDeadline:
		s.drop(reasonShutdown)
	default:
		reason := lossReason(o.Err)
		s.drop(reason)
		s.reportLoss(reason, o.Err)
	}
}

// lossReason names the stage a failed delivery stopped at.
func lossReason(err error) string {
	switch {
	case errors.Is(err, errSchemaUnavailable):
		return reasonSchemaMissing
	case errors.Is(err, errSchemaIncompatible):
		return reasonSchemaIncompatible
	case errors.Is(err, bus.ErrTopicNotExist):
		return reasonDestinationUnready
	case errors.Is(err, errLocateFailed):
		return reasonLocateFailed
	default:
		return reasonPublishFailed
	}
}

// reportLoss writes a lost delivery to stderr, at most once a second for each
// reason: an outcome arrives per event, and a batch of them shares one cause.
// The sink never logs through the logging path it publishes on, so a failure
// goes straight to stderr rather than back into itself.
func (s *Sink) reportLoss(reason string, err error) {
	last, ok := s.reported[reason]
	if !ok {
		return
	}
	now := time.Now().UnixNano()
	prev := last.Load()
	if now-prev < int64(time.Second) || !last.CompareAndSwap(prev, now) {
		return
	}
	reportf("native log events dropped as %s: %v", reason, err)
}

// get takes a request from the pool.
func (s *Sink) get() *request {
	return s.pool.Get().(*request)
}

// release returns a request to the pool once nothing uses it any more: after
// its write finished, or when it is dropped.
func (s *Sink) release(r *request) {
	if r == nil {
		return
	}
	reset(r)
	s.pool.Put(r)
}

// reset clears what the event owns, so a pooled request cannot keep an event's
// strings or bytes alive, and keeps the structure for the next event.
func reset(r *request) {
	for _, str := range r.str {
		str.Value = ""
	}
	r.binary.BinaryData = nil
	r.data.Value = r.null
	r.fields.Reset()
	if r.fields.Cap() > maxRetainedFieldsBytes {
		r.fields = bytes.Buffer{}
	}
	r.Element.ElementId = ""
	r.Element.Timestamp.Seconds, r.Element.Timestamp.Nanos = 0, 0
	r.MessageId = 0
	r.seq = 0
	r.encodeFailed.Store(false)
}

// build turns one encoded event into a write request taken from the pool.
func (s *Sink) build(level zerolog.Level, module string, line []byte) (*request, error) {
	r := s.get()
	if err := s.fill(r, level, module, line); err != nil {
		s.release(r)
		return nil, err
	}
	return r, nil
}

// fill writes one encoded event into r. The known keys become tags and only
// what is left goes into the data family, so nothing is stored twice and the
// original line is not retained.
func (s *Sink) fill(r *request, level zerolog.Level, module string, line []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(line, &raw); err != nil {
		return err
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
	if len(raw) > 0 {
		// Encoded into the request's own buffer. The write path copies binary
		// tag values, so the buffer is free again once the write returns.
		if err := json.NewEncoder(&r.fields).Encode(raw); err != nil {
			return err
		}
		// Encode ends the value with a newline that Marshal does not add.
		r.binary.BinaryData = bytes.TrimSuffix(r.fields.Bytes(), []byte{'\n'})
		r.data.Value = r.binary
	}

	r.Metadata.Group, r.Metadata.Name = GroupName, StreamName
	r.MessageId = uint64(time.Now().UnixNano())
	r.Element.Timestamp.Seconds = eventTime.Unix()
	r.Element.Timestamp.Nanos = int32(eventTime.Nanosecond())
	// The identity tags, node_id, node_type, grpc_address, http_address and
	// log_id, are left blank here and filled by stamp at flush. The node's own
	// id is not known until its services start, while admission begins as soon
	// as logging is initialized, so stamping them now would store the whole
	// startup window under an empty node id -- half the series key, and an
	// input to the shard.
	r.str[2].Value = module
	r.str[3].Value = levelName(level)
	r.str[6].Value = message
	return nil
}

// stamp fills the identity a request could not carry at admission. It runs at
// delivery, by which point the node has started and published its own
// identity, so a line buffered during startup is stored under the same node as
// one emitted an hour later.
func (s *Sink) stamp(r *request) {
	node := s.node.Load()
	if node == nil {
		node = &NodeInfo{}
	}
	elementID := fmt.Sprintf("%s-%d-%d", node.NodeID, s.epoch, r.seq)
	tags := r.GetElement().GetTagFamilies()[0].GetTags()
	setStr(tags[0], node.NodeID)
	setStr(tags[1], node.NodeType)
	setStr(tags[4], node.GRPCAddress)
	setStr(tags[5], node.HTTPAddress)
	setStr(tags[7], elementID)
	r.Element.ElementId = elementID
}

func setStr(t *modelv1.TagValue, v string) {
	if str, ok := t.GetValue().(*modelv1.TagValue_Str); ok {
		str.Str.Value = v
	}
}

// levelName is the stored level. zerolog reports an empty string for an event
// logged without one, through Log(), and level is half the series key: an
// empty value would make the row unfindable by its own identity. Such an event
// is stored as "none", which is what it is.
func levelName(l zerolog.Level) string {
	if name := l.String(); name != "" {
		return name
	}
	return "none"
}

// moduleFieldName is the key zerolog stamps the module under. It has a tag of
// its own, so it never reaches the data family.
const moduleFieldName = "module"
