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

package logging

import (
	"encoding/json"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// sampleLine is a real zerolog line: its time is RFC3339 with whole seconds,
// which is what the default TimeFieldFormat produces. An earlier version of
// this constant carried nanoseconds, which zerolog never emits, and a test
// built on it reported that truncation worked on an input that cannot occur.
const sampleLine = `{"level":"warn","module":"MEASURE","group":"sw_metric",` +
	`"time":"2026-09-11T10:23:45Z","message":"flush took longer than expected"}`

func testSink(t *testing.T) *Sink {
	t.Helper()
	s := NewSink(&logger.NativeLogging{
		Enabled:       true,
		MaxBytes:      1 << 20,
		MaxEventBytes: 64 << 10,
		FlushSize:     100,
		ShardNum:      2,
	})
	// Every field is distinct and non-empty, so a test can assert on all eight
	// positions. An identity with blanks in it would let a tag written into the
	// wrong position pass unnoticed, because both sides would read empty.
	s.SetNode(NodeInfo{
		NodeID: "data-hot-0", NodeType: "data",
		GRPCAddress: "10.1.2.3:17912", HTTPAddress: "10.1.2.3:17913",
	})
	return s
}

func tagStr(t *testing.T, v *modelv1.TagValue) string {
	t.Helper()
	return v.GetStr().GetValue()
}

// TestBuildSplitsKnownKeysFromTheRest is the schema contract: a key with a tag
// of its own goes to that tag, and only what is left lands in the data family.
// Nothing is stored twice and the original line is not retained.
func TestBuildSplitsKnownKeysFromTheRest(t *testing.T) {
	s := testSink(t)
	req, err := s.build(zerolog.WarnLevel, "MEASURE", []byte(sampleLine))
	if err != nil {
		t.Fatalf("build: %v", err)
	}

	// Identity arrives from the consumer, so stamp before asserting on it.
	s.stamp(entry{req: req, seq: 1})
	searchable := req.Element.TagFamilies[0].Tags
	if len(searchable) != len(searchableTags) {
		t.Fatalf("searchable family has %d tags, schema declares %d",
			len(searchable), len(searchableTags))
	}
	// Every position, not a subset. A tag left out here is a position the write
	// path could fill with anything -- including a value that belongs to its
	// neighbor -- without a single test noticing.
	want := []string{
		"data-hot-0", "data", "MEASURE", "warn",
		"10.1.2.3:17912", "10.1.2.3:17913",
		"flush took longer than expected",
		req.Element.ElementId,
	}
	if len(want) != len(searchableTags) {
		t.Fatalf("this test checks %d positions, the schema has %d", len(want), len(searchableTags))
	}
	for i, w := range want {
		if got := tagStr(t, searchable[i]); got != w {
			t.Fatalf("tag %s = %q, want %q", searchableTags[i], got, w)
		}
	}
	if req.Element.ElementId == "" {
		t.Fatal("log_id and element id are both empty, so the position check above proved nothing")
	}

	fields := req.Element.TagFamilies[1].Tags[0].GetBinaryData()
	var leftover map[string]any
	if err = json.Unmarshal(fields, &leftover); err != nil {
		t.Fatalf("fields is not valid JSON: %v", err)
	}
	if len(leftover) != 1 || leftover["group"] != "sw_metric" {
		t.Fatalf("fields = %v, want only the keys with no tag of their own", leftover)
	}
	for _, stored := range []string{"level", "module", "message", "time"} {
		if _, dup := leftover[stored]; dup {
			t.Fatalf("fields repeats %q, which already has a tag", stored)
		}
	}
}

// TestBuildOmitsAnEmptyFieldsTag covers the common case: a plain message with
// no extra keys should not carry an empty payload.
func TestBuildOmitsAnEmptyFieldsTag(t *testing.T) {
	s := testSink(t)
	line := `{"level":"info","module":"MEASURE","time":"2026-09-11T10:23:45.123Z","message":"started"}`
	req, err := s.build(zerolog.InfoLevel, "MEASURE", []byte(line))
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if data := req.Element.TagFamilies[1].Tags[0].GetBinaryData(); len(data) != 0 {
		t.Fatalf("fields = %q, want empty when the line has no extra keys", data)
	}
}

// TestOversizeEventIsDroppedWhole pins that a large event is refused rather
// than truncated: half a JSON body is unparseable, so it is worth nothing.
func TestOversizeEventIsDroppedWhole(t *testing.T) {
	s := testSink(t)
	s.cfg.MaxEventBytes = 64

	s.Admit(zerolog.ErrorLevel, "MEASURE", []byte(sampleLine))

	if got := s.Dropped(reasonOversizeEvent); got != 1 {
		t.Fatalf("oversize drops = %d, want 1", got)
	}
	if len(s.queue) != 0 {
		t.Fatal("an oversized event reached the buffer")
	}
}

// TestOverBudgetDropsTheNewest pins the overflow policy: what is already queued
// survives, so a burst keeps the head that explains it.
func TestOverBudgetDropsTheNewest(t *testing.T) {
	s := testSink(t)
	s.Admit(zerolog.ErrorLevel, "MEASURE", []byte(sampleLine))
	queued := s.QueuedBytes()
	if queued == 0 {
		t.Fatal("the first event was not accounted")
	}

	// A budget below what is already held refuses everything new.
	s.SetBudget(func() int64 { return 1 })
	s.Admit(zerolog.ErrorLevel, "MEASURE", []byte(sampleLine))

	if got := s.Dropped(reasonMemoryPressure); got != 1 {
		t.Fatalf("memory_pressure drops = %d, want 1", got)
	}
	if s.QueuedBytes() != queued {
		t.Fatalf("queued bytes moved from %d to %d; a queued event was evicted",
			queued, s.QueuedBytes())
	}
	if len(s.queue) != 1 {
		t.Fatalf("buffer holds %d events, want the 1 admitted before the budget fell", len(s.queue))
	}
}

// TestAdmitNeverBlocks is the property the whole design rests on: the sink sits
// on every code path, including the write path it publishes through, so a full
// buffer must refuse rather than wait.
func TestAdmitNeverBlocks(t *testing.T) {
	s := testSink(t)
	// A buffer with no room at all, and no consumer to make room.
	s.queue = make(chan entry)

	done := make(chan struct{})
	//panicdiag:allow-rawgo test goroutine asserting that Admit returns rather than blocking
	go func() {
		s.Admit(zerolog.ErrorLevel, "MEASURE", []byte(sampleLine))
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Admit blocked on a full buffer")
	}
	if got := s.Dropped(reasonBufferFull); got != 1 {
		t.Fatalf("buffer_full drops = %d, want 1", got)
	}
}

// TestMalformedLineIsCounted covers an event that is not the JSON the sink
// expects: it is dropped with a reason rather than crashing the caller.
func TestMalformedLineIsCounted(t *testing.T) {
	s := testSink(t)
	s.Admit(zerolog.ErrorLevel, "MEASURE", []byte("not json at all"))
	if got := s.Dropped(reasonEncodeFailed); got != 1 {
		t.Fatalf("encode_failed drops = %d, want 1", got)
	}
}

// TestEntityOfFollowsTheSchemaOrder pins that the entity values handed to the
// write path match entity.tag_names, which is positional.
func TestEntityOfFollowsTheSchemaOrder(t *testing.T) {
	s := testSink(t)
	req, err := s.build(zerolog.WarnLevel, "MEASURE", []byte(sampleLine))
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	// Identity is stamped by the consumer, so the entity is only complete once
	// the request has been through it -- exactly as the write path sees it.
	s.stamp(entry{req: req, seq: 1})
	entity := entityOf(req)
	if len(entity) != len(entityTags) {
		t.Fatalf("entity has %d values, schema declares %d", len(entity), len(entityTags))
	}
	if got := entity[0].GetStr().GetValue(); got != "data-hot-0" {
		t.Fatalf("entity[0] = %q, want the node id", got)
	}
	if got := entity[1].GetStr().GetValue(); got != "warn" {
		t.Fatalf("entity[1] = %q, want the level", got)
	}
}

// TestEachTagHoldsTheValueItsNamePromises is the check that catches a silent
// mismatch: a tag value written into the wrong position is stored without any
// error, because a oneof that does not match its declared type lands as empty.
//
// The expected values are keyed by tag name, not by position, so the assertion
// fails when either side of the pairing moves: the declared order in
// searchableTags, or the order build and stamp fill. Comparing the two lists
// with each other would pass either way, because both are built from
// searchableTags.
func TestEachTagHoldsTheValueItsNamePromises(t *testing.T) {
	s := testSink(t)
	line := []byte(`{"level":"warn","module":"MEASURE","time":"2020-01-01T00:00:00Z","message":"a message"}`)
	req, err := s.build(zerolog.WarnLevel, "MEASURE", line)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	s.stamp(entry{req: req, seq: 7})

	want := map[string]string{
		"node_id":      "data-hot-0",
		"node_type":    "data",
		"module":       "MEASURE",
		"level":        "warn",
		"grpc_address": "10.1.2.3:17912",
		"http_address": "10.1.2.3:17913",
		"message":      "a message",
		"log_id":       req.GetElement().GetElementId(),
	}
	declaredNames := streamSpec().GetTagFamilies()[0].GetTags()
	stored := req.GetElement().GetTagFamilies()[0].GetTags()
	if len(declaredNames) != len(stored) {
		t.Fatalf("the schema declares %d searchable tags, the write path fills %d", len(declaredNames), len(stored))
	}
	for i, tag := range declaredNames {
		expected, ok := want[tag.GetName()]
		if !ok {
			t.Fatalf("tag %q has no expected value; add one when a tag is added", tag.GetName())
		}
		if got := stored[i].GetStr().GetValue(); got != expected {
			t.Errorf("tag %d is %q, which holds %q, want %q", i, tag.GetName(), got, expected)
		}
	}
	for name := range want {
		var declared bool
		for _, tag := range declaredNames {
			declared = declared || tag.GetName() == name
		}
		if !declared {
			t.Errorf("tag %q is expected by this test but is not declared", name)
		}
	}
}

// TestSchemaAndWritePathAgreeOnTagOrder checks the two lists the schema and the
// write path share, which TestEachTagHoldsTheValueItsNamePromises cannot: the
// count, and that every entity tag is searchable.
func TestSchemaAndWritePathAgreeOnTagOrder(t *testing.T) {
	spec := streamSpec()
	if len(spec.TagFamilies) != 2 {
		t.Fatalf("stream declares %d tag families, want 2", len(spec.TagFamilies))
	}
	declared := spec.TagFamilies[0].Tags
	if len(declared) != len(searchableTags) {
		t.Fatalf("schema declares %d searchable tags, the write path fills %d",
			len(declared), len(searchableTags))
	}
	for _, name := range entityTags {
		// A whole-element match, not a substring one: joining and searching
		// would accept "node" as evidence that "node_id" is declared.
		if !slices.Contains(searchableTags, name) {
			t.Fatalf("entity tag %q is not among the searchable tags", name)
		}
	}
}

// TestTimestampComesFromAdmissionNotTheEncodedLine is the falsifying assertion
// for the stored-order property. zerolog's default TimeFieldFormat is RFC3339,
// which carries whole seconds, so a timestamp parsed back out of the line
// cannot order two events from the same second -- and with no index rules the
// timestamp is the only orderable key this stream has.
//
// The assertion is that the stored time is NOT the one written in the line.
func TestTimestampComesFromAdmissionNotTheEncodedLine(t *testing.T) {
	s := testSink(t)
	// A whole-second time, exactly as zerolog emits it, and far in the past so
	// that reusing it would be unmistakable.
	line := []byte(`{"level":"info","module":"MEASURE","time":"2020-01-01T00:00:00Z","message":"a"}`)

	req, err := s.build(zerolog.InfoLevel, "MEASURE", line)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	ts := req.Element.Timestamp.AsTime()
	if ts.Year() == 2020 {
		t.Fatalf("stored timestamp %v was parsed out of the line; it carries only whole seconds", ts)
	}
	if ts.Nanosecond()%int(time.Millisecond) != 0 {
		t.Fatalf("timestamp %v carries a sub-millisecond remainder, which both write paths reject", ts)
	}

	// Two events more than a millisecond apart must be distinguishable, which
	// is what a second-resolution source could never provide.
	time.Sleep(2 * time.Millisecond)
	later, err := s.build(zerolog.InfoLevel, "MEASURE", line)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if later.Element.Timestamp.AsTime().UnixMilli() == ts.UnixMilli() {
		t.Fatalf("two events 2ms apart share timestamp %v; stored logs cannot be ordered", ts)
	}
}

// TestIdentityIsStampedAtFlushNotAdmission is the falsifying assertion for the
// startup window: an event admitted before the node knows its own identity must
// NOT be stored with an empty node_id, because node_id is half the series key
// and an input to the shard.
func TestIdentityIsStampedAtFlushNotAdmission(t *testing.T) {
	s := NewSink(&logger.NativeLogging{
		Enabled: true, MaxBytes: 1 << 20, MaxEventBytes: 64 << 10, FlushSize: 100, ShardNum: 2,
	})
	// No SetNode yet: this is the window between logger initialisation and the
	// node's services starting.
	s.Admit(zerolog.InfoLevel, "MEASURE", []byte(sampleLine))

	e := <-s.queue
	if got := e.req.Element.TagFamilies[0].Tags[0].GetStr().GetValue(); got != "" {
		t.Fatalf("node_id was %q at admission; it cannot be known yet", got)
	}

	// The node starts and publishes its identity; the consumer stamps it.
	s.SetNode(NodeInfo{
		NodeID: "data-hot-0", NodeType: "data",
		GRPCAddress: "10.1.2.3:17912", HTTPAddress: "10.1.2.3:17913",
	})
	s.stamp(e)

	tags := e.req.Element.TagFamilies[0].Tags
	for i, want := range map[int]string{
		0: "data-hot-0", 1: "data", 4: "10.1.2.3:17912", 5: "10.1.2.3:17913",
	} {
		if got := tags[i].GetStr().GetValue(); got != want {
			t.Fatalf("after stamping, tag %s = %q, want %q", searchableTags[i], got, want)
		}
	}
	if e.req.Element.ElementId == "" || !strings.HasPrefix(e.req.Element.ElementId, "data-hot-0-") {
		t.Fatalf("element id %q does not carry the node identity", e.req.Element.ElementId)
	}
	if got := tags[7].GetStr().GetValue(); got != e.req.Element.ElementId {
		t.Fatalf("log_id %q does not mirror the element id %q", got, e.req.Element.ElementId)
	}
}

// TestQueuedEntryFitsItsCharge holds entryOverheadBytes to what an entry
// really costs. Charging the line alone under-counted the heap by more than
// ten times, so a 32MiB budget could hold far more than 32MiB.
func TestQueuedEntryFitsItsCharge(t *testing.T) {
	s := testSink(t)
	line := []byte(`{"level":"info","module":"MEASURE","time":"2026-09-23T10:00:00Z","message":"a typical log line with a few fields","shard":3,"path":"/tmp/measure/data"}`)
	const n = 20000
	held := make([]*streamv1.WriteRequest, 0, n)
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for i := 0; i < n; i++ {
		req, err := s.build(zerolog.InfoLevel, "MEASURE", line)
		if err != nil {
			t.Fatalf("build: %v", err)
		}
		s.stamp(entry{req: req, seq: uint64(i)})
		held = append(held, req)
	}
	runtime.GC()
	runtime.ReadMemStats(&after)
	perEntry := int64(after.HeapAlloc-before.HeapAlloc) / n
	runtime.KeepAlive(held)
	if charged := entryCost(int64(len(line))); perEntry > charged {
		t.Errorf("an entry holds %d bytes and charges %d; raise entryOverheadBytes", perEntry, charged)
	}
}

// TestBudgetAndRingReportDifferentReasons keeps the two bounds distinguishable.
// A budget refusal is memory_pressure and a full ring is buffer_full; reporting
// both as buffer_full hid which knob was binding.
func TestBudgetAndRingReportDifferentReasons(t *testing.T) {
	s := testSink(t)
	line := []byte(`{"level":"info","module":"MEASURE","message":"x"}`)
	// A budget below one entry's charge refuses every event.
	tiny := int64(1)
	s.SetBudget(func() int64 { return tiny })
	s.Admit(zerolog.InfoLevel, "MEASURE", line)
	if got := s.dropped[reasonMemoryPressure].Load(); got != 1 {
		t.Errorf("memory_pressure counted %d drops, want 1", got)
	}
	if got := s.dropped[reasonBufferFull].Load(); got != 0 {
		t.Errorf("buffer_full counted %d drops for a budget refusal, want 0", got)
	}
}

// TestLevelLessEventKeepsItsEntity is the falsifying assertion for the series
// key. zerolog reports an empty level for an event logged through Log(), and
// level is half the entity: an empty value makes the row unfindable by its own
// identity, and every such event shares one series.
func TestLevelLessEventKeepsItsEntity(t *testing.T) {
	s := testSink(t)
	req, err := s.build(zerolog.NoLevel, "MEASURE", []byte(`{"module":"MEASURE","message":"no level"}`))
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	s.stamp(entry{req: req, seq: 1})
	tags := req.GetElement().GetTagFamilies()[0].GetTags()
	if got := tags[3].GetStr().GetValue(); got == "" {
		t.Error("a level-less event is stored with an empty level, which is half the entity")
	} else if got != "none" {
		t.Errorf("a level-less event is stored as %q, want \"none\"", got)
	}
}
