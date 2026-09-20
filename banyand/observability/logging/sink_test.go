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
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

const sampleLine = `{"level":"warn","module":"MEASURE","group":"sw_metric",` +
	`"time":"2026-09-11T10:23:45.123456789Z","message":"flush took longer than expected"}`

func testSink(t *testing.T) *Sink {
	t.Helper()
	s := NewSink(&logger.NativeLogging{
		Enabled:       true,
		MaxBytes:      1 << 20,
		MaxEventBytes: 64 << 10,
		FlushSize:     100,
		ShardNum:      2,
	})
	s.SetNode(NodeInfo{NodeID: "data-hot-0", NodeType: "data", GRPCAddress: "10.1.2.3:17912"})
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

	searchable := req.Element.TagFamilies[0].Tags
	if len(searchable) != len(searchableTags) {
		t.Fatalf("searchable family has %d tags, schema declares %d",
			len(searchable), len(searchableTags))
	}
	for i, want := range map[int]string{
		0: "data-hot-0", 1: "data", 2: "MEASURE", 3: "warn",
		4: "10.1.2.3:17912", 6: "flush took longer than expected",
	} {
		if got := tagStr(t, searchable[i]); got != want {
			t.Fatalf("tag %s = %q, want %q", searchableTags[i], got, want)
		}
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

// TestBuildTruncatesToMilliseconds pins the constraint that would otherwise
// reject every write: both the liaison validator and the data node refuse a
// timestamp carrying a sub-millisecond remainder, and zerolog stamps nanoseconds.
func TestBuildTruncatesToMilliseconds(t *testing.T) {
	s := testSink(t)
	req, err := s.build(zerolog.WarnLevel, "MEASURE", []byte(sampleLine))
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	ts := req.Element.Timestamp.AsTime()
	if ts.Nanosecond()%int(time.Millisecond) != 0 {
		t.Fatalf("timestamp %v carries a sub-millisecond remainder", ts)
	}
	if want := int64(1789122225123); ts.UnixMilli() != want {
		t.Fatalf("timestamp = %d ms, want %d — the event's own time, not the flush time",
			ts.UnixMilli(), want)
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

	if got := s.Dropped(reasonBufferFull); got != 1 {
		t.Fatalf("buffer_full drops = %d, want 1", got)
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
	s.queue = make(chan *streamv1.WriteRequest)

	done := make(chan struct{})
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

// TestSchemaAndWritePathAgreeOnTagOrder is the check that catches a silent
// mismatch: a tag value written into the wrong position is stored without any
// error, because a oneof that does not match its declared type lands as empty.
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
	for i, tag := range declared {
		if tag.Name != searchableTags[i] {
			t.Fatalf("searchable tag %d is %q in the schema and %q in the write path",
				i, tag.Name, searchableTags[i])
		}
	}
	for _, name := range entityTags {
		if !strings.Contains(strings.Join(searchableTags, ","), name) {
			t.Fatalf("entity tag %q is not among the searchable tags", name)
		}
	}
}
