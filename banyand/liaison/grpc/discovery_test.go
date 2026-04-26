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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// entityRepo tombstone GC helpers follow.

// minimalStream returns a Stream proto suitable for entityRepo event tests.
// Group and name are fixed because every caller uses the same values; introducing
// variation would only add noise without changing what's being asserted.
func minimalStream(modRevision int64) *databasev1.Stream {
	return &databasev1.Stream{
		Metadata: &commonv1.Metadata{Group: "g", Name: "s", ModRevision: modRevision},
		Entity:   &databasev1.Entity{TagNames: []string{"svc"}},
		TagFamilies: []*databasev1.TagFamilySpec{{
			Name: "default",
			Tags: []*databasev1.TagSpec{{Name: "svc", Type: databasev1.TagType_TAG_TYPE_STRING}},
		}},
	}
}

// minimalMeasure returns a Measure proto suitable for entityRepo event tests.
func minimalMeasure(group, name string, modRevision int64) *databasev1.Measure {
	return &databasev1.Measure{
		Metadata: &commonv1.Metadata{Group: group, Name: name, ModRevision: modRevision},
		Entity:   &databasev1.Entity{TagNames: []string{"svc"}},
		TagFamilies: []*databasev1.TagFamilySpec{{
			Name: "default",
			Tags: []*databasev1.TagSpec{{Name: "svc", Type: databasev1.TagType_TAG_TYPE_STRING}},
		}},
	}
}

// minimalTrace returns a Trace proto suitable for entityRepo event tests.
// tagNames is the ordered list of tag names; traceIDTagName is the one to index.
func minimalTrace(group, name string, modRevision int64, traceIDTagName string, tagNames ...string) *databasev1.Trace {
	tags := make([]*databasev1.TraceTagSpec, len(tagNames))
	for i, n := range tagNames {
		tags[i] = &databasev1.TraceTagSpec{Name: n}
	}
	return &databasev1.Trace{
		Metadata:       &commonv1.Metadata{Group: group, Name: name, ModRevision: modRevision},
		TraceIdTagName: traceIDTagName,
		Tags:           tags,
	}
}

func streamAddEvent(s *databasev1.Stream) schema.Metadata {
	return schema.Metadata{TypeMeta: schema.TypeMeta{Kind: schema.KindStream}, Spec: s}
}

func measureAddEvent(m *databasev1.Measure) schema.Metadata {
	return schema.Metadata{TypeMeta: schema.TypeMeta{Kind: schema.KindMeasure}, Spec: m}
}

func traceAddEvent(tr *databasev1.Trace) schema.Metadata {
	return schema.Metadata{TypeMeta: schema.TypeMeta{Kind: schema.KindTrace}, Spec: tr}
}

// entityRepo tombstone GC unit tests follow.

// TestEntityRepo_OnAddOrUpdate_Stream verifies that OnAddOrUpdate for a stream
// populates entitiesMap with the correct ModRevision and streamMap with the spec.
func TestEntityRepo_OnAddOrUpdate_Stream(t *testing.T) {
	er := newEmptyEntityRepo()
	s := minimalStream(42)
	er.OnAddOrUpdate(streamAddEvent(s))

	id := identity{group: "g", name: "s"}
	loc, ok := er.getLocator(id)
	require.True(t, ok, "entitiesMap must contain the stream locator after OnAddOrUpdate")
	assert.Equal(t, int64(42), loc.ModRevision)

	gotStream, streamOK := er.getStream(id)
	require.True(t, streamOK, "streamMap must contain the stream after OnAddOrUpdate")
	assert.Equal(t, "s", gotStream.GetMetadata().GetName())
}

// TestEntityRepo_OnAddOrUpdate_Measure verifies that OnAddOrUpdate for a measure
// populates entitiesMap with the correct ModRevision and measureMap with the spec.
func TestEntityRepo_OnAddOrUpdate_Measure(t *testing.T) {
	er := newEmptyEntityRepo()
	m := minimalMeasure("g", "m", 77)
	er.OnAddOrUpdate(measureAddEvent(m))

	id := identity{group: "g", name: "m"}
	loc, ok := er.getLocator(id)
	require.True(t, ok, "entitiesMap must contain the measure locator after OnAddOrUpdate")
	assert.Equal(t, int64(77), loc.ModRevision)

	gotMeasure, measureOK := er.getMeasure(id)
	require.True(t, measureOK, "measureMap must contain the measure after OnAddOrUpdate")
	assert.Equal(t, "m", gotMeasure.GetMetadata().GetName())
}

// TestEntityRepo_OnAddOrUpdate_Trace verifies that OnAddOrUpdate for a trace
// populates traceMap and pre-computes traceIDIndex at the correct position.
func TestEntityRepo_OnAddOrUpdate_Trace(t *testing.T) {
	er := newEmptyEntityRepo()
	// Tags: ["span_id", "trace_id", "svc"] — trace_id is at index 1.
	tr := minimalTrace("g", "t", 55, "trace_id", "span_id", "trace_id", "svc")
	er.OnAddOrUpdate(traceAddEvent(tr))

	id := identity{group: "g", name: "t"}
	gotTrace, traceOK := er.getTrace(id)
	require.True(t, traceOK, "traceMap must contain the trace after OnAddOrUpdate")
	assert.Equal(t, "trace_id", gotTrace.GetTraceIdTagName())

	idx, idxOK := er.getTraceIDIndex(id)
	require.True(t, idxOK)
	assert.Equal(t, 1, idx, "traceIDIndex must point to the position of trace_id_tag_name in tags")
}

// TestEntityRepo_OnAddOrUpdate_Trace_MissingTraceID verifies that when
// trace_id_tag_name does not appear in the tags list the stored index is -1.
func TestEntityRepo_OnAddOrUpdate_Trace_MissingTraceID(t *testing.T) {
	er := newEmptyEntityRepo()
	tr := minimalTrace("g", "t", 10, "absent_tag", "span_id", "svc")
	er.OnAddOrUpdate(traceAddEvent(tr))

	id := identity{group: "g", name: "t"}
	idx, _ := er.getTraceIDIndex(id)
	assert.Equal(t, -1, idx, "traceIDIndex must be -1 when trace_id_tag_name is not found in tags")
}

// TestEntityRepo_OnDelete_Stream verifies that OnDelete for a stream removes
// the locator from entitiesMap and the entry from streamMap (tombstone GC).
func TestEntityRepo_OnDelete_Stream(t *testing.T) {
	er := newEmptyEntityRepo()
	s := minimalStream(42)
	er.OnAddOrUpdate(streamAddEvent(s))

	er.OnDelete(streamAddEvent(s))

	id := identity{group: "g", name: "s"}
	_, locOK := er.getLocator(id)
	assert.False(t, locOK, "entitiesMap must be cleared for stream after OnDelete (tombstone GC)")

	_, streamOK := er.getStream(id)
	assert.False(t, streamOK, "streamMap must be cleared for stream after OnDelete (tombstone GC)")
}

// TestEntityRepo_OnDelete_Measure verifies that OnDelete for a measure removes
// the locator from entitiesMap and the entry from measureMap (tombstone GC).
func TestEntityRepo_OnDelete_Measure(t *testing.T) {
	er := newEmptyEntityRepo()
	m := minimalMeasure("g", "m", 77)
	er.OnAddOrUpdate(measureAddEvent(m))

	er.OnDelete(measureAddEvent(m))

	id := identity{group: "g", name: "m"}
	_, locOK := er.getLocator(id)
	assert.False(t, locOK, "entitiesMap must be cleared for measure after OnDelete (tombstone GC)")

	_, measureOK := er.getMeasure(id)
	assert.False(t, measureOK, "measureMap must be cleared for measure after OnDelete (tombstone GC)")
}

// TestEntityRepo_OnDelete_Trace verifies that OnDelete for a trace removes
// the entry from traceMap and traceIDIndexMap (tombstone GC).
func TestEntityRepo_OnDelete_Trace(t *testing.T) {
	er := newEmptyEntityRepo()
	tr := minimalTrace("g", "t", 55, "trace_id", "trace_id")
	er.OnAddOrUpdate(traceAddEvent(tr))

	er.OnDelete(traceAddEvent(tr))

	id := identity{group: "g", name: "t"}
	_, traceOK := er.getTrace(id)
	assert.False(t, traceOK, "traceMap must be cleared for trace after OnDelete (tombstone GC)")

	idx, _ := er.getTraceIDIndex(id)
	assert.Equal(t, -1, idx, "traceIDIndexMap must be cleared for trace after OnDelete (tombstone GC)")
}

// TestEntityRepo_AddDeleteAdd_Sequence verifies the full tombstone GC lifecycle:
// add → present; delete → absent; add again with new revision → present with new revision.
func TestEntityRepo_AddDeleteAdd_Sequence(t *testing.T) {
	er := newEmptyEntityRepo()
	s := minimalStream(10)
	id := identity{group: "g", name: "s"}

	er.OnAddOrUpdate(streamAddEvent(s))
	_, ok := er.getLocator(id)
	require.True(t, ok, "entity must be present after first OnAddOrUpdate")

	er.OnDelete(streamAddEvent(s))
	_, ok = er.getLocator(id)
	require.False(t, ok, "entity must be absent after OnDelete (tombstone)")

	s2 := minimalStream(20)
	er.OnAddOrUpdate(streamAddEvent(s2))
	loc, ok := er.getLocator(id)
	require.True(t, ok, "entity must be present after second OnAddOrUpdate")
	assert.Equal(t, int64(20), loc.ModRevision, "re-added entity must carry the new ModRevision")
}

// TestEntityRepo_OnDelete_UnknownKind_IsNoop verifies that OnDelete for an
// unrecognized kind does not panic and leaves existing entries intact.
func TestEntityRepo_OnDelete_UnknownKind_IsNoop(t *testing.T) {
	er := newEmptyEntityRepo()
	s := minimalStream(1)
	er.OnAddOrUpdate(streamAddEvent(s))

	assert.NotPanics(t, func() {
		er.OnDelete(schema.Metadata{
			TypeMeta: schema.TypeMeta{Kind: schema.KindGroup},
			Spec:     &commonv1.Group{Metadata: &commonv1.Metadata{Group: "g", Name: "s"}},
		})
	}, "OnDelete with unsupported kind must not panic")

	id := identity{group: "g", name: "s"}
	_, ok := er.getLocator(id)
	assert.True(t, ok, "unsupported kind OnDelete must not remove the stream locator")
}

func TestGroupRepo_AcquireAndRelease(t *testing.T) {
	gr := &groupRepo{
		log:          logger.GetLogger("test"),
		resourceOpts: make(map[string]*commonv1.ResourceOpts),
		inflight:     make(map[string]*groupInflight),
	}

	require.NoError(t, gr.acquireRequest("test-group"))
	require.NoError(t, gr.acquireRequest("test-group"))
	gr.releaseRequest("test-group")
	gr.releaseRequest("test-group")

	gr.RWMutex.RLock()
	item, ok := gr.inflight["test-group"]
	gr.RWMutex.RUnlock()
	assert.True(t, ok)
	assert.Nil(t, item.done)
}

func TestGroupRepo_AcquireDuringPendingDeletion(t *testing.T) {
	gr := &groupRepo{
		log:          logger.GetLogger("test"),
		resourceOpts: make(map[string]*commonv1.ResourceOpts),
		inflight:     make(map[string]*groupInflight),
	}

	_ = gr.waitInflightRequests("del-group")
	err := gr.acquireRequest("del-group")
	assert.ErrorIs(t, err, errGroupPendingDeletion)
}

func TestGroupRepo_WaitInflightRequests(t *testing.T) {
	tests := []struct {
		name       string
		acquireNum int
	}{
		{name: "completes after release", acquireNum: 1},
		{name: "no inflight", acquireNum: 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gr := &groupRepo{
				log:          logger.GetLogger("test"),
				resourceOpts: make(map[string]*commonv1.ResourceOpts),
				inflight:     make(map[string]*groupInflight),
			}
			for range tc.acquireNum {
				require.NoError(t, gr.acquireRequest("g"))
			}
			done := gr.waitInflightRequests("g")
			if tc.acquireNum > 0 {
				select {
				case <-done:
					t.Fatal("done channel should not be closed while requests are in flight")
				default:
				}
				for range tc.acquireNum {
					gr.releaseRequest("g")
				}
			}
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("done channel did not close after all requests were released")
			}
		})
	}
}

func TestGroupRepo_MarkDeleted(t *testing.T) {
	gr := &groupRepo{
		log:          logger.GetLogger("test"),
		resourceOpts: make(map[string]*commonv1.ResourceOpts),
		inflight:     make(map[string]*groupInflight),
	}

	gr.waitInflightRequests("g3")
	gr.markDeleted("g3")

	gr.RWMutex.RLock()
	_, ok := gr.inflight["g3"]
	gr.RWMutex.RUnlock()
	assert.False(t, ok)
}
