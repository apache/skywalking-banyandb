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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	bydbqlv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/bydbql/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

func newTestBydbQLService() *bydbQLService {
	m := newBypassMetrics()
	return &bydbQLService{metrics: m, cache: newPreparedCache(16, 1<<20, m)}
}

func bydbqlStrParam(v string) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: v}}}
}

func TestBydbQLQuery_ParseError_ReturnsInvalidArgument(t *testing.T) {
	svc := newTestBydbQLService()
	_, err := svc.Query(context.Background(), &bydbqlv1.QueryRequest{Query: "NOT A QUERY"})
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.InvalidArgument, st.Code())
	assert.Contains(t, st.Message(), "failed to parse query")
}

func TestBydbQLQuery_MissingParams_ReturnsInvalidArgument(t *testing.T) {
	svc := newTestBydbQLService()
	_, err := svc.Query(context.Background(), &bydbqlv1.QueryRequest{
		Query: "SELECT * FROM STREAM sw IN default WHERE service_id = ?",
	})
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.InvalidArgument, st.Code())
	assert.Contains(t, st.Message(), "failed to bind parameters")
	assert.Contains(t, st.Message(), "parameter count mismatch")
}

func TestBydbQLQuery_ExtraParams_ReturnsInvalidArgument(t *testing.T) {
	svc := newTestBydbQLService()
	_, err := svc.Query(context.Background(), &bydbqlv1.QueryRequest{
		Query:  "SELECT * FROM STREAM sw IN default",
		Params: []*modelv1.TagValue{bydbqlStrParam("unused")},
	})
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.InvalidArgument, st.Code())
	assert.Contains(t, st.Message(), "0 placeholder(s) but 1 parameter(s)")
}

func TestBydbQLQuery_ParamTypeMismatch_ReturnsInvalidArgument(t *testing.T) {
	svc := newTestBydbQLService()
	_, err := svc.Query(context.Background(), &bydbqlv1.QueryRequest{
		Query: "SELECT * FROM STREAM sw IN default WHERE service_id = ?",
		Params: []*modelv1.TagValue{
			{Value: &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{Value: []string{"a", "b"}}}},
		},
	})
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.InvalidArgument, st.Code())
	assert.Contains(t, st.Message(), "failed to bind parameters")
}

// newTestDumper builds a topKDumper without starting the dump goroutine.
func newTestDumper(l *logger.Logger) *topKDumper {
	return &topKDumper{reparse: newTopK(bydbqlTopKSize), slow: newTopK(bydbqlTopKSize), l: l}
}

// attachTestDumper gives svc a dumper without starting its dump goroutine, so the
// service-level Query path can record re-parses. Query itself decides what to track
// (if reparse ...), so no further wiring is needed.
func attachTestDumper(svc *bydbQLService) {
	svc.dumper = newTestDumper(nil)
}

// A first-ever compile is not thrashing: every template pays it exactly once, and it
// is unavoidable. Tracking it was what buried the real signal and, past
// bydbqlTopKSize distinct templates, inflated every reported count via Space-Saving.
func TestBydbQLQuery_DoesNotTrackColdStartCompile(t *testing.T) {
	svc := newTestBydbQLService()
	attachTestDumper(svc)
	_, _ = svc.Query(context.Background(), &bydbqlv1.QueryRequest{
		Query: "SELECT * FROM STREAM sw IN default WHERE service_id = ?",
	})
	assert.Empty(t, svc.dumper.reparse.snapshot(), "a first-ever compile must not be tracked")
}

// A template evicted under cache pressure and then requested again really is being
// re-parsed, and that is what the log exists to surface.
func TestBydbQLQuery_TracksReparseAfterEviction(t *testing.T) {
	m := newBypassMetrics()
	svc := &bydbQLService{metrics: m, cache: newPreparedCache(1, 1<<20, m)} // one slot
	attachTestDumper(svc)
	victim := &bydbqlv1.QueryRequest{Query: bydbqlQuery(0)}
	other := &bydbqlv1.QueryRequest{Query: bydbqlQuery(1)}

	_, _ = svc.Query(context.Background(), victim) // cold-start compile, not tracked
	_, _ = svc.Query(context.Background(), other)  // evicts victim
	assert.Empty(t, svc.dumper.reparse.snapshot(), "cold-start compiles stay untracked")

	_, _ = svc.Query(context.Background(), victim) // victim is back: a real re-parse
	snap := svc.dumper.reparse.snapshot()
	require.Len(t, snap, 1, "only the re-parsed template is tracked")
	assert.Equal(t, bydbqlQuery(0), snap[0].key)
	assert.Equal(t, uint64(1), snap[0].count, "count is the true re-parse count")
}

func TestBydbQLQuery_TracksSlowQuery(t *testing.T) {
	svc := newTestBydbQLService()
	svc.slowThreshold = time.Nanosecond // any query exceeds it
	attachTestDumper(svc)
	_, _ = svc.Query(context.Background(), &bydbqlv1.QueryRequest{
		Query: "SELECT * FROM STREAM sw IN default WHERE service_id = ?",
	})
	assert.NotEmpty(t, svc.dumper.slow.snapshot(), "a query over the threshold must be tracked")
}

func TestBydbQLDumpTopK(t *testing.T) {
	d := newTestDumper(logger.GetLogger("test-bydbql"))
	d.observeReparse("q-reparse")
	d.observeReparse("q-reparse") // a second re-parse of the same template
	d.slow.observe("q-slow", time.Millisecond)
	d.dump() // must not panic; the cumulative trackers keep their entries
	assert.NotEmpty(t, d.reparse.snapshot())
	assert.NotEmpty(t, d.slow.snapshot())
}

func TestFormatTopKAppliesItsMinCount(t *testing.T) {
	entries := []topKSlot{
		{key: "frequent", count: 5},
		{key: "once", count: 1},
	}
	assert.Equal(t, []string{"frequent"}, formatTopK(entries, 2, func(s topKSlot) string { return s.key }))
	// The dumps pass 1, i.e. no filtering: every tracked entry is already meaningful.
	assert.Equal(t, []string{"frequent", "once"}, formatTopK(entries, 1, func(s topKSlot) string { return s.key }))
}

// captureAccessLog records the service tag of every WriteQuery call.
type captureAccessLog struct{ services []string }

func (c *captureAccessLog) Write(proto.Message) error { return nil }

func (c *captureAccessLog) WriteQuery(service string, _ time.Time, _ time.Duration, _ proto.Message, _ error) error {
	c.services = append(c.services, service)
	return nil
}

func (c *captureAccessLog) Close() error { return nil }

// A re-parse is a cache miss to anyone searching the access log for un-cached queries;
// only the top-K tracker distinguishes it. The access log must therefore tag it
// "bydbql-miss", never "bydbql-reparse", or a "bydbql-miss" filter would silently drop
// the re-parsed queries — which are exactly the un-cached ones an operator is after.
func TestBydbQLQuery_ReparseIsLoggedAsMissNotReparse(t *testing.T) {
	m := newBypassMetrics()
	svc := &bydbQLService{metrics: m, cache: newPreparedCache(1, 1<<20, m)} // one slot
	attachTestDumper(svc)
	alog := &captureAccessLog{}
	svc.queryAccessLog = alog

	// Bind fails (no params), but that is after getOrPrepare set cacheResult, and the
	// access log is written from a defer, so every call is logged with its cache tag.
	_, _ = svc.Query(context.Background(), &bydbqlv1.QueryRequest{Query: bydbqlQuery(0)}) // cold-start miss
	_, _ = svc.Query(context.Background(), &bydbqlv1.QueryRequest{Query: bydbqlQuery(1)}) // evicts 0
	_, _ = svc.Query(context.Background(), &bydbqlv1.QueryRequest{Query: bydbqlQuery(0)}) // re-parse

	require.Len(t, alog.services, 3)
	assert.Equal(t, "bydbql-miss", alog.services[2], "a re-parse must be logged as a miss")
	assert.NotContains(t, alog.services, "bydbql-reparse", "reparse must never leak into the access log")

	// The distinction survives — but only in the tracker, which is the whole point.
	snap := svc.dumper.reparse.snapshot()
	require.Len(t, snap, 1)
	assert.Equal(t, bydbqlQuery(0), snap[0].key)
}
