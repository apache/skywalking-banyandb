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
	return &topKDumper{miss: newTopK(bydbqlTopKSize), slow: newTopK(bydbqlTopKSize), l: l}
}

func TestBydbQLQuery_TracksCacheMiss(t *testing.T) {
	svc := newTestBydbQLService()
	svc.dumper = newTestDumper(nil)
	// A cacheable query misses on first sight; the miss is observed before Bind runs
	// (Bind then fails on missing params, but the miss was already recorded).
	_, _ = svc.Query(context.Background(), &bydbqlv1.QueryRequest{
		Query: "SELECT * FROM STREAM sw IN default WHERE service_id = ?",
	})
	assert.NotEmpty(t, svc.dumper.miss.snapshot(), "a cacheable miss must be tracked")
}

func TestBydbQLQuery_TracksSlowQuery(t *testing.T) {
	svc := newTestBydbQLService()
	svc.slowThreshold = time.Nanosecond // any query exceeds it
	svc.dumper = newTestDumper(nil)
	_, _ = svc.Query(context.Background(), &bydbqlv1.QueryRequest{
		Query: "SELECT * FROM STREAM sw IN default WHERE service_id = ?",
	})
	assert.NotEmpty(t, svc.dumper.slow.snapshot(), "a query over the threshold must be tracked")
}

func TestBydbQLDumpTopK(t *testing.T) {
	d := newTestDumper(logger.GetLogger("test-bydbql"))
	d.miss.observe("q-miss", 0)
	d.slow.observe("q-slow", time.Millisecond)
	d.dump() // must not panic; the cumulative trackers keep their entries
	assert.NotEmpty(t, d.miss.snapshot())
	assert.NotEmpty(t, d.slow.snapshot())
}
