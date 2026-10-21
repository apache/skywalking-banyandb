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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

func TestMeasureCallersReleasePartialGroupAcquisition(t *testing.T) {
	for _, batches := range []bool{false, true} {
		t.Run(map[bool]string{false: "native", true: "ql"}[batches], func(t *testing.T) {
			repo := &groupRepo{inflight: make(map[string]*groupInflight)}
			repo.waitInflightRequests("deleting")
			svc := &measureService{discoveryService: &discoveryService{groupRepo: repo}}
			_, err := svc.query(context.Background(), &measurev1.QueryRequest{Groups: []string{"first", "deleting"}}, batches)
			require.Equal(t, codes.FailedPrecondition, status.Code(err))
			select {
			case <-repo.waitInflightRequests("first"):
			case <-time.After(time.Second):
				t.Fatal("earlier group acquisition leaked after later group failed")
			}
		})
	}
}

func TestMeasureCallersCanceledBeforeAdmission(t *testing.T) {
	for _, batches := range []bool{false, true} {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		svc := &measureService{}
		_, err := svc.query(ctx, &measurev1.QueryRequest{Groups: []string{"g"}}, batches)
		require.Equal(t, codes.Canceled, status.Code(err))
	}
}

func TestMeasureCallersShareRevisionGate(t *testing.T) {
	for _, batches := range []bool{false, true} {
		er := seededLocatorRepo(identity{group: "g", name: "m"})
		svc := newTestMeasureService(er, 0)
		svc.groupRepo = &groupRepo{inflight: make(map[string]*groupInflight)}
		req := &measurev1.QueryRequest{
			Groups: []string{"g"}, Name: "m", GroupModRevisions: map[string]int64{"g": 50},
			TimeRange: &modelv1.TimeRange{
				Begin: timestamppb.New(time.Now().Add(-time.Minute).Truncate(time.Millisecond)),
				End:   timestamppb.New(time.Now().Truncate(time.Millisecond)),
			},
			Trace: true,
		}
		resp, err := svc.query(context.Background(), req, batches)
		require.NoError(t, err)
		require.Equal(t, modelv1.Status_STATUS_EXPIRED_SCHEMA, resp.GroupStatuses["g"])
		require.NotNil(t, resp.Trace)
	}
}

// failingBatchResult proves that errors from final egress retain their identity and
// release the source even when initialization fails before the first row.
type failingBatchResult struct {
	failure error
	closed  bool
}

func (*failingBatchResult) Init(context.Context) error            { return nil }
func (*failingBatchResult) OutputSchema() *vectorized.BatchSchema { return nil }
func (r *failingBatchResult) NextBatch(context.Context) (*vectorized.RecordBatch, error) {
	return nil, r.failure
}
func (r *failingBatchResult) Close() error              { r.closed = true; return nil }
func (*failingBatchResult) QueryTrace() *commonv1.Trace { return nil }
func (r *failingBatchResult) NativeIteratorFrom(ctx context.Context, source vectorized.PullOperator) (executor.MIterator, error) {
	_, err := source.NextBatch(ctx)
	return nil, err
}

func TestCollectMeasureBatchesClosesOnExecutionError(t *testing.T) {
	failure := errors.New("native execution failure")
	result := &failingBatchResult{failure: failure}
	_, err := collectMeasureBatches(context.Background(), result)
	require.ErrorIs(t, err, failure)
	require.True(t, result.closed)
}

func TestCollectMeasureBatchesClosesOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result := &failingBatchResult{}
	_, err := collectMeasureBatches(ctx, result)
	require.ErrorIs(t, err, context.Canceled)
	require.True(t, result.closed)
}

func TestMeasureCallersHoldParentLeaseThroughTransport(t *testing.T) {
	for _, batches := range []bool{false, true} {
		svc := newTestMeasureService(seededLocatorRepo(identity{group: "g", name: "m"}), 0)
		svc.groupRepo = &groupRepo{inflight: make(map[string]*groupInflight)}
		svc.queryBudget = protector.NewQueryBudget(nil)
		ctx, scope := query.NewTransportScope(context.Background())
		now := time.Now().Truncate(time.Millisecond)
		response, err := svc.query(ctx, &measurev1.QueryRequest{
			Groups: []string{"g"}, Name: "m", GroupModRevisions: map[string]int64{"g": 50},
			TimeRange: &modelv1.TimeRange{Begin: timestamppb.New(now.Add(-time.Minute)), End: timestamppb.New(now)},
		}, batches)
		require.NoError(t, err)
		require.Equal(t, modelv1.Status_STATUS_EXPIRED_SCHEMA, response.GroupStatuses["g"])
		require.Positive(t, svc.queryBudget.Reserved(), "the parent lease must survive response materialization")
		scope.Close()
		require.Zero(t, svc.queryBudget.Reserved(), "the transport must release the parent lease")
	}
}

func TestMeasureCallersAdmit32TinyQueriesWhileTransportsRemainOpen(t *testing.T) {
	for _, batches := range []bool{false, true} {
		t.Run(map[bool]string{false: "native", true: "ql"}[batches], func(t *testing.T) {
			svc := newTestMeasureService(seededLocatorRepo(identity{group: "g", name: "m"}), 0)
			svc.groupRepo = &groupRepo{inflight: make(map[string]*groupInflight)}
			svc.queryBudget = protector.NewQueryBudget(nil)
			t.Cleanup(func() { require.Zero(t, svc.queryBudget.Reserved()) })
			now := time.Now().Truncate(time.Millisecond)
			request := &measurev1.QueryRequest{
				Groups: []string{"g"}, Name: "m", GroupModRevisions: map[string]int64{"g": 50},
				TimeRange: &modelv1.TimeRange{Begin: timestamppb.New(now.Add(-time.Minute)), End: timestamppb.New(now)},
			}
			// A revision-gated response needs only admission overhead. Keeping every
			// transport scope open makes the admission regression deterministic.
			for i := 0; i < 32; i++ {
				ctx, scope := query.NewTransportScope(t.Context())
				t.Cleanup(scope.Close)
				response, err := svc.query(ctx, request, batches)
				require.NoError(t, err, "tiny query %d must not reserve its whole 8MiB allowance", i+1)
				require.Equal(t, modelv1.Status_STATUS_EXPIRED_SCHEMA, response.GroupStatuses["g"])
			}
			require.GreaterOrEqual(t, svc.queryBudget.Reserved(), uint64(32<<20))
			require.Less(t, svc.queryBudget.Reserved(), uint64(33<<20), "only fixed overhead and the tiny encoded responses remain held")
		})
	}
}
