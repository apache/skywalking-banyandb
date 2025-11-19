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
	"fmt"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"

	bydbqlv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/bydbql/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/pkg/accesslog"
	"github.com/apache/skywalking-banyandb/pkg/bydbql"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

type bydbQLService struct {
	bydbqlv1.UnimplementedBydbQLServiceServer
	queryAccessLog accesslog.Log
	l              *logger.Logger
	metrics        *metrics
	repo           metadata.Repo
	transformer    *bydbql.Transformer
	streamSvc      *streamService
	measureSvc     *measureService
	traceSvc       *traceService
	propertyServer *propertyServer
}

func (b *bydbQLService) setLogger(log *logger.Logger) {
	b.l = log
}

func (b *bydbQLService) activeQueryAccessLog(root string, sampled bool) (err error) {
	if b.queryAccessLog, err = accesslog.
		NewFileLog(root, "bydbql-query-%s", 10*time.Minute, b.l, sampled); err != nil {
		return err
	}
	return nil
}

func (b *bydbQLService) Query(ctx context.Context, req *bydbqlv1.QueryRequest) (resp *bydbqlv1.QueryResponse, err error) {
	start := time.Now()
	b.metrics.totalStarted.Inc(1, "", "bydbql", "query")
	defer func() {
		duration := time.Since(start)
		b.metrics.totalFinished.Inc(1, "", "bydbql", "query")
		if err != nil {
			b.metrics.totalErr.Inc(1, "", "bydbql", "query")
		}
		b.metrics.totalLatency.Inc(duration.Seconds(), "", "bydbql", "query")

		if b.queryAccessLog != nil {
			if errAccessLog := b.queryAccessLog.WriteQuery("bydbql", start, duration, req, err); errAccessLog != nil {
				b.l.Error().Err(errAccessLog).Msg("bydbql access log error")
			}
		}
	}()

	// parse query and transform to native request
	parseStart := time.Now()
	query, err := bydbql.ParseQuery(req.Query)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "failed to parse query: %v", err)
	}
	result, err := b.transformer.Transform(ctx, query)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to transform to native request: %v", err)
	}
	parseDuration := time.Since(parseStart)
	if dl := b.l.Debug(); dl.Enabled() {
		requestJSON, err := protojson.Marshal(result.QueryRequest)
		if err != nil {
			dl.Err(err).Msg("failed to marshal the request to json")
		} else {
			dl.Str("ql", req.Query).Stringer("type", result.Type).
				Str("to_request", string(requestJSON)).Stringer("duration", parseDuration).Msg("bydbql query")
		}
	}

	// execute native request
	resp = &bydbqlv1.QueryResponse{}
	switch result.Type {
	case bydbql.QueryTypeStream:
		streamResponse, err := b.streamSvc.Query(ctx, result.QueryRequest.(*streamv1.QueryRequest))
		if err != nil {
			return nil, err
		}
		resp.Result = &bydbqlv1.QueryResponse_StreamResult{StreamResult: streamResponse}
	case bydbql.QueryTypeMeasure:
		measureResponse, err := b.measureSvc.Query(ctx, result.QueryRequest.(*measurev1.QueryRequest))
		if err != nil {
			return nil, err
		}
		resp.Result = &bydbqlv1.QueryResponse_MeasureResult{MeasureResult: measureResponse}
	case bydbql.QueryTypeTrace:
		traceResponse, err := b.traceSvc.Query(ctx, result.QueryRequest.(*tracev1.QueryRequest))
		if err != nil {
			return nil, err
		}
		resp.Result = &bydbqlv1.QueryResponse_TraceResult{TraceResult: traceResponse}
	case bydbql.QueryTypeTopN:
		topNResponse, err := b.measureSvc.TopN(ctx, result.QueryRequest.(*measurev1.TopNRequest))
		if err != nil {
			return nil, err
		}
		resp.Result = &bydbqlv1.QueryResponse_TopnResult{TopnResult: topNResponse}
	case bydbql.QueryTypeProperty:
		propertyResponse, err := b.propertyServer.Query(ctx, result.QueryRequest.(*propertyv1.QueryRequest))
		if err != nil {
			return nil, err
		}
		resp.Result = &bydbqlv1.QueryResponse_PropertyResult{PropertyResult: propertyResponse}
	default:
		return nil, fmt.Errorf("unknown query type: %v", result.Type)
	}
	return resp, nil
}

func (b *bydbQLService) Close() error {
	if b.queryAccessLog != nil {
		if err := b.queryAccessLog.Close(); err != nil {
			return err
		}
	}
	return nil
}
