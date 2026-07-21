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
	"github.com/apache/skywalking-banyandb/pkg/run"
)

type bydbQLService struct {
	bydbqlv1.UnimplementedBydbQLServiceServer
	queryAccessLog accesslog.Log
	l              *logger.Logger
	metrics        *metrics
	repo           metadata.Repo
	transformer    *bydbql.Transformer
	cache          *preparedCache
	streamSvc      *streamService
	measureSvc     *measureService
	traceSvc       *traceService
	propertyServer *propertyServer
	// dumper tracks and periodically logs the top cache-miss and slow queries; it is
	// nil (its observe methods no-op) unless the periodic top-K log is enabled.
	dumper        *topKDumper
	slowThreshold time.Duration
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
	// cacheResult tags the access-log entry with the prepared-statement cache
	// outcome so operators can find un-cached queries: entries logged under
	// "bydbql-miss" / "bydbql-bypass" are the ones that did not hit the cache.
	// A "reparse" is folded into "miss" here — it is a miss to anyone searching the
	// access log; only the top-K tracker cares about the distinction.
	var cacheResult string
	defer func() {
		duration := time.Since(start)
		b.metrics.totalFinished.Inc(1, "", "bydbql", "query")
		if err != nil {
			b.metrics.totalErr.Inc(1, "", "bydbql", "query")
		}
		b.metrics.totalLatency.Inc(duration.Seconds(), "", "bydbql", "query")

		if b.slowThreshold > 0 && duration > b.slowThreshold {
			b.metrics.bydbqlSlowQueryTotal.Inc(1)
			b.dumper.observeSlow(req.Query, duration)
		}

		if b.queryAccessLog != nil {
			service := "bydbql"
			if cacheResult != "" {
				tag := cacheResult
				if tag == cacheResultReparse {
					tag = "miss"
				}
				service = "bydbql-" + tag
			}
			if errAccessLog := b.queryAccessLog.WriteQuery(service, start, duration, req, err); errAccessLog != nil {
				b.l.Error().Err(errAccessLog).Msg("bydbql access log error")
			}
		}
	}()

	// prepare (parse once, cached), bind parameters, and transform to native request
	parseStart := time.Now()
	stmt, cacheResult, err := b.cache.getOrPrepare(req.Query)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "failed to parse query: %v", err)
	}
	// Track only re-parses, not first-ever compiles. Every template pays one unavoidable
	// cold-start compile; feeding those to the tracker both buries the real signal and,
	// once the distinct-template count passes bydbqlTopKSize, inflates every count through
	// Space-Saving's inheritance. The cache decides (it alone can), the caller acts.
	if cacheResult == cacheResultReparse {
		b.dumper.observeReparse(req.Query)
	}
	bound, err := stmt.Bind(req.Params)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "failed to bind parameters: %v", err)
	}
	result, err := b.transformer.TransformBound(ctx, bound)
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

// topKDumper tracks the top re-parsed and slow queries and, on a supervised
// goroutine, periodically logs the top-K within each tracker's TTL window. All methods are nil-safe, so the
// call sites need no guards when the top-K log is disabled (the dumper is nil).
type topKDumper struct {
	// reparse holds only templates the cache had already compiled once and had to
	// compile again. First-ever compiles are excluded at the call site, which is what
	// keeps this tracker near-empty on a healthy cluster — and therefore keeps its
	// counts exact, since Space-Saving only distorts them once it saturates.
	reparse *topK
	slow    *topK
	l       *logger.Logger
	cancel  context.CancelFunc
}

// newTopKDumper starts the trackers and the dump goroutine; a non-positive interval
// disables the feature and returns nil. Each tracker expires entries on its own TTL:
// a template nobody has re-parsed, or a query nobody has run slowly, for that long stops
// being reported. A non-positive TTL keeps that tracker's entries for the process lifetime.
func newTopKDumper(interval, reparseTTL, slowTTL time.Duration, l *logger.Logger) *topKDumper {
	if interval <= 0 {
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	d := &topKDumper{
		reparse: newTopK(bydbqlTopKSize, reparseTTL, nil),
		slow:    newTopK(bydbqlTopKSize, slowTTL, nil),
		l:       l, cancel: cancel,
	}
	run.Go(ctx, "liaison.grpc.bydbql.topk-dump", l, func(ctx context.Context) {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				d.dump()
			}
		}
	})
	return d
}

func (d *topKDumper) observeReparse(query string) {
	if d != nil {
		d.reparse.observe(query, 0)
	}
}

func (d *topKDumper) observeSlow(query string, dur time.Duration) {
	if d != nil {
		d.slow.observe(query, dur)
	}
}

func (d *topKDumper) close() {
	if d != nil && d.cancel != nil {
		d.cancel()
	}
}

func (d *topKDumper) dump() {
	// No count threshold, same as the slow dump: the tracker is fed only re-parses, so
	// every entry already means the cache compiled a template it had compiled before.
	//
	// A threshold of 2 used to stand here as the ONLY thing separating thrashing from
	// cold starts, and it could not do that job. Cold-start misses were fed to the
	// tracker, and once the distinct-template count passed bydbqlTopKSize, Space-Saving's
	// "new key inherits the evicted minimum's count + 1" ratcheted every count to N/k.
	// Measured against SkyWalking OAP (2495 distinct templates, zero evictions, so every
	// true count was 1): 2495/128 = 19.5, all 128 slots reported 19-20, and every one of
	// them cleared the threshold. Excluding cold starts at the source fixes that at the
	// root; keeping the threshold on top of it would only re-hide the real re-parses the
	// exclusion just made visible.
	d.logTopK(d.reparse.snapshot(), 1, "top bydbql cache-miss queries", func(s topKSlot) string {
		return fmt.Sprintf("%q count=%d last_seen=%s", s.key, s.count, formatTopKTime(s.lastSeen))
	})
	// max_latency is a running peak, so it can outlive the condition that caused it by
	// as long as the TTL allows. max_latency_at dates it, and last_seen says when the
	// query last ran at all, which together separate a live problem from a stale peak.
	d.logTopK(d.slow.snapshotByLatency(), 1, "top bydbql slow queries", func(s topKSlot) string {
		return fmt.Sprintf("%q count=%d max_latency=%s max_latency_at=%s last_seen=%s",
			s.key, s.count, s.maxDur, formatTopKTime(s.maxDurAt), formatTopKTime(s.lastSeen))
	})
}

// formatTopKTime renders a tracker timestamp in the same layout the surrounding logs use.
func formatTopKTime(t time.Time) string {
	return t.UTC().Format(time.RFC3339)
}

// logTopK logs the entries with at least minCount occurrences, formatted by line.
func (d *topKDumper) logTopK(entries []topKSlot, minCount uint64, msg string, line func(topKSlot) string) {
	lines := formatTopK(entries, minCount, line)
	if len(lines) == 0 {
		return
	}
	d.l.Info().Strs("top", lines).Msg(msg)
}

// formatTopK renders the entries with at least minCount occurrences into log lines.
func formatTopK(entries []topKSlot, minCount uint64, line func(topKSlot) string) []string {
	lines := make([]string, 0, len(entries))
	for _, s := range entries {
		if s.count < minCount {
			continue
		}
		lines = append(lines, line(s))
	}
	return lines
}

func (b *bydbQLService) Close() error {
	if b.queryAccessLog != nil {
		if err := b.queryAccessLog.Close(); err != nil {
			return err
		}
	}
	return nil
}
