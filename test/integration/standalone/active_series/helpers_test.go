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

package active_series_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	g "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	grpclib "google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
)

const (
	measureActiveSeries = "banyandb_measure_active_series"
	streamActiveSeries  = "banyandb_stream_storage_active_series"

	measureGroup   = "sw_metric"
	measureName    = "service_cpm_minute"
	indexModeGroup = "index_mode"
	indexModeName  = "service_traffic"
	streamGroup    = "default"
	streamName     = "sw"

	roundInterval = 2 * time.Second
	batchRows     = 250
	// shardCount is the number of shards of every group the suite writes to.
	shardCount = 2
	tolerance  = 3.0 // percent
)

var metricsClient = &http.Client{Timeout: 5 * time.Second}

// scrape fetches and parses the Prometheus text exposition at url.
func scrape(url string) (map[string]*dto.MetricFamily, error) {
	resp, err := metricsClient.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("scrape %s: status %d", url, resp.StatusCode)
	}
	parser := expfmt.NewTextParser(model.UTF8Validation)
	families, err := parser.TextToMetricFamilies(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", url, err)
	}
	return families, nil
}

func labelValue(m *dto.Metric, name string) (string, bool) {
	for _, lp := range m.GetLabel() {
		if lp.GetName() == name {
			return lp.GetValue(), true
		}
	}
	return "", false
}

// seriesByShard returns the exported value of metric of group at url, keyed by the shard label.
// A second sample of the same shard is an error.
func seriesByShard(url, metric, group string) (map[string]float64, error) {
	families, err := scrape(url)
	if err != nil {
		return nil, err
	}
	result := make(map[string]float64)
	for _, m := range families[metric].GetMetric() {
		if grp, ok := labelValue(m, "group"); !ok || grp != group {
			continue
		}
		shard, ok := labelValue(m, "shard")
		if !ok {
			return nil, fmt.Errorf("%s at %s has a sample without the shard label", metric, url)
		}
		if _, dup := result[shard]; dup {
			return nil, fmt.Errorf("%s of group %s at %s has more than one sample for shard %s", metric, group, url, shard)
		}
		result[shard] = m.GetGauge().GetValue()
	}
	return result, nil
}

// labelNames returns the label names of the samples of metric of group at url.
func labelNames(url, metric, group string) ([]string, error) {
	families, err := scrape(url)
	if err != nil {
		return nil, err
	}
	seen := make(map[string]struct{})
	for _, m := range families[metric].GetMetric() {
		if grp, ok := labelValue(m, "group"); !ok || grp != group {
			continue
		}
		for _, lp := range m.GetLabel() {
			seen[lp.GetName()] = struct{}{}
		}
	}
	names := make([]string, 0, len(seen))
	for n := range seen {
		names = append(names, n)
	}
	slices.Sort(names)
	return names, nil
}

func sum(values map[string]float64) float64 {
	var s float64
	for _, v := range values {
		s += v
	}
	return s
}

// withinPercent reports whether got is within pct percent of want.
func withinPercent(got, want, pct float64) bool {
	return got >= want*(1-pct/100) && got <= want*(1+pct/100)
}

// expectSum asserts that the shards of group add up to want within the tolerance and that every shard of the group exports a value.
func expectSum(gm gomega.Gomega, url, metric, group string, want float64) {
	values, err := seriesByShard(url, metric, group)
	gm.Expect(err).NotTo(gomega.HaveOccurred())
	gm.Expect(values).To(gomega.HaveLen(shardCount), "shards that export %s of %s", metric, group)
	for shard, v := range values {
		gm.Expect(v).To(gomega.BeNumerically(">", 0), "shard %s of %s", shard, group)
	}
	gm.Expect(withinPercent(sum(values), want, tolerance)).To(gomega.BeTrue(), "sum of %s of %s is %v, want %v +-%v%%", metric, group, sum(values), want, tolerance)
}

// seriesWriter keeps writing a fixed set of series to one node, one round after another, until stopped.
type seriesWriter struct {
	conn     *grpclib.ClientConn
	cancel   context.CancelFunc
	done     chan struct{}
	prefix   string
	measureN int
	indexN   int
	streamN  int
	mu       sync.Mutex
	repeat   atomic.Int32
	messages atomic.Uint64
	accepted atomic.Uint64
	rejected atomic.Uint64
}

// startWriter starts a writer of measureN series of sw_metric, streamN series of the default stream and indexN series of the
// index-mode measure. The series ids start with prefix, so two writers with different prefixes write disjoint series.
func startWriter(conn *grpclib.ClientConn, prefix string, measureN, streamN, indexN int) *seriesWriter {
	ctx, cancel := context.WithCancel(context.Background())
	w := &seriesWriter{conn: conn, prefix: prefix, measureN: measureN, streamN: streamN, indexN: indexN, cancel: cancel, done: make(chan struct{})}
	w.repeat.Store(1)
	//panicdiag:allow-rawgo test helper goroutine that writes rounds until the writer is stopped
	go func() {
		defer close(w.done)
		ticker := time.NewTicker(roundInterval)
		defer ticker.Stop()
		for {
			w.round(ctx)
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	return w
}

// SetRepeat sets how many rows each series gets per round.
func (w *seriesWriter) SetRepeat(n int) {
	w.repeat.Store(int32(n))
}

// SetIndexSeries sets the number of index-mode series written per round.
func (w *seriesWriter) SetIndexSeries(n int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.indexN = n
}

// Accepted returns the number of rows acknowledged with a success status.
func (w *seriesWriter) Accepted() uint64 { return w.accepted.Load() }

// Rejected returns the number of rows that got a non-success status.
func (w *seriesWriter) Rejected() uint64 { return w.rejected.Load() }

// Stop ends the writer.
func (w *seriesWriter) Stop() {
	w.cancel()
	<-w.done
}

func (w *seriesWriter) round(ctx context.Context) {
	w.mu.Lock()
	indexN := w.indexN
	w.mu.Unlock()
	repeat := int(w.repeat.Load())
	base := time.Now().Add(-time.Minute).Truncate(time.Millisecond)

	var measureRows, indexRows []*measurev1.WriteRequest
	var streamRows []*streamv1.WriteRequest
	for r := range repeat {
		ts := timestamppb.New(base.Add(time.Duration(r) * time.Millisecond))
		for i := range w.measureN {
			measureRows = append(measureRows, w.measureRow(ts, i))
		}
		for i := range indexN {
			indexRows = append(indexRows, w.indexRow(ts, i))
		}
		for i := range w.streamN {
			streamRows = append(streamRows, w.streamRow(ts, i, r))
		}
	}
	for _, rows := range chunks(measureRows) {
		w.sendMeasure(ctx, rows)
	}
	for _, rows := range chunks(indexRows) {
		w.sendMeasure(ctx, rows)
	}
	for _, rows := range chunks(streamRows) {
		w.sendStream(ctx, rows)
	}
}

func chunks[T any](rows []T) [][]T {
	var out [][]T
	for len(rows) > 0 {
		n := min(batchRows, len(rows))
		out = append(out, rows[:n])
		rows = rows[n:]
	}
	return out
}

func (w *seriesWriter) measureRow(ts *timestamppb.Timestamp, i int) *measurev1.WriteRequest {
	id := fmt.Sprintf("%s-%d", w.prefix, i)
	return &measurev1.WriteRequest{
		MessageId: w.messages.Add(1),
		Metadata:  &commonv1.Metadata{Group: measureGroup, Name: measureName},
		DataPoint: &measurev1.DataPointValue{
			Timestamp:   ts,
			TagFamilies: []*modelv1.TagFamilyForWrite{{Tags: []*modelv1.TagValue{str(id), str(id)}}},
			Fields: []*modelv1.FieldValue{
				{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: 100}}},
				{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: 1}}},
			},
		},
	}
}

func (w *seriesWriter) indexRow(ts *timestamppb.Timestamp, i int) *measurev1.WriteRequest {
	id := fmt.Sprintf("%s-index-%d", w.prefix, i)
	return &measurev1.WriteRequest{
		MessageId: w.messages.Add(1),
		Metadata:  &commonv1.Metadata{Group: indexModeGroup, Name: indexModeName},
		DataPoint: &measurev1.DataPointValue{
			Timestamp: ts,
			TagFamilies: []*modelv1.TagFamilyForWrite{{Tags: []*modelv1.TagValue{
				str(id), str(id), str("name-" + id), str("short"), str("group"), integer(1),
			}}},
		},
	}
}

func (w *seriesWriter) streamRow(ts *timestamppb.Timestamp, i, r int) *streamv1.WriteRequest {
	svc := fmt.Sprintf("%s-svc-%d", w.prefix, i)
	return &streamv1.WriteRequest{
		MessageId: w.messages.Add(1),
		Metadata:  &commonv1.Metadata{Group: streamGroup, Name: streamName},
		Element: &streamv1.ElementValue{
			ElementId: fmt.Sprintf("%s-%d-%d", svc, ts.AsTime().UnixMilli(), r),
			Timestamp: ts,
			TagFamilies: []*modelv1.TagFamilyForWrite{
				{Tags: []*modelv1.TagValue{{Value: &modelv1.TagValue_BinaryData{BinaryData: []byte(svc)}}}},
				{Tags: []*modelv1.TagValue{str(svc), integer(0), str(svc), str("instance"), str("/endpoint"), integer(10), integer(1)}},
			},
		},
	}
}

func (w *seriesWriter) count(status string) {
	if status == modelv1.Status_STATUS_SUCCEED.String() {
		w.accepted.Add(1)
		return
	}
	w.rejected.Add(1)
	g.GinkgoWriter.Printf("write rejected with status %s\n", status)
}

func (w *seriesWriter) sendMeasure(ctx context.Context, rows []*measurev1.WriteRequest) {
	wctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	stream, err := measurev1.NewMeasureServiceClient(w.conn).Write(wctx)
	if err != nil {
		g.GinkgoWriter.Printf("open measure write: %v\n", err)
		return
	}
	for _, row := range rows {
		if err = stream.Send(row); err != nil {
			g.GinkgoWriter.Printf("send measure row: %v\n", err)
			return
		}
	}
	_ = stream.CloseSend()
	for {
		resp, recvErr := stream.Recv()
		if recvErr != nil {
			if !errors.Is(recvErr, io.EOF) {
				g.GinkgoWriter.Printf("measure write: %v\n", recvErr)
			}
			return
		}
		w.count(resp.GetStatus())
	}
}

func (w *seriesWriter) sendStream(ctx context.Context, rows []*streamv1.WriteRequest) {
	wctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	stream, err := streamv1.NewStreamServiceClient(w.conn).Write(wctx)
	if err != nil {
		g.GinkgoWriter.Printf("open stream write: %v\n", err)
		return
	}
	for _, row := range rows {
		if err = stream.Send(row); err != nil {
			g.GinkgoWriter.Printf("send stream row: %v\n", err)
			return
		}
	}
	_ = stream.CloseSend()
	for {
		resp, recvErr := stream.Recv()
		if recvErr != nil {
			if !errors.Is(recvErr, io.EOF) {
				g.GinkgoWriter.Printf("stream write: %v\n", recvErr)
			}
			return
		}
		w.count(resp.GetStatus())
	}
}

func eqCriteria(tag, value string) *modelv1.Criteria {
	return &modelv1.Criteria{Exp: &modelv1.Criteria_Condition{Condition: &modelv1.Condition{
		Name: tag, Op: modelv1.Condition_BINARY_OP_EQ, Value: str(value),
	}}}
}

// queryRange covers the rows the suite writes: they are written one minute before the current time.
func queryRange() *modelv1.TimeRange {
	now := time.Now().Truncate(time.Millisecond)
	return &modelv1.TimeRange{Begin: timestamppb.New(now.Add(-time.Hour)), End: timestamppb.New(now.Add(time.Minute))}
}

// measureHits returns the number of data points of the series of sw_metric whose entity id is id.
func measureHits(conn *grpclib.ClientConn, id string) int {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	resp, err := measurev1.NewMeasureServiceClient(conn).Query(ctx, &measurev1.QueryRequest{
		Groups:    []string{measureGroup},
		Name:      measureName,
		TimeRange: queryRange(),
		Criteria:  eqCriteria("entity_id", id),
		TagProjection: &modelv1.TagProjection{TagFamilies: []*modelv1.TagProjection_TagFamily{
			{Name: "default", Tags: []string{"id", "entity_id"}},
		}},
		FieldProjection: &measurev1.QueryRequest_FieldProjection{Names: []string{"total", "value"}},
		Limit:           100,
	})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	return len(resp.GetDataPoints())
}

// streamHits returns the number of elements of the series of the default stream whose service id is id.
func streamHits(conn *grpclib.ClientConn, id string) int {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	resp, err := streamv1.NewStreamServiceClient(conn).Query(ctx, &streamv1.QueryRequest{
		Groups:    []string{streamGroup},
		Name:      streamName,
		TimeRange: queryRange(),
		Criteria:  eqCriteria("service_id", id),
		Projection: &modelv1.TagProjection{TagFamilies: []*modelv1.TagProjection_TagFamily{
			{Name: "searchable", Tags: []string{"trace_id", "service_id"}},
		}},
		Limit: 100,
	})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	return len(resp.GetElements())
}

func str(v string) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: v}}}
}

func integer(v int64) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: v}}}
}
