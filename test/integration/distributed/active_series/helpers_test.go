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
	"strings"
	"sync"
	"sync/atomic"
	"time"

	g "github.com/onsi/ginkgo/v2"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
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

// activeSeriesByShard returns the exported value of metric of group at url, keyed by the shard label.
// A sample whose labels are not exactly group and shard, or a second sample of the same shard, is an error.
func activeSeriesByShard(url, metric, group string) (map[string]float64, error) {
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
		if len(m.GetLabel()) != 2 {
			return nil, fmt.Errorf("%s at %s has labels %v, want group and shard only", metric, url, m.GetLabel())
		}
		if _, dup := result[shard]; dup {
			return nil, fmt.Errorf("%s of group %s at %s has more than one sample for shard %s", metric, group, url, shard)
		}
		result[shard] = m.GetGauge().GetValue()
	}
	return result, nil
}

// activeSeriesFamilies returns the names of the metric families at url that contain active_series and the number of banyandb_ families
// the scrape returned, which shows that an empty list is not the result of an empty scrape.
func activeSeriesFamilies(url string) ([]string, int, error) {
	families, err := scrape(url)
	if err != nil {
		return nil, 0, err
	}
	var names []string
	banyandb := 0
	for name := range families {
		if strings.Contains(name, "active_series") {
			names = append(names, name)
		}
		if strings.HasPrefix(name, "banyandb_") {
			banyandb++
		}
	}
	slices.Sort(names)
	return names, banyandb, nil
}

// sumOfMaxByShard sums, over every shard label, the largest value any node reports for it.
func sumOfMaxByShard(perNode []map[string]float64) float64 {
	maxByShard := make(map[string]float64)
	for _, shards := range perNode {
		for shard, v := range shards {
			maxByShard[shard] = max(maxByShard[shard], v)
		}
	}
	var sum float64
	for _, v := range maxByShard {
		sum += v
	}
	return sum
}

// aggregate is what the alert rule sees for one group.
type aggregate struct {
	// writers counts, per shard, the nodes that export a non-zero value.
	writers map[string]int
	// byLiaison holds the exported values of every scraped liaison, keyed by its metrics URL and then by shard.
	byLiaison map[string]map[string]float64
	sum       float64
}

// liaisonURLs returns the metrics URLs of the liaisons that are running now.
func liaisonURLs() []string {
	urls := make([]string, 0, len(liaisons))
	for _, n := range liaisons {
		urls = append(urls, n.MetricsURL)
	}
	return urls
}

// aggregateOf scrapes every running liaison for metric of group.
func aggregateOf(metric, group string) (aggregate, error) {
	return aggregateAt(liaisonURLs(), metric, group)
}

// aggregateAt scrapes the liaisons at urls for metric of group.
func aggregateAt(urls []string, metric, group string) (aggregate, error) {
	agg := aggregate{writers: make(map[string]int), byLiaison: make(map[string]map[string]float64)}
	var perNode []map[string]float64
	for _, url := range urls {
		shards, err := activeSeriesByShard(url, metric, group)
		if err != nil {
			return agg, err
		}
		perNode = append(perNode, shards)
		agg.byLiaison[url] = shards
		for shard, v := range shards {
			if v > 0 {
				agg.writers[shard]++
			}
		}
	}
	agg.sum = sumOfMaxByShard(perNode)
	return agg, nil
}

// withinPercent reports whether got is within pct percent of want.
func withinPercent(got, want, pct float64) bool {
	return got >= want*(1-pct/100) && got <= want*(1+pct/100)
}

// seriesWriter keeps writing a fixed set of series to the liaisons, one round after another, until stopped.
// The measure series of one tracker window are disjoint from those of the adjacent windows, so a window that is not
// reset or that mixes in a neighbor reports more series than one window holds.
type seriesWriter struct {
	conns  map[string]*grpclib.ClientConn
	cancel context.CancelFunc
	done   chan struct{}
	// prefix starts the series IDs of every row, so two writers write disjoint series.
	prefix   string
	addrs    []string
	measureN int
	indexN   int
	streamN  int
	mu       sync.Mutex
	connMu   sync.Mutex
	repeat   atomic.Int32
	messages atomic.Uint64
	accepted atomic.Uint64
	rejected atomic.Uint64
}

// startWriter starts a writer of measureN series of sw_metric, streamN series of the default stream and, when indexN is not zero, indexN
// series of the index-mode measure.
func startWriter(addrs []string, measureN, streamN, indexN int) *seriesWriter {
	return startPrefixedWriter("", addrs, measureN, streamN, indexN)
}

// startPrefixedWriter is startWriter for series whose IDs start with prefix.
func startPrefixedWriter(prefix string, addrs []string, measureN, streamN, indexN int) *seriesWriter {
	ctx, cancel := context.WithCancel(context.Background())
	w := &seriesWriter{
		addrs: slices.Clone(addrs), prefix: prefix, measureN: measureN, streamN: streamN, indexN: indexN,
		conns: make(map[string]*grpclib.ClientConn), cancel: cancel, done: make(chan struct{}),
	}
	w.repeat.Store(1)
	//panicdiag:allow-rawgo test helper goroutine
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

// SetTargets replaces the liaison gRPC addresses the writer sends to.
func (w *seriesWriter) SetTargets(addrs []string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.addrs = slices.Clone(addrs)
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

// Accepted returns the number of rows the liaisons acknowledged with a success status.
func (w *seriesWriter) Accepted() uint64 {
	return w.accepted.Load()
}

// Rejected returns the number of rows that got a non-success status.
func (w *seriesWriter) Rejected() uint64 {
	return w.rejected.Load()
}

// Stop ends the writer and closes its connections.
func (w *seriesWriter) Stop() {
	w.cancel()
	<-w.done
	for _, conn := range w.conns {
		_ = conn.Close()
	}
}

func (w *seriesWriter) conn(addr string) (*grpclib.ClientConn, error) {
	w.connMu.Lock()
	defer w.connMu.Unlock()
	if conn, ok := w.conns[addr]; ok {
		return conn, nil
	}
	conn, err := grpclib.NewClient(addr, grpclib.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	w.conns[addr] = conn
	return conn, nil
}

// windowClock is the clock of every active series tracker in this suite. It reads the real time but does not leave a window while a
// measure batch whose series set was chosen for that window is unacknowledged. A liaison counts a row before it acknowledges it,
// so the tracker counts every such batch in the window its series set belongs to, however long the batch takes.
type windowClock struct {
	timestamp.Clock
	cond *sync.Cond
	// window is the index of the window Now reads, counted from the Unix epoch.
	window   int64
	inflight int
	mu       sync.Mutex
}

func newWindowClock() *windowClock {
	c := &windowClock{Clock: timestamp.NewClock(), window: time.Now().UnixNano() / int64(activeSeriesWindow)}
	c.cond = sync.NewCond(&c.mu)
	return c
}

// Now returns the real time, or the last instant of the current window while the real time is past it and a batch is in flight.
func (c *windowClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	if c.advanceLocked(now) {
		return now
	}
	return time.Unix(0, (c.window+1)*int64(activeSeriesWindow)-1)
}

// advanceLocked moves the clock to the window of now unless a batch is in flight, and reports whether the clock is in that window.
func (c *windowClock) advanceLocked(now time.Time) bool {
	w := now.UnixNano() / int64(activeSeriesWindow)
	if w <= c.window {
		return true
	}
	if c.inflight > 0 {
		return false
	}
	c.window = w
	return true
}

// begin returns the parity of the window the clock is in and keeps the clock in that window until end is called.
// Once the real time has left the window, it waits until the batches in flight end and the clock follows.
func (c *windowClock) begin() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	for !c.advanceLocked(time.Now()) {
		c.cond.Wait()
	}
	c.inflight++
	return int(c.window % 2)
}

// end releases the window that the matching begin held.
func (c *windowClock) end() {
	c.mu.Lock()
	c.inflight--
	c.mu.Unlock()
	c.cond.Broadcast()
}

// round sends every series once per repeat, splitting the rows into batches that alternate between the liaisons.
// The batches of a round are sent concurrently, and the series set of a measure batch is chosen when the batch is sent.
func (w *seriesWriter) round(ctx context.Context) {
	w.mu.Lock()
	addrs := slices.Clone(w.addrs)
	indexN := w.indexN
	w.mu.Unlock()
	if len(addrs) == 0 {
		return
	}
	repeat := int(w.repeat.Load())
	now := time.Now().Add(-time.Minute).Truncate(time.Millisecond)

	var indexRows []*measurev1.WriteRequest
	var streamRows []*streamv1.WriteRequest
	for r := range repeat {
		ts := timestamppb.New(now.Add(time.Duration(r) * time.Millisecond))
		for i := range indexN {
			indexRows = append(indexRows, w.indexRow(ts, i))
		}
		for i := range w.streamN {
			streamRows = append(streamRows, w.streamRow(ts, i, r))
		}
	}
	var wg sync.WaitGroup
	batch := 0
	for lo := 0; lo < repeat*w.measureN; lo += batchRows {
		hi := min(lo+batchRows, repeat*w.measureN)
		addr := addrs[batch%len(addrs)]
		batch++
		wg.Add(1)
		//panicdiag:allow-rawgo test helper goroutine
		go func() {
			defer wg.Done()
			parity := trackerClock.begin()
			defer trackerClock.end()
			rows := make([]*measurev1.WriteRequest, 0, hi-lo)
			for idx := lo; idx < hi; idx++ {
				ts := timestamppb.New(now.Add(time.Duration(idx/w.measureN) * time.Millisecond))
				rows = append(rows, w.measureRow(ts, parity, idx%w.measureN))
			}
			w.sendMeasure(ctx, addr, rows)
		}()
	}
	for _, rows := range chunks(indexRows) {
		addr := addrs[batch%len(addrs)]
		batch++
		wg.Add(1)
		//panicdiag:allow-rawgo test helper goroutine
		go func() {
			defer wg.Done()
			w.sendMeasure(ctx, addr, rows)
		}()
	}
	for _, rows := range chunks(streamRows) {
		addr := addrs[batch%len(addrs)]
		batch++
		wg.Add(1)
		//panicdiag:allow-rawgo test helper goroutine
		go func() {
			defer wg.Done()
			w.sendStream(ctx, addr, rows)
		}()
	}
	wg.Wait()
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

func (w *seriesWriter) measureRow(ts *timestamppb.Timestamp, parity, i int) *measurev1.WriteRequest {
	id := fmt.Sprintf("%sseries-%d-%d", w.prefix, parity, i)
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
	id := fmt.Sprintf("index-%d", i)
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
	svc := fmt.Sprintf("%ssvc-%d", w.prefix, i)
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

func (w *seriesWriter) sendMeasure(ctx context.Context, addr string, rows []*measurev1.WriteRequest) {
	conn, err := w.conn(addr)
	if err != nil {
		g.GinkgoWriter.Printf("dial %s: %v\n", addr, err)
		return
	}
	wctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	stream, err := measurev1.NewMeasureServiceClient(conn).Write(wctx)
	if err != nil {
		g.GinkgoWriter.Printf("open measure write to %s: %v\n", addr, err)
		return
	}
	for _, row := range rows {
		if err = stream.Send(row); err != nil {
			g.GinkgoWriter.Printf("send measure row to %s: %v\n", addr, err)
			return
		}
	}
	_ = stream.CloseSend()
	for {
		resp, recvErr := stream.Recv()
		if recvErr != nil {
			if !errors.Is(recvErr, io.EOF) {
				g.GinkgoWriter.Printf("measure write to %s: %v\n", addr, recvErr)
			}
			return
		}
		w.count(resp.GetStatus())
	}
}

func (w *seriesWriter) sendStream(ctx context.Context, addr string, rows []*streamv1.WriteRequest) {
	conn, err := w.conn(addr)
	if err != nil {
		g.GinkgoWriter.Printf("dial %s: %v\n", addr, err)
		return
	}
	wctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	stream, err := streamv1.NewStreamServiceClient(conn).Write(wctx)
	if err != nil {
		g.GinkgoWriter.Printf("open stream write to %s: %v\n", addr, err)
		return
	}
	for _, row := range rows {
		if err = stream.Send(row); err != nil {
			g.GinkgoWriter.Printf("send stream row to %s: %v\n", addr, err)
			return
		}
	}
	_ = stream.CloseSend()
	for {
		resp, recvErr := stream.Recv()
		if recvErr != nil {
			if !errors.Is(recvErr, io.EOF) {
				g.GinkgoWriter.Printf("stream write to %s: %v\n", addr, recvErr)
			}
			return
		}
		w.count(resp.GetStatus())
	}
}

func str(v string) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: v}}}
}

func integer(v int64) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: v}}}
}
