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
	"encoding/base64"
	"fmt"
	"time"

	g "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/banyand/queue/pub"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/partition"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
)

const (
	directMeasureSeries = 400
	directStreamSeries  = 100
)

// directPublisher sends internal write requests straight to one data node, the way the lifecycle row replay does:
// a queue client without metadata, the data node registered by hand and a batch publisher.
type directPublisher struct {
	client   queue.Client
	nodeName string
}

func newDirectPublisher(dataAddr string) *directPublisher {
	conn, err := grpchelper.Conn(dataAddr, 10*time.Second, grpc.WithTransportCredentials(insecure.NewCredentials()))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	defer func() { _ = conn.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), flags.EventuallyTimeout)
	defer cancel()
	current, err := databasev1.NewNodeQueryServiceClient(conn).GetCurrentNode(ctx, &databasev1.GetCurrentNodeRequest{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	client := pub.NewWithoutMetadata(nil)
	client.OnAddOrUpdate(schema.Metadata{TypeMeta: schema.TypeMeta{Kind: schema.KindNode}, Spec: current.GetNode()})
	return &directPublisher{client: client, nodeName: current.GetNode().GetMetadata().GetName()}
}

func (p *directPublisher) close() {
	p.client.GracefulStop()
}

// publish sends the requests to topic in one batch and returns an error unless the data node acknowledges all of them.
func (p *directPublisher) publish(topic bus.Topic, requests []any) error {
	publisher := p.client.NewBatchPublisher(30 * time.Second)
	messages := make([]bus.Message, 0, len(requests))
	for _, r := range requests {
		messages = append(messages, bus.NewBatchMessageWithNode(bus.MessageID(time.Now().UnixNano()), p.nodeName, r))
	}
	_, publishErr := publisher.Publish(context.Background(), topic, messages...)
	failures, closeErr := publisher.Close()
	if publishErr != nil {
		return publishErr
	}
	if closeErr != nil {
		return closeErr
	}
	if len(failures) > 0 {
		return fmt.Errorf("nodes that rejected the batch: %v", failures)
	}
	return nil
}

// directMeasureRequests builds one row for each of count series with the given id prefix, routed with the schema of the measure.
func directMeasureRequests(conn *grpc.ClientConn, prefix string, count int, at time.Time) []any {
	ctx, cancel := context.WithTimeout(context.Background(), flags.EventuallyTimeout)
	defer cancel()
	resp, err := databasev1.NewMeasureRegistryServiceClient(conn).Get(ctx,
		&databasev1.MeasureRegistryServiceGetRequest{Metadata: &commonv1.Metadata{Group: measureGroup, Name: measureName}})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	m := resp.GetMeasure()
	locator := partition.NewEntityLocator(m.GetTagFamilies(), m.GetEntity(), m.GetMetadata().GetModRevision())
	requests := make([]any, 0, count)
	for i := range count {
		id := fmt.Sprintf("%s-%d", prefix, i)
		row := &measurev1.WriteRequest{
			MessageId: uint64(time.Now().UnixNano()) + uint64(i),
			Metadata:  &commonv1.Metadata{Group: measureGroup, Name: measureName},
			DataPoint: &measurev1.DataPointValue{
				Timestamp:   timestamppb.New(at),
				Version:     1,
				TagFamilies: []*modelv1.TagFamilyForWrite{{Tags: []*modelv1.TagValue{str(id), str(id)}}},
				Fields: []*modelv1.FieldValue{
					{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: 100}}},
					{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: 1}}},
				},
			},
		}
		values, shardID, routeErr := partition.ApplyLocators(measureName, row.GetDataPoint().GetTagFamilies(), locator, nil, shardCount)
		gomega.Expect(routeErr).NotTo(gomega.HaveOccurred())
		requests = append(requests, &measurev1.InternalWriteRequest{ShardId: uint32(shardID), EntityValues: values[1:].Encode(), Request: row})
	}
	return requests
}

// directStreamRequests builds one element for each of count series with the given id prefix, routed with the schema of the stream.
// The element ids start after idBase: elements of different nodes with the same id are merged by a query.
func directStreamRequests(conn *grpc.ClientConn, prefix string, idBase, count int, at time.Time) []any {
	ctx, cancel := context.WithTimeout(context.Background(), flags.EventuallyTimeout)
	defer cancel()
	resp, err := databasev1.NewStreamRegistryServiceClient(conn).Get(ctx,
		&databasev1.StreamRegistryServiceGetRequest{Metadata: &commonv1.Metadata{Group: streamGroup, Name: streamName}})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	s := resp.GetStream()
	locator := partition.NewEntityLocator(s.GetTagFamilies(), s.GetEntity(), s.GetMetadata().GetModRevision())
	requests := make([]any, 0, count)
	for i := range count {
		svc := fmt.Sprintf("%s-svc-%d", prefix, i)
		rawID := uint64(idBase + i + 1)
		row := &streamv1.WriteRequest{
			MessageId: uint64(time.Now().UnixNano()) + uint64(i),
			Metadata:  &commonv1.Metadata{Group: streamGroup, Name: streamName},
			Element: &streamv1.ElementValue{
				ElementId: base64.StdEncoding.EncodeToString(convert.Uint64ToBytes(rawID)),
				Timestamp: timestamppb.New(at),
				TagFamilies: []*modelv1.TagFamilyForWrite{
					{Tags: []*modelv1.TagValue{{Value: &modelv1.TagValue_BinaryData{BinaryData: []byte(svc)}}}},
					{Tags: []*modelv1.TagValue{str(svc), integer(0), str(svc), str("instance"), str("/endpoint"), integer(10), integer(1)}},
				},
			},
		}
		values, shardID, routeErr := partition.ApplyLocators(streamName, row.GetElement().GetTagFamilies(), locator, nil, shardCount)
		gomega.Expect(routeErr).NotTo(gomega.HaveOccurred())
		requests = append(requests, &streamv1.InternalWriteRequest{
			ShardId: uint32(shardID), EntityValues: values[1:].Encode(), Request: row, RawElementId: rawID,
		})
	}
	return requests
}

// directRows counts the rows in [at-1s, at+1s] of the measure and the stream through a liaison.
// The rows of the series writer are written one minute before the current time, so they are outside the range.
func directRows(conn *grpc.ClientConn, at time.Time) (measureRows, streamRows int) {
	tr := &modelv1.TimeRange{Begin: timestamppb.New(at.Add(-time.Second)), End: timestamppb.New(at.Add(time.Second))}
	ctx, cancel := context.WithTimeout(context.Background(), flags.EventuallyTimeout)
	defer cancel()
	mresp, err := measurev1.NewMeasureServiceClient(conn).Query(ctx, &measurev1.QueryRequest{
		Groups:    []string{measureGroup},
		Name:      measureName,
		TimeRange: tr,
		TagProjection: &modelv1.TagProjection{TagFamilies: []*modelv1.TagProjection_TagFamily{
			{Name: "default", Tags: []string{"id", "entity_id"}},
		}},
		FieldProjection: &measurev1.QueryRequest_FieldProjection{Names: []string{"total", "value"}},
		Limit:           10000,
	})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	sresp, err := streamv1.NewStreamServiceClient(conn).Query(ctx, &streamv1.QueryRequest{
		Groups:    []string{streamGroup},
		Name:      streamName,
		TimeRange: tr,
		Projection: &modelv1.TagProjection{TagFamilies: []*modelv1.TagProjection_TagFamily{
			{Name: "searchable", Tags: []string{"trace_id", "service_id"}},
		}},
		Limit: 10000,
	})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	return len(mresp.GetDataPoints()), len(sresp.GetElements())
}

// liaisonConn dials the first liaison and closes the connection when the spec ends.
func liaisonConn() *grpc.ClientConn {
	conn, err := grpchelper.Conn(liaisons[0].GRPCAddr, 10*time.Second, grpc.WithTransportCredentials(insecure.NewCredentials()))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	g.DeferCleanup(func() { _ = conn.Close() })
	return conn
}

// directRowsSpec sends rows of fresh series straight to the data nodes. A data node subscribes to TopicMeasureWrite and TopicStreamWrite
// with the write callback that also serves the lifecycle row replay. Neither its metrics nor the liaisons' may change when rows arrive there.
// The series writer of the suite keeps running, so the values of the liaisons stay in every window.
func directRowsSpec() {
	conn := liaisonConn()
	at := time.Now().Add(-30 * time.Minute).Truncate(time.Millisecond)
	perNode := len(dataNodes)
	for i, n := range dataNodes {
		direct := newDirectPublisher(n.GRPCAddr)
		g.DeferCleanup(direct.close)
		prefix := fmt.Sprintf("direct%d", i)
		measureRequests := directMeasureRequests(conn, prefix, directMeasureSeries/perNode, at)
		streamRequests := directStreamRequests(conn, prefix, i*(directStreamSeries/perNode), directStreamSeries/perNode, at)
		// The queue client connects in the background, so the first batch may find no healthy node.
		gomega.Eventually(func() error {
			return direct.publish(data.TopicMeasureWrite, measureRequests)
		}, flags.EventuallyTimeout, time.Second).Should(gomega.Succeed())
		gomega.Expect(direct.publish(data.TopicStreamWrite, streamRequests)).To(gomega.Succeed())
	}

	// The rows are in the data nodes: a query through a liaison returns them.
	gomega.Eventually(func(gm gomega.Gomega) {
		measureRows, streamRows := directRows(conn, at)
		gm.Expect(measureRows).To(gomega.Equal(directMeasureSeries))
		gm.Expect(streamRows).To(gomega.Equal(directStreamSeries))
	}, 3*flags.EventuallyTimeout, time.Second).Should(gomega.Succeed())

	gomega.Consistently(func(gm gomega.Gomega) {
		expectDataNodesWithoutActiveSeries(gm)
		expectAggregate(gm, measureActiveSeries, measureGroup, measureSeries)
		expectAggregate(gm, streamActiveSeries, streamGroup, streamSeries)
	}, observe, time.Second).Should(gomega.Succeed())
}
