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

// Command m4-seed seeds a live BanyanDB cluster with demo data for the
// Canopy M4 query console review. It covers all four catalogs (measure,
// stream, topn, trace) so the builder+results UI is exercised end-to-end.
//
// It uses native gRPC clients because BanyanDB only exposes data writes
// over streaming RPCs — the HTTP gateway serves query + schema but not write.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
)

const (
	// groupMeasure is named `sw_metric` to match the handoff design's
	// canonical group name (see .handoff-import/banyandb/project/data.jsx).
	// Renaming any other demo group is unnecessary: only the measure catalog
	// is in scope of the default /query render.
	groupMeasure = "sw_metric"
	groupStream  = "m4-stream"
	groupTrace   = "m4-traces"

	measureName      = "service_traffic"
	measureCPMName  = "service_cpm_minute" // canonical handoff-matched measure
	streamName       = "service_logs"
	traceName        = "service_spans"
	topnName         = "top_service"

	tagFamily = "default"
)

var (
	services     = []string{"frontend", "api-gateway", "orders", "cart", "checkout", "payments", "auth", "catalog"}
	regions      = []string{"us-east-1", "eu-west-1", "ap-northeast-1"}
	severities   = []string{"DEBUG", "INFO", "INFO", "INFO", "INFO", "WARN", "WARN", "ERROR"}
	endpoints    = []string{"/v1/login", "/v1/orders", "/v1/cart", "/v1/checkout", "/v1/payments", "/healthz"}
	httpStatuses = []int{200, 200, 200, 201, 204, 400, 401, 404, 500, 503}
)

// ignoreAlreadyExists returns true when err is a gRPC AlreadyExists or
// InvalidArgument that wraps a "resource already exists" message. BanyanDB
// returns either code depending on which service rejects the request.
func ignoreAlreadyExists(err error) bool {
	if err == nil {
		return false
	}
	st, ok := status.FromError(err)
	if !ok {
		return false
	}
	return st.Code() == codes.AlreadyExists ||
		(st.Code() == codes.InvalidArgument && errors.Is(err, err))
}

func main() {
	addr := flag.String("addr", "127.0.0.1:17912", "BanyanDB gRPC address")
	rows := flag.Int("rows", 80, "rows per resource")
	wipe := flag.Bool("wipe", false, "delete + recreate the m4 groups before seeding")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial %s: %v", *addr, err)
	}
	defer conn.Close()

	gc := databasev1.NewGroupRegistryServiceClient(conn)
	mc := databasev1.NewMeasureRegistryServiceClient(conn)
	sc := databasev1.NewStreamRegistryServiceClient(conn)
	tc := databasev1.NewTraceRegistryServiceClient(conn)
	topc := databasev1.NewTopNAggregationRegistryServiceClient(conn)
	mw := measurev1.NewMeasureServiceClient(conn)
	stq := streamv1.NewStreamServiceClient(conn)
	trw := tracev1.NewTraceServiceClient(conn)
	mq := measurev1.NewMeasureServiceClient(conn)

	log.Printf("m4-seed: addr=%s rows=%d wipe=%v", *addr, *rows, *wipe)

	if *wipe {
		for _, n := range []string{groupMeasure, groupStream, groupTrace} {
			if _, err := gc.Delete(ctx, &databasev1.GroupRegistryServiceDeleteRequest{Group: n, Force: true}); err != nil {
				log.Printf("delete group %s: %v (continuing)", n, err)
			}
		}
		log.Println("waiting 3s for group-delete flush...")
		time.Sleep(3 * time.Second)
	}

	groups := []struct {
		name, catalog string
	}{
		{groupMeasure, "CATALOG_MEASURE"},
		{groupStream, "CATALOG_STREAM"},
		{groupTrace, "CATALOG_TRACE"},
	}
	for _, g := range groups {
		_, err := gc.Create(ctx, &databasev1.GroupRegistryServiceCreateRequest{
			Group: &commonv1.Group{
				Metadata:  &commonv1.Metadata{Name: g.name},
				Catalog:   commonv1.Catalog(commonv1.Catalog_value[g.catalog]),
				ResourceOpts: &commonv1.ResourceOpts{
					ShardNum:        1,
					SegmentInterval: &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 1},
					Ttl:             &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 7},
				},
				UpdatedAt: timestamppb.Now(),
				CreatedAt: timestamppb.Now(),
			},
		})
		if err != nil && !ignoreAlreadyExists(err) {
			log.Fatalf("create group %s: %v", g.name, err)
		}
		if err != nil {
			log.Printf("group %s already exists (skipping)", g.name)
		}
		log.Printf("group %s (%s) created", g.name, g.catalog)
	}

	if _, err := mc.Create(ctx, &databasev1.MeasureRegistryServiceCreateRequest{
		Measure: &databasev1.Measure{
			Metadata: &commonv1.Metadata{Group: groupMeasure, Name: measureName},
			Entity:   &databasev1.Entity{TagNames: []string{"service", "instance"}},
			TagFamilies: []*databasev1.TagFamilySpec{{
				Name: tagFamily,
				Tags: []*databasev1.TagSpec{
					{Name: "service", Type: databasev1.TagType_TAG_TYPE_STRING},
					{Name: "instance", Type: databasev1.TagType_TAG_TYPE_STRING},
					{Name: "region", Type: databasev1.TagType_TAG_TYPE_STRING},
				},
			}},
			Fields: []*databasev1.FieldSpec{
				{Name: "request_count", FieldType: databasev1.FieldType_FIELD_TYPE_INT, EncodingMethod: databasev1.EncodingMethod_ENCODING_METHOD_GORILLA, CompressionMethod: databasev1.CompressionMethod_COMPRESSION_METHOD_ZSTD},
				{Name: "cpu_usage", FieldType: databasev1.FieldType_FIELD_TYPE_FLOAT, EncodingMethod: databasev1.EncodingMethod_ENCODING_METHOD_GORILLA, CompressionMethod: databasev1.CompressionMethod_COMPRESSION_METHOD_ZSTD},
				{Name: "memory_usage", FieldType: databasev1.FieldType_FIELD_TYPE_FLOAT, EncodingMethod: databasev1.EncodingMethod_ENCODING_METHOD_GORILLA, CompressionMethod: databasev1.CompressionMethod_COMPRESSION_METHOD_ZSTD},
				{Name: "latency_ms", FieldType: databasev1.FieldType_FIELD_TYPE_FLOAT, EncodingMethod: databasev1.EncodingMethod_ENCODING_METHOD_GORILLA, CompressionMethod: databasev1.CompressionMethod_COMPRESSION_METHOD_ZSTD},
			},
			Interval: "30s",
		},
	}); err != nil && !ignoreAlreadyExists(err) {
		log.Fatalf("create measure schema: %v", err)
	}
	log.Println("measure schema ready")

	// service_cpm_minute — canonical measure that mirrors the handoff design's
	// `sw_metric / service_cpm_minute` schema (entity_id, service_id,
	// service_name + total / value int fields). Used to drive the M4 Query
	// page-head rail + run results in a way that matches the handoff pixel
	// baseline.
	if _, err := mc.Create(ctx, &databasev1.MeasureRegistryServiceCreateRequest{
		Measure: &databasev1.Measure{
			Metadata: &commonv1.Metadata{Group: groupMeasure, Name: measureCPMName},
			Entity:   &databasev1.Entity{TagNames: []string{"entity_id"}},
			TagFamilies: []*databasev1.TagFamilySpec{{
				Name: tagFamily,
				Tags: []*databasev1.TagSpec{
					{Name: "entity_id", Type: databasev1.TagType_TAG_TYPE_STRING},
					{Name: "service_id", Type: databasev1.TagType_TAG_TYPE_STRING},
					{Name: "service_name", Type: databasev1.TagType_TAG_TYPE_STRING},
				},
			}},
			Fields: []*databasev1.FieldSpec{
				{Name: "total", FieldType: databasev1.FieldType_FIELD_TYPE_INT, EncodingMethod: databasev1.EncodingMethod_ENCODING_METHOD_GORILLA, CompressionMethod: databasev1.CompressionMethod_COMPRESSION_METHOD_ZSTD},
				{Name: "value", FieldType: databasev1.FieldType_FIELD_TYPE_INT, EncodingMethod: databasev1.EncodingMethod_ENCODING_METHOD_GORILLA, CompressionMethod: databasev1.CompressionMethod_COMPRESSION_METHOD_ZSTD},
			},
			Interval: "1m",
		},
	}); err != nil && !ignoreAlreadyExists(err) {
		log.Fatalf("create cpm measure schema: %v", err)
	}
	log.Println("cpm measure schema ready")

	if _, err := sc.Create(ctx, &databasev1.StreamRegistryServiceCreateRequest{
		Stream: &databasev1.Stream{
			Metadata: &commonv1.Metadata{Group: groupStream, Name: streamName},
			Entity:   &databasev1.Entity{TagNames: []string{"service"}},
			TagFamilies: []*databasev1.TagFamilySpec{{
				Name: tagFamily,
				Tags: []*databasev1.TagSpec{
					{Name: "service", Type: databasev1.TagType_TAG_TYPE_STRING},
					{Name: "severity", Type: databasev1.TagType_TAG_TYPE_STRING},
					{Name: "trace_id", Type: databasev1.TagType_TAG_TYPE_STRING},
					{Name: "span_id", Type: databasev1.TagType_TAG_TYPE_STRING},
					{Name: "endpoint", Type: databasev1.TagType_TAG_TYPE_STRING},
					{Name: "status_code", Type: databasev1.TagType_TAG_TYPE_STRING},
				},
			}},
		},
	}); err != nil && !ignoreAlreadyExists(err) {
		log.Fatalf("create stream schema: %v", err)
	}
	log.Println("stream schema ready")

	if _, err := tc.Create(ctx, &databasev1.TraceRegistryServiceCreateRequest{
		Trace: &databasev1.Trace{
			Metadata:         &commonv1.Metadata{Group: groupTrace, Name: traceName},
			Tags: []*databasev1.TraceTagSpec{
				{Name: "service", Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: "trace_id", Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: "span_id", Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: "parent_span_id", Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: "duration_ms", Type: databasev1.TagType_TAG_TYPE_INT},
				{Name: "status", Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: "endpoint", Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: "timestamp", Type: databasev1.TagType_TAG_TYPE_TIMESTAMP},
			},
			TraceIdTagName:   "trace_id",
			SpanIdTagName:    "span_id",
			TimestampTagName: "timestamp",
		},
	}); err != nil && !ignoreAlreadyExists(err) {
		log.Fatalf("create trace schema: %v", err)
	}
	log.Println("trace schema ready")

	irc := databasev1.NewIndexRuleRegistryServiceClient(conn)
	irbc := databasev1.NewIndexRuleBindingRegistryServiceClient(conn)
	// Tree index rules for every trace tag so the query builder can filter and
	// order by service, duration, timestamp, etc. as well as trace_id.
	traceIndexTags := []string{
		"service",
		"trace_id",
		"span_id",
		"parent_span_id",
		"duration_ms",
		"status",
		"endpoint",
		"timestamp",
	}
	beginAt := timestamppb.New(time.UnixMilli(baseTimestamp(*rows)).Truncate(time.Millisecond))
	expireAt := timestamppb.New(time.Now().Add(7 * 24 * time.Hour).Truncate(time.Millisecond))
	for _, tagName := range traceIndexTags {
		if _, err := irc.Create(ctx, &databasev1.IndexRuleRegistryServiceCreateRequest{
			IndexRule: &databasev1.IndexRule{
				Metadata: &commonv1.Metadata{Group: groupTrace, Name: tagName},
				Type:     databasev1.IndexRule_TYPE_TREE,
				Tags:     []string{tagName},
			},
		}); err != nil && !ignoreAlreadyExists(err) {
			log.Fatalf("create index rule %s: %v", tagName, err)
		}
		if _, err := irbc.Create(ctx, &databasev1.IndexRuleBindingRegistryServiceCreateRequest{
			IndexRuleBinding: &databasev1.IndexRuleBinding{
				Metadata: &commonv1.Metadata{Group: groupTrace, Name: tagName + "-on-spans"},
				Rules:    []string{tagName},
				Subject: &databasev1.Subject{
					Catalog: commonv1.Catalog(commonv1.Catalog_value["CATALOG_TRACE"]),
					Name:    traceName,
				},
				BeginAt:  beginAt,
				ExpireAt: expireAt,
			},
		}); err != nil && !ignoreAlreadyExists(err) {
			log.Fatalf("bind index rule %s: %v", tagName, err)
		}
	}
	log.Println("trace index rules ready")

	if _, err := topc.Create(ctx, &databasev1.TopNAggregationRegistryServiceCreateRequest{
		TopNAggregation: &databasev1.TopNAggregation{
			Metadata:        &commonv1.Metadata{Group: groupMeasure, Name: topnName},
			SourceMeasure:   &commonv1.Metadata{Group: groupMeasure, Name: measureName},
			FieldName:       "request_count",
			FieldValueSort:  modelv1.Sort_SORT_DESC,
			GroupByTagNames: []string{"service"},
			CountersNumber:  50,
			LruSize:         16,
		},
	}); err != nil && !ignoreAlreadyExists(err) {
		log.Fatalf("create topn-agg schema: %v", err)
	}
	log.Println("topn-agg schema ready")

	log.Println("waiting 8s for schema flush...")
	time.Sleep(8 * time.Second)

	baseTs := baseTimestamp(*rows)

	if err := writeMeasure(ctx, mw, baseTs, *rows); err != nil {
		log.Fatalf("write measure: %v", err)
	}
	if err := writeCpmMeasure(ctx, mw, baseTs, *rows); err != nil {
		log.Fatalf("write cpm measure: %v", err)
	}
	log.Printf("%d measure data points written", *rows)

	if err := writeStream(ctx, stq, baseTs, *rows); err != nil {
		log.Fatalf("write stream: %v", err)
	}
	log.Printf("%d stream elements written", *rows)

	if err := writeTrace(ctx, trw, baseTs, *rows); err != nil {
		log.Fatalf("write trace: %v", err)
	}
	log.Printf("trace spans written")

	log.Println("waiting 8s for data flush...")
	time.Sleep(8 * time.Second)

	mc2, err := countMeasure(ctx, mq, baseTs)
	if err != nil {
		log.Printf("measure count error: %v", err)
	} else {
		log.Printf("measure data_points visible: %d", mc2)
	}
	sc2, err := countStream(ctx, stq, baseTs)
	if err != nil {
		log.Printf("stream count error: %v", err)
	} else {
		log.Printf("stream elements visible: %d", sc2)
	}
	tc2, err := countTrace(ctx, trw, baseTs)
	if err != nil {
		log.Printf("trace count error: %v", err)
	} else {
		log.Printf("trace traces visible: %d", tc2)
	}

	log.Println("m4-seed complete.")
}

func writeMeasure(ctx context.Context, c measurev1.MeasureServiceClient, baseMs int64, rows int) error {
	rng := rand.New(rand.NewSource(42))
	w, err := c.Write(ctx)
	if err != nil {
		return err
	}
	md := &commonv1.Metadata{Group: groupMeasure, Name: measureName}
	for i := 0; i < rows; i++ {
		t := time.UnixMilli(baseMs + int64(i)*30_000)
		req := &measurev1.WriteRequest{
			Metadata:  md,
			MessageId: uint64(time.Now().UnixNano()),
			DataPoint: &measurev1.DataPointValue{
				Timestamp: timestamppb.New(t.Truncate(time.Millisecond)),
				TagFamilies: []*modelv1.TagFamilyForWrite{{Tags: []*modelv1.TagValue{
					{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: services[rng.Intn(len(services))]}}},
					{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: fmt.Sprintf("pod-%03d", i%8)}}},
					{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: regions[rng.Intn(len(regions))]}}},
				}}},
				Fields: []*modelv1.FieldValue{
					{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: int64(rng.Intn(4900) + 100)}}},
					{Value: &modelv1.FieldValue_Float{Float: &modelv1.Float{Value: 0.05 + rng.Float64()*0.9}}},
					{Value: &modelv1.FieldValue_Float{Float: &modelv1.Float{Value: 0.10 + rng.Float64()*0.85}}},
					{Value: &modelv1.FieldValue_Float{Float: &modelv1.Float{Value: 5.0 + rng.Float64()*495}}},
				},
			},
		}
		if err := w.Send(req); err != nil {
			return fmt.Errorf("send measure row %d: %w", i, err)
		}
	}
	if err := w.CloseSend(); err != nil {
		return err
	}
	return recvMeasureWrites(w)
}

// writeCpmMeasure writes the canonical service_cpm_minute rows that mirror
// the handoff's sw_metric / service_cpm_minute schema (entity_id + service_id
// + service_name + total / value). 80 rows = one row per minute over ~80
// minutes, which is enough to render a populated result view.
func writeCpmMeasure(ctx context.Context, c measurev1.MeasureServiceClient, baseMs int64, rows int) error {
	rng := rand.New(rand.NewSource(73))
	w, err := c.Write(ctx)
	if err != nil {
		return err
	}
	md := &commonv1.Metadata{Group: groupMeasure, Name: measureCPMName}
	for i := 0; i < rows; i++ {
		t := time.UnixMilli(baseMs + int64(i)*60_000)
		svc := services[rng.Intn(len(services))]
		req := &measurev1.WriteRequest{
			Metadata:  md,
			MessageId: uint64(time.Now().UnixNano()),
			DataPoint: &measurev1.DataPointValue{
				Timestamp: timestamppb.New(t.Truncate(time.Millisecond)),
				TagFamilies: []*modelv1.TagFamilyForWrite{{Tags: []*modelv1.TagValue{
					{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: svc + "/" + rngRand(rng)}}},
					{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: svc}}},
					{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: svc}}},
				}}},
				Fields: []*modelv1.FieldValue{
					{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: int64(rng.Intn(4900) + 100)}}},
					{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: int64(rng.Intn(99) + 1)}}},
				},
			},
		}
		if err := w.Send(req); err != nil {
			return fmt.Errorf("send cpm row %d: %w", i, err)
		}
	}
	if err := w.CloseSend(); err != nil {
		return err
	}
	return recvMeasureWrites(w)
}

func rngRand(r *rand.Rand) string {
	const chars = "0123456789abcdef"
	b := make([]byte, 8)
	for i := range b {
		b[i] = chars[r.Intn(len(chars))]
	}
	return string(b)
}

func writeStream(ctx context.Context, c streamv1.StreamServiceClient, baseMs int64, rows int) error {
	rng := rand.New(rand.NewSource(73))
	w, err := c.Write(ctx)
	if err != nil {
		return err
	}
	md := &commonv1.Metadata{Group: groupStream, Name: streamName}
	for i := 0; i < rows; i++ {
		t := time.UnixMilli(baseMs + int64(i)*15_000)
		req := &streamv1.WriteRequest{
			Metadata:  md,
			MessageId: uint64(time.Now().UnixNano()),
			Element: &streamv1.ElementValue{
				ElementId: fmt.Sprintf("log-%05d", i),
				Timestamp: timestamppb.New(t.Truncate(time.Millisecond)),
				TagFamilies: []*modelv1.TagFamilyForWrite{{Tags: []*modelv1.TagValue{
					{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: services[rng.Intn(len(services))]}}},
					{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: severities[rng.Intn(len(severities))]}}},
					{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: fmt.Sprintf("%016x", rng.Uint64())}}},
					{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: fmt.Sprintf("%08x", rng.Uint32())}}},
					{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: endpoints[rng.Intn(len(endpoints))]}}},
					{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: fmt.Sprintf("%d", httpStatuses[rng.Intn(len(httpStatuses))])}}},
				}}},
			},
		}
		if err := w.Send(req); err != nil {
			return fmt.Errorf("send stream row %d: %w", i, err)
		}
	}
	if err := w.CloseSend(); err != nil {
		return err
	}
	return recvStreamWrites(w)
}

func writeTrace(ctx context.Context, c tracev1.TraceServiceClient, baseMs int64, rows int) error {
	rng := rand.New(rand.NewSource(101))
	w, err := c.Write(ctx)
	if err != nil {
		return err
	}
	md := &commonv1.Metadata{Group: groupTrace, Name: traceName}
	for i := 0; i < rows; i++ {
		traceID := fmt.Sprintf("%08x", i)
		svc := services[rng.Intn(len(services))]
		ep := endpoints[rng.Intn(len(endpoints))]
		st := "OK"
		if rng.Float64() < 0.05 {
			st = "ERROR"
		}
		duration := int64(20 + rng.Intn(780))
		span := fmt.Sprintf("%08x", rng.Uint32())
		t := time.UnixMilli(baseMs + int64(i)*60_000)
		req := &tracev1.WriteRequest{
			Metadata: md,
			Version:  uint64(time.Now().UnixNano()),
			TagSpec:  &tracev1.TagSpec{TagNames: []string{"service", "trace_id", "span_id", "parent_span_id", "duration_ms", "status", "endpoint", "timestamp"}},
			Tags: []*modelv1.TagValue{
				{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: svc}}},
				{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: traceID}}},
				{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: span}}},
				{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: ""}}},
				{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: duration}}},
				{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: st}}},
				{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: ep}}},
				{Value: &modelv1.TagValue_Timestamp{Timestamp: timestamppb.New(t.Truncate(time.Millisecond))}},
			},
		}
		if err := w.Send(req); err != nil {
			return fmt.Errorf("send trace row %d: %w", i, err)
		}
	}
	if err := w.CloseSend(); err != nil {
		return err
	}
	return recvTraceWrites(w)
}

// recvMeasureWrites drains a measure-v1 streaming Write RPC. BanyanDB
// returns per-row rejections via Status != STATUS_SUCCEED; transport-level
// errors propagate as RunErr.
func recvMeasureWrites(w measurev1.MeasureService_WriteClient) error {
	for {
		resp, err := w.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if resp.GetStatus() != "STATUS_SUCCEED" {
			return fmt.Errorf("measure write status=%s msg_id=%d", resp.GetStatus(), resp.GetMessageId())
		}
	}
}

func recvStreamWrites(w streamv1.StreamService_WriteClient) error {
	for {
		resp, err := w.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if resp.GetStatus() != "STATUS_SUCCEED" {
			return fmt.Errorf("stream write status=%s msg_id=%d", resp.GetStatus(), resp.GetMessageId())
		}
	}
}

func recvTraceWrites(w tracev1.TraceService_WriteClient) error {
	for {
		resp, err := w.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if resp.GetStatus() != "STATUS_SUCCEED" {
			return fmt.Errorf("trace write status=%s version=%d", resp.GetStatus(), resp.GetVersion())
		}
	}
}

func countMeasure(ctx context.Context, c measurev1.MeasureServiceClient, baseMs int64) (int, error) {
	resp, err := c.Query(ctx, &measurev1.QueryRequest{
		Groups: []string{groupMeasure},
		Name:   measureName,
		TimeRange: &modelv1.TimeRange{
			Begin: timestamppb.New(time.UnixMilli(baseMs - 60_000).Truncate(time.Millisecond)),
			End:   timestamppb.New(time.Now().Add(60_000 * time.Millisecond).Truncate(time.Millisecond)),
		},
		Limit: 1000,
		TagProjection: &modelv1.TagProjection{
			TagFamilies: []*modelv1.TagProjection_TagFamily{{Name: tagFamily, Tags: []string{"service"}}},
		},
	})
	if err != nil {
		return 0, err
	}
	return len(resp.GetDataPoints()), nil
}

func countStream(ctx context.Context, c streamv1.StreamServiceClient, baseMs int64) (int, error) {
	resp, err := c.Query(ctx, &streamv1.QueryRequest{
		Groups: []string{groupStream},
		Name:   streamName,
		TimeRange: &modelv1.TimeRange{
			Begin: timestamppb.New(time.UnixMilli(baseMs - 60_000).Truncate(time.Millisecond)),
			End:   timestamppb.New(time.Now().Add(60_000 * time.Millisecond).Truncate(time.Millisecond)),
		},
		Limit: 1000,
		Projection: &modelv1.TagProjection{
			TagFamilies: []*modelv1.TagProjection_TagFamily{{Name: tagFamily, Tags: []string{"service"}}},
		},
	})
	if err != nil {
		return 0, err
	}
	return len(resp.GetElements()), nil
}

func countTrace(ctx context.Context, c tracev1.TraceServiceClient, baseMs int64) (int, error) {
	resp, err := c.Query(ctx, &tracev1.QueryRequest{
		Groups: []string{groupTrace},
		Name:   traceName,
		TimeRange: &modelv1.TimeRange{
			Begin: timestamppb.New(time.UnixMilli(baseMs - 60_000).Truncate(time.Millisecond)),
			End:   timestamppb.New(time.Now().Add(60_000 * time.Millisecond).Truncate(time.Millisecond)),
		},
		OrderBy:        &modelv1.QueryOrder{Sort: modelv1.Sort_SORT_DESC},
		Limit:          1000,
		TagProjection:  []string{"service", "trace_id", "span_id", "duration_ms", "status", "endpoint"},
	})
	if err != nil {
		return 0, err
	}
	return len(resp.GetTraces()), nil
}

func baseTimestamp(rows int) int64 {
	now := time.Now().UnixMilli()
	// Start just far enough in the past so the full batch lands within the
	// default "Last 30 minutes" relative range in the Canopy query builder.
	return now - int64(rows)*30_000 - 60_000
}
