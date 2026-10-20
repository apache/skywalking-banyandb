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

package schemasync

import (
	"bytes"
	"context"
	"hash/crc32"
	"maps"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
)

// fakeRegistry answers the schema registries' List calls from in-memory resources and
// records the "<kind>/<group>" of every call.
type fakeRegistry struct {
	failKind   string // List of this kind answers UNAVAILABLE
	calls      []string
	streams    []*databasev1.Stream
	measures   []*databasev1.Measure
	traces     []*databasev1.Trace
	properties []*databasev1.Property
	rules      []*databasev1.IndexRule
	bindings   []*databasev1.IndexRuleBinding
	topNs      []*databasev1.TopNAggregation
	mu         sync.Mutex
}

func (r *fakeRegistry) record(kind, group string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, kind+"/"+group)
	if kind == r.failKind {
		return status.Errorf(codes.Unavailable, "%s registry unavailable (injected)", kind)
	}
	return nil
}

func inGroup[T interface{ GetMetadata() *commonv1.Metadata }](items []T, group string) []T {
	var out []T
	for _, it := range items {
		if it.GetMetadata().GetGroup() == group {
			out = append(out, it)
		}
	}
	return out
}

type (
	streamRegistry struct {
		databasev1.UnimplementedStreamRegistryServiceServer
		r *fakeRegistry
	}
	measureRegistry struct {
		databasev1.UnimplementedMeasureRegistryServiceServer
		r *fakeRegistry
	}
	traceRegistry struct {
		databasev1.UnimplementedTraceRegistryServiceServer
		r *fakeRegistry
	}
	propertyRegistry struct {
		databasev1.UnimplementedPropertyRegistryServiceServer
		r *fakeRegistry
	}
	indexRuleRegistry struct {
		databasev1.UnimplementedIndexRuleRegistryServiceServer
		r *fakeRegistry
	}
	bindingRegistry struct {
		databasev1.UnimplementedIndexRuleBindingRegistryServiceServer
		r *fakeRegistry
	}
	topNRegistry struct {
		databasev1.UnimplementedTopNAggregationRegistryServiceServer
		r *fakeRegistry
	}
)

func (s streamRegistry) List(_ context.Context, req *databasev1.StreamRegistryServiceListRequest) (*databasev1.StreamRegistryServiceListResponse, error) {
	if err := s.r.record(KindStream, req.GetGroup()); err != nil {
		return nil, err
	}
	return &databasev1.StreamRegistryServiceListResponse{Stream: inGroup(s.r.streams, req.GetGroup())}, nil
}

func (s measureRegistry) List(_ context.Context, req *databasev1.MeasureRegistryServiceListRequest) (*databasev1.MeasureRegistryServiceListResponse, error) {
	if err := s.r.record(KindMeasure, req.GetGroup()); err != nil {
		return nil, err
	}
	return &databasev1.MeasureRegistryServiceListResponse{Measure: inGroup(s.r.measures, req.GetGroup())}, nil
}

func (s traceRegistry) List(_ context.Context, req *databasev1.TraceRegistryServiceListRequest) (*databasev1.TraceRegistryServiceListResponse, error) {
	if err := s.r.record(KindTrace, req.GetGroup()); err != nil {
		return nil, err
	}
	return &databasev1.TraceRegistryServiceListResponse{Trace: inGroup(s.r.traces, req.GetGroup())}, nil
}

func (s propertyRegistry) List(_ context.Context, req *databasev1.PropertyRegistryServiceListRequest) (*databasev1.PropertyRegistryServiceListResponse, error) {
	if err := s.r.record(KindProperty, req.GetGroup()); err != nil {
		return nil, err
	}
	return &databasev1.PropertyRegistryServiceListResponse{Properties: inGroup(s.r.properties, req.GetGroup())}, nil
}

func (s indexRuleRegistry) List(_ context.Context, req *databasev1.IndexRuleRegistryServiceListRequest) (*databasev1.IndexRuleRegistryServiceListResponse, error) {
	if err := s.r.record(KindIndexRule, req.GetGroup()); err != nil {
		return nil, err
	}
	return &databasev1.IndexRuleRegistryServiceListResponse{IndexRule: inGroup(s.r.rules, req.GetGroup())}, nil
}

func (s bindingRegistry) List(_ context.Context, req *databasev1.IndexRuleBindingRegistryServiceListRequest) (
	*databasev1.IndexRuleBindingRegistryServiceListResponse, error,
) {
	if err := s.r.record(KindIndexRuleBinding, req.GetGroup()); err != nil {
		return nil, err
	}
	return &databasev1.IndexRuleBindingRegistryServiceListResponse{IndexRuleBinding: inGroup(s.r.bindings, req.GetGroup())}, nil
}

func (s topNRegistry) List(_ context.Context, req *databasev1.TopNAggregationRegistryServiceListRequest) (
	*databasev1.TopNAggregationRegistryServiceListResponse, error,
) {
	if err := s.r.record(KindTopNAggregation, req.GetGroup()); err != nil {
		return nil, err
	}
	return &databasev1.TopNAggregationRegistryServiceListResponse{TopNAggregation: inGroup(s.r.topNs, req.GetGroup())}, nil
}

// serve starts the fake registries on 127.0.0.1:0 and returns a client connection to them.
func serve(t *testing.T, r *fakeRegistry) *grpc.ClientConn {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	databasev1.RegisterStreamRegistryServiceServer(srv, streamRegistry{r: r})
	databasev1.RegisterMeasureRegistryServiceServer(srv, measureRegistry{r: r})
	databasev1.RegisterTraceRegistryServiceServer(srv, traceRegistry{r: r})
	databasev1.RegisterPropertyRegistryServiceServer(srv, propertyRegistry{r: r})
	databasev1.RegisterIndexRuleRegistryServiceServer(srv, indexRuleRegistry{r: r})
	databasev1.RegisterIndexRuleBindingRegistryServiceServer(srv, bindingRegistry{r: r})
	databasev1.RegisterTopNAggregationRegistryServiceServer(srv, topNRegistry{r: r})
	//panicdiag:allow-rawgo test server
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func md(group, name string, rev int64) *commonv1.Metadata {
	return &commonv1.Metadata{Group: group, Name: name, ModRevision: rev}
}

func group(name string, catalog commonv1.Catalog) *commonv1.Group {
	return &commonv1.Group{Metadata: &commonv1.Metadata{Name: name}, Catalog: catalog}
}

// registry holds one group of every catalog, each with resources and index rules.
func registry() (*fakeRegistry, []*commonv1.Group) {
	r := &fakeRegistry{
		streams:    []*databasev1.Stream{{Metadata: md("sw_record", "log", 3)}},
		measures:   []*databasev1.Measure{{Metadata: md("sw_metric", "service_cpm", 7)}, {Metadata: md("sw_metric", "service_idx", 8), IndexMode: true}},
		traces:     []*databasev1.Trace{{Metadata: md("sw_trace", "segment", 9)}},
		properties: []*databasev1.Property{{Metadata: md("ui", "template", 5)}},
		rules:      []*databasev1.IndexRule{{Metadata: md("sw_metric", "r1", 1)}, {Metadata: md("sw_record", "r2", 1)}, {Metadata: md("sw_trace", "r3", 1)}},
		bindings:   []*databasev1.IndexRuleBinding{{Metadata: md("sw_record", "b1", 1)}},
		topNs:      []*databasev1.TopNAggregation{{Metadata: md("sw_metric", "top", 1)}},
	}
	groups := []*commonv1.Group{
		group("sw_record", commonv1.Catalog_CATALOG_STREAM),
		group("sw_metric", commonv1.Catalog_CATALOG_MEASURE),
		group("sw_trace", commonv1.Catalog_CATALOG_TRACE),
		group("ui", commonv1.Catalog_CATALOG_PROPERTY),
	}
	return r, groups
}

func TestExport_FilesMatchTheBytesOnDisk(t *testing.T) {
	r, groups := registry()
	dir := t.TempDir()
	res, err := Export(context.Background(), serve(t, r), groups, dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Files) != len(Kinds) {
		t.Fatalf("one file per kind, got %+v", res.Files)
	}
	wantLines := map[string]int{
		KindGroup: 4, KindStream: 1, KindMeasure: 2, KindTrace: 1, KindProperty: 1,
		KindIndexRule: 3, KindIndexRuleBinding: 1, KindTopNAggregation: 1,
	}
	for i, f := range res.Files {
		if f.Path != "schema/"+Kinds[i]+".jsonl" {
			t.Fatalf("file %d is %s, want the %s kind", i, f.Path, Kinds[i])
		}
		body, readErr := os.ReadFile(filepath.Join(dir, filepath.FromSlash(f.Path)))
		if readErr != nil {
			t.Fatal(readErr)
		}
		if f.Bytes != uint64(len(body)) || f.CRC32 != crc32.ChecksumIEEE(body) {
			t.Fatalf("%s: recorded %d bytes crc32 %08x, on disk %d bytes crc32 %08x", f.Path, f.Bytes, f.CRC32, len(body), crc32.ChecksumIEEE(body))
		}
		if got := bytes.Count(body, []byte("\n")); got != wantLines[Kinds[i]] {
			t.Fatalf("%s: %d lines, want %d", f.Path, got, wantLines[Kinds[i]])
		}
	}
	if got := res.GroupRevisions(commonv1.Catalog_CATALOG_TRACE, "sw_trace"); !maps.Equal(got, map[string]int64{"segment": 9}) {
		t.Fatalf("trace mod revisions = %v", got)
	}
	if got := res.GroupRevisions(commonv1.Catalog_CATALOG_PROPERTY, "ui"); !maps.Equal(got, map[string]int64{"template": 5}) {
		t.Fatalf("property mod revisions = %v", got)
	}
}

func TestExport_ListsOnlyTheKindsOfEachCatalog(t *testing.T) {
	r, groups := registry()
	if _, err := Export(context.Background(), serve(t, r), groups, t.TempDir()); err != nil {
		t.Fatal(err)
	}
	byGroup := map[string][]string{}
	for _, c := range r.calls {
		kind, g, _ := strings.Cut(c, "/")
		byGroup[g] = append(byGroup[g], kind)
	}
	want := map[string][]string{
		"sw_record": {KindIndexRule, KindIndexRuleBinding, KindStream},
		"sw_metric": {KindIndexRule, KindIndexRuleBinding, KindMeasure, KindTopNAggregation},
		"sw_trace":  {KindIndexRule, KindIndexRuleBinding, KindTrace},
		"ui":        {KindProperty},
	}
	for g, kinds := range want {
		got := byGroup[g]
		sort.Strings(got)
		if strings.Join(got, ",") != strings.Join(kinds, ",") {
			t.Fatalf("group %s listed %v, want %v", g, got, kinds)
		}
	}
	if len(byGroup) != len(want) {
		t.Fatalf("calls = %v", r.calls)
	}
}

func TestExport_RegistryErrorWritesNoFile(t *testing.T) {
	r, groups := registry()
	r.failKind = KindTrace
	dir := t.TempDir()
	_, err := Export(context.Background(), serve(t, r), groups, dir)
	if err == nil || !strings.Contains(err.Error(), "list trace of group sw_trace") {
		t.Fatalf("want the registry error, got %v", err)
	}
	if _, statErr := os.Stat(filepath.Join(dir, Dir)); !os.IsNotExist(statErr) {
		t.Fatalf("nothing is written before every list succeeded: %v", statErr)
	}
}

// A message that cannot be encoded after another one was written removes the partial file.
func TestWriteKind_ErrorRemovesThePartialFile(t *testing.T) {
	dir := t.TempDir()
	msgs := []proto.Message{
		&databasev1.Stream{Metadata: md("g", "ok", 1)},
		&databasev1.Stream{Metadata: md("g", "bad\xff", 1)}, // invalid UTF-8 fails protojson
	}
	if _, err := writeKind(dir, KindStream, msgs); err == nil || !strings.Contains(err.Error(), "encode schema/stream.jsonl") {
		t.Fatalf("want an encode error, got %v", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("a failed write leaves %v behind", entries)
	}
	// The kind can be written again once the cause is gone.
	f, err := writeKind(dir, KindStream, msgs[:1])
	if err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(filepath.Join(dir, "stream.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if f.Bytes != uint64(len(body)) || f.CRC32 != crc32.ChecksumIEEE(body) {
		t.Fatalf("recorded %d bytes crc32 %08x, on disk %d bytes crc32 %08x", f.Bytes, f.CRC32, len(body), crc32.ChecksumIEEE(body))
	}
}
