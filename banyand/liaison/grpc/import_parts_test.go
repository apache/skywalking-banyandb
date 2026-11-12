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
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/apache/skywalking-banyandb/api/data"
	apiversion "github.com/apache/skywalking-banyandb/api/proto/banyandb"
	clusterv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/cluster/v1"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/transfer"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/nativefmt"
)

type partInfo = transferv1.ImportPartsRequest_PartInfo

// tooLargePartsStream fails its first receive as a frame over the server's message size does.
type tooLargePartsStream struct {
	fakePartsStream
}

func (f *tooLargePartsStream) Recv() (*transferv1.ImportPartsRequest, error) {
	return nil, status.Error(codes.ResourceExhausted, "grpc: received message larger than max (9 vs. 8)")
}

func TestImportParts_MessageTooLargeDetail(t *testing.T) {
	svc := standalonePartsService(&recordingSync{})
	err := svc.ImportParts(&tooLargePartsStream{fakePartsStream{ctx: context.Background()}})
	wantCode(t, err, codes.ResourceExhausted, "larger than max")
	wantErrorInfo(t, err, transfer.ImportReasonMessageTooLarge, nil)
}

func TestSyncParts(t *testing.T) {
	core := func(id uint64) *partInfo { return &partInfo{Id: id, MinTimestamp: 10, TotalCount: 3} }
	sec := func(id uint64, typ string) *partInfo { return &partInfo{Id: id, PartType: typ, MinTimestamp: 10} }
	cases := []struct {
		name    string
		init    *transferv1.ImportPartsRequest_Init
		wantErr string
		want    int
	}{
		{name: "property placeholder", init: &transferv1.ImportPartsRequest_Init{Kind: nativefmt.KindProperty}, want: 1},
		{name: "property with a part", init: &transferv1.ImportPartsRequest_Init{Kind: nativefmt.KindProperty, Parts: []*partInfo{{}}}, wantErr: "no part entries"},
		{name: "index ok", init: &transferv1.ImportPartsRequest_Init{Kind: nativefmt.KindSeriesIndex, Parts: []*partInfo{{MinTimestamp: 5}}}, want: 1},
		{name: "index without time", init: &transferv1.ImportPartsRequest_Init{Kind: nativefmt.KindElementIndex, Parts: []*partInfo{{}}}, wantErr: "time range"},
		{name: "index with an id", init: &transferv1.ImportPartsRequest_Init{Kind: nativefmt.KindSeriesIndex, Parts: []*partInfo{{Id: 3, MinTimestamp: 5}}}, wantErr: "id 0"},
		{
			name: "index with a part type", wantErr: "no part type",
			init: &transferv1.ImportPartsRequest_Init{Kind: nativefmt.KindElementIndex, Parts: []*partInfo{{PartType: "x", MinTimestamp: 5}}},
		},
		{name: "index without part", init: &transferv1.ImportPartsRequest_Init{Kind: nativefmt.KindSeriesIndex}, wantErr: "time range"},
		{name: "part ok", init: &transferv1.ImportPartsRequest_Init{Catalog: commonv1.Catalog_CATALOG_STREAM, Kind: nativefmt.KindPart, Parts: []*partInfo{core(1)}}, want: 1},
		{
			name: "trace with secondary", want: 2,
			init: &transferv1.ImportPartsRequest_Init{Catalog: commonv1.Catalog_CATALOG_TRACE, Kind: nativefmt.KindPart, Parts: []*partInfo{core(1), sec(1, "dur")}},
		},
		{
			name: "stream with secondary", wantErr: "no secondary",
			init: &transferv1.ImportPartsRequest_Init{Catalog: commonv1.Catalog_CATALOG_STREAM, Kind: nativefmt.KindPart, Parts: []*partInfo{core(1), sec(1, "dur")}},
		},
		{name: "no parts", init: &transferv1.ImportPartsRequest_Init{Kind: nativefmt.KindPart}, wantErr: "core part first"},
		{
			name: "secondary first", wantErr: "core part first",
			init: &transferv1.ImportPartsRequest_Init{Catalog: commonv1.Catalog_CATALOG_TRACE, Kind: nativefmt.KindPart, Parts: []*partInfo{sec(1, "dur")}},
		},
		{
			name: "two ids", wantErr: "one part id",
			init: &transferv1.ImportPartsRequest_Init{Catalog: commonv1.Catalog_CATALOG_TRACE, Kind: nativefmt.KindPart, Parts: []*partInfo{core(1), sec(2, "dur")}},
		},
		{
			name: "duplicate type", wantErr: "twice",
			init: &transferv1.ImportPartsRequest_Init{Catalog: commonv1.Catalog_CATALOG_TRACE, Kind: nativefmt.KindPart, Parts: []*partInfo{core(1), sec(1, "d"), sec(1, "d")}},
		},
		{
			name: "secondary named core", wantErr: "invalid secondary",
			init: &transferv1.ImportPartsRequest_Init{Catalog: commonv1.Catalog_CATALOG_TRACE, Kind: nativefmt.KindPart, Parts: []*partInfo{core(1), sec(1, "core")}},
		},
		{
			name: "secondary escaping the sidx directory", wantErr: "invalid secondary",
			init: &transferv1.ImportPartsRequest_Init{Catalog: commonv1.Catalog_CATALOG_TRACE, Kind: nativefmt.KindPart, Parts: []*partInfo{core(1), sec(1, "..")}},
		},
		{
			name: "secondary with a path", wantErr: "invalid secondary",
			init: &transferv1.ImportPartsRequest_Init{Catalog: commonv1.Catalog_CATALOG_TRACE, Kind: nativefmt.KindPart, Parts: []*partInfo{core(1), sec(1, "../x")}},
		},
		{
			name: "secondary absolute", wantErr: "invalid secondary",
			init: &transferv1.ImportPartsRequest_Init{Catalog: commonv1.Catalog_CATALOG_TRACE, Kind: nativefmt.KindPart, Parts: []*partInfo{core(1), sec(1, "/x")}},
		},
		{
			name: "no min timestamp", wantErr: "no minimum timestamp",
			init: &transferv1.ImportPartsRequest_Init{Catalog: commonv1.Catalog_CATALOG_STREAM, Kind: nativefmt.KindPart, Parts: []*partInfo{{Id: 1}}},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			parts, err := syncParts(c.init)
			if c.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), c.wantErr) {
					t.Fatalf("syncParts() error = %v, want it to contain %q", err, c.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("syncParts() = %v", err)
			}
			if len(parts) != c.want {
				t.Fatalf("got %d parts, want %d", len(parts), c.want)
			}
		})
	}
	parts, err := syncParts(&transferv1.ImportPartsRequest_Init{Catalog: commonv1.Catalog_CATALOG_TRACE, Kind: nativefmt.KindPart, Parts: []*partInfo{
		{Id: 7, MinTimestamp: 1, MaxTimestamp: 2, TotalCount: 3, BlocksCount: 4, CompressedSizeBytes: 5, UncompressedSizeBytes: 6},
		{Id: 7, PartType: "dur", MinTimestamp: 1, MinKey: -1, MaxKey: 9},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if p := parts[0]; p.GetId() != 7 || p.GetMaxTimestamp() != 2 || p.GetTotalCount() != 3 || p.GetBlocksCount() != 4 ||
		p.GetCompressedSizeBytes() != 5 || p.GetUncompressedSizeBytes() != 6 {
		t.Fatalf("core part = %v", p)
	}
	if p := parts[1]; p.GetPartType() != "dur" || p.GetMinKey() != -1 || p.GetMaxKey() != 9 {
		t.Fatalf("secondary part = %v", p)
	}
}

func TestPayloadNodes(t *testing.T) {
	r := &importRoute{group: "g", stage: "hot", shardNum: 3, copies: 2, shards: [][]string{{"a", "b"}, {"b", "c"}, {"c", "d"}}}
	cases := []struct {
		name    string
		init    *transferv1.ImportPartsRequest_Init
		want    []string
		wantErr codes.Code
	}{
		{name: "shard", init: &transferv1.ImportPartsRequest_Init{Kind: nativefmt.KindPart, ShardId: 1}, want: []string{"b", "c"}},
		{name: "series index union", init: &transferv1.ImportPartsRequest_Init{Kind: nativefmt.KindSeriesIndex, ShardIds: []uint32{0, 2}}, want: []string{"a", "b", "c", "d"}},
		{name: "series index every shard", init: &transferv1.ImportPartsRequest_Init{Kind: nativefmt.KindSeriesIndex}, want: []string{"a", "b", "c", "d"}},
		{
			name: "series index ignores shard_id", want: []string{"a", "b"},
			init: &transferv1.ImportPartsRequest_Init{Kind: nativefmt.KindSeriesIndex, ShardId: 2, ShardIds: []uint32{0}},
		},
		{name: "retry narrows", init: &transferv1.ImportPartsRequest_Init{Kind: nativefmt.KindPart, ShardId: 0, RetryNodes: []string{"b"}}, want: []string{"b"}},
		{
			name: "retry outside targets", wantErr: codes.InvalidArgument,
			init: &transferv1.ImportPartsRequest_Init{Kind: nativefmt.KindPart, ShardId: 0, RetryNodes: []string{"c"}},
		},
		{name: "unknown shard", init: &transferv1.ImportPartsRequest_Init{Kind: nativefmt.KindPart, ShardId: 3}, wantErr: codes.FailedPrecondition},
		{name: "unknown index shard", init: &transferv1.ImportPartsRequest_Init{Kind: nativefmt.KindSeriesIndex, ShardIds: []uint32{1, 5}}, wantErr: codes.FailedPrecondition},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := payloadNodes(r, c.init)
			if c.wantErr != codes.OK {
				wantCode(t, err, c.wantErr, "")
				return
			}
			if err != nil || !slices.Equal(got, c.want) {
				t.Fatalf("payloadNodes() = %v, %v; want %v", got, err, c.want)
			}
		})
	}
	empty := &importRoute{group: "g", stage: "hot", shardNum: 1, shards: [][]string{nil}}
	_, err := payloadNodes(empty, &transferv1.ImportPartsRequest_Init{Kind: nativefmt.KindPart})
	wantCode(t, err, codes.FailedPrecondition, "no data node")
}

func TestPartsPayloadSlices(t *testing.T) {
	p := &partsPayload{
		init: &transferv1.ImportPartsRequest_Init{Catalog: commonv1.Catalog_CATALOG_TRACE, Kind: nativefmt.KindPart, Parts: []*partInfo{
			{Id: 1, MinTimestamp: 5}, {Id: 1, PartType: "dur", MinTimestamp: 5, MinKey: 3},
		}},
		parts: []*clusterv1.PartInfo{{Id: 1, PartType: queue.PartTypeCore, MinTimestamp: 5}, {Id: 1, PartType: "dur", MinTimestamp: 5, MinKey: 3}},
		seen:  make([]bool, 2),
	}
	infos, err := p.slices(&transferv1.ImportPartsRequest_Chunk{
		Data: []byte("0123456789"),
		Files: []*transferv1.ImportPartsRequest_FileSlice{
			{Name: "primary.bin", Size: 2},
			{Name: "svc.t", Size: 3},
			{Name: "keys.bin", Size: 4, PartIndex: 1},
			{Name: "x.td", Size: 1, PartIndex: 1},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(infos) != 2 {
		t.Fatalf("got %d part entries, want 2", len(infos))
	}
	type file struct {
		name         string
		offset, size uint32
	}
	files := func(info *clusterv1.PartInfo) []file {
		var out []file
		for _, f := range info.GetFiles() {
			out = append(out, file{f.GetName(), f.GetOffset(), f.GetSize()})
		}
		return out
	}
	if got, want := files(infos[0]), []file{{"primary", 0, 2}, {"t:svc", 2, 3}}; !slices.Equal(got, want) {
		t.Fatalf("core files = %v, want %v", got, want)
	}
	if got, want := files(infos[1]), []file{{"keys", 5, 4}, {"td:x", 9, 1}}; !slices.Equal(got, want) {
		t.Fatalf("secondary files = %v, want %v", got, want)
	}
	if infos[1].GetPartType() != "dur" || infos[1].GetMinKey() != 3 || infos[0].GetMinTimestamp() != 5 {
		t.Fatalf("part descriptions not carried: %v", infos)
	}
	// The slices must not share the payload's descriptions: files of one chunk would leak into the next.
	if len(p.parts[0].GetFiles()) != 0 {
		t.Fatal("slices modified the payload's part description")
	}

	bad := []struct {
		name  string
		chunk *transferv1.ImportPartsRequest_Chunk
		want  string
	}{
		{name: "skipped file", want: "not sent", chunk: &transferv1.ImportPartsRequest_Chunk{
			Data: []byte("x"), Files: []*transferv1.ImportPartsRequest_FileSlice{{Name: "metadata.json", Size: 1}},
		}},
		{name: "unknown file", want: "unknown", chunk: &transferv1.ImportPartsRequest_Chunk{
			Data: []byte("x"), Files: []*transferv1.ImportPartsRequest_FileSlice{{Name: "what.bin", Size: 1}},
		}},
		{name: "bad part index", want: "names part", chunk: &transferv1.ImportPartsRequest_Chunk{
			Data: []byte("x"), Files: []*transferv1.ImportPartsRequest_FileSlice{{Name: "primary.bin", Size: 1, PartIndex: 2}},
		}},
		{name: "too few bytes", want: "add up", chunk: &transferv1.ImportPartsRequest_Chunk{
			Data: []byte("xy"), Files: []*transferv1.ImportPartsRequest_FileSlice{{Name: "primary.bin", Size: 1}},
		}},
		{name: "too many bytes", want: "add up", chunk: &transferv1.ImportPartsRequest_Chunk{
			Data: []byte("x"), Files: []*transferv1.ImportPartsRequest_FileSlice{{Name: "primary.bin", Size: 2}},
		}},
	}
	for _, c := range bad {
		t.Run(c.name, func(t *testing.T) {
			p.lastPart = 0
			_, err := p.slices(c.chunk)
			wantCode(t, err, codes.InvalidArgument, c.want)
		})
	}
}

func TestPartsPayloadSlices_PartOrder(t *testing.T) {
	newPayload := func() *partsPayload {
		return &partsPayload{
			init: &transferv1.ImportPartsRequest_Init{Catalog: commonv1.Catalog_CATALOG_TRACE, Kind: nativefmt.KindPart, Parts: []*partInfo{
				{Id: 1, MinTimestamp: 5}, {Id: 1, PartType: "dur", MinTimestamp: 5},
			}},
			parts: []*clusterv1.PartInfo{{Id: 1, PartType: queue.PartTypeCore, MinTimestamp: 5}, {Id: 1, PartType: "dur", MinTimestamp: 5}},
			seen:  make([]bool, 2),
		}
	}
	chunk := func(files ...*transferv1.ImportPartsRequest_FileSlice) *transferv1.ImportPartsRequest_Chunk {
		var size uint32
		for _, f := range files {
			size += f.GetSize()
		}
		return &transferv1.ImportPartsRequest_Chunk{Data: make([]byte, size), Files: files}
	}
	core := &transferv1.ImportPartsRequest_FileSlice{Name: "primary.bin", Size: 1}
	sidx := &transferv1.ImportPartsRequest_FileSlice{Name: "keys.bin", Size: 1, PartIndex: 1}

	_, err := newPayload().slices(chunk(core, sidx, core))
	wantCode(t, err, codes.InvalidArgument, "contiguous")

	p := newPayload()
	if _, err = p.slices(chunk(core, sidx)); err != nil {
		t.Fatal(err)
	}
	_, err = p.slices(chunk(core))
	wantCode(t, err, codes.InvalidArgument, "contiguous")

	// A part may go on across a chunk boundary.
	p = newPayload()
	if _, err = p.slices(chunk(core)); err != nil {
		t.Fatal(err)
	}
	if _, err = p.slices(chunk(core, sidx)); err != nil {
		t.Fatal(err)
	}
	if _, err = p.slices(chunk(sidx)); err != nil {
		t.Fatal(err)
	}
}

// recordingSync is a chunked sync handler that records what a data node receives.
type recordingSync struct {
	failOn string
	events []string
	mu     sync.Mutex
}

func (h *recordingSync) addf(format string, args ...any) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.events = append(h.events, fmt.Sprintf(format, args...))
}

func (h *recordingSync) got() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return slices.Clone(h.events)
}

func (h *recordingSync) CreatePartHandler(ctx *queue.ChunkedSyncPartContext) (queue.PartHandler, error) {
	h.addf("create group=%s shard=%d id=%d type=%q min=%d total=%d", ctx.Group, ctx.ShardID, ctx.ID, ctx.PartType, ctx.MinTimestamp, ctx.TotalCount)
	return &recordingPart{h: h}, nil
}

func (h *recordingSync) HandleFileChunk(ctx *queue.ChunkedSyncPartContext, chunk []byte) error {
	if h.failOn != "" && ctx.FileName == h.failOn {
		return errors.New("disk full")
	}
	h.addf("file %s=%s", ctx.FileName, chunk)
	return nil
}

type recordingPart struct{ h *recordingSync }

func (p *recordingPart) NewPartType(ctx *queue.ChunkedSyncPartContext) error {
	p.h.addf("newtype %q", ctx.PartType)
	return nil
}

func (p *recordingPart) FinishSync() error {
	p.h.addf("finish")
	return nil
}

func (p *recordingPart) Close() error {
	p.h.addf("close")
	return nil
}

// fakePartsStream is the server side of an ImportParts stream fed from frames.
type fakePartsStream struct {
	grpclib.ServerStream
	ctx    context.Context
	resp   *transferv1.ImportPartsResponse
	frames []*transferv1.ImportPartsRequest
	next   int
}

func (f *fakePartsStream) Context() context.Context { return f.ctx }

func (f *fakePartsStream) Recv() (*transferv1.ImportPartsRequest, error) {
	if f.next >= len(f.frames) {
		return nil, io.EOF
	}
	f.next++
	return f.frames[f.next-1], nil
}

func (f *fakePartsStream) SendAndClose(r *transferv1.ImportPartsResponse) error {
	f.resp = r
	return nil
}

// standalonePartsService is an ImportService of a standalone process whose local pipeline
// feeds h for the stream part sync topic.
func standalonePartsService(h queue.ChunkedSyncHandler) *importService {
	pipeline := queue.Local()
	pipeline.RegisterChunkedSyncHandler(data.TopicStreamPartSync, h)
	groups := testGroups()
	return newImportService(importDeps{
		groups: func(_ context.Context, name string) (*commonv1.Group, error) {
			if g, ok := groups[name]; ok {
				return g, nil
			}
			return nil, fmt.Errorf("group %s: %w", name, schema.ErrGRPCResourceNotFound)
		},
		selfNode:  func() string { return testSelfNode },
		tire2:     pipeline,
		groupRepo: &groupRepo{inflight: make(map[string]*groupInflight)},
	}, logger.GetLogger("import-parts-test"))
}

func partsInit(init *transferv1.ImportPartsRequest_Init) *transferv1.ImportPartsRequest {
	return &transferv1.ImportPartsRequest{Frame: &transferv1.ImportPartsRequest_Init_{Init: init}}
}

func partsChunk(data string, files ...*transferv1.ImportPartsRequest_FileSlice) *transferv1.ImportPartsRequest {
	return &transferv1.ImportPartsRequest{Frame: &transferv1.ImportPartsRequest_Chunk_{Chunk: &transferv1.ImportPartsRequest_Chunk{Data: []byte(data), Files: files}}}
}

func partsCompletion(bytes uint64, chunks uint32) *transferv1.ImportPartsRequest {
	return &transferv1.ImportPartsRequest{Frame: &transferv1.ImportPartsRequest_Completion_{
		Completion: &transferv1.ImportPartsRequest_Completion{TotalBytes: bytes, TotalChunks: chunks},
	}}
}

func streamPartInit() *transferv1.ImportPartsRequest_Init {
	return &transferv1.ImportPartsRequest_Init{
		Catalog: commonv1.Catalog_CATALOG_STREAM, Group: testStreamGroup, ShardId: 2, Kind: nativefmt.KindPart,
		Parts:          []*partInfo{{Id: 9, MinTimestamp: 100, MaxTimestamp: 200, TotalCount: 4}},
		SegmentVersion: storage.GetCurrentVersion(),
	}
}

func TestImportParts_RelaysToStandaloneHandler(t *testing.T) {
	h := &recordingSync{}
	svc := standalonePartsService(h)
	s := &fakePartsStream{ctx: context.Background(), frames: []*transferv1.ImportPartsRequest{
		partsInit(streamPartInit()),
		partsChunk("PPmm", &transferv1.ImportPartsRequest_FileSlice{Name: "primary.bin", Size: 2}, &transferv1.ImportPartsRequest_FileSlice{Name: "meta.bin", Size: 2}),
		partsChunk("tt", &transferv1.ImportPartsRequest_FileSlice{Name: "default.tf", Size: 2}),
		partsCompletion(6, 2),
	}}
	if err := svc.ImportParts(s); err != nil {
		t.Fatalf("ImportParts() = %v", err)
	}
	if s.resp.GetRequestedCopies() != 1 || len(s.resp.GetTargets()) != 1 {
		t.Fatalf("response = %v, want one target of one copy", s.resp)
	}
	if tr := s.resp.GetTargets()[0]; tr.GetNode() != testSelfNode || !tr.GetSuccess() || tr.GetError() != "" {
		t.Fatalf("target = %v, want %s succeeded", tr, testSelfNode)
	}
	want := []string{
		`create group=sg shard=2 id=9 type="core" min=100 total=4`,
		"file primary=PP", "file meta=mm", "file tf:default=tt", "finish",
	}
	if got := h.got(); !slices.Equal(got, want) {
		t.Fatalf("handler saw %v, want %v", got, want)
	}
}

func TestImportParts_FailedTargetIsReported(t *testing.T) {
	h := &recordingSync{failOn: "meta"}
	svc := standalonePartsService(h)
	s := &fakePartsStream{ctx: context.Background(), frames: []*transferv1.ImportPartsRequest{
		partsInit(streamPartInit()),
		partsChunk("PPmm", &transferv1.ImportPartsRequest_FileSlice{Name: "primary.bin", Size: 2}, &transferv1.ImportPartsRequest_FileSlice{Name: "meta.bin", Size: 2}),
		partsChunk("tt", &transferv1.ImportPartsRequest_FileSlice{Name: "default.tf", Size: 2}),
		partsCompletion(6, 2),
	}}
	if err := svc.ImportParts(s); err != nil {
		t.Fatalf("ImportParts() = %v", err)
	}
	// Every target failed after the first chunk, so the liaison answers without reading on.
	if s.next != 2 {
		t.Fatalf("the relay read %d frames, want it to stop after the failing chunk", s.next)
	}
	tr := s.resp.GetTargets()
	if len(tr) != 1 || tr[0].GetSuccess() || !strings.Contains(tr[0].GetError(), "disk full") {
		t.Fatalf("targets = %v, want one failed target naming the cause", tr)
	}
	if slices.Contains(h.got(), "finish") {
		t.Fatal("a failed part was finished")
	}
}

func TestImportParts_Validation(t *testing.T) {
	svc := standalonePartsService(&recordingSync{})
	run := func(frames ...*transferv1.ImportPartsRequest) error {
		return svc.ImportParts(&fakePartsStream{ctx: context.Background(), frames: frames})
	}
	wantCode(t, run(), codes.InvalidArgument, "before the init frame")
	wantCode(t, run(partsChunk("x")), codes.InvalidArgument, "")
	wantCode(t, run(partsInit(&transferv1.ImportPartsRequest_Init{Catalog: commonv1.Catalog_CATALOG_STREAM, Group: testStreamGroup})), codes.InvalidArgument, "")
	measurePart := streamPartInit()
	measurePart.Catalog = commonv1.Catalog_CATALOG_MEASURE
	wantCode(t, run(partsInit(measurePart)), codes.InvalidArgument, "is a CATALOG_STREAM group")
	noVersion := streamPartInit()
	noVersion.SegmentVersion = ""
	wantCode(t, run(partsInit(noVersion)), codes.InvalidArgument, "segment_version")
	unknownShard := streamPartInit()
	unknownShard.ShardId = 7
	wantCode(t, run(partsInit(unknownShard)), codes.FailedPrecondition, "not a shard")
	wantCode(t, run(partsInit(streamPartInit()), partsInit(streamPartInit())), codes.InvalidArgument, "only the first")
	wantCode(t, run(partsInit(streamPartInit()), partsChunk("x", &transferv1.ImportPartsRequest_FileSlice{Name: "primary.bin", Size: 1})),
		codes.InvalidArgument, "before the completion")
	wantCode(t, run(partsInit(streamPartInit()), partsCompletion(0, 0)), codes.InvalidArgument, "at least one chunk")
	wantCode(t, run(partsInit(streamPartInit()), partsChunk("x", &transferv1.ImportPartsRequest_FileSlice{Name: "primary.bin", Size: 1}), partsCompletion(5, 1)),
		codes.InvalidArgument, "completion counts")
}

func TestPartsPayload_TargetsFailIndependently(t *testing.T) {
	good, bad := &recordingSync{}, &recordingSync{failOn: "primary"}
	open := func(h queue.ChunkedSyncHandler, node string) *partTarget {
		pipeline := queue.Local()
		pipeline.RegisterChunkedSyncHandler(data.TopicStreamPartSync, h)
		client, err := pipeline.NewChunkedSyncRelayClient(node)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		stream, err := client.SyncPart(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return &partTarget{node: node, stream: stream, cancel: cancel}
	}
	init := streamPartInit()
	parts, err := syncParts(init)
	if err != nil {
		t.Fatal(err)
	}
	p := &partsPayload{
		init: init, parts: parts, seen: make([]bool, len(parts)), session: "s", copies: 3,
		version: &clusterv1.VersionInfo{
			ApiVersion: apiversion.Version, FileFormatVersion: storage.GetCurrentVersion(), CompatibleFileFormatVersion: storage.GetCompatibleVersions(),
		},
		metadata: &clusterv1.SyncMetadata{Group: testStreamGroup, ShardId: 2, Topic: data.TopicStreamPartSync.String()},
		// payloadNodes sorts the nodes, so the targets are in node order.
		targets: []*partTarget{open(good, "a"), open(bad, "b"), {node: "c", err: errors.New("no active client")}},
	}
	defer p.close()
	chunk := &transferv1.ImportPartsRequest_Chunk{Data: []byte("PP"), Files: []*transferv1.ImportPartsRequest_FileSlice{{Name: "primary.bin", Size: 2}}}
	if err = p.chunk(chunk); err != nil {
		t.Fatal(err)
	}
	if p.alive() != 1 {
		t.Fatalf("alive = %d, want 1", p.alive())
	}
	if err = p.complete(&transferv1.ImportPartsRequest_Completion{TotalBytes: 2, TotalChunks: 1}); err != nil {
		t.Fatal(err)
	}
	resp := p.response()
	if resp.GetRequestedCopies() != 3 {
		t.Fatalf("requested copies = %d, want 3", resp.GetRequestedCopies())
	}
	var nodes []string
	for _, tr := range resp.GetTargets() {
		nodes = append(nodes, fmt.Sprintf("%s:%v", tr.GetNode(), tr.GetSuccess()))
	}
	if want := []string{"a:true", "b:false", "c:false"}; !slices.Equal(nodes, want) {
		t.Fatalf("targets = %v, want %v", nodes, want)
	}
	if !slices.Contains(good.got(), "finish") {
		t.Fatalf("the healthy target did not finish the part: %v", good.got())
	}
}

func TestImportParts_RelaysEmptyFileAtChunkTail(t *testing.T) {
	h := &recordingSync{}
	svc := standalonePartsService(h)
	s := &fakePartsStream{ctx: context.Background(), frames: []*transferv1.ImportPartsRequest{
		partsInit(streamPartInit()),
		partsChunk("PP", &transferv1.ImportPartsRequest_FileSlice{Name: "primary.bin", Size: 2}, &transferv1.ImportPartsRequest_FileSlice{Name: "meta.bin"}),
		partsChunk("", &transferv1.ImportPartsRequest_FileSlice{Name: "default.tf"}),
		partsCompletion(2, 2),
	}}
	if err := svc.ImportParts(s); err != nil {
		t.Fatalf("ImportParts() = %v", err)
	}
	if tr := s.resp.GetTargets(); len(tr) != 1 || !tr[0].GetSuccess() {
		t.Fatalf("targets = %v, want one successful target", tr)
	}
	want := []string{
		`create group=sg shard=2 id=9 type="core" min=100 total=4`,
		"file primary=PP", "file meta=", "file tf:default=", "finish",
	}
	if got := h.got(); !slices.Equal(got, want) {
		t.Fatalf("handler saw %v, want %v: an empty file at a chunk tail must reach the receiver", got, want)
	}
}

func TestPartsPayload_EveryPartNeedsItsFiles(t *testing.T) {
	newPayload := func() *partsPayload {
		init := &transferv1.ImportPartsRequest_Init{Catalog: commonv1.Catalog_CATALOG_TRACE, Kind: nativefmt.KindPart, Parts: []*partInfo{
			{Id: 7, MinTimestamp: 5}, {Id: 7, PartType: "sidx_x", MinTimestamp: 5},
		}}
		parts, err := syncParts(init)
		if err != nil {
			t.Fatal(err)
		}
		return &partsPayload{init: init, parts: parts, seen: make([]bool, len(parts))}
	}
	core := &transferv1.ImportPartsRequest_FileSlice{Name: "primary.bin", Size: 1}
	sidx := &transferv1.ImportPartsRequest_FileSlice{Name: "keys.bin", Size: 1, PartIndex: 1}

	// A secondary part must not come before its core part.
	_, err := newPayload().slices(&transferv1.ImportPartsRequest_Chunk{Data: []byte("k"), Files: []*transferv1.ImportPartsRequest_FileSlice{sidx}})
	wantCode(t, err, codes.InvalidArgument, "before the core part")

	// A listed part that got no file fails the completion.
	p := newPayload()
	if err = p.chunk(&transferv1.ImportPartsRequest_Chunk{Data: []byte("P"), Files: []*transferv1.ImportPartsRequest_FileSlice{core}}); err != nil {
		t.Fatal(err)
	}
	err = p.complete(&transferv1.ImportPartsRequest_Completion{TotalBytes: 1, TotalChunks: 1})
	wantCode(t, err, codes.InvalidArgument, `part 1 "sidx_x" of the payload received no file`)

	p = newPayload()
	if err = p.chunk(&transferv1.ImportPartsRequest_Chunk{Data: []byte("Pk"), Files: []*transferv1.ImportPartsRequest_FileSlice{core, sidx}}); err != nil {
		t.Fatal(err)
	}
	if err = p.complete(&transferv1.ImportPartsRequest_Completion{TotalBytes: 2, TotalChunks: 1}); err != nil {
		t.Fatalf("complete() = %v, want every part covered", err)
	}
}

func TestImportParts_PartPayloadWithoutFilesIsRefused(t *testing.T) {
	h := &recordingSync{}
	svc := standalonePartsService(h)
	err := svc.ImportParts(&fakePartsStream{ctx: context.Background(), frames: []*transferv1.ImportPartsRequest{
		partsInit(streamPartInit()), partsChunk(""), partsCompletion(0, 1),
	}})
	wantCode(t, err, codes.InvalidArgument, "received no file")
	if slices.Contains(h.got(), "finish") {
		t.Fatalf("handler saw %v, want no part finished", h.got())
	}
}

func TestImportParts_OversizedChunkIsRefused(t *testing.T) {
	h := &recordingSync{}
	svc := standalonePartsService(h)
	size := nativefmt.MaxChunkSize + 1
	err := svc.ImportParts(&fakePartsStream{ctx: context.Background(), frames: []*transferv1.ImportPartsRequest{
		partsInit(streamPartInit()),
		partsChunk(strings.Repeat("x", size), &transferv1.ImportPartsRequest_FileSlice{Name: "primary.bin", Size: uint32(size)}),
		partsCompletion(uint64(size), 1),
	}})
	wantCode(t, err, codes.InvalidArgument, "maximum of a part frame")
	if got := h.got(); len(got) != 0 {
		t.Fatalf("handler saw %v, want nothing relayed", got)
	}
}

// cancelingPartsStream cancels its context once its frames run out, as a client that gives up.
type cancelingPartsStream struct {
	*fakePartsStream
	cancel context.CancelFunc
}

func (c *cancelingPartsStream) Recv() (*transferv1.ImportPartsRequest, error) {
	if c.next < len(c.frames) {
		return c.fakePartsStream.Recv()
	}
	c.cancel()
	return nil, status.FromContextError(c.ctx.Err()).Err()
}

func TestImportParts_AbortClosesDownstreamPart(t *testing.T) {
	frames := func() []*transferv1.ImportPartsRequest {
		return []*transferv1.ImportPartsRequest{partsInit(streamPartInit()), partsChunk("PP", &transferv1.ImportPartsRequest_FileSlice{Name: "primary.bin", Size: 2})}
	}
	waitClosed := func(t *testing.T, h *recordingSync) {
		t.Helper()
		require.Eventually(t, func() bool { return slices.Contains(h.got(), "close") }, 5*time.Second, 10*time.Millisecond,
			"the downstream part was not closed: %v", h.got())
		if slices.Contains(h.got(), "finish") {
			t.Fatalf("handler saw %v, want an aborted part never finished", h.got())
		}
	}
	t.Run("closed without completion", func(t *testing.T) {
		h := &recordingSync{}
		err := standalonePartsService(h).ImportParts(&fakePartsStream{ctx: context.Background(), frames: frames()})
		wantCode(t, err, codes.InvalidArgument, "before the completion")
		waitClosed(t, h)
	})
	t.Run("canceled", func(t *testing.T) {
		h := &recordingSync{}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		err := standalonePartsService(h).ImportParts(&cancelingPartsStream{fakePartsStream: &fakePartsStream{ctx: ctx, frames: frames()}, cancel: cancel})
		wantCode(t, err, codes.Canceled, "")
		waitClosed(t, h)
	})
}

// fakeSyncStream is a downstream SyncPart stream whose answers a test decides.
type fakeSyncStream struct {
	grpclib.ClientStream
	ctx context.Context
	// recv answers the last frame sent; nil acknowledges it.
	recv func(ctx context.Context, last *clusterv1.SyncPartRequest) (*clusterv1.SyncPartResponse, error)
	last *clusterv1.SyncPartRequest
}

func (f *fakeSyncStream) Send(req *clusterv1.SyncPartRequest) error {
	f.last = req
	return nil
}

func (f *fakeSyncStream) Recv() (*clusterv1.SyncPartResponse, error) {
	if f.recv != nil {
		return f.recv(f.ctx, f.last)
	}
	return ackSync(f.last), nil
}

func (f *fakeSyncStream) CloseSend() error { return nil }

func ackSync(req *clusterv1.SyncPartRequest) *clusterv1.SyncPartResponse {
	if req.GetCompletion() != nil {
		return &clusterv1.SyncPartResponse{Status: clusterv1.SyncStatus_SYNC_STATUS_SYNC_COMPLETE}
	}
	return &clusterv1.SyncPartResponse{Status: clusterv1.SyncStatus_SYNC_STATUS_CHUNK_RECEIVED}
}

func fakeTarget(node string, recv func(context.Context, *clusterv1.SyncPartRequest) (*clusterv1.SyncPartResponse, error)) *partTarget {
	ctx, cancel := context.WithCancel(context.Background())
	return &partTarget{node: node, stream: &fakeSyncStream{ctx: ctx, recv: recv}, cancel: cancel}
}

func fakeTargetsPayload(t *testing.T, targets ...*partTarget) *partsPayload {
	t.Helper()
	init := streamPartInit()
	parts, err := syncParts(init)
	if err != nil {
		t.Fatal(err)
	}
	return &partsPayload{init: init, parts: parts, seen: make([]bool, len(parts)), session: "s", targets: targets}
}

func relayOnePart(t *testing.T, p *partsPayload) map[string]*transferv1.ImportPartsResponse_TargetResult {
	t.Helper()
	defer p.close()
	chunk := &transferv1.ImportPartsRequest_Chunk{Data: []byte("PP"), Files: []*transferv1.ImportPartsRequest_FileSlice{{Name: "primary.bin", Size: 2}}}
	if err := p.chunk(chunk); err != nil {
		t.Fatal(err)
	}
	if err := p.complete(&transferv1.ImportPartsRequest_Completion{TotalBytes: 2, TotalChunks: 1}); err != nil {
		t.Fatal(err)
	}
	out := map[string]*transferv1.ImportPartsResponse_TargetResult{}
	for _, tr := range p.response().GetTargets() {
		out[tr.GetNode()] = tr
	}
	return out
}

func TestPartsPayload_StalledTargetTimesOut(t *testing.T) {
	old := relayFrameTimeout
	relayFrameTimeout = 50 * time.Millisecond
	t.Cleanup(func() { relayFrameTimeout = old })
	stalled := func(ctx context.Context, _ *clusterv1.SyncPartRequest) (*clusterv1.SyncPartResponse, error) {
		<-ctx.Done()
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	got := relayOnePart(t, fakeTargetsPayload(t, fakeTarget("a", stalled), fakeTarget("b", nil)))
	if a := got["a"]; a.GetSuccess() || !strings.Contains(a.GetError(), "answered nothing within") {
		t.Fatalf("stalled target = %v, want a timeout failure", a)
	}
	if b := got["b"]; !b.GetSuccess() {
		t.Fatalf("healthy target = %v, want success", b)
	}
}

func TestPartsPayload_CompletionSentMarksAmbiguousFailures(t *testing.T) {
	brokenAfterCompletion := func(_ context.Context, last *clusterv1.SyncPartRequest) (*clusterv1.SyncPartResponse, error) {
		if last.GetCompletion() != nil {
			return nil, status.Error(codes.Unavailable, "connection reset")
		}
		return ackSync(last), nil
	}
	refusesCompletion := func(_ context.Context, last *clusterv1.SyncPartRequest) (*clusterv1.SyncPartResponse, error) {
		if last.GetCompletion() != nil {
			return &clusterv1.SyncPartResponse{Status: clusterv1.SyncStatus_SYNC_STATUS_FORMAT_VERSION_MISMATCH, Error: "no"}, nil
		}
		return ackSync(last), nil
	}
	brokenOnChunk := func(context.Context, *clusterv1.SyncPartRequest) (*clusterv1.SyncPartResponse, error) {
		return nil, status.Error(codes.Unavailable, "connection reset")
	}
	got := relayOnePart(t, fakeTargetsPayload(t,
		fakeTarget("a", brokenAfterCompletion), fakeTarget("b", refusesCompletion), fakeTarget("c", brokenOnChunk), fakeTarget("d", nil)))
	want := map[string][2]bool{"a": {false, true}, "b": {false, false}, "c": {false, false}, "d": {true, false}}
	for node, w := range want {
		if tr := got[node]; tr.GetSuccess() != w[0] || tr.GetCompletionSent() != w[1] {
			t.Fatalf("target %s = %v, want success=%v completion_sent=%v", node, tr, w[0], w[1])
		}
	}
}

// openErrRelay is a relay client whose SyncPart fails.
type openErrRelay struct{ err error }

func (o openErrRelay) SyncPart(context.Context, ...grpclib.CallOption) (syncPartStream, error) {
	return nil, o.err
}

// versionRelay relays to a queue.Local pipeline and records the file format version of every frame.
type versionRelay struct {
	inner    clusterv1.ChunkedSyncServiceClient
	versions *[]string
	mu       *sync.Mutex
}

func (v versionRelay) SyncPart(ctx context.Context, opts ...grpclib.CallOption) (syncPartStream, error) {
	s, err := v.inner.SyncPart(ctx, opts...)
	if err != nil {
		return nil, err
	}
	return versionStream{syncPartStream: s, v: v}, nil
}

type versionStream struct {
	syncPartStream
	v versionRelay
}

func (s versionStream) Send(req *clusterv1.SyncPartRequest) error {
	s.v.mu.Lock()
	*s.v.versions = append(*s.v.versions, req.GetVersionInfo().GetFileFormatVersion())
	s.v.mu.Unlock()
	return s.syncPartStream.Send(req)
}

func localRelay(t *testing.T, h queue.ChunkedSyncHandler, node string) clusterv1.ChunkedSyncServiceClient {
	t.Helper()
	pipeline := queue.Local()
	pipeline.RegisterChunkedSyncHandler(data.TopicStreamPartSync, h)
	pipeline.RegisterChunkedSyncHandler(data.TopicPropertyPartSync, h)
	client, err := pipeline.NewChunkedSyncRelayClient(node)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func streamPartFrames() []*transferv1.ImportPartsRequest {
	return []*transferv1.ImportPartsRequest{
		partsInit(streamPartInit()),
		partsChunk("PP", &transferv1.ImportPartsRequest_FileSlice{Name: "primary.bin", Size: 2}),
		partsCompletion(2, 1),
	}
}

func TestImportParts_ClusterRoute(t *testing.T) {
	f := newImportFixture(t, clusterNodes())
	var nodes []string
	route := targetRoute(t, f, testStreamGroup, "")
	for _, sh := range route.GetShards() {
		if sh.GetShardId() == 2 {
			nodes = sh.GetNodeIds()
		}
	}
	if len(nodes) < 2 {
		t.Fatalf("stream shard 2 is served by %v, want replicas", nodes)
	}
	sorted := slices.Sorted(slices.Values(nodes))
	failures := []struct {
		name     string
		relayErr error
		openErr  error
		want     string
	}{
		{name: "no relay client", relayErr: errors.New("no active client"), want: "no active client"},
		{name: "stream not opened", openErr: errors.New("stream refused"), want: "stream refused"},
	}
	for _, c := range failures {
		t.Run(c.name, func(t *testing.T) {
			handlers := map[string]*recordingSync{}
			f.client.relay = map[string]clusterv1.ChunkedSyncServiceClient{}
			f.client.relayErr = map[string]error{}
			if c.relayErr != nil {
				f.client.relayErr[sorted[0]] = c.relayErr
			} else {
				f.client.relay[sorted[0]] = openErrRelay{err: c.openErr}
			}
			for _, n := range sorted[1:] {
				handlers[n] = &recordingSync{}
				f.client.relay[n] = localRelay(t, handlers[n], n)
			}
			s := &fakePartsStream{ctx: context.Background(), frames: streamPartFrames()}
			if err := f.svc.ImportParts(s); err != nil {
				t.Fatalf("ImportParts() = %v", err)
			}
			if s.resp.GetRequestedCopies() != route.GetCopies() {
				t.Fatalf("requested copies = %d, want %d", s.resp.GetRequestedCopies(), route.GetCopies())
			}
			var got []string
			for _, tr := range s.resp.GetTargets() {
				got = append(got, tr.GetNode())
			}
			if !slices.Equal(got, sorted) {
				t.Fatalf("targets = %v, want %v in node order", got, sorted)
			}
			for i, tr := range s.resp.GetTargets() {
				if i == 0 {
					if tr.GetSuccess() || !strings.Contains(tr.GetError(), c.want) {
						t.Fatalf("target %v, want the failure %q", tr, c.want)
					}
					continue
				}
				if !tr.GetSuccess() || !slices.Contains(handlers[tr.GetNode()].got(), "finish") {
					t.Fatalf("target %v saw %v, want it finished", tr, handlers[tr.GetNode()].got())
				}
			}
		})
	}

	// retry_nodes narrows the targets.
	retry := streamPartFrames()
	retry[0].GetInit().RetryNodes = []string{sorted[1]}
	s := &fakePartsStream{ctx: context.Background(), frames: retry}
	if err := f.svc.ImportParts(s); err != nil {
		t.Fatalf("ImportParts() = %v", err)
	}
	if tr := s.resp.GetTargets(); len(tr) != 1 || tr[0].GetNode() != sorted[1] || !tr[0].GetSuccess() {
		t.Fatalf("targets = %v, want only %s, succeeded", tr, sorted[1])
	}
}

func TestImportParts_PropertyDefaultsToCurrentVersion(t *testing.T) {
	f := newImportFixture(t, clusterNodes())
	var versions []string
	mu := &sync.Mutex{}
	f.client.relay = map[string]clusterv1.ChunkedSyncServiceClient{}
	for _, n := range []string{"p1", "p2", "p3"} {
		f.client.relay[n] = versionRelay{inner: localRelay(t, &recordingSync{}, n), versions: &versions, mu: mu}
	}
	s := &fakePartsStream{ctx: context.Background(), frames: []*transferv1.ImportPartsRequest{
		partsInit(&transferv1.ImportPartsRequest_Init{Catalog: commonv1.Catalog_CATALOG_PROPERTY, Group: testPropertyGroup, Kind: nativefmt.KindProperty}),
		partsChunk("SS", &transferv1.ImportPartsRequest_FileSlice{Name: "000000000012.seg", Size: 2}),
		partsCompletion(2, 1),
	}}
	if err := f.svc.ImportParts(s); err != nil {
		t.Fatalf("ImportParts() = %v", err)
	}
	for _, tr := range s.resp.GetTargets() {
		if !tr.GetSuccess() {
			t.Fatalf("target %v, want success", tr)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(versions) == 0 {
		t.Fatal("no frame was relayed")
	}
	for _, v := range versions {
		if v != storage.GetCurrentVersion() {
			t.Fatalf("relayed file format version %q, want this liaison's %q", v, storage.GetCurrentVersion())
		}
	}
}

func TestImportParts_PendingDeletionIsRefused(t *testing.T) {
	f := newImportFixture(t, clusterNodes())
	f.svc.groupRepo.waitInflightRequests(testStreamGroup)
	err := f.svc.ImportParts(&fakePartsStream{ctx: context.Background(), frames: streamPartFrames()})
	wantCode(t, err, codes.FailedPrecondition, "being deleted")
}

// blockingPartsStream sends the init frame, then blocks its next Recv until unblock closes.
type blockingPartsStream struct {
	grpclib.ServerStream
	ctx     context.Context
	init    *transferv1.ImportPartsRequest
	opened  chan struct{}
	unblock chan struct{}
	calls   int
}

func (b *blockingPartsStream) Context() context.Context { return b.ctx }

func (b *blockingPartsStream) Recv() (*transferv1.ImportPartsRequest, error) {
	b.calls++
	if b.calls == 1 {
		return b.init, nil
	}
	close(b.opened)
	<-b.unblock
	return nil, io.EOF
}

func (b *blockingPartsStream) SendAndClose(*transferv1.ImportPartsResponse) error { return nil }

func TestImportParts_GroupDeletionWaitsForRelay(t *testing.T) {
	f := newImportFixture(t, clusterNodes())
	s := &blockingPartsStream{
		ctx: context.Background(), init: partsInit(streamPartInit()),
		opened: make(chan struct{}), unblock: make(chan struct{}),
	}
	result := make(chan error, 1)
	go func() { result <- f.svc.ImportParts(s) }()
	select {
	case <-s.opened:
	case <-time.After(10 * time.Second):
		t.Fatal("the payload did not open")
	}
	done := f.svc.groupRepo.waitInflightRequests(testStreamGroup)
	select {
	case <-done:
		t.Fatal("group deletion went on while a relay was running")
	case <-time.After(100 * time.Millisecond):
	}
	close(s.unblock)
	if err := <-result; err == nil {
		t.Fatal("ImportParts() = nil, want an error for a stream closed before its completion")
	}
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("group deletion did not go on after the relay ended")
	}
}
