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

package importer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer"
	"github.com/apache/skywalking-banyandb/pkg/transfer/cluster"
	"github.com/apache/skywalking-banyandb/pkg/transfer/config"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/bnc"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/manifest"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/nativefmt"
	"github.com/apache/skywalking-banyandb/pkg/transfer/output"
)

func TestContainerPrefix(t *testing.T) {
	cases := map[string]struct {
		prefix string
		ok     bool
	}{
		"a/seg-1/shard-0-00001.bnc": {"a/seg-1/shard-0", true},
		"a/seg-1/sidx-00012.bnc":    {"a/seg-1/sidx", true},
		"a/seg-1/metadata":          {"", false},
		"a/seg-1/shard-0-001.bnc":   {"", false},
		"a/seg-1/shard.bnc":         {"", false},
		"shard-0-00001.csv.gz":      {"", false},
	}
	for p, want := range cases {
		prefix, _, ok := bnc.ParseVolumeName(p)
		assert.Equal(t, want.ok, ok, p)
		assert.Equal(t, want.prefix, prefix, p)
	}
}

func TestContainerVolumes(t *testing.T) {
	got, err := containerVolumes([]manifest.File{
		{Path: "s/shard-0-00002.bnc"}, {Path: "s/metadata"}, {Path: "s/sidx-00001.bnc"}, {Path: "s/shard-0-00001.bnc"},
	})
	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.Equal(t, []manifest.File{{Path: "s/shard-0-00001.bnc"}, {Path: "s/shard-0-00002.bnc"}}, got["s/shard-0"])
	assert.Equal(t, []manifest.File{{Path: "s/sidx-00001.bnc"}}, got["s/sidx"])
	_, err = containerVolumes([]manifest.File{{Path: "s/shard.bnc"}})
	require.ErrorContains(t, err, "s/shard.bnc is not named as a container volume")
}

// member is one file of a container written by writeContainer.
type member struct {
	path string
	data string
}

// writeContainer writes members as the single volume <dir>/<rel>-00001.bnc and returns its
// manifest entry, with rel as the path relative to dir.
func writeContainer(t *testing.T, dir, rel string, members ...member) manifest.File {
	t.Helper()
	var buf bytes.Buffer
	w := bnc.NewWriter(&buf)
	for _, m := range members {
		require.NoError(t, w.Add(context.Background(), m.path, int64(len(m.data)), bytes.NewReader([]byte(m.data))))
	}
	sum, err := w.Close()
	require.NoError(t, err)
	path := rel + "-00001.bnc"
	full := filepath.Join(dir, filepath.FromSlash(path))
	require.NoError(t, os.MkdirAll(filepath.Dir(full), 0o755))
	require.NoError(t, os.WriteFile(full, buf.Bytes(), 0o600))
	return manifest.File{Path: path, Bytes: sum.Bytes, CRC32: manifest.FormatCRC32(sum.CRC32), EntryCount: sum.Entries}
}

func openTestVolumes(t *testing.T, dir string, f manifest.File) *bnc.VolumeSet {
	t.Helper()
	vs, err := bnc.OpenVolumes([]string{filepath.Join(dir, filepath.FromSlash(f.Path))})
	require.NoError(t, err)
	t.Cleanup(func() { _ = vs.Close() })
	return vs
}

func TestChunkerSplitsFilesAcrossChunks(t *testing.T) {
	dir := t.TempDir()
	f := writeContainer(t, dir, "c", member{"a", "0123456789"}, member{"empty", ""}, member{"b", "xyz"})
	vs := openTestVolumes(t, dir, f)
	var sent []*transferv1.ImportPartsRequest_Chunk
	c := &chunker{size: 4, send: func(r *transferv1.ImportPartsRequest) error {
		sent = append(sent, r.GetChunk())
		return nil
	}}
	for i, file := range vs.Files() {
		require.NoError(t, c.addFile(context.Background(), payloadFile{vs: vs, file: file, part: uint32(i)}))
	}
	require.NoError(t, c.flush(context.Background(), true))
	type slice struct {
		name string
		size uint32
		part uint32
	}
	var got [][]slice
	var data []string
	for _, ch := range sent {
		var ss []slice
		for _, s := range ch.GetFiles() {
			ss = append(ss, slice{s.GetName(), s.GetSize(), s.GetPartIndex()})
		}
		got = append(got, ss)
		data = append(data, string(ch.GetData()))
	}
	assert.Equal(t, []string{"0123", "4567", "89xy", "z"}, data)
	assert.Equal(t, [][]slice{
		{{"a", 4, 0}},
		{{"a", 4, 0}},
		{{"a", 2, 0}, {"empty", 0, 1}, {"b", 2, 2}},
		{{"b", 1, 2}},
	}, got)
	assert.Equal(t, uint64(13), c.total)
	assert.Equal(t, uint32(4), c.chunks)
}

func TestChunkerSendsOneChunkForEmptyFiles(t *testing.T) {
	dir := t.TempDir()
	f := writeContainer(t, dir, "c", member{"empty", ""})
	vs := openTestVolumes(t, dir, f)
	var sent []*transferv1.ImportPartsRequest_Chunk
	c := &chunker{size: 4, send: func(r *transferv1.ImportPartsRequest) error {
		sent = append(sent, r.GetChunk())
		return nil
	}}
	require.NoError(t, c.addFile(context.Background(), payloadFile{vs: vs, file: vs.Files()[0]}))
	require.NoError(t, c.flush(context.Background(), true))
	require.Len(t, sent, 1)
	assert.Empty(t, sent[0].GetData())
	require.Len(t, sent[0].GetFiles(), 1)
	assert.Equal(t, "empty", sent[0].GetFiles()[0].GetName())
	// Nothing more to send once a chunk went out.
	require.NoError(t, c.flush(context.Background(), true))
	assert.Len(t, sent, 1)
}

func TestChunkerSurfacesACorruptFile(t *testing.T) {
	dir := t.TempDir()
	f := writeContainer(t, dir, "c", member{"a", "0123456789"})
	full := filepath.Join(dir, f.Path)
	raw, err := os.ReadFile(full)
	require.NoError(t, err)
	raw[3] ^= 0xff // member bytes start at offset 0
	require.NoError(t, os.WriteFile(full, raw, 0o600))
	vs := openTestVolumes(t, dir, f)
	c := &chunker{size: 64, send: func(*transferv1.ImportPartsRequest) error { return nil }}
	err = c.addFile(context.Background(), payloadFile{vs: vs, file: vs.Files()[0]})
	require.Error(t, err)
	assert.ErrorIs(t, err, errCorrupt)
}

// recordedPayload is one ImportParts stream the fake liaison received.
type recordedPayload struct {
	init  *transferv1.ImportPartsRequest_Init
	files map[string]string
	// partFiles lists the names of the files of each part index, in order.
	partFiles map[uint32][]string
	order     []string
	chunks    int
}

// fakePartsLiaison serves ImportParts: it records every payload and answers through answer.
type fakePartsLiaison struct {
	transferv1.UnimplementedImportServiceServer
	answer func(n int, p *recordedPayload) (*transferv1.ImportPartsResponse, error)
	// early, when it returns a response, answers right after the init frame without reading the
	// rest of the stream, as a liaison whose targets all failed does.
	early      func(init *transferv1.ImportPartsRequest_Init) *transferv1.ImportPartsResponse
	payloads   []*recordedPayload
	serverOpts []grpc.ServerOption
	mu         sync.Mutex
}

func (f *fakePartsLiaison) ImportParts(stream transferv1.ImportService_ImportPartsServer) error {
	p := &recordedPayload{files: map[string]string{}, partFiles: map[uint32][]string{}}
	var gotBytes uint64
	completed := false
	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		switch fr := req.GetFrame().(type) {
		case *transferv1.ImportPartsRequest_Init_:
			p.init = fr.Init
			if f.early == nil {
				break
			}
			if resp := f.early(fr.Init); resp != nil {
				f.mu.Lock()
				f.payloads = append(f.payloads, p)
				f.mu.Unlock()
				return stream.SendAndClose(resp)
			}
		case *transferv1.ImportPartsRequest_Chunk_:
			if len(fr.Chunk.GetData()) > config.MaxPartChunkSize {
				return status.Errorf(codes.InvalidArgument, "a chunk of %d bytes is over %d", len(fr.Chunk.GetData()), config.MaxPartChunkSize)
			}
			p.chunks++
			gotBytes += uint64(len(fr.Chunk.GetData()))
			var off uint32
			for _, s := range fr.Chunk.GetFiles() {
				if _, seen := p.files[s.GetName()]; !seen {
					p.order = append(p.order, s.GetName())
				}
				if names := p.partFiles[s.GetPartIndex()]; !slices.Contains(names, s.GetName()) {
					p.partFiles[s.GetPartIndex()] = append(names, s.GetName())
				}
				p.files[s.GetName()] += string(fr.Chunk.GetData()[off : off+s.GetSize()])
				off += s.GetSize()
			}
			if int(off) != len(fr.Chunk.GetData()) {
				return status.Errorf(codes.InvalidArgument, "slices add up to %d of %d bytes", off, len(fr.Chunk.GetData()))
			}
		case *transferv1.ImportPartsRequest_Completion_:
			completed = true
			if p.chunks < 1 || fr.Completion.GetTotalBytes() != gotBytes || fr.Completion.GetTotalChunks() != uint32(p.chunks) {
				return status.Errorf(codes.InvalidArgument, "completion says %d bytes in %d chunks, got %d bytes in %d chunks",
					fr.Completion.GetTotalBytes(), fr.Completion.GetTotalChunks(), gotBytes, p.chunks)
			}
		}
	}
	if !completed {
		return status.Error(codes.InvalidArgument, "the stream closed before the completion")
	}
	f.mu.Lock()
	n := len(f.payloads)
	f.payloads = append(f.payloads, p)
	f.mu.Unlock()
	resp, err := f.answer(n, p)
	if err != nil {
		return err
	}
	return stream.SendAndClose(resp)
}

func (f *fakePartsLiaison) serve(t *testing.T) *cluster.Liaison {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := grpc.NewServer(f.serverOpts...)
	transferv1.RegisterImportServiceServer(srv, f)
	//panicdiag:allow-rawgo test server
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return &cluster.Liaison{Addr: lis.Addr().String(), Conn: conn}
}

func okTargets(nodes ...string) *transferv1.ImportPartsResponse {
	resp := &transferv1.ImportPartsResponse{RequestedCopies: uint32(len(nodes))}
	for _, n := range nodes {
		resp.Targets = append(resp.Targets, &transferv1.ImportPartsResponse_TargetResult{Node: n, Success: true})
	}
	return resp
}

const (
	nativeSegTime = int64(1792281600000000000) // 2026-10-18T00:00:00Z
	nativeSegEnd  = nativeSegTime + int64(24*time.Hour) - 1
)

// nativeArtifact writes a native artifact of one stream segment of source node n1: its series
// index container and a shard container with one core part, the part list and the element index.
func nativeArtifact(t *testing.T) (*Artifact, []Entry) {
	t.Helper()
	return nativeArtifactWithPrimary(t, "PRIMARY")
}

// nativeArtifactWithPrimary is nativeArtifact with primary as the content of the part's primary.bin.
func nativeArtifactWithPrimary(t *testing.T, primary string) (*Artifact, []Entry) {
	t.Helper()
	dir := t.TempDir()
	const prefix = "nodes/n1/stream/sg/seg-20261018/"
	meta := fmt.Sprintf(`{"compressedSizeBytes":30,"uncompressedSizeBytes":60,"totalCount":4,"blocksCount":1,"minTimestamp":%d,"maxTimestamp":%d}`,
		nativeSegTime+1, nativeSegTime+2)
	shard := writeContainer(t, dir, prefix+"shard-0",
		member{"0000000000000001/metadata.json", meta},
		member{"0000000000000001/primary.bin", primary},
		member{"0000000000000001/meta.bin", "META"},
		member{"0000000000000001/timestamps.bin", "TS"},
		member{"0000000000000001/tag.type", ""},
		member{"0000000000000001/x.tf", "TAGS"},
		member{"0000000000000001/smeta.bin", "SMETA"},
		member{"0000000000000001.snp", `["0000000000000001"]`},
		member{"idx/000000000001.seg", "ELEMENTSEG"},
		member{"idx/000000000001.snp", "ELEMENTSNP"},
	)
	sidx := writeContainer(t, dir, prefix+"sidx", member{"000000000001.seg", "SERIESSEG"}, member{"000000000001.snp", "SERIESSNP"})
	segMeta := manifest.File{Path: prefix + "metadata", Bytes: 2}
	require.NoError(t, os.WriteFile(filepath.Join(dir, filepath.FromSlash(segMeta.Path)), []byte("{}"), 0o600))
	art := &Artifact{Dir: dir, Manifest: &manifest.Manifest{Format: manifest.FormatNative}, Schemas: NewSchemas()}
	base := WorkItem{Catalog: commonv1.Catalog_CATALOG_STREAM, Group: "sg", Stage: "hot", Segment: "20261018", SegmentVersion: "1.5.0"}
	shardItem := base
	shardItem.Kind = ShardItem
	shardItem.Sources = []WorkSource{{Node: "n1", Files: []manifest.File{shard}, MinTimestamp: nativeSegTime + 1, MaxTimestamp: nativeSegTime + 2, Rows: 4}}
	segItem := base
	segItem.Kind, segItem.Shards = SegmentItem, []uint32{0}
	segItem.Sources = []WorkSource{{Node: "n1", Files: []manifest.File{segMeta, sidx}, MinTimestamp: nativeSegTime, MaxTimestamp: nativeSegEnd}}
	// The shard entry comes first: PushParts pushes the series index first anyway.
	return art, []Entry{
		{Item: shardItem, TargetStage: "hot", TargetNodes: []string{"d1", "d2"}, Copies: 2},
		{Item: segItem, TargetStage: "hot", TargetNodes: []string{"d1", "d2"}, Copies: 2},
	}
}

func nativePushOptions() PushOptions {
	return PushOptions{Import: config.Import{
		Parallelism: math.MaxInt, PartChunkSize: 4, SendRetryTimeout: time.Second,
	}}
}

func shortenPartRetries(t *testing.T) {
	t.Helper()
	retry := transfer.RetryBase
	transfer.RetryBase = time.Millisecond
	t.Cleanup(func() { transfer.RetryBase = retry })
}

func TestPushParts_RoundsKindsAndRetry(t *testing.T) {
	shortenPartRetries(t)
	art, entries := nativeArtifact(t)
	f := &fakePartsLiaison{}
	partAttempts := 0
	f.answer = func(_ int, p *recordedPayload) (*transferv1.ImportPartsResponse, error) {
		if p.init.GetKind() != nativefmt.KindPart {
			return okTargets("d1", "d2"), nil
		}
		partAttempts++
		if partAttempts == 1 {
			resp := okTargets("d1", "d2")
			resp.Targets[1].Success, resp.Targets[1].Error = false, "disk full"
			return resp, nil
		}
		return okTargets(p.init.GetRetryNodes()...), nil
	}
	var log bytes.Buffer
	opts := nativePushOptions()
	opts.Log = &log
	res, err := PushParts(context.Background(), []*cluster.Liaison{f.serve(t)}, art, entries, opts)
	require.NoError(t, err, log.String())

	var kinds []transferv1.ImportPartsRequest_Kind
	for _, p := range f.payloads {
		kinds = append(kinds, p.init.GetKind())
	}
	assert.Equal(t, []transferv1.ImportPartsRequest_Kind{
		nativefmt.KindSeriesIndex, nativefmt.KindPart, nativefmt.KindPart, nativefmt.KindElementIndex,
	}, kinds, "the series index goes first, then the part, its retry and the element index")

	series := f.payloads[0]
	assert.Equal(t, map[string]string{"000000000001.seg": "SERIESSEG"}, series.files, "the index manifest is not sent")
	assert.Equal(t, []uint32{0}, series.init.GetShardIds())
	require.Len(t, series.init.GetParts(), 1)
	assert.Equal(t, nativeSegTime, series.init.GetParts()[0].GetMinTimestamp())
	assert.Equal(t, "1.5.0", series.init.GetSegmentVersion())
	assert.Equal(t, "hot", series.init.GetStage())

	part := f.payloads[1]
	assert.Equal(t, map[string]string{
		"primary.bin": "PRIMARY", "meta.bin": "META", "timestamps.bin": "TS", "tag.type": "", "x.tf": "TAGS",
	}, part.files, "metadata.json and smeta.bin are not sent")
	assert.Greater(t, part.chunks, 1, "files are split into chunks of --part-chunk-size")
	require.Len(t, part.init.GetParts(), 1)
	core := part.init.GetParts()[0]
	assert.Equal(t, uint64(1), core.GetId())
	assert.Equal(t, uint64(4), core.GetTotalCount())
	assert.Equal(t, nativeSegTime+1, core.GetMinTimestamp())
	assert.Empty(t, part.init.GetRetryNodes())
	assert.Equal(t, uint32(0), part.init.GetShardId())

	retry := f.payloads[2]
	assert.Equal(t, []string{"d2"}, retry.init.GetRetryNodes(), "only the failed target is pushed again")
	assert.Equal(t, part.files, retry.files)
	assert.Contains(t, log.String(), "disk full")

	element := f.payloads[3]
	assert.Equal(t, map[string]string{"000000000001.seg": "ELEMENTSEG"}, element.files)
	assert.Equal(t, nativeSegTime+1, element.init.GetParts()[0].GetMinTimestamp())

	sort.Strings(res.Done)
	assert.Equal(t, []string{"stream/sg/hot/20261018/segment", "stream/sg/hot/20261018/shard-0"}, res.Done)
	k := streamKey{catalog: commonv1.Catalog_CATALOG_STREAM, group: "sg", stage: "hot"}
	assert.Equal(t, uint64(4), res.Rows[PushKey{streamKey: k, Kind: ShardItem}])
	assert.Equal(t, uint64(0), res.Rows[PushKey{streamKey: k, Kind: SegmentItem}])
}

func TestPushParts_ElementIndexOfAnotherCatalogFails(t *testing.T) {
	art, entries := nativeArtifact(t)
	shard := entries[0]
	shard.Item.Catalog = commonv1.Catalog_CATALOG_MEASURE
	f := &fakePartsLiaison{answer: func(int, *recordedPayload) (*transferv1.ImportPartsResponse, error) {
		return okTargets("d1", "d2"), nil
	}}
	_, err := PushParts(context.Background(), []*cluster.Liaison{f.serve(t)}, art, []Entry{shard}, nativePushOptions())
	require.ErrorContains(t, err, "has no KIND_ELEMENT_INDEX payload", "an element index of a catalog without one fails the import instead of being skipped")
	assert.Empty(t, f.payloads, "the shard fails before any of its parts is stored")
}

func TestPushParts_ElementIndexWithoutTimeRangeFailsBeforeParts(t *testing.T) {
	art, entries := nativeArtifact(t)
	shard := entries[0]
	shard.Item.Sources[0].MinTimestamp = 0
	f := &fakePartsLiaison{answer: func(int, *recordedPayload) (*transferv1.ImportPartsResponse, error) {
		return okTargets("d1", "d2"), nil
	}}
	_, err := PushParts(context.Background(), []*cluster.Liaison{f.serve(t)}, art, []Entry{shard}, nativePushOptions())
	require.ErrorContains(t, err, "has no time range")
	assert.Empty(t, f.payloads, "the shard fails before any of its parts is stored")
}

func TestPushParts_ReportsProgress(t *testing.T) {
	art, entries := nativeArtifact(t)
	f := &fakePartsLiaison{answer: func(int, *recordedPayload) (*transferv1.ImportPartsResponse, error) {
		return okTargets("d1", "d2"), nil
	}}
	var out bytes.Buffer
	opts := nativePushOptions()
	opts.Progress = output.NewProgress(&out, nil, "group(s) importing")
	_, err := PushParts(context.Background(), []*cluster.Liaison{f.serve(t)}, art, entries, opts)
	require.NoError(t, err)
	assert.Contains(t, out.String(), "[1/2]")
	assert.Contains(t, out.String(), "[2/2]")
	assert.Contains(t, out.String(), "✓ stream sg/hot  done: 2 work item(s), 4 rows in")
}

func TestPushParts_TargetStillFailingFailsTheImport(t *testing.T) {
	shortenPartRetries(t)
	art, entries := nativeArtifact(t)
	f := &fakePartsLiaison{answer: func(_ int, p *recordedPayload) (*transferv1.ImportPartsResponse, error) {
		resp := okTargets("d1")
		if p.init.GetKind() == nativefmt.KindPart {
			resp.Targets[0].Success, resp.Targets[0].Error = false, "disk full"
		}
		return resp, nil
	}}
	var log bytes.Buffer
	opts := nativePushOptions()
	opts.Log = &log
	res, err := PushParts(context.Background(), []*cluster.Liaison{f.serve(t)}, art, entries, opts)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not written to 1 node(s) after 3 attempt(s)")
	assert.Equal(t, []string{"stream/sg/hot/20261018/segment"}, res.Done)
	assert.Contains(t, log.String(), "work items not done (1)")
	var parts int
	for _, p := range f.payloads {
		if p.init.GetKind() == nativefmt.KindPart {
			parts++
		}
	}
	assert.Equal(t, partRetryAttempts, parts)
}

func TestPushParts_UnavailableLiaisonMovesOn(t *testing.T) {
	shortenPartRetries(t)
	art, entries := nativeArtifact(t)
	down := &fakePartsLiaison{answer: func(_ int, p *recordedPayload) (*transferv1.ImportPartsResponse, error) {
		if nativefmt.Duplicable(p.init.GetCatalog(), p.init.GetKind()) {
			// A stream part or element index that fails with UNAVAILABLE after its completion is
			// final; load shedding comes before anything is relayed.
			return nil, status.Error(codes.ResourceExhausted, "busy")
		}
		return nil, status.Error(codes.Unavailable, "draining")
	}}
	up := &fakePartsLiaison{answer: func(int, *recordedPayload) (*transferv1.ImportPartsResponse, error) { return okTargets("d1"), nil }}
	opts := nativePushOptions()
	opts.Parallelism = 1
	_, err := PushParts(context.Background(), []*cluster.Liaison{down.serve(t), up.serve(t)}, art, entries, opts)
	require.NoError(t, err)
	assert.Len(t, up.payloads, 3, "every payload lands on the liaison that answers")
}

func TestPushParts_FinalRefusalIsNotRetried(t *testing.T) {
	shortenPartRetries(t)
	art, entries := nativeArtifact(t)
	f := &fakePartsLiaison{answer: func(int, *recordedPayload) (*transferv1.ImportPartsResponse, error) {
		return nil, status.Error(codes.FailedPrecondition, "unknown shard")
	}}
	_, err := PushParts(context.Background(), []*cluster.Liaison{f.serve(t)}, art, entries, nativePushOptions())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown shard")
	assert.Len(t, f.payloads, 1, "a refusal that would fail again stops the import")
}

func TestPushParts_CorruptContainerFails(t *testing.T) {
	art, entries := nativeArtifact(t)
	f := entries[0].Item.Sources[0].Files[0]
	full := filepath.Join(art.Dir, filepath.FromSlash(f.Path))
	raw, err := os.ReadFile(full)
	require.NoError(t, err)
	idx := bytes.Index(raw, []byte("PRIMARY"))
	require.GreaterOrEqual(t, idx, 0)
	raw[idx] = 'X'
	require.NoError(t, os.WriteFile(full, raw, 0o600))
	srv := &fakePartsLiaison{answer: func(int, *recordedPayload) (*transferv1.ImportPartsResponse, error) { return okTargets("d1"), nil }}
	_, err = PushParts(context.Background(), []*cluster.Liaison{srv.serve(t)}, art, entries, nativePushOptions())
	require.Error(t, err)
	assert.ErrorIs(t, err, errCorrupt)
	for _, p := range srv.payloads {
		assert.NotEqual(t, nativefmt.KindPart, p.init.GetKind(), "a corrupt part is never completed")
	}
}

func TestRetryablePayload(t *testing.T) {
	cases := []struct {
		name           string
		reason         string
		code           codes.Code
		sentDuplicable bool
		want           bool
	}{
		{"unavailable before completion", "", codes.Unavailable, false, true},
		{"unavailable after a stream part completed", "", codes.Unavailable, true, false},
		{"aborted after a stream part completed", "", codes.Aborted, true, false},
		{"unavailable with a liaison reason", "NOT_WRITTEN", codes.Unavailable, true, true},
		{"load shedding", "", codes.ResourceExhausted, true, true},
		{"message too large", transfer.ImportReasonMessageTooLarge, codes.ResourceExhausted, false, false},
		{"schema not live yet", "", codes.NotFound, true, true},
		{"final refusal", "", codes.FailedPrecondition, false, false},
		{"invalid", "", codes.InvalidArgument, false, false},
	}
	for _, tc := range cases {
		assert.Equal(t, tc.want, retryablePayload(tc.code, tc.reason, tc.sentDuplicable), tc.name)
	}
}

func TestPushParts_StreamPartBrokenAfterCompletionIsNotResent(t *testing.T) {
	shortenPartRetries(t)
	art, entries := nativeArtifact(t)
	f := &fakePartsLiaison{answer: func(_ int, p *recordedPayload) (*transferv1.ImportPartsResponse, error) {
		if p.init.GetKind() == nativefmt.KindPart {
			// The stream breaks after the completion was relayed: the targets may hold the part.
			return nil, status.Error(codes.Unavailable, "connection reset")
		}
		return okTargets("d1"), nil
	}}
	var log bytes.Buffer
	opts := nativePushOptions()
	opts.Log = &log
	res, err := PushParts(context.Background(), []*cluster.Liaison{f.serve(t)}, art, entries, opts)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "may be stored on its targets already")
	assert.Equal(t, []string{"stream/sg/hot/20261018/segment"}, res.Done)
	assert.Contains(t, log.String(), "work items not done (1)")
	var parts int
	for _, p := range f.payloads {
		if p.init.GetKind() == nativefmt.KindPart {
			parts++
		}
	}
	assert.Equal(t, 1, parts, "a stream part that may have been committed is not sent again")
}

func TestPushParts_TargetFailedAfterCompletionIsNotResent(t *testing.T) {
	shortenPartRetries(t)
	art, entries := nativeArtifact(t)
	f := &fakePartsLiaison{answer: func(_ int, p *recordedPayload) (*transferv1.ImportPartsResponse, error) {
		resp := okTargets("d1", "d2")
		if p.init.GetKind() == nativefmt.KindPart {
			// d2 broke after the completion reached it: it may hold the part already.
			resp.Targets[1].Success, resp.Targets[1].Error, resp.Targets[1].CompletionSent = false, "stream reset", true
		}
		return resp, nil
	}}
	_, err := PushParts(context.Background(), []*cluster.Liaison{f.serve(t)}, art, entries, nativePushOptions())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "may be stored on them already")
	assert.Contains(t, err.Error(), "d2")
	var parts int
	for _, p := range f.payloads {
		if p.init.GetKind() == nativefmt.KindPart {
			parts++
		}
	}
	assert.Equal(t, 1, parts, "a stream part a target may have committed is not pushed to it again")
}

func TestPushParts_EarlyAnswerIsRetriedOnFailedTargets(t *testing.T) {
	shortenPartRetries(t)
	// A part far larger than the server's stream window: the client cannot finish sending it
	// before the liaison answers, so its Send fails with io.EOF and it reads the early answer.
	art, entries := nativeArtifactWithPrimary(t, strings.Repeat("P", 1<<20))
	partAttempts := 0
	f := &fakePartsLiaison{
		serverOpts: []grpc.ServerOption{grpc.InitialWindowSize(1 << 16), grpc.InitialConnWindowSize(1 << 16)},
		early: func(init *transferv1.ImportPartsRequest_Init) *transferv1.ImportPartsResponse {
			if init.GetKind() != nativefmt.KindPart {
				return nil
			}
			partAttempts++
			if partAttempts > 1 {
				return nil
			}
			resp := okTargets("d1", "d2")
			for _, tr := range resp.Targets {
				tr.Success, tr.Error = false, "disk full"
			}
			return resp
		},
		answer: func(_ int, p *recordedPayload) (*transferv1.ImportPartsResponse, error) {
			if p.init.GetKind() == nativefmt.KindPart {
				return okTargets(p.init.GetRetryNodes()...), nil
			}
			return okTargets("d1", "d2"), nil
		},
	}
	opts := nativePushOptions()
	opts.PartChunkSize = 1 << 14
	var log bytes.Buffer
	opts.Log = &log
	_, err := PushParts(context.Background(), []*cluster.Liaison{f.serve(t)}, art, entries, opts)
	require.NoError(t, err, log.String())
	var parts []*recordedPayload
	for _, p := range f.payloads {
		if p.init.GetKind() == nativefmt.KindPart {
			parts = append(parts, p)
		}
	}
	require.Len(t, parts, 2)
	assert.Empty(t, parts[0].init.GetRetryNodes())
	assert.Equal(t, []string{"d1", "d2"}, parts[1].init.GetRetryNodes(), "the early answer's failed targets are pushed again")
	assert.Len(t, parts[1].files["primary.bin"], 1<<20)
	assert.Contains(t, log.String(), "disk full")
}

func TestPushParts_TraceSecondaryPartsAndProperty(t *testing.T) {
	dir := t.TempDir()
	const tracePrefix = "nodes/n1/trace/tg/seg-20261018/"
	coreMeta := fmt.Sprintf(`{"compressedSizeBytes":30,"uncompressedSpanSizeBytes":60,"totalCount":3,"blocksCount":1,"minTimestamp":%d,"maxTimestamp":%d}`,
		nativeSegTime+1, nativeSegTime+2)
	traceShard := writeContainer(t, dir, tracePrefix+"shard-0",
		member{"0000000000000001/metadata.json", coreMeta},
		member{"0000000000000001/spans.bin", "SPANS"},
		member{"0000000000000001/primary.bin", "TPRIMARY"},
		member{"sidx/duration/0000000000000001/manifest.json", `{"compressedSizeBytes":5,"totalCount":3,"minKey":1,"maxKey":9}`},
		member{"sidx/duration/0000000000000001/primary.bin", "SPRIMARY"},
		member{"sidx/duration/0000000000000001/data.bin", "SDATA"},
		member{"0000000000000001.snp", `["0000000000000001"]`},
	)
	const propPrefix = "nodes/n1/property/pg/"
	propShard := writeContainer(t, dir, propPrefix+"shard-0",
		member{"000000000001.seg", "PROPSEG"}, member{"000000000001.snp", "PROPSNP"},
	)
	art := &Artifact{Dir: dir, Manifest: &manifest.Manifest{Format: manifest.FormatNative}, Schemas: NewSchemas()}
	traceItem := WorkItem{
		Catalog: commonv1.Catalog_CATALOG_TRACE, Group: "tg", Stage: "hot", Segment: "20261018", SegmentVersion: "1.5.0", Kind: ShardItem,
		Sources: []WorkSource{{Node: "n1", Files: []manifest.File{traceShard}, MinTimestamp: nativeSegTime + 1, MaxTimestamp: nativeSegTime + 2, Rows: 3}},
	}
	propItem := WorkItem{
		Catalog: commonv1.Catalog_CATALOG_PROPERTY, Group: "pg", Kind: PropertyItem,
		Sources: []WorkSource{{Node: "n1", Files: []manifest.File{propShard}}},
	}
	entries := []Entry{
		{Item: traceItem, TargetStage: "hot", TargetNodes: []string{"d1"}, Copies: 1},
		{Item: propItem, TargetNodes: []string{"d1"}, Copies: 1},
	}
	f := &fakePartsLiaison{answer: func(int, *recordedPayload) (*transferv1.ImportPartsResponse, error) { return okTargets("d1"), nil }}
	var log bytes.Buffer
	opts := nativePushOptions()
	opts.Log = &log
	res, err := PushParts(context.Background(), []*cluster.Liaison{f.serve(t)}, art, entries, opts)
	require.NoError(t, err, log.String())
	sort.Strings(res.Done)
	assert.Equal(t, []string{"property/pg/shard-0", "trace/tg/hot/20261018/shard-0"}, res.Done)

	byKind := map[transferv1.ImportPartsRequest_Kind]*recordedPayload{}
	for _, p := range f.payloads {
		require.NotContains(t, byKind, p.init.GetKind())
		byKind[p.init.GetKind()] = p
	}
	part := byKind[nativefmt.KindPart]
	require.NotNil(t, part)
	require.Len(t, part.init.GetParts(), 2)
	core, sidx := part.init.GetParts()[0], part.init.GetParts()[1]
	assert.Empty(t, core.GetPartType())
	assert.Equal(t, uint64(3), core.GetTotalCount())
	assert.Equal(t, "duration", sidx.GetPartType())
	assert.Equal(t, uint64(1), sidx.GetId())
	assert.Equal(t, int64(9), sidx.GetMaxKey())
	assert.Equal(t, core.GetMinTimestamp(), sidx.GetMinTimestamp(), "a secondary part inherits the core time range")
	assert.ElementsMatch(t, []string{"spans.bin", "primary.bin"}, part.partFiles[0])
	assert.ElementsMatch(t, []string{"primary.bin", "data.bin"}, part.partFiles[1], "the secondary part's files carry part index 1")
	assert.Len(t, part.partFiles, 2)
	assert.Equal(t, "SPANS", part.files["spans.bin"])
	assert.Equal(t, "SDATA", part.files["data.bin"])
	assert.NotContains(t, part.files, "metadata.json")
	assert.NotContains(t, part.files, "manifest.json")
	assert.Equal(t, uint64(3), res.Rows[PushKey{streamKey: streamKey{catalog: commonv1.Catalog_CATALOG_TRACE, group: "tg", stage: "hot"}, Kind: ShardItem}])

	prop := byKind[nativefmt.KindProperty]
	require.NotNil(t, prop)
	assert.Equal(t, map[string]string{"000000000001.seg": "PROPSEG", "000000000001.snp": "PROPSNP"}, prop.files, "a property payload carries its snapshot")
	assert.Empty(t, prop.init.GetSegmentVersion())
}

func TestPushParts_RateLimitSlowsThePush(t *testing.T) {
	art, entries := nativeArtifactWithPrimary(t, strings.Repeat("x", 30000))
	push := func(rateLimit float64) time.Duration {
		f := &fakePartsLiaison{answer: func(int, *recordedPayload) (*transferv1.ImportPartsResponse, error) {
			return okTargets("d1", "d2"), nil
		}}
		opts := nativePushOptions()
		opts.PartChunkSize, opts.PartRateLimit = 4096, rateLimit
		start := time.Now()
		res, err := PushParts(context.Background(), []*cluster.Liaison{f.serve(t)}, art, entries, opts)
		require.NoError(t, err)
		require.Len(t, res.Done, 2)
		return time.Since(start)
	}
	// The bucket starts with one second's worth (20000 bytes); the other 10000+ bytes of the
	// part wait for refills, so the push takes at least half a second.
	limited := push(20000)
	assert.GreaterOrEqual(t, limited, 400*time.Millisecond, "a push above one second's worth of the rate waits for the bucket")
	assert.Less(t, push(0), limited, "an unlimited push does not wait")
}

func TestPushParts_RateBelowChunkSize(t *testing.T) {
	art, entries := nativeArtifactWithPrimary(t, strings.Repeat("x", 2000))
	f := &fakePartsLiaison{answer: func(int, *recordedPayload) (*transferv1.ImportPartsResponse, error) {
		return okTargets("d1", "d2"), nil
	}}
	opts := nativePushOptions()
	// A chunk larger than the rate still goes through: the burst covers a whole chunk.
	opts.PartChunkSize, opts.PartRateLimit = 4096, 1000
	res, err := PushParts(context.Background(), []*cluster.Liaison{f.serve(t)}, art, entries, opts)
	require.NoError(t, err)
	assert.Len(t, res.Done, 2)
	assert.Len(t, f.payloads, 3)
}
