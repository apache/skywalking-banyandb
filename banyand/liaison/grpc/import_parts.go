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
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	apiversion "github.com/apache/skywalking-banyandb/api/proto/banyandb"
	clusterv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/cluster/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/nativefmt"
)

// relayFrameTimeout bounds the wait for one target's answer to one frame, so a stalled or
// partitioned node fails on its own instead of holding the barrier of every other target. It
// must outlast the data node's --{stream,measure,trace}-lifecycle-receive-mem-wait-timeout, so a
// busy receiver's own SERVER_BUSY or error reaches the client rather than a relay timeout; it
// is twice that flag's default. A variable so tests can shorten it.
var relayFrameTimeout = 2 * queue.DefaultReceiveMemWaitTimeout

// syncPartStream is the client side of one cluster.v1 SyncPart stream.
type syncPartStream = grpc.BidiStreamingClient[clusterv1.SyncPartRequest, clusterv1.SyncPartResponse]

// ImportParts relays one payload of a native artifact to every target data node of its
// (group, stage, shard) over the chunked sync protocol, the protocol a lifecycle migration
// moves parts with. It stores nothing: each client chunk is turned into one sync frame, sent to
// every target still alive and acknowledged by all of them before the next chunk is read, so
// memory stays flat whatever the speed of the targets. A target that fails is dropped and
// reported on its own; the others go on. The response lists every target.
func (s *importService) ImportParts(stream transferv1.ImportService_ImportPartsServer) error {
	ctx := stream.Context()
	first, err := stream.Recv()
	if errors.Is(err, io.EOF) {
		return status.Error(codes.InvalidArgument, "import parts stream closed before the init frame")
	}
	if err != nil {
		return withTooLargeDetail(err)
	}
	if err = first.ValidateAll(); err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	init := first.GetInit()
	if init == nil {
		return status.Error(codes.InvalidArgument, "the first import parts frame must be init")
	}
	payload, err := s.openPayload(ctx, init)
	if err != nil {
		return err
	}
	defer payload.close()
	for {
		req, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			return status.Error(codes.InvalidArgument, "import parts stream closed before the completion frame")
		}
		if recvErr != nil {
			return withTooLargeDetail(recvErr)
		}
		if validateErr := req.ValidateAll(); validateErr != nil {
			return status.Error(codes.InvalidArgument, validateErr.Error())
		}
		switch frame := req.GetFrame().(type) {
		case *transferv1.ImportPartsRequest_Chunk_:
			if chunkErr := payload.chunk(frame.Chunk); chunkErr != nil {
				return chunkErr
			}
			if payload.alive() == 0 {
				// Every target failed: nothing more can land, so answer at once; the client
				// sees its next Send fail and reads this response.
				return stream.SendAndClose(payload.response())
			}
		case *transferv1.ImportPartsRequest_Completion_:
			if completeErr := payload.complete(frame.Completion); completeErr != nil {
				return completeErr
			}
			return stream.SendAndClose(payload.response())
		default:
			return status.Error(codes.InvalidArgument, "only the first import parts frame may be init")
		}
	}
}

// partsPayload is the relay state of one ImportParts stream.
type partsPayload struct {
	init     *transferv1.ImportPartsRequest_Init
	version  *clusterv1.VersionInfo
	metadata *clusterv1.SyncMetadata
	// release ends the in-flight request the payload holds on its group, so a group deletion
	// waits for the relay.
	release func()
	session string
	parts   []*clusterv1.PartInfo
	targets []*partTarget
	// seen marks the parts that got at least one file: a part payload must give every part it
	// lists its files, or a receiver would commit a core part without its secondary index.
	seen  []bool
	bytes uint64
	// lastPart is the highest part index relayed so far: the files of a part must be
	// contiguous, or a receiver would reopen a part type it already wrote.
	lastPart int
	chunks   uint32
	copies   uint32
}

// partTarget is one target data node of a payload.
type partTarget struct {
	stream syncPartStream
	cancel context.CancelFunc
	err    error
	node   string
	// timedOut tells that the frame timer canceled the stream.
	timedOut atomic.Bool
	// completionSent tells that the completion reached the target and the failure came after
	// it, so the target may have committed the payload.
	completionSent bool
}

// openPayload validates init, routes it and opens one SyncPart stream per target.
func (s *importService) openPayload(ctx context.Context, init *transferv1.ImportPartsRequest_Init) (*partsPayload, error) {
	topic, err := nativefmt.Topic(init.GetCatalog(), init.GetKind())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	parts, err := syncParts(init)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	g, err := s.group(ctx, init.GetGroup())
	if err != nil {
		return nil, err
	}
	if g.GetCatalog() != init.GetCatalog() {
		return nil, status.Errorf(codes.InvalidArgument, "group %s is a %s group, not %s", init.GetGroup(), g.GetCatalog(), init.GetCatalog())
	}
	group := init.GetGroup()
	if acquireErr := s.groupRepo.acquireRequest(group); acquireErr != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "group %s is being deleted", group)
	}
	release := func() { s.groupRepo.releaseRequest(group) }
	opened := false
	defer func() {
		if !opened {
			release()
		}
	}()
	r, err := s.routeFor(ctx, g, init.GetStage())
	if err != nil {
		return nil, err
	}
	nodes, err := payloadNodes(r, init)
	if err != nil {
		return nil, err
	}
	segmentVersion := init.GetSegmentVersion()
	if segmentVersion == "" {
		// Only a property payload has no segment; any other one must name the version of its
		// source segment, or the data nodes would check this liaison's version against itself.
		if nativefmt.Segmented(init.GetCatalog()) {
			return nil, status.Errorf(codes.InvalidArgument, "a %s payload needs the segment_version of its source segment", init.GetKind())
		}
		segmentVersion = storage.GetCurrentVersion()
	}
	p := &partsPayload{
		init:    init,
		parts:   parts,
		seen:    make([]bool, len(parts)),
		session: uuid.NewString(),
		copies:  r.copies,
		// The file format version guards the source data, so it is the client's; the API and
		// compatible versions are this liaison's own.
		version: &clusterv1.VersionInfo{
			ApiVersion:                  apiversion.Version,
			FileFormatVersion:           segmentVersion,
			CompatibleFileFormatVersion: storage.GetCompatibleVersions(),
		},
		metadata: &clusterv1.SyncMetadata{
			Group:      init.GetGroup(),
			ShardId:    init.GetShardId(),
			Topic:      topic,
			Timestamp:  time.Now().UnixMilli(),
			TotalParts: uint32(len(parts)),
			SenderNode: s.selfNode(),
			SenderRole: "liaison",
		},
	}
	for _, n := range nodes {
		t := &partTarget{node: n}
		p.targets = append(p.targets, t)
		client, clientErr := s.tire2.NewChunkedSyncRelayClient(n)
		if clientErr != nil {
			t.err = clientErr
			continue
		}
		// The relay context derives from the client's stream, so a canceled import closes
		// every downstream stream instead of leaving it behind.
		streamCtx, cancel := context.WithCancel(ctx)
		syncStream, openErr := client.SyncPart(streamCtx)
		if openErr != nil {
			cancel()
			t.err = openErr
			continue
		}
		t.stream, t.cancel = syncStream, cancel
	}
	p.release = release
	opened = true
	return p, nil
}

// syncParts checks the parts of init and converts them to the sync protocol's. A part payload
// carries one core part first and, for trace only, secondary index parts of the same id; the
// receivers place every part by its time range, so a part or index payload needs one. A
// property payload needs no description, so it must send none, and gets one placeholder part.
func syncParts(init *transferv1.ImportPartsRequest_Init) ([]*clusterv1.PartInfo, error) {
	parts := init.GetParts()
	switch init.GetKind() {
	case nativefmt.KindProperty:
		if len(parts) != 0 {
			return nil, fmt.Errorf("a property payload carries no part entries, got %d", len(parts))
		}
		return []*clusterv1.PartInfo{{Id: 1}}, nil
	case nativefmt.KindSeriesIndex, nativefmt.KindElementIndex:
		if len(parts) != 1 || parts[0].GetMinTimestamp() <= 0 || parts[0].GetId() != 0 || parts[0].GetPartType() != "" {
			return nil, fmt.Errorf("an index payload carries one part entry with its time range, id 0 and no part type")
		}
	case nativefmt.KindPart:
		if len(parts) == 0 || parts[0].GetPartType() != "" {
			return nil, fmt.Errorf("a part payload carries its core part first")
		}
		seen := map[string]bool{}
		for i, p := range parts {
			if p.GetId() != parts[0].GetId() {
				return nil, fmt.Errorf("a part payload carries one part id, got %d and %d", parts[0].GetId(), p.GetId())
			}
			if seen[p.GetPartType()] {
				return nil, fmt.Errorf("part type %q is listed twice", p.GetPartType())
			}
			seen[p.GetPartType()] = true
			if i > 0 && !nativefmt.HasSecondaryParts(init.GetCatalog()) {
				return nil, fmt.Errorf("catalog %s has no secondary index parts", init.GetCatalog())
			}
			if i > 0 && !validSecondaryPartType(p.GetPartType()) {
				return nil, fmt.Errorf("invalid secondary index part type %q", p.GetPartType())
			}
			if p.GetMinTimestamp() <= 0 {
				return nil, fmt.Errorf("part %d %q has no minimum timestamp", p.GetId(), p.GetPartType())
			}
		}
	}
	out := make([]*clusterv1.PartInfo, len(parts))
	for i, p := range parts {
		// Every payload is one sync session, so the source part id is unique in it; a data
		// node allocates its own id anyway. Index payloads keep id 0, as a migration sends.
		partType := p.GetPartType()
		if init.GetKind() == nativefmt.KindPart && partType == "" {
			partType = queue.PartTypeCore
		}
		out[i] = &clusterv1.PartInfo{
			Id:                    p.GetId(),
			PartType:              partType,
			CompressedSizeBytes:   p.GetCompressedSizeBytes(),
			UncompressedSizeBytes: p.GetUncompressedSizeBytes(),
			TotalCount:            p.GetTotalCount(),
			BlocksCount:           p.GetBlocksCount(),
			MinTimestamp:          p.GetMinTimestamp(),
			MaxTimestamp:          p.GetMaxTimestamp(),
			MinKey:                p.GetMinKey(),
			MaxKey:                p.GetMaxKey(),
		}
	}
	return out, nil
}

// validSecondaryPartType reports whether name can be a trace secondary index part type: the
// data nodes use it as a directory name, so it must be one plain local name, and "core" names
// the core part on the wire.
func validSecondaryPartType(name string) bool {
	return name != queue.PartTypeCore && nativefmt.ValidName(name)
}

// payloadNodes resolves the target nodes of a payload: the nodes of its shard, or for a series
// index the union of the nodes of every shard it covers, narrowed to retry_nodes when set.
func payloadNodes(r *importRoute, init *transferv1.ImportPartsRequest_Init) ([]string, error) {
	shards := []uint32{init.GetShardId()}
	if init.GetKind() == nativefmt.KindSeriesIndex {
		shards = init.GetShardIds()
		if len(shards) == 0 {
			shards = make([]uint32, r.shardNum)
			for i := range shards {
				shards[i] = uint32(i)
			}
		}
	}
	set := map[string]struct{}{}
	for _, sh := range shards {
		if sh >= r.shardNum {
			return nil, importError(codes.FailedPrecondition, "UNKNOWN_SHARD", nil,
				fmt.Sprintf("shard %d is not a shard of group %s stage %s (shard number %d)", sh, r.group, r.stage, r.shardNum))
		}
		for _, n := range r.targets(sh) {
			set[n] = struct{}{}
		}
	}
	if len(set) == 0 {
		return nil, status.Errorf(codes.FailedPrecondition, "group %s stage %s has no data node for shards %v", r.group, r.stage, shards)
	}
	if retry := init.GetRetryNodes(); len(retry) > 0 {
		narrowed := map[string]struct{}{}
		for _, n := range retry {
			if _, ok := set[n]; !ok {
				return nil, status.Errorf(codes.InvalidArgument, "retry node %s is not a target of group %s stage %s", n, r.group, r.stage)
			}
			narrowed[n] = struct{}{}
		}
		set = narrowed
	}
	nodes := make([]string, 0, len(set))
	for n := range set {
		nodes = append(nodes, n)
	}
	sort.Strings(nodes)
	return nodes, nil
}

// chunk converts one client chunk into a sync frame and sends it to every live target.
func (p *partsPayload) chunk(c *transferv1.ImportPartsRequest_Chunk) error {
	// The import protocol bounds a part frame to nativefmt.MaxChunkSize, refused here so every
	// target sees the same limit; a data node's --max-recv-msg-size must exceed it plus framing.
	if n := len(c.GetData()); n > nativefmt.MaxChunkSize {
		return status.Errorf(codes.InvalidArgument, "chunk %d holds %d bytes, above the %d-byte maximum of a part frame", p.chunks, n, nativefmt.MaxChunkSize)
	}
	infos, err := p.slices(c)
	if err != nil {
		return err
	}
	req := &clusterv1.SyncPartRequest{
		SessionId:     p.session,
		ChunkIndex:    p.chunks,
		ChunkData:     c.GetData(),
		ChunkChecksum: queue.ChunkChecksum(c.GetData()),
		PartsInfo:     infos,
		VersionInfo:   p.version,
	}
	if p.chunks == 0 {
		req.Content = &clusterv1.SyncPartRequest_Metadata{Metadata: p.metadata}
	}
	p.chunks++
	p.bytes += uint64(len(c.GetData()))
	p.exchange(req, clusterv1.SyncStatus_SYNC_STATUS_CHUNK_RECEIVED)
	return nil
}

// slices turns the file slices of a chunk into the sync protocol's parts: consecutive slices
// of one part form one entry, with offsets into the chunk and the names the data node expects.
func (p *partsPayload) slices(c *transferv1.ImportPartsRequest_Chunk) ([]*clusterv1.PartInfo, error) {
	var infos []*clusterv1.PartInfo
	var offset uint64
	last := -1
	for _, f := range c.GetFiles() {
		idx := int(f.GetPartIndex())
		if idx >= len(p.parts) {
			return nil, status.Errorf(codes.InvalidArgument, "file %s names part %d of %d", f.GetName(), idx, len(p.parts))
		}
		if p.init.GetKind() == nativefmt.KindPart && idx != 0 && !p.seen[0] {
			// A secondary part without its core is deleted by the receiver, which still answers success.
			return nil, status.Errorf(codes.InvalidArgument, "file %s names part %d before the core part", f.GetName(), idx)
		}
		if idx < p.lastPart {
			return nil, status.Errorf(codes.InvalidArgument, "file %s returns to part %d after part %d: the files of a part must be contiguous",
				f.GetName(), idx, p.lastPart)
		}
		p.lastPart = idx
		p.seen[idx] = true
		// The client's part type, "" for the core part, names the files on disk.
		name, skip, err := nativefmt.SyncFileName(p.init.GetCatalog(), p.init.GetKind(), nativefmt.PartTypeAt(p.init, f.GetPartIndex()), f.GetName())
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		if skip {
			return nil, status.Errorf(codes.InvalidArgument, "file %s is not sent in a %s payload", f.GetName(), p.init.GetKind())
		}
		if idx != last {
			// A copy, so the files of one chunk do not leak into the payload's description: the
			// parts in p.parts carry no files, so the clone gets only this chunk's files.
			infos = append(infos, proto.Clone(p.parts[idx]).(*clusterv1.PartInfo))
			last = idx
		}
		cur := infos[len(infos)-1]
		off := uint32(offset)
		if f.GetSize() == 0 && offset == uint64(len(c.GetData())) && len(c.GetData()) > 0 {
			// A receiver before this release refuses a slice at offset == len(chunk_data); an
			// empty slice reads nothing, so any offset inside the chunk is equivalent.
			off = 0
		}
		cur.Files = append(cur.Files, &clusterv1.FileInfo{Name: name, Offset: off, Size: f.GetSize()})
		offset += uint64(f.GetSize())
	}
	if offset != uint64(len(c.GetData())) {
		return nil, status.Errorf(codes.InvalidArgument, "the file slices of chunk %d add up to %d bytes, the chunk holds %d", p.chunks, offset, len(c.GetData()))
	}
	return infos, nil
}

// complete sends the completion to every live target and waits for each to commit.
func (p *partsPayload) complete(c *transferv1.ImportPartsRequest_Completion) error {
	if c.GetTotalBytes() != p.bytes || c.GetTotalChunks() != p.chunks {
		return status.Errorf(codes.InvalidArgument, "the completion counts %d bytes in %d chunks, the relay received %d bytes in %d chunks",
			c.GetTotalBytes(), c.GetTotalChunks(), p.bytes, p.chunks)
	}
	if p.chunks == 0 {
		return status.Error(codes.InvalidArgument, "a payload carries at least one chunk")
	}
	if p.init.GetKind() == nativefmt.KindPart {
		// An index or property payload has one placeholder part, which may legitimately be empty.
		for i, ok := range p.seen {
			if !ok {
				return status.Errorf(codes.InvalidArgument, "part %d %q of the payload received no file", i, p.init.GetParts()[i].GetPartType())
			}
		}
	}
	p.exchange(&clusterv1.SyncPartRequest{
		SessionId:  p.session,
		ChunkIndex: p.chunks,
		Content: &clusterv1.SyncPartRequest_Completion{Completion: &clusterv1.SyncCompletion{
			TotalBytesSent: p.bytes, TotalPartsSent: uint32(len(p.parts)), TotalChunks: p.chunks,
		}},
		VersionInfo: p.version,
	}, clusterv1.SyncStatus_SYNC_STATUS_SYNC_COMPLETE)
	return nil
}

// exchange sends req to every live target and waits until each answered, marking a target
// that failed or answered anything but want as failed.
func (p *partsPayload) exchange(req *clusterv1.SyncPartRequest, want clusterv1.SyncStatus) {
	if p.alive() == 1 {
		// One target, as on a standalone process or without replicas: no goroutine per frame.
		for _, t := range p.targets {
			if t.err == nil {
				t.run(req, want)
			}
		}
		return
	}
	var wg sync.WaitGroup
	for _, t := range p.targets {
		if t.err != nil {
			continue
		}
		wg.Add(1)
		//panicdiag:allow-rawgo one goroutine per target of a per-frame barrier, joined below
		go func(t *partTarget) {
			defer wg.Done()
			t.run(req, want)
		}(t)
	}
	wg.Wait()
}

// run exchanges req with the target and cancels its stream when the exchange fails.
func (t *partTarget) run(req *clusterv1.SyncPartRequest, want clusterv1.SyncStatus) {
	if t.err = t.exchange(req, want); t.err != nil {
		t.cancel()
	}
}

func (t *partTarget) exchange(req *clusterv1.SyncPartRequest, want clusterv1.SyncStatus) error {
	timer := time.AfterFunc(relayFrameTimeout, func() {
		t.timedOut.Store(true)
		t.cancel()
	})
	defer timer.Stop()
	fail := func(err error) error {
		if t.timedOut.Load() {
			return t.timeoutErr(req)
		}
		return err
	}
	if err := t.stream.Send(req); err != nil {
		return fail(fmt.Errorf("send chunk %d: %w", req.GetChunkIndex(), recvFailure(t.stream, err)))
	}
	resp, err := t.stream.Recv()
	if err != nil {
		// The receiver commits before it answers the completion, so a failure here leaves the
		// outcome unknown. An answer, even a refusal, is a definite outcome.
		t.completionSent = req.GetCompletion() != nil
		return fail(fmt.Errorf("chunk %d: %w", req.GetChunkIndex(), err))
	}
	if resp.GetStatus() != want {
		msg := resp.GetError()
		if msg == "" {
			msg = "no detail"
		}
		return fmt.Errorf("chunk %d answered %s: %s", req.GetChunkIndex(), resp.GetStatus(), msg)
	}
	return nil
}

func (t *partTarget) timeoutErr(req *clusterv1.SyncPartRequest) error {
	return fmt.Errorf("chunk %d: node %s answered nothing within %s", req.GetChunkIndex(), t.node, relayFrameTimeout)
}

// recvFailure returns the status a stream ended with when Send reports io.EOF, which only
// tells that the server closed it.
func recvFailure(stream syncPartStream, err error) error {
	if !errors.Is(err, io.EOF) {
		return err
	}
	if _, recvErr := stream.Recv(); recvErr != nil && !errors.Is(recvErr, io.EOF) {
		return recvErr
	}
	return err
}

// alive counts the targets that have not failed.
func (p *partsPayload) alive() int {
	n := 0
	for _, t := range p.targets {
		if t.err == nil {
			n++
		}
	}
	return n
}

// response reports every target, in node order: payloadNodes sorts the nodes and openPayload
// keeps that order.
func (p *partsPayload) response() *transferv1.ImportPartsResponse {
	resp := &transferv1.ImportPartsResponse{RequestedCopies: p.copies}
	for _, t := range p.targets {
		r := &transferv1.ImportPartsResponse_TargetResult{Node: t.node, Success: t.err == nil}
		if t.err != nil {
			r.Error = t.err.Error()
			r.CompletionSent = t.completionSent
		}
		resp.Targets = append(resp.Targets, r)
	}
	return resp
}

// close ends every downstream stream.
func (p *partsPayload) close() {
	for _, t := range p.targets {
		if t.stream != nil {
			_ = t.stream.CloseSend()
		}
		if t.cancel != nil {
			t.cancel()
		}
	}
	if p.release != nil {
		p.release()
		p.release = nil
	}
}
