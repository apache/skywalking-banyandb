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

package export

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/banyand/transfer/export/csv"
)

// Export implements transferv1.ExportServiceServer on a data node: it reads the units the
// client sends one at a time out of the session snapshot and streams the produced files
// back. Frame order: ready, then per unit unit_started, (file_started, chunk*,
// file_finished)*, unit_finished. A client half-close ends the stream.
func (s *Service) Export(stream transferv1.ExportService_ExportServer) error {
	ctx := stream.Context()
	first, err := stream.Recv()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return status.Error(codes.InvalidArgument, "export stream closed before the init frame")
		}
		return err
	}
	if err = first.ValidateAll(); err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	init := first.GetInit()
	if init == nil {
		return status.Error(codes.InvalidArgument, "the first export frame must be init")
	}
	producer := s.producerFor(ctx, init, stream.Send)
	if producer == nil {
		return status.Errorf(codes.Unimplemented, "%s export is not implemented yet", formatName(init.GetFormat()))
	}
	id := init.GetSessionId()
	if _, err = s.liveSession(id); err != nil {
		return err
	}
	caller := "unknown"
	if p, found := peer.FromContext(ctx); found && p.Addr != nil {
		caller = p.Addr.String()
	}
	audit := s.l.Info().Str("caller", caller).Str("session", id).Str("target", init.GetTargetNode()).
		Stringer("format", init.GetFormat()).Stringer("compress", init.GetCompress())
	audit.Msg("export stream opened")
	if err = stream.Send(&transferv1.ExportResponse{Frame: &transferv1.ExportResponse_Ready{Ready: &transferv1.Ready{}}}); err != nil {
		return err
	}
	var units, rows uint64
	defer func() {
		s.l.Info().Str("caller", caller).Str("session", id).Uint64("units", units).Uint64("rows", rows).Msg("export stream closed")
	}()
	for {
		req, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			return nil
		}
		if recvErr != nil {
			return recvErr
		}
		if err = req.ValidateAll(); err != nil {
			return status.Error(codes.InvalidArgument, err.Error())
		}
		var unitRows uint64
		if unitRows, err = s.exportUnit(id, req, producer, stream.Send); err != nil {
			return err
		}
		units++
		rows += unitRows
	}
}

// unitProducer turns one unit of a session snapshot into files and returns the unit's
// warnings and row count. Each artifact format has one implementation; only CSV exists so
// far (native answers Unimplemented before any unit).
type unitProducer interface {
	// Segment produces a stream, measure or trace unit; segDir is <root>/<group>/seg-<suffix>
	// and every requested shard directory under it exists.
	Segment(unit *transferv1.SegmentUnit, segDir string) ([]string, uint64, error)
	// Property produces a property unit; groupDir is <root>/<group> and every requested
	// shard directory under it exists.
	Property(unit *transferv1.PropertyUnit, groupDir string) ([]string, uint64, error)
}

// exportUnit renews the session, locates the unit in the snapshot and produces it,
// returning the unit's row count.
func (s *Service) exportUnit(id string, req *transferv1.ExportRequest, producer unitProducer,
	send func(*transferv1.ExportResponse) error,
) (uint64, error) {
	roots, err := s.liveSession(id)
	if err != nil {
		return 0, err
	}
	var shards []uint32
	var parts uint32
	var produce func() ([]string, uint64, error)
	switch frame := req.GetFrame().(type) {
	case *transferv1.ExportRequest_Segment:
		unit := frame.Segment
		segDir, locateErr := s.locateInSession(id, roots, unit.GetCatalog(), unit.GetGroup(), segDirPrefix+unit.GetSegmentSuffix(), unit.GetShardIds())
		if locateErr != nil {
			return 0, locateErr
		}
		shards = unit.GetShardIds()
		if parts, err = csv.CountLiveParts(segDir, shards); err != nil {
			return 0, err
		}
		produce = func() ([]string, uint64, error) { return producer.Segment(unit, segDir) }
	case *transferv1.ExportRequest_Property:
		unit := frame.Property
		groupDir, locateErr := s.locateInSession(id, roots, commonv1.Catalog_CATALOG_PROPERTY, unit.GetGroup(), "", unit.GetShardIds())
		if locateErr != nil {
			return 0, locateErr
		}
		shards = unit.GetShardIds()
		produce = func() ([]string, uint64, error) { return producer.Property(unit, groupDir) }
	default:
		return 0, status.Error(codes.InvalidArgument, "only the first export frame may be init; later frames must be units")
	}
	if err = send(&transferv1.ExportResponse{Frame: &transferv1.ExportResponse_UnitStarted{
		UnitStarted: &transferv1.UnitStarted{TotalShards: uint32(len(shards)), TotalParts: parts},
	}}); err != nil {
		return 0, err
	}
	warnings, rows, err := produce()
	if err != nil {
		return 0, err
	}
	// The readers take a file missing from the snapshot for an absent optional one, so a
	// release or takeover that removed the snapshot while the unit was read must be caught
	// before the unit is reported complete.
	if _, err = s.liveSession(id); err != nil {
		return 0, err
	}
	return rows, send(&transferv1.ExportResponse{Frame: &transferv1.ExportResponse_UnitFinished{
		UnitFinished: &transferv1.UnitFinished{Warnings: warnings},
	}})
}

// liveSession renews the session and returns its snapshot roots. A session this node does
// not hold is FailedPrecondition, not NotFound: the client reopens a stream refused with
// NotFound through another liaison, which only helps when a liaison cannot see the node.
func (s *Service) liveSession(id string) (map[commonv1.Catalog]string, error) {
	roots, err := s.planner.sessions.locateAndHeartbeat(id)
	if status.Code(err) == codes.NotFound {
		return nil, status.Errorf(codes.FailedPrecondition, "export session %s is not held on this node (released, preempted or never created)", id)
	}
	return roots, err
}

// locateInSession locates the unit and tells a session released or preempted after the
// renewal (FailedPrecondition) from a unit the live snapshot lacks (Internal).
func (s *Service) locateInSession(id string, roots map[commonv1.Catalog]string, catalog commonv1.Catalog, group, segment string,
	shards []uint32,
) (string, error) {
	dir, err := locateUnit(roots, catalog, group, segment, shards)
	if status.Code(err) != codes.Internal {
		return dir, err
	}
	if s.planner.sessions.verify(id) != nil {
		return "", status.Errorf(codes.FailedPrecondition, "export session %s is gone (released or preempted)", id)
	}
	return "", err
}

// locateUnit resolves <root>/<group>[/<segment>] and checks that it and every requested
// shard directory exist. A catalog the session did not snapshot is the client's mistake
// (InvalidArgument). The units come from the session's own inventory and the snapshot is
// frozen, so a missing group, segment or shard is a node-side fault (Internal), unless the
// session went away meanwhile, which locateInSession checks.
func locateUnit(roots map[commonv1.Catalog]string, catalog commonv1.Catalog, group, segment string, shards []uint32) (string, error) {
	root, ok := roots[catalog]
	if !ok {
		return "", status.Errorf(codes.InvalidArgument, "the export session holds no %s snapshot on this node", catalog)
	}
	if !validPathComponent(group) {
		return "", status.Errorf(codes.InvalidArgument, "invalid group name %q", group)
	}
	dir := filepath.Join(root, group)
	if segment != "" {
		dir = filepath.Join(dir, segment)
	}
	if err := requireDir(dir); err != nil {
		return "", err
	}
	for _, shard := range shards {
		if err := requireDir(filepath.Join(dir, fmt.Sprintf("%s%d", shardDirPrefix, shard))); err != nil {
			return "", err
		}
	}
	return dir, nil
}

// requireDir checks that dir is a directory of the snapshot, failing with Internal otherwise.
func requireDir(dir string) error {
	info, err := os.Stat(dir)
	if err != nil {
		return status.Errorf(codes.Internal, "export unit directory %s is not in the session snapshot: %v", dir, err)
	}
	if !info.IsDir() {
		return status.Errorf(codes.Internal, "export unit path %s is not a directory", dir)
	}
	return nil
}

// validPathComponent rejects group names that could leave the snapshot root.
func validPathComponent(name string) bool {
	return name != "" && name != "." && name != ".." && filepath.Base(name) == name && !filepath.IsAbs(name)
}

// producerFor returns the unit producer of a format for one stream, or nil when the
// format is not implemented (native).
func (s *Service) producerFor(ctx context.Context, init *transferv1.ExportRequest_Init,
	send func(*transferv1.ExportResponse) error,
) unitProducer {
	if init.GetFormat() == transferv1.Format_FORMAT_CSV {
		indexMode, _ := s.backends[commonv1.Catalog_CATALOG_MEASURE].(csv.IndexModeReader)
		return csv.NewProducer(ctx, s.meta, indexMode, init, send)
	}
	return nil
}

func formatName(f transferv1.Format) string {
	switch f {
	case transferv1.Format_FORMAT_NATIVE:
		return "native"
	case transferv1.Format_FORMAT_CSV:
		return "csv"
	default:
		return f.String()
	}
}
