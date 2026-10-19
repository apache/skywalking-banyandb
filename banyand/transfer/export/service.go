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
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/apache/skywalking-banyandb/api/common"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

// Service is the data-node export unit. It builds the Planner once node labels are known
// (PreRun), registers it as the gRPC ExportService of the data-node pipeline, exposes the
// same operations in-process for the standalone liaison, and sweeps expired sessions.
type Service struct {
	transferv1.UnimplementedExportServiceServer
	meta     metadata.Repo
	pipeline queue.Server
	planner  *Planner
	closer   *run.Closer
	l        *logger.Logger
	backends Backends
}

var (
	_ run.PreRunner                  = (*Service)(nil)
	_ run.Service                    = (*Service)(nil)
	_ transferv1.ExportServiceServer = (*Service)(nil)
)

// NewService creates the export unit. pipeline is the data-node gRPC server the
// ExportService registers on; the local queue ignores the registration.
func NewService(meta metadata.Repo, pipeline queue.Server, backends Backends) *Service {
	return &Service{meta: meta, pipeline: pipeline, backends: backends, closer: run.NewCloser(1)}
}

// Name implements run.Unit.
func (s *Service) Name() string { return "export" }

// PreRun implements run.PreRunner: reject colliding export snapshot directories (the
// storage services resolve them in their own PreRun, which runs first), read the node
// labels from the context, build the planner and register the gRPC service.
func (s *Service) PreRun(ctx context.Context) error {
	s.l = logger.GetLogger(s.Name())
	if err := s.backends.validateDirs(); err != nil {
		return err
	}
	if err := s.backends.validatePartReaders(); err != nil {
		return err
	}
	var labels map[string]string
	if v := ctx.Value(common.ContextNodeKey); v != nil {
		labels = v.(common.Node).Labels
	}
	s.planner = NewPlanner(s.meta, s.backends, labels, s.l)
	s.pipeline.SetExportServer(s)
	return nil
}

// Serve implements run.Service: the sweep of expired export sessions, first right away to
// reclaim sessions that expired while the node was down (live ones are kept, which is how
// a restarted node recovers its sessions), then every SweepInterval.
func (s *Service) Serve() run.StopNotify {
	run.Go(s.closer.Ctx(), "export-session-sweep", s.l, func(ctx context.Context) {
		defer s.closer.Done()
		s.planner.sessions.sweep()
		ticker := time.NewTicker(SweepInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.planner.sessions.sweep()
			}
		}
	})
	return s.closer.CloseNotify()
}

// GracefulStop implements run.Service.
func (s *Service) GracefulStop() {
	s.closer.CloseThenWait()
}

// Plan implements transferv1.ExportServiceServer. The data-node stream chain has no
// validator interceptor, so the request is validated here.
func (s *Service) Plan(req *transferv1.PlanRequest, stream transferv1.ExportService_PlanServer) error {
	if err := req.ValidateAll(); err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	return s.planner.PlanInventory(stream.Context(), req, stream.Send)
}

// Sessions implements transferv1.ExportServiceServer on a data node: a single frame per
// call (the liaison stamps node_id).
func (s *Service) Sessions(req *transferv1.SessionsRequest, stream transferv1.ExportService_SessionsServer) error {
	if err := req.ValidateAll(); err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	frame, err := s.planner.Sessions(req)
	if err != nil {
		return err
	}
	return stream.Send(frame)
}
