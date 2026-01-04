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

// Package grpc provides functionality for the gRPC server.
package grpc

import (
	"net"

	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	fodcv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/fodc/v1"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// Server wraps the gRPC server.
type Server struct {
	service    *FODCService
	server     *grpclib.Server
	logger     *logger.Logger
	listenAddr string
}

// NewServer creates a new gRPC server instance.
func NewServer(service *FODCService, listenAddr string, maxMsgSize int, logger *logger.Logger) *Server {
	opts := []grpclib.ServerOption{
		grpclib.MaxRecvMsgSize(maxMsgSize),
		grpclib.Creds(insecure.NewCredentials()),
	}

	return &Server{
		service:    service,
		server:     grpclib.NewServer(opts...),
		listenAddr: listenAddr,
		logger:     logger,
	}
}

// Start starts the gRPC server.
func (s *Server) Start() error {
	fodcv1.RegisterFODCServiceServer(s.server, s.service)

	lis, listenErr := net.Listen("tcp", s.listenAddr)
	if listenErr != nil {
		return listenErr
	}

	s.logger.Info().Str("addr", s.listenAddr).Msg("Starting gRPC server")

	go func() {
		if serveErr := s.server.Serve(lis); serveErr != nil {
			s.logger.Error().Err(serveErr).Msg("gRPC server error")
		}
	}()

	return nil
}

// Stop gracefully stops the gRPC server.
func (s *Server) Stop() {
	if s.server != nil {
		s.server.GracefulStop()
	}
}
