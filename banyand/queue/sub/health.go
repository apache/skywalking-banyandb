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

package sub

import (
	"context"

	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/apache/skywalking-banyandb/pkg/bus"
)

type healthServer struct {
	*health.Server
	listeners map[string]bus.MessageListener
}

func newHealthServer(listeners map[bus.Topic]bus.MessageListener) *healthServer {
	s := &healthServer{
		Server:    health.NewServer(),
		listeners: make(map[string]bus.MessageListener, len(listeners)),
	}
	for t, l := range listeners {
		topic := t.String()
		s.listeners[topic] = l
	}
	return s
}

func (s *healthServer) Check(ctx context.Context, in *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	if l, ok := s.listeners[in.Service]; ok {
		if l.CheckHealth() != nil {
			s.SetServingStatus(in.Service, healthpb.HealthCheckResponse_NOT_SERVING)
			return &healthpb.HealthCheckResponse{
				Status: healthpb.HealthCheckResponse_NOT_SERVING,
			}, nil
		}
		s.SetServingStatus(in.Service, healthpb.HealthCheckResponse_SERVING)
		return &healthpb.HealthCheckResponse{
			Status: healthpb.HealthCheckResponse_SERVING,
		}, nil
	}
	return s.Server.Check(ctx, in)
}
