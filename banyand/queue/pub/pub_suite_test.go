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

package pub

import (
	"io"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	clusterv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/cluster/v1"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
)

func TestPub(t *testing.T) {
	gomega.RegisterFailHandler(ginkgo.Fail)
	ginkgo.RunSpecs(t, "Publish Suite")
}

var _ = ginkgo.BeforeSuite(func() {
	gomega.Expect(logger.Init(logger.Logging{
		Env:   "dev",
		Level: flags.LogLevel,
	})).To(gomega.Succeed())
})

type mockServer struct {
	clusterv1.UnimplementedServiceServer
	healthServer *health.Server
	latency      time.Duration
	code         codes.Code
}

func (s *mockServer) Send(stream clusterv1.Service_SendServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}

		if s.code != codes.OK {
			s.healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)
			return status.Error(s.code, "mock error")
		}

		if s.latency > 0 {
			time.Sleep(s.latency)
		}

		res := &clusterv1.SendResponse{
			MessageId: req.MessageId,
			Error:     "",
			Body:      req.Body,
		}

		if err := stream.Send(res); err != nil {
			return err
		}
	}
}

func setup(address string, code codes.Code, latency time.Duration) func() {
	s := grpc.NewServer()
	hs := health.NewServer()
	clusterv1.RegisterServiceServer(s, &mockServer{
		code:         code,
		latency:      latency,
		healthServer: hs,
	})
	grpc_health_v1.RegisterHealthServer(s, hs)
	go func() {
		lis, err := net.Listen("tcp", address)
		if err != nil {
			logger.Panicf("failed to listen: %v", err)
			return
		}
		if err := s.Serve(lis); err != nil {
			logger.Panicf("Server exited with error: %v", err)
		}
	}()
	return s.GracefulStop
}

func getAddress() string {
	ports, err := test.AllocateFreePorts(1)
	if err != nil {
		logger.Panicf("failed to allocate free ports: %v", err)
		return ""
	}
	return net.JoinHostPort("localhost", strconv.Itoa(ports[0]))
}

type mockHandler struct {
	addOrUpdateCount int
	deleteCount      int
}

func (m *mockHandler) OnInit(_ []schema.Kind) (bool, []int64) {
	panic("no implemented")
}

func (m *mockHandler) OnAddOrUpdate(_ schema.Metadata) {
	m.addOrUpdateCount++
}

func (m *mockHandler) OnDelete(_ schema.Metadata) {
	m.deleteCount++
}

func newPub() *pub {
	p := New(nil).(*pub)
	p.log = logger.GetLogger("pub")
	p.handler = &mockHandler{}
	return p
}

func getDataNode(name string, address string) schema.Metadata {
	return schema.Metadata{
		TypeMeta: schema.TypeMeta{
			Name: name,
			Kind: schema.KindNode,
		},
		Spec: &databasev1.Node{
			Metadata: &commonv1.Metadata{
				Name: name,
			},
			Roles:       []databasev1.Role{databasev1.Role_ROLE_DATA},
			GrpcAddress: address,
		},
	}
}
