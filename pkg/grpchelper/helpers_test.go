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

package grpchelper

import (
	"google.golang.org/grpc"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
)

// mockClient implements the Client interface for tests.
type mockClient struct {
	closed bool
}

func (m *mockClient) Close() error {
	m.closed = true
	return nil
}

// mockHandler implements ConnectionHandler[*mockClient] for tests.
type mockHandler struct {
	activeNodes   map[string]*mockClient
	inactiveNodes map[string]*mockClient
}

func (h *mockHandler) AddressOf(node *databasev1.Node) string {
	return node.GetGrpcAddress()
}

func (h *mockHandler) GetDialOptions(_ string) ([]grpc.DialOption, error) {
	return nil, nil
}

func (h *mockHandler) NewClient(_ *grpc.ClientConn, _ *databasev1.Node) (*mockClient, error) {
	return &mockClient{}, nil
}

func (h *mockHandler) OnActive(name string, client *mockClient) {
	if h.activeNodes == nil {
		h.activeNodes = make(map[string]*mockClient)
	}
	h.activeNodes[name] = client
}

func (h *mockHandler) OnInactive(name string, client *mockClient) {
	if h.inactiveNodes == nil {
		h.inactiveNodes = make(map[string]*mockClient)
	}
	h.inactiveNodes[name] = client
	delete(h.activeNodes, name)
}
