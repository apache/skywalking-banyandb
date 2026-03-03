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

// Package property implements a property-based schema registry client.
package property

import (
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	schemav1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/schema/v1"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pkgtls "github.com/apache/skywalking-banyandb/pkg/tls"
)

type schemaClient struct {
	management schemav1.SchemaManagementServiceClient
	update     schemav1.SchemaUpdateServiceClient
}

// Close implements grpchelper.Client.
func (c *schemaClient) Close() error {
	return nil
}

var _ grpchelper.Client = (*schemaClient)(nil)

type connectionHandler struct {
	l              *logger.Logger
	caCertReloader *pkgtls.Reloader
	tlsEnabled     bool
}

var _ grpchelper.ConnectionHandler[*schemaClient] = (*connectionHandler)(nil)

// AddressOf extracts the schema gRPC address from a node.
func (h *connectionHandler) AddressOf(node *databasev1.Node) string {
	return node.GetPropertySchemaGrpcAddress()
}

// GetDialOptions returns gRPC dial options with optional TLS support.
func (h *connectionHandler) GetDialOptions() ([]grpc.DialOption, error) {
	if !h.tlsEnabled {
		return grpchelper.SecureOptions(nil, false, false, "")
	}
	if h.caCertReloader != nil {
		tlsConfig, configErr := h.caCertReloader.GetClientTLSConfig("")
		if configErr != nil {
			return nil, fmt.Errorf("failed to get TLS config from reloader: %w", configErr)
		}
		creds := credentials.NewTLS(tlsConfig)
		return []grpc.DialOption{grpc.WithTransportCredentials(creds)}, nil
	}
	return grpchelper.SecureOptions(nil, true, false, "")
}

// NewClient creates a schemaClient from a gRPC connection.
func (h *connectionHandler) NewClient(conn *grpc.ClientConn, _ *databasev1.Node) (*schemaClient, error) {
	return &schemaClient{
		management: schemav1.NewSchemaManagementServiceClient(conn),
		update:     schemav1.NewSchemaUpdateServiceClient(conn),
	}, nil
}

// OnActive is called when a node transitions to active.
func (h *connectionHandler) OnActive(name string, _ *schemaClient) {
	h.l.Info().Str("node", name).Msg("schema server node is active")
}

// OnInactive is called when a node leaves active.
func (h *connectionHandler) OnInactive(name string, _ *schemaClient) {
	h.l.Info().Str("node", name).Msg("schema server node is inactive")
}
