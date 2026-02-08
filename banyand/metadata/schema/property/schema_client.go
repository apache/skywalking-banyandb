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

package property

import (
	"fmt"

	"google.golang.org/grpc"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	schemav1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/schema/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/banyand/queue/pub"
)

// schemaClient implements queue.PubClient interface for schema operations.
type schemaClient struct {
	mgrClient    schemav1.SchemaManagementServiceClient
	updateClient schemav1.SchemaUpdateServiceClient
	conn         *grpc.ClientConn
	md           schema.Metadata
}

// Conn returns the underlying gRPC connection.
func (c *schemaClient) Conn() *grpc.ClientConn { return c.conn }

// Metadata returns the node's metadata.
func (c *schemaClient) Metadata() schema.Metadata { return c.md }

// Close closes the connection.
func (c *schemaClient) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// MgrClient returns the schema management service client.
func (c *schemaClient) MgrClient() schemav1.SchemaManagementServiceClient {
	return c.mgrClient
}

// UpdateClient returns the schema update service client.
func (c *schemaClient) UpdateClient() schemav1.SchemaUpdateServiceClient {
	return c.updateClient
}

// schemaClientFactory creates schemaClient instances.
func schemaClientFactory(registry *SchemaRegistry) func(conn *grpc.ClientConn, md schema.Metadata) (queue.PubClient, error) {
	return func(conn *grpc.ClientConn, md schema.Metadata) (queue.PubClient, error) {
		if conn == nil {
			n, ok := md.Spec.(*databasev1.Node)
			if !ok {
				return nil, fmt.Errorf("invalid metadata spec: %T", md.Spec)
			}
			if n.GrpcAddress != pub.SelfGrpcAddress {
				return nil, fmt.Errorf("expected gRPC address %s, got %s", pub.SelfGrpcAddress, n.GrpcAddress)
			}
			return &schemaClient{
				mgrClient:    registry.localMgrClient,
				updateClient: registry.localUpdateClient,
				conn:         nil,
				md:           md,
			}, nil
		}
		return &schemaClient{
			mgrClient:    schemav1.NewSchemaManagementServiceClient(conn),
			updateClient: schemav1.NewSchemaUpdateServiceClient(conn),
			conn:         conn,
			md:           md,
		}, nil
	}
}
