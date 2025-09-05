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

package streamvstrace

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
)

// TraceClient provides methods to interact with trace services.
type TraceClient struct {
	registryClient         databasev1.TraceRegistryServiceClient
	serviceClient          tracev1.TraceServiceClient
	groupClient            databasev1.GroupRegistryServiceClient
	indexRuleClient        databasev1.IndexRuleRegistryServiceClient
	indexRuleBindingClient databasev1.IndexRuleBindingRegistryServiceClient
}

// NewTraceClient creates a new TraceClient instance.
func NewTraceClient(conn *grpc.ClientConn) *TraceClient {
	return &TraceClient{
		registryClient:         databasev1.NewTraceRegistryServiceClient(conn),
		serviceClient:          tracev1.NewTraceServiceClient(conn),
		groupClient:            databasev1.NewGroupRegistryServiceClient(conn),
		indexRuleClient:        databasev1.NewIndexRuleRegistryServiceClient(conn),
		indexRuleBindingClient: databasev1.NewIndexRuleBindingRegistryServiceClient(conn),
	}
}

// VerifySchema checks if a trace schema exists.
func (c *TraceClient) VerifySchema(ctx context.Context, group, name string) (bool, error) {
	req := &databasev1.TraceRegistryServiceGetRequest{
		Metadata: &commonv1.Metadata{
			Group: group,
			Name:  name,
		},
	}

	_, err := c.registryClient.Get(ctx, req)
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return false, nil
		}
		return false, fmt.Errorf("failed to get trace schema: %w", err)
	}

	return true, nil
}

func (c *TraceClient) Write(ctx context.Context, _ *tracev1.WriteRequest) (tracev1.TraceService_WriteClient, error) {
	return c.serviceClient.Write(ctx)
}

// Query executes a query against the trace service.
func (c *TraceClient) Query(ctx context.Context, req *tracev1.QueryRequest) (*tracev1.QueryResponse, error) {
	return c.serviceClient.Query(ctx, req)
}

// VerifyGroup checks if a group exists.
func (c *TraceClient) VerifyGroup(ctx context.Context, group string) (bool, error) {
	req := &databasev1.GroupRegistryServiceGetRequest{
		Group: group,
	}

	_, err := c.groupClient.Get(ctx, req)
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return false, nil
		}
		return false, fmt.Errorf("failed to get group: %w", err)
	}

	return true, nil
}

// VerifyIndexRule checks if an index rule exists.
func (c *TraceClient) VerifyIndexRule(ctx context.Context, group, name string) (bool, error) {
	req := &databasev1.IndexRuleRegistryServiceGetRequest{
		Metadata: &commonv1.Metadata{
			Group: group,
			Name:  name,
		},
	}

	_, err := c.indexRuleClient.Get(ctx, req)
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return false, nil
		}
		return false, fmt.Errorf("failed to get index rule: %w", err)
	}

	return true, nil
}

// VerifyIndexRuleBinding checks if an index rule binding exists.
func (c *TraceClient) VerifyIndexRuleBinding(ctx context.Context, group, name string) (bool, error) {
	req := &databasev1.IndexRuleBindingRegistryServiceGetRequest{
		Metadata: &commonv1.Metadata{
			Group: group,
			Name:  name,
		},
	}

	_, err := c.indexRuleBindingClient.Get(ctx, req)
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return false, nil
		}
		return false, fmt.Errorf("failed to get index rule binding: %w", err)
	}

	return true, nil
}
