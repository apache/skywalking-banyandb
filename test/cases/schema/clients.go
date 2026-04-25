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

package schema

import (
	"context"
	"fmt"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	schemav1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/schema/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
)

// Clients holds all gRPC clients required by the schema integration test helpers.
type Clients struct {
	GroupClient              databasev1.GroupRegistryServiceClient
	MeasureRegClient         databasev1.MeasureRegistryServiceClient
	StreamRegClient          databasev1.StreamRegistryServiceClient
	TraceRegClient           databasev1.TraceRegistryServiceClient
	IndexRuleClient          databasev1.IndexRuleRegistryServiceClient
	IndexRuleBindingClient   databasev1.IndexRuleBindingRegistryServiceClient
	TopNAggregationRegClient databasev1.TopNAggregationRegistryServiceClient
	MeasureWriteClient       measurev1.MeasureServiceClient
	StreamWriteClient        streamv1.StreamServiceClient
	TraceWriteClient         tracev1.TraceServiceClient
	BarrierClient            schemav1.SchemaBarrierServiceClient
}

// NewClients constructs the registry/service client bundle used by the schema integration tests.
func NewClients(conn *grpc.ClientConn) *Clients {
	return &Clients{
		GroupClient:              databasev1.NewGroupRegistryServiceClient(conn),
		MeasureRegClient:         databasev1.NewMeasureRegistryServiceClient(conn),
		StreamRegClient:          databasev1.NewStreamRegistryServiceClient(conn),
		TraceRegClient:           databasev1.NewTraceRegistryServiceClient(conn),
		IndexRuleClient:          databasev1.NewIndexRuleRegistryServiceClient(conn),
		IndexRuleBindingClient:   databasev1.NewIndexRuleBindingRegistryServiceClient(conn),
		TopNAggregationRegClient: databasev1.NewTopNAggregationRegistryServiceClient(conn),
		MeasureWriteClient:       measurev1.NewMeasureServiceClient(conn),
		StreamWriteClient:        streamv1.NewStreamServiceClient(conn),
		TraceWriteClient:         tracev1.NewTraceServiceClient(conn),
		BarrierClient:            schemav1.NewSchemaBarrierServiceClient(conn),
	}
}

// AwaitRevision blocks until the cluster's schema cache has advanced to the given
// mod_revision or the timeout elapses. Delegates to SchemaBarrierService.AwaitRevisionApplied.
func (c *Clients) AwaitRevision(ctx context.Context, target int64, timeout time.Duration) error {
	resp, rpcErr := c.BarrierClient.AwaitRevisionApplied(ctx, &schemav1.AwaitRevisionAppliedRequest{
		MinRevision: target,
		Timeout:     durationpb.New(timeout),
	})
	if rpcErr != nil {
		return fmt.Errorf("AwaitRevisionApplied RPC failed: %w", rpcErr)
	}
	if !resp.GetApplied() {
		return fmt.Errorf("timeout waiting for schema revision %d", target)
	}
	return nil
}

// AwaitApplied blocks until every key in the list reports as present in the
// cluster's schema cache or the timeout elapses. Each key is encoded as
// "kind:group/name"; supported kinds are "measure", "stream", and "trace".
// Delegates to SchemaBarrierService.AwaitSchemaApplied.
func (c *Clients) AwaitApplied(ctx context.Context, keys []string, timeout time.Duration) error {
	schemaKeys, parseErr := parseSchemaKeys(keys)
	if parseErr != nil {
		return parseErr
	}
	resp, rpcErr := c.BarrierClient.AwaitSchemaApplied(ctx, &schemav1.AwaitSchemaAppliedRequest{
		Keys:    schemaKeys,
		Timeout: durationpb.New(timeout),
	})
	if rpcErr != nil {
		return fmt.Errorf("AwaitSchemaApplied RPC failed: %w", rpcErr)
	}
	if !resp.GetApplied() {
		return fmt.Errorf("timeout waiting for schema keys to be applied: %v", keys)
	}
	return nil
}

// AwaitDeleted blocks until every key in the list is absent from the cluster's
// schema cache or the timeout elapses. Encoding follows AwaitApplied.
// Delegates to SchemaBarrierService.AwaitSchemaDeleted.
func (c *Clients) AwaitDeleted(ctx context.Context, keys []string, timeout time.Duration) error {
	schemaKeys, parseErr := parseSchemaKeys(keys)
	if parseErr != nil {
		return parseErr
	}
	resp, rpcErr := c.BarrierClient.AwaitSchemaDeleted(ctx, &schemav1.AwaitSchemaDeletedRequest{
		Keys:    schemaKeys,
		Timeout: durationpb.New(timeout),
	})
	if rpcErr != nil {
		return fmt.Errorf("AwaitSchemaDeleted RPC failed: %w", rpcErr)
	}
	if !resp.GetApplied() {
		return fmt.Errorf("timeout waiting for schema keys to be deleted: %v", keys)
	}
	return nil
}

// internalTopNResultMeasureName is the auto-created measure that backs TopN
// aggregations. The measure subsystem provisions it in every measure group, so
// list assertions in the §6 specs need to skip it.
const internalTopNResultMeasureName = "_top_n_result"

// userMeasures returns measures with the auto-provisioned _top_n_result entry
// removed, so spec assertions can count user-created measures only without
// hard-coding off-by-one for the internal entry.
func userMeasures(in []*databasev1.Measure) []*databasev1.Measure {
	out := make([]*databasev1.Measure, 0, len(in))
	for _, m := range in {
		if m.GetMetadata().GetName() == internalTopNResultMeasureName {
			continue
		}
		out = append(out, m)
	}
	return out
}

// parseSchemaKeys converts a slice of "kind:group/name" encoded strings to SchemaKey protos.
func parseSchemaKeys(keys []string) ([]*schemav1.SchemaKey, error) {
	result := make([]*schemav1.SchemaKey, 0, len(keys))
	for _, key := range keys {
		sk, parseErr := parseSchemaKey(key)
		if parseErr != nil {
			return nil, parseErr
		}
		result = append(result, sk)
	}
	return result, nil
}

// parseSchemaKey splits an encoded "kind:group/name" key into a SchemaKey proto.
func parseSchemaKey(key string) (*schemav1.SchemaKey, error) {
	colon := strings.IndexByte(key, ':')
	if colon <= 0 || colon == len(key)-1 {
		return nil, fmt.Errorf("invalid schema key %q: expected kind:group/name", key)
	}
	kind := key[:colon]
	rest := key[colon+1:]
	slash := strings.IndexByte(rest, '/')
	if slash <= 0 || slash == len(rest)-1 {
		return nil, fmt.Errorf("invalid schema key %q: expected kind:group/name", key)
	}
	return &schemav1.SchemaKey{
		Kind:  kind,
		Group: rest[:slash],
		Name:  rest[slash+1:],
	}, nil
}
