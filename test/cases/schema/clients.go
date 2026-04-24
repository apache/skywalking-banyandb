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
	"errors"
	"fmt"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	schemav1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/schema/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
)

// Clients holds all gRPC clients required by the schema integration test
// helpers. The BarrierClient is left nil at this step (Step 1.0) and will be
// populated by Step 1.8 once the SchemaBarrierService server is registered.
type Clients struct {
	GroupClient            databasev1.GroupRegistryServiceClient
	MeasureRegClient       databasev1.MeasureRegistryServiceClient
	StreamRegClient        databasev1.StreamRegistryServiceClient
	TraceRegClient         databasev1.TraceRegistryServiceClient
	IndexRuleClient        databasev1.IndexRuleRegistryServiceClient
	IndexRuleBindingClient databasev1.IndexRuleBindingRegistryServiceClient
	MeasureWriteClient     measurev1.MeasureServiceClient
	StreamWriteClient      streamv1.StreamServiceClient
	TraceWriteClient       tracev1.TraceServiceClient
	// BarrierClient is intentionally nil pre-Step-1.8. Step 1.8 will replace
	// the constructor body to wire schemav1.NewSchemaBarrierServiceClient(conn).
	BarrierClient schemav1.SchemaBarrierServiceClient
}

// NewClients constructs the registry/service client bundle used by the schema
// integration tests. Step 1.8 will extend this constructor to wire the
// SchemaBarrierService client; until then the BarrierClient field stays nil
// and the Await* helpers fall back to direct registry polling.
func NewClients(conn *grpc.ClientConn) *Clients {
	return &Clients{
		GroupClient:            databasev1.NewGroupRegistryServiceClient(conn),
		MeasureRegClient:       databasev1.NewMeasureRegistryServiceClient(conn),
		StreamRegClient:        databasev1.NewStreamRegistryServiceClient(conn),
		TraceRegClient:         databasev1.NewTraceRegistryServiceClient(conn),
		IndexRuleClient:        databasev1.NewIndexRuleRegistryServiceClient(conn),
		IndexRuleBindingClient: databasev1.NewIndexRuleBindingRegistryServiceClient(conn),
		MeasureWriteClient:     measurev1.NewMeasureServiceClient(conn),
		StreamWriteClient:      streamv1.NewStreamServiceClient(conn),
		TraceWriteClient:       tracev1.NewTraceServiceClient(conn),
		// BarrierClient: schemav1.NewSchemaBarrierServiceClient(conn), // enabled by Step 1.8.
	}
}

const (
	awaitInitialInterval = 10 * time.Millisecond
	awaitMaxInterval     = 500 * time.Millisecond
	awaitGrowthFactor    = 1.5
	awaitRevisionBudget  = 200 * time.Millisecond
)

// AwaitRevision blocks until the cluster's schema cache has advanced to the
// given mod_revision or the timeout elapses. The pre-Step-1.8 implementation
// is a bounded sleep placeholder because the surrounding flows already issue
// synchronous registry RPCs; Step 1.8 swaps the body to delegate to
// BarrierClient.AwaitRevisionApplied.
func (c *Clients) AwaitRevision(ctx context.Context, _ int64, timeout time.Duration) error {
	// TODO(Step 1.8): replace body with BarrierClient delegation.
	wait := awaitRevisionBudget
	if timeout > 0 && timeout < wait {
		wait = timeout
	}
	select {
	case <-time.After(wait):
		return nil
	case <-ctx.Done():
		if errors.Is(ctx.Err(), context.Canceled) {
			return fmt.Errorf("schema revision await canceled: %w", ctx.Err())
		}
		return fmt.Errorf("timeout waiting for schema revision: %w", ctx.Err())
	}
}

// AwaitApplied blocks until every key in the list reports as present in the
// cluster's schema cache or the timeout elapses. Each key is encoded as
// "kind:group/name"; supported kinds are "measure", "stream", and "trace".
// Pre-Step-1.8 the body polls the registry Get RPCs; Step 1.8 delegates to
// BarrierClient.AwaitSchemaApplied.
func (c *Clients) AwaitApplied(ctx context.Context, keys []string, timeout time.Duration) error {
	// TODO(Step 1.8): replace body with BarrierClient delegation.
	return c.pollKeys(ctx, keys, timeout, true)
}

// AwaitDeleted blocks until every key in the list is absent from the cluster's
// schema cache or the timeout elapses. Encoding follows AwaitApplied.
// Pre-Step-1.8 the body polls the registry Get RPCs for NotFound; Step 1.8
// delegates to BarrierClient.AwaitSchemaDeleted.
func (c *Clients) AwaitDeleted(ctx context.Context, keys []string, timeout time.Duration) error {
	// TODO(Step 1.8): replace body with BarrierClient delegation.
	return c.pollKeys(ctx, keys, timeout, false)
}

// pollKeys polls each key with bounded exponential backoff until either every
// key satisfies the present/absent predicate or the deadline expires.
func (c *Clients) pollKeys(ctx context.Context, keys []string, timeout time.Duration, wantPresent bool) error {
	deadline := time.Now().Add(timeout)
	pollCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	interval := awaitInitialInterval
	for {
		allDone := true
		var lastErr error
		for _, key := range keys {
			present, checkErr := c.checkKey(pollCtx, key)
			if checkErr != nil {
				lastErr = checkErr
				allDone = false
				break
			}
			if present != wantPresent {
				allDone = false
				break
			}
		}
		if allDone {
			return nil
		}
		if lastErr != nil && !isTransient(lastErr) {
			return lastErr
		}
		if time.Now().After(deadline) {
			if wantPresent {
				return fmt.Errorf("timeout waiting for schema keys to be applied: %v", keys)
			}
			return fmt.Errorf("timeout waiting for schema keys to be deleted: %v", keys)
		}
		select {
		case <-time.After(interval):
		case <-pollCtx.Done():
			// Distinguish parent-context cancellation from a real deadline expiry so callers aren't
			// misled by a "timeout" message when the test itself canceled the context.
			if errors.Is(pollCtx.Err(), context.Canceled) {
				return fmt.Errorf("schema await canceled while waiting on keys %v: %w", keys, pollCtx.Err())
			}
			if wantPresent {
				return fmt.Errorf("timeout waiting for schema keys to be applied: %v", keys)
			}
			return fmt.Errorf("timeout waiting for schema keys to be deleted: %v", keys)
		}
		interval = time.Duration(float64(interval) * awaitGrowthFactor)
		if interval > awaitMaxInterval {
			interval = awaitMaxInterval
		}
	}
}

// checkKey returns whether the given encoded key currently resolves to a live
// schema entry. Unknown kinds yield an error so callers fail fast rather than
// silently waiting forever.
func (c *Clients) checkKey(ctx context.Context, key string) (bool, error) {
	kind, group, name, parseErr := parseSchemaKey(key)
	if parseErr != nil {
		return false, parseErr
	}
	metadata := &commonv1.Metadata{Group: group, Name: name}
	var rpcErr error
	switch kind {
	case "measure":
		_, rpcErr = c.MeasureRegClient.Get(ctx, &databasev1.MeasureRegistryServiceGetRequest{Metadata: metadata})
	case "stream":
		_, rpcErr = c.StreamRegClient.Get(ctx, &databasev1.StreamRegistryServiceGetRequest{Metadata: metadata})
	case "trace":
		_, rpcErr = c.TraceRegClient.Get(ctx, &databasev1.TraceRegistryServiceGetRequest{Metadata: metadata})
	default:
		return false, fmt.Errorf("unsupported schema key kind %q in %q", kind, key)
	}
	if rpcErr == nil {
		return true, nil
	}
	if st, ok := status.FromError(rpcErr); ok && st.Code() == codes.NotFound {
		return false, nil
	}
	return false, rpcErr
}

// parseSchemaKey splits an encoded "kind:group/name" key into its components.
func parseSchemaKey(key string) (string, string, string, error) {
	colon := strings.IndexByte(key, ':')
	if colon <= 0 || colon == len(key)-1 {
		return "", "", "", fmt.Errorf("invalid schema key %q: expected kind:group/name", key)
	}
	kind := key[:colon]
	rest := key[colon+1:]
	slash := strings.IndexByte(rest, '/')
	if slash <= 0 || slash == len(rest)-1 {
		return "", "", "", fmt.Errorf("invalid schema key %q: expected kind:group/name", key)
	}
	return kind, rest[:slash], rest[slash+1:], nil
}

// isTransient reports whether an RPC error is worth retrying inside the poll
// loop. Context errors signal we have already exhausted the deadline.
func isTransient(err error) bool {
	if err == nil {
		return false
	}
	if st, ok := status.FromError(err); ok {
		switch st.Code() {
		case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted, codes.Aborted:
			return true
		}
	}
	return false
}
