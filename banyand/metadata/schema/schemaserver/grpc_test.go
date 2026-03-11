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

package schemaserver

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	schemav1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/schema/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/observability"
)

func startTestServer(t *testing.T) (schemav1.SchemaManagementServiceClient, schemav1.SchemaUpdateServiceClient) {
	t.Helper()
	srv := NewServer(observability.BypassRegistry).(*server)
	flagSet := srv.FlagSet()
	require.NoError(t, flagSet.Parse([]string{}))
	srv.root = t.TempDir()
	srv.host = "127.0.0.1"
	srv.port = getFreePort(t)
	require.NoError(t, srv.Validate())
	require.NoError(t, srv.PreRun(context.Background()))
	srv.Serve()
	t.Cleanup(func() { srv.GracefulStop() })
	// Wait deterministically for the server to start accepting connections.
	deadline := time.Now().Add(5 * time.Second)
	for {
		conn, err := net.DialTimeout("tcp", srv.addr, 100*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			break
		}
		if time.Now().After(deadline) {
			require.FailNowf(t, "server did not start listening in time", "last error: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	conn, dialErr := grpc.NewClient(srv.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, dialErr)
	t.Cleanup(func() { _ = conn.Close() })
	return schemav1.NewSchemaManagementServiceClient(conn), schemav1.NewSchemaUpdateServiceClient(conn)
}

func getFreePort(t *testing.T) uint32 {
	t.Helper()
	lis, lisErr := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, lisErr)
	port := lis.Addr().(*net.TCPAddr).Port
	_ = lis.Close()
	return uint32(port)
}

func collectListResponses(stream schemav1.SchemaManagementService_ListSchemasClient) ([]*schemav1.ListSchemasResponse, error) {
	var responses []*schemav1.ListSchemasResponse
	for {
		resp, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			return responses, nil
		}
		if recvErr != nil {
			return responses, recvErr
		}
		responses = append(responses, resp)
	}
}

func testProperty(name, id string) *propertyv1.Property {
	return &propertyv1.Property{
		Metadata: &commonv1.Metadata{
			Group:       "test-group",
			Name:        name,
			ModRevision: 1,
		},
		Id: id,
		Tags: []*modelv1.Tag{
			{Key: "k", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "v"}}}},
		},
	}
}

func insertProperty(t *testing.T, mgmt schemav1.SchemaManagementServiceClient, name, id string) {
	t.Helper()
	_, rpcErr := mgmt.InsertSchema(context.Background(), &schemav1.InsertSchemaRequest{
		Property: testProperty(name, id),
	})
	require.NoError(t, rpcErr)
}

func TestInsertSchema(t *testing.T) {
	t.Run("nil property", func(t *testing.T) {
		mgmt, _ := startTestServer(t)
		_, rpcErr := mgmt.InsertSchema(context.Background(), &schemav1.InsertSchemaRequest{})
		require.Error(t, rpcErr)
		assert.Equal(t, codes.InvalidArgument, grpcstatus.Code(rpcErr))
		assert.Contains(t, rpcErr.Error(), "property is required")
	})
	t.Run("nil metadata", func(t *testing.T) {
		mgmt, _ := startTestServer(t)
		_, rpcErr := mgmt.InsertSchema(context.Background(), &schemav1.InsertSchemaRequest{
			Property: &propertyv1.Property{},
		})
		require.Error(t, rpcErr)
		assert.Equal(t, codes.InvalidArgument, grpcstatus.Code(rpcErr))
		assert.Contains(t, rpcErr.Error(), "Metadata")
	})
	t.Run("successful insert", func(t *testing.T) {
		mgmt, _ := startTestServer(t)
		insertProperty(t, mgmt, "svc", "id1")
		stream, streamErr := mgmt.ListSchemas(context.Background(), &schemav1.ListSchemasRequest{
			Query: &propertyv1.QueryRequest{Groups: []string{schema.SchemaGroup}, Name: "svc", Ids: []string{"id1"}},
		})
		require.NoError(t, streamErr)
		responses, recvErr := collectListResponses(stream)
		require.NoError(t, recvErr)
		require.Len(t, responses, 1)
		assert.Len(t, responses[0].Properties, 1)
	})
	t.Run("duplicate insert", func(t *testing.T) {
		mgmt, _ := startTestServer(t)
		insertProperty(t, mgmt, "svc", "id1")
		_, rpcErr := mgmt.InsertSchema(context.Background(), &schemav1.InsertSchemaRequest{
			Property: testProperty("svc", "id1"),
		})
		require.Error(t, rpcErr)
		assert.Equal(t, codes.AlreadyExists, grpcstatus.Code(rpcErr))
		assert.Contains(t, rpcErr.Error(), "schema already exists")
	})
	t.Run("insert after delete", func(t *testing.T) {
		mgmt, _ := startTestServer(t)
		insertProperty(t, mgmt, "svc", "id1")
		_, deleteErr := mgmt.DeleteSchema(context.Background(), &schemav1.DeleteSchemaRequest{
			Delete: &propertyv1.DeleteRequest{Group: schema.SchemaGroup, Name: "svc", Id: "id1"}, UpdateAt: timestamppb.Now(),
		})
		require.NoError(t, deleteErr)
		_, rpcErr := mgmt.InsertSchema(context.Background(), &schemav1.InsertSchemaRequest{
			Property: testProperty("svc", "id1"),
		})
		require.NoError(t, rpcErr)
	})
	t.Run("group overridden to _schema", func(t *testing.T) {
		mgmt, _ := startTestServer(t)
		prop := testProperty("svc", "id1")
		prop.Metadata.Group = "custom-group"
		_, rpcErr := mgmt.InsertSchema(context.Background(), &schemav1.InsertSchemaRequest{Property: prop})
		require.NoError(t, rpcErr)
		stream, streamErr := mgmt.ListSchemas(context.Background(), &schemav1.ListSchemasRequest{
			Query: &propertyv1.QueryRequest{Name: "svc"},
		})
		require.NoError(t, streamErr)
		responses, recvErr := collectListResponses(stream)
		require.NoError(t, recvErr)
		require.Len(t, responses, 1)
		require.Len(t, responses[0].Properties, 1)
		assert.Equal(t, schema.SchemaGroup, responses[0].Properties[0].Metadata.Group)
	})
}

func TestUpdateSchema(t *testing.T) {
	t.Run("nil property", func(t *testing.T) {
		mgmt, _ := startTestServer(t)
		_, rpcErr := mgmt.UpdateSchema(context.Background(), &schemav1.UpdateSchemaRequest{})
		require.Error(t, rpcErr)
		assert.Equal(t, codes.InvalidArgument, grpcstatus.Code(rpcErr))
		assert.Contains(t, rpcErr.Error(), "property is required")
	})
	t.Run("nil metadata", func(t *testing.T) {
		mgmt, _ := startTestServer(t)
		_, rpcErr := mgmt.UpdateSchema(context.Background(), &schemav1.UpdateSchemaRequest{
			Property: &propertyv1.Property{},
		})
		require.Error(t, rpcErr)
		assert.Equal(t, codes.InvalidArgument, grpcstatus.Code(rpcErr))
		assert.Contains(t, rpcErr.Error(), "Metadata")
	})
	t.Run("successful update", func(t *testing.T) {
		mgmt, _ := startTestServer(t)
		insertProperty(t, mgmt, "svc", "id1")
		updated := testProperty("svc", "id1")
		updated.Tags = []*modelv1.Tag{
			{Key: "k", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "updated"}}}},
		}
		_, rpcErr := mgmt.UpdateSchema(context.Background(), &schemav1.UpdateSchemaRequest{Property: updated})
		require.NoError(t, rpcErr)
		stream, streamErr := mgmt.ListSchemas(context.Background(), &schemav1.ListSchemasRequest{
			Query: &propertyv1.QueryRequest{Name: "svc", Ids: []string{"id1"}},
		})
		require.NoError(t, streamErr)
		responses, recvErr := collectListResponses(stream)
		require.NoError(t, recvErr)
		require.Len(t, responses, 1)
		require.NotEmpty(t, responses[0].Properties)
		assert.Equal(t, "updated", responses[0].Properties[0].Tags[0].Value.GetStr().Value)
	})
	t.Run("group overridden", func(t *testing.T) {
		mgmt, _ := startTestServer(t)
		prop := testProperty("svc", "id1")
		prop.Metadata.Group = "custom-group"
		_, rpcErr := mgmt.UpdateSchema(context.Background(), &schemav1.UpdateSchemaRequest{Property: prop})
		require.NoError(t, rpcErr)
		stream, streamErr := mgmt.ListSchemas(context.Background(), &schemav1.ListSchemasRequest{
			Query: &propertyv1.QueryRequest{Name: "svc"},
		})
		require.NoError(t, streamErr)
		responses, recvErr := collectListResponses(stream)
		require.NoError(t, recvErr)
		require.Len(t, responses, 1)
		require.Len(t, responses[0].Properties, 1)
		assert.Equal(t, schema.SchemaGroup, responses[0].Properties[0].Metadata.Group)
	})
}

func TestListSchemas(t *testing.T) {
	t.Run("nil query", func(t *testing.T) {
		mgmt, _ := startTestServer(t)
		stream, streamErr := mgmt.ListSchemas(context.Background(), &schemav1.ListSchemasRequest{})
		require.NoError(t, streamErr)
		_, recvErr := collectListResponses(stream)
		require.Error(t, recvErr)
		assert.Equal(t, codes.InvalidArgument, grpcstatus.Code(recvErr))
		assert.Contains(t, recvErr.Error(), "query is required")
	})
	t.Run("empty results", func(t *testing.T) {
		mgmt, _ := startTestServer(t)
		stream, streamErr := mgmt.ListSchemas(context.Background(), &schemav1.ListSchemasRequest{
			Query: &propertyv1.QueryRequest{},
		})
		require.NoError(t, streamErr)
		responses, recvErr := collectListResponses(stream)
		require.NoError(t, recvErr)
		assert.Empty(t, responses)
	})
	t.Run("single result", func(t *testing.T) {
		mgmt, _ := startTestServer(t)
		insertProperty(t, mgmt, "svc-a", "id1")
		stream, streamErr := mgmt.ListSchemas(context.Background(), &schemav1.ListSchemasRequest{
			Query: &propertyv1.QueryRequest{Name: "svc-a"},
		})
		require.NoError(t, streamErr)
		responses, recvErr := collectListResponses(stream)
		require.NoError(t, recvErr)
		require.Len(t, responses, 1)
		assert.Len(t, responses[0].Properties, 1)
	})
	t.Run("101 results batch boundary", func(t *testing.T) {
		mgmt, _ := startTestServer(t)
		for idx := 0; idx < 101; idx++ {
			insertProperty(t, mgmt, "batch-svc", fmt.Sprintf("id-%03d", idx))
		}
		stream, streamErr := mgmt.ListSchemas(context.Background(), &schemav1.ListSchemasRequest{
			Query: &propertyv1.QueryRequest{Name: "batch-svc"},
		})
		require.NoError(t, streamErr)
		responses, recvErr := collectListResponses(stream)
		require.NoError(t, recvErr)
		require.Len(t, responses, 2)
		assert.Len(t, responses[0].Properties, 100)
		assert.Len(t, responses[1].Properties, 1)
	})
	t.Run("delete times populated", func(t *testing.T) {
		mgmt, _ := startTestServer(t)
		insertProperty(t, mgmt, "dt-svc", "id1")
		insertProperty(t, mgmt, "dt-svc", "id2")
		_, deleteErr := mgmt.DeleteSchema(context.Background(), &schemav1.DeleteSchemaRequest{
			Delete: &propertyv1.DeleteRequest{Group: schema.SchemaGroup, Name: "dt-svc", Id: "id1"}, UpdateAt: timestamppb.Now(),
		})
		require.NoError(t, deleteErr)
		stream, streamErr := mgmt.ListSchemas(context.Background(), &schemav1.ListSchemasRequest{
			Query: &propertyv1.QueryRequest{Name: "dt-svc"},
		})
		require.NoError(t, streamErr)
		responses, recvErr := collectListResponses(stream)
		require.NoError(t, recvErr)
		require.Len(t, responses, 1)
		require.Len(t, responses[0].Properties, 2)
		require.Len(t, responses[0].DeleteTimes, 2)
		var hasZero, hasNonZero bool
		for _, dt := range responses[0].DeleteTimes {
			if dt == 0 {
				hasZero = true
			} else {
				hasNonZero = true
			}
		}
		assert.True(t, hasZero, "expected one property with deleteTime=0")
		assert.True(t, hasNonZero, "expected one property with deleteTime>0")
	})
}

func TestDeleteSchema(t *testing.T) {
	t.Run("nil delete request", func(t *testing.T) {
		mgmt, _ := startTestServer(t)
		_, rpcErr := mgmt.DeleteSchema(context.Background(), &schemav1.DeleteSchemaRequest{})
		require.Error(t, rpcErr)
		assert.Equal(t, codes.InvalidArgument, grpcstatus.Code(rpcErr))
		assert.Contains(t, rpcErr.Error(), "delete request is required")
	})
	t.Run("no results", func(t *testing.T) {
		mgmt, _ := startTestServer(t)
		resp, rpcErr := mgmt.DeleteSchema(context.Background(), &schemav1.DeleteSchemaRequest{
			Delete: &propertyv1.DeleteRequest{Group: schema.SchemaGroup, Name: "nonexistent"}, UpdateAt: timestamppb.Now(),
		})
		require.NoError(t, rpcErr)
		assert.False(t, resp.Found)
	})
	t.Run("delete existing", func(t *testing.T) {
		mgmt, _ := startTestServer(t)
		insertProperty(t, mgmt, "svc", "id1")
		resp, rpcErr := mgmt.DeleteSchema(context.Background(), &schemav1.DeleteSchemaRequest{
			Delete: &propertyv1.DeleteRequest{Group: schema.SchemaGroup, Name: "svc"}, UpdateAt: timestamppb.Now(),
		})
		require.NoError(t, rpcErr)
		assert.True(t, resp.Found)
	})
	t.Run("delete by name only", func(t *testing.T) {
		mgmt, _ := startTestServer(t)
		insertProperty(t, mgmt, "svc", "id1")
		resp, rpcErr := mgmt.DeleteSchema(context.Background(), &schemav1.DeleteSchemaRequest{
			Delete: &propertyv1.DeleteRequest{Group: schema.SchemaGroup, Name: "svc"}, UpdateAt: timestamppb.Now(),
		})
		require.NoError(t, rpcErr)
		assert.True(t, resp.Found)
	})
	t.Run("delete by name and id", func(t *testing.T) {
		mgmt, _ := startTestServer(t)
		insertProperty(t, mgmt, "svc", "id1")
		resp, rpcErr := mgmt.DeleteSchema(context.Background(), &schemav1.DeleteSchemaRequest{
			Delete: &propertyv1.DeleteRequest{Group: schema.SchemaGroup, Name: "svc", Id: "id1"}, UpdateAt: timestamppb.Now(),
		})
		require.NoError(t, rpcErr)
		assert.True(t, resp.Found)
	})
}

func TestRepairSchema(t *testing.T) {
	t.Run("nil property", func(t *testing.T) {
		mgmt, _ := startTestServer(t)
		_, rpcErr := mgmt.RepairSchema(context.Background(), &schemav1.RepairSchemaRequest{})
		require.Error(t, rpcErr)
		assert.Equal(t, codes.InvalidArgument, grpcstatus.Code(rpcErr))
		assert.Contains(t, rpcErr.Error(), "property is required")
	})
	t.Run("nil metadata", func(t *testing.T) {
		mgmt, _ := startTestServer(t)
		_, rpcErr := mgmt.RepairSchema(context.Background(), &schemav1.RepairSchemaRequest{
			Property: &propertyv1.Property{},
		})
		require.Error(t, rpcErr)
		assert.Equal(t, codes.InvalidArgument, grpcstatus.Code(rpcErr))
		assert.Contains(t, rpcErr.Error(), "Metadata")
	})
	t.Run("successful repair", func(t *testing.T) {
		mgmt, _ := startTestServer(t)
		_, rpcErr := mgmt.RepairSchema(context.Background(), &schemav1.RepairSchemaRequest{
			Property: testProperty("svc", "id1"),
		})
		require.NoError(t, rpcErr)
		stream, streamErr := mgmt.ListSchemas(context.Background(), &schemav1.ListSchemasRequest{
			Query: &propertyv1.QueryRequest{Groups: []string{schema.SchemaGroup}, Name: "svc", Ids: []string{"id1"}},
		})
		require.NoError(t, streamErr)
		responses, recvErr := collectListResponses(stream)
		require.NoError(t, recvErr)
		require.Len(t, responses, 1)
		assert.Len(t, responses[0].Properties, 1)
	})
}

func TestWatchSchemas_BiDirReplay(t *testing.T) {
	t.Run("replay_from_revision", func(t *testing.T) {
		mgmt, upd := startTestServer(t)
		// Insert a property.
		insertProperty(t, mgmt, "svc-a", "id1")
		// Wait a bit then insert another.
		time.Sleep(10 * time.Millisecond)
		// Get the first property's updatedAt to use as max_revision.
		stream, streamErr := mgmt.ListSchemas(context.Background(), &schemav1.ListSchemasRequest{
			Query: &propertyv1.QueryRequest{Groups: []string{schema.SchemaGroup}, Name: "svc-a", Ids: []string{"id1"}},
		})
		require.NoError(t, streamErr)
		responses, recvErr := collectListResponses(stream)
		require.NoError(t, recvErr)
		require.Len(t, responses, 1)
		// Insert a second property after the first.
		insertProperty(t, mgmt, "svc-b", "id2")
		// Start bidi watch with max_revision = 0 (replay all).
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		watchStream, watchErr := upd.WatchSchemas(ctx)
		require.NoError(t, watchErr)
		require.NoError(t, watchStream.Send(&schemav1.WatchSchemasRequest{}))
		// Collect events until REPLAY_DONE.
		var events []*schemav1.WatchSchemasResponse
		for {
			resp, err := watchStream.Recv()
			require.NoError(t, err)
			if resp.EventType == schemav1.SchemaEventType_SCHEMA_EVENT_TYPE_REPLAY_DONE {
				break
			}
			events = append(events, resp)
		}
		assert.GreaterOrEqual(t, len(events), 2, "should replay all properties")
	})

	t.Run("metadata_only_replay", func(t *testing.T) {
		mgmt, upd := startTestServer(t)
		insertProperty(t, mgmt, "svc-meta", "id-meta")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		watchStream, watchErr := upd.WatchSchemas(ctx)
		require.NoError(t, watchErr)
		require.NoError(t, watchStream.Send(&schemav1.WatchSchemasRequest{
			TagProjection: []string{"kind", "group", "name", "updated_at"},
		}))
		var events []*schemav1.WatchSchemasResponse
		for {
			resp, err := watchStream.Recv()
			require.NoError(t, err)
			if resp.EventType == schemav1.SchemaEventType_SCHEMA_EVENT_TYPE_REPLAY_DONE {
				break
			}
			events = append(events, resp)
		}
		require.NotEmpty(t, events)
		for _, evt := range events {
			assert.True(t, evt.MetadataOnly, "replay events should have metadata_only=true")
			// Verify that only projected tags are returned, not the full property tags.
			allowedKeys := map[string]bool{"kind": true, "group": true, "name": true, "updated_at": true}
			for _, tag := range evt.Property.GetTags() {
				assert.True(t, allowedKeys[tag.GetKey()],
					"unexpected tag key %q in metadata_only replay; only projected tags should be returned", tag.GetKey())
			}
		}
	})

	t.Run("live_events_after_replay", func(t *testing.T) {
		mgmt, upd := startTestServer(t)
		insertProperty(t, mgmt, "svc-live", "id-live")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		watchStream, watchErr := upd.WatchSchemas(ctx)
		require.NoError(t, watchErr)
		require.NoError(t, watchStream.Send(&schemav1.WatchSchemasRequest{}))
		// Drain replay.
		for {
			resp, err := watchStream.Recv()
			require.NoError(t, err)
			if resp.EventType == schemav1.SchemaEventType_SCHEMA_EVENT_TYPE_REPLAY_DONE {
				break
			}
		}
		// Insert new data - should come through as live event.
		insertProperty(t, mgmt, "svc-new", "id-new")
		resp, err := watchStream.Recv()
		require.NoError(t, err)
		assert.Equal(t, schemav1.SchemaEventType_SCHEMA_EVENT_TYPE_INSERT, resp.EventType)
		assert.Equal(t, "id-new", resp.Property.GetId())
	})

	t.Run("multiple_requests", func(t *testing.T) {
		mgmt, upd := startTestServer(t)
		insertProperty(t, mgmt, "svc-multi", "id-multi")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		watchStream, watchErr := upd.WatchSchemas(ctx)
		require.NoError(t, watchErr)
		// First request.
		require.NoError(t, watchStream.Send(&schemav1.WatchSchemasRequest{}))
		for {
			resp, err := watchStream.Recv()
			require.NoError(t, err)
			if resp.EventType == schemav1.SchemaEventType_SCHEMA_EVENT_TYPE_REPLAY_DONE {
				break
			}
		}
		// Second request on same stream (metadata only).
		require.NoError(t, watchStream.Send(&schemav1.WatchSchemasRequest{
			TagProjection: []string{"kind", "group", "name", "updated_at"},
		}))
		for {
			resp, err := watchStream.Recv()
			require.NoError(t, err)
			if resp.EventType == schemav1.SchemaEventType_SCHEMA_EVENT_TYPE_REPLAY_DONE {
				break
			}
			assert.True(t, resp.MetadataOnly)
		}
	})

	t.Run("replay_includes_deleted", func(t *testing.T) {
		mgmt, upd := startTestServer(t)
		insertProperty(t, mgmt, "svc-del", "id-del")
		_, deleteErr := mgmt.DeleteSchema(context.Background(), &schemav1.DeleteSchemaRequest{
			Delete:   &propertyv1.DeleteRequest{Group: schema.SchemaGroup, Name: "svc-del", Id: "id-del"},
			UpdateAt: timestamppb.Now(),
		})
		require.NoError(t, deleteErr)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		watchStream, watchErr := upd.WatchSchemas(ctx)
		require.NoError(t, watchErr)
		require.NoError(t, watchStream.Send(&schemav1.WatchSchemasRequest{}))
		var hasDeleteEvent bool
		for {
			resp, err := watchStream.Recv()
			require.NoError(t, err)
			if resp.EventType == schemav1.SchemaEventType_SCHEMA_EVENT_TYPE_REPLAY_DONE {
				break
			}
			if resp.EventType == schemav1.SchemaEventType_SCHEMA_EVENT_TYPE_DELETE && resp.Property.GetId() == "id-del" {
				hasDeleteEvent = true
				assert.Greater(t, resp.DeleteTime, int64(0), "delete event should carry a non-zero delete_time")
			}
		}
		assert.True(t, hasDeleteEvent, "replay should include deleted entries as DELETE events")
	})
}
