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
	"context"
	"io"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	schemav1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/schema/v1"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/banyand/property"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/meter"
)

type serverMetrics struct {
	totalStarted  meter.Counter
	totalFinished meter.Counter
	totalErr      meter.Counter
	totalLatency  meter.Counter
}

func newServerMetrics(factory observability.Factory) *serverMetrics {
	return &serverMetrics{
		totalStarted:  factory.NewCounter("property_total_started", "method", "kind", "group"),
		totalFinished: factory.NewCounter("property_total_finished", "method", "kind", "group"),
		totalErr:      factory.NewCounter("property_total_err", "method", "kind", "group"),
		totalLatency:  factory.NewCounter("property_total_latency", "method", "kind", "group"),
	}
}

func extractKindAndGroupFromProperty(prop *propertyv1.Property) (string, string) {
	kind := prop.GetMetadata().GetName()
	group := ""
	for _, tag := range prop.GetTags() {
		if tag.Key == TagKeyGroup {
			group = tag.Value.GetStr().GetValue()
			break
		}
	}
	return kind, group
}

func extractGroupFromCriteria(criteria *modelv1.Criteria) string {
	if criteria == nil {
		return ""
	}
	cond := criteria.GetCondition()
	if cond == nil || cond.Name != TagKeyGroup {
		return ""
	}
	return cond.Value.GetStr().GetValue()
}

type schemaManagementServer struct {
	schemav1.UnimplementedSchemaManagementServiceServer
	server  *Server
	l       *logger.Logger
	metrics *serverMetrics
}

// InsertSchema inserts a new schema.
func (s *schemaManagementServer) InsertSchema(ctx context.Context, req *schemav1.InsertSchemaRequest) (*schemav1.InsertSchemaResponse, error) {
	if req.Property == nil {
		return nil, errInvalidRequest("property is required")
	}
	kind, group := extractKindAndGroupFromProperty(req.Property)
	s.metrics.totalStarted.Inc(1, "insert", kind, group)
	start := time.Now()
	defer func() {
		s.metrics.totalFinished.Inc(1, "insert", kind, group)
		s.metrics.totalLatency.Inc(time.Since(start).Seconds(), "insert", kind, group)
	}()
	if insertErr := s.server.insert(ctx, req.Property); insertErr != nil {
		s.metrics.totalErr.Inc(1, "insert", kind, group)
		s.l.Error().Err(insertErr).Msg("failed to insert schema")
		return nil, insertErr
	}
	return &schemav1.InsertSchemaResponse{}, nil
}

// UpdateSchema updates an existing schema.
func (s *schemaManagementServer) UpdateSchema(ctx context.Context, req *schemav1.UpdateSchemaRequest) (*schemav1.UpdateSchemaResponse, error) {
	if req.Property == nil {
		return nil, errInvalidRequest("property is required")
	}
	kind, group := extractKindAndGroupFromProperty(req.Property)
	s.metrics.totalStarted.Inc(1, "update", kind, group)
	start := time.Now()
	defer func() {
		s.metrics.totalFinished.Inc(1, "update", kind, group)
		s.metrics.totalLatency.Inc(time.Since(start).Seconds(), "update", kind, group)
	}()
	if updateErr := s.server.update(ctx, req.Property); updateErr != nil {
		s.metrics.totalErr.Inc(1, "update", kind, group)
		s.l.Error().Err(updateErr).Msg("failed to update schema")
		return nil, updateErr
	}
	return &schemav1.UpdateSchemaResponse{}, nil
}

const listSchemasBatchSize = 100

// ListSchemas lists schemas based on the query request via server streaming.
func (s *schemaManagementServer) ListSchemas(req *schemav1.ListSchemasRequest, stream grpc.ServerStreamingServer[schemav1.ListSchemasResponse]) error {
	if req.Query == nil {
		return errInvalidRequest("query is required")
	}
	kind := req.Query.GetName()
	group := extractGroupFromCriteria(req.Query.GetCriteria())
	s.metrics.totalStarted.Inc(1, "list", kind, group)
	start := time.Now()
	defer func() {
		s.metrics.totalFinished.Inc(1, "list", kind, group)
		s.metrics.totalLatency.Inc(time.Since(start).Seconds(), "list", kind, group)
	}()
	results, listErr := s.server.list(stream.Context(), req.Query)
	if listErr != nil {
		s.metrics.totalErr.Inc(1, "list", kind, group)
		s.l.Error().Err(listErr).Msg("failed to list schemas")
		return listErr
	}
	for batchStart := 0; batchStart < len(results); batchStart += listSchemasBatchSize {
		batchEnd := batchStart + listSchemasBatchSize
		if batchEnd > len(results) {
			batchEnd = len(results)
		}
		batch := results[batchStart:batchEnd]
		props := make([]*propertyv1.Property, 0, len(batch))
		deleteTimes := make([]int64, 0, len(batch))
		for _, r := range batch {
			props = append(props, r.Property)
			deleteTimes = append(deleteTimes, r.DeleteTime)
		}
		if sendErr := stream.Send(&schemav1.ListSchemasResponse{Properties: props, DeleteTimes: deleteTimes}); sendErr != nil {
			s.metrics.totalErr.Inc(1, "list", kind, group)
			return sendErr
		}
	}
	return nil
}

// GetSchema retrieves a single schema.
func (s *schemaManagementServer) GetSchema(ctx context.Context, req *schemav1.GetSchemaRequest) (*schemav1.GetSchemaResponse, error) {
	if req.Query == nil {
		return nil, errInvalidRequest("query is required")
	}
	if len(req.Query.Groups) == 0 {
		return nil, errInvalidRequest("groups is required")
	}
	if len(req.Query.Ids) == 0 {
		return nil, errInvalidRequest("ids is required")
	}
	kind := req.Query.GetName()
	group := extractGroupFromCriteria(req.Query.GetCriteria())
	s.metrics.totalStarted.Inc(1, "get", kind, group)
	start := time.Now()
	defer func() {
		s.metrics.totalFinished.Inc(1, "get", kind, group)
		s.metrics.totalLatency.Inc(time.Since(start).Seconds(), "get", kind, group)
	}()
	prop, getErr := s.server.get(ctx, req.Query.Groups[0], req.Query.Name, req.Query.Ids[0])
	if getErr != nil {
		s.metrics.totalErr.Inc(1, "get", kind, group)
		s.l.Error().Err(getErr).Msg("failed to get schema")
		return nil, getErr
	}
	return &schemav1.GetSchemaResponse{Properties: prop}, nil
}

// DeleteSchema deletes a schema.
func (s *schemaManagementServer) DeleteSchema(ctx context.Context, req *schemav1.DeleteSchemaRequest) (*schemav1.DeleteSchemaResponse, error) {
	if req.Delete == nil {
		return nil, errInvalidRequest("delete request is required")
	}
	kind := req.Delete.GetName()
	_, group, _ := parsePropertyID(req.Delete.GetId())
	s.metrics.totalStarted.Inc(1, "delete", kind, group)
	start := time.Now()
	defer func() {
		s.metrics.totalFinished.Inc(1, "delete", kind, group)
		s.metrics.totalLatency.Inc(time.Since(start).Seconds(), "delete", kind, group)
	}()
	found, deleteErr := s.server.delete(ctx, req.Delete.Group, req.Delete.Name, req.Delete.Id, req.UpdateAt)
	if deleteErr != nil {
		s.metrics.totalErr.Inc(1, "delete", kind, group)
		s.l.Error().Err(deleteErr).Msg("failed to delete schema")
		return nil, deleteErr
	}
	return &schemav1.DeleteSchemaResponse{Found: found}, nil
}

// ExistSchema checks if a schema exists without returning its full data.
func (s *schemaManagementServer) ExistSchema(ctx context.Context, req *schemav1.ExistSchemaRequest) (*schemav1.ExistSchemaResponse, error) {
	if req.Query == nil {
		return nil, errInvalidRequest("query is required")
	}
	if len(req.Query.Groups) == 0 {
		return nil, errInvalidRequest("groups is required")
	}
	if req.Query.Name == "" {
		return nil, errInvalidRequest("name is required")
	}
	if len(req.Query.Ids) == 0 {
		return nil, errInvalidRequest("ids is required")
	}
	kind := req.Query.GetName()
	group := extractGroupFromCriteria(req.Query.GetCriteria())
	s.metrics.totalStarted.Inc(1, "exist", kind, group)
	start := time.Now()
	defer func() {
		s.metrics.totalFinished.Inc(1, "exist", kind, group)
		s.metrics.totalLatency.Inc(time.Since(start).Seconds(), "exist", kind, group)
	}()
	hasSchema, existErr := s.server.exist(ctx, req.Query.Groups[0], req.Query.Name, req.Query.Ids[0])
	if existErr != nil {
		s.metrics.totalErr.Inc(1, "exist", kind, group)
		s.l.Error().Err(existErr).Msg("failed to check schema existence")
		return nil, existErr
	}
	return &schemav1.ExistSchemaResponse{HasSchema: hasSchema}, nil
}

// RepairSchema repairs a schema property on this node with specified deleteTime.
func (s *schemaManagementServer) RepairSchema(ctx context.Context, req *schemav1.RepairSchemaRequest) (*schemav1.RepairSchemaResponse, error) {
	if req.Property == nil {
		return nil, errInvalidRequest("property is required")
	}
	kind, group := extractKindAndGroupFromProperty(req.Property)
	s.metrics.totalStarted.Inc(1, "repair", kind, group)
	start := time.Now()
	defer func() {
		s.metrics.totalFinished.Inc(1, "repair", kind, group)
		s.metrics.totalLatency.Inc(time.Since(start).Seconds(), "repair", kind, group)
	}()
	repairErr := s.server.propertyService.DirectRepair(ctx, 0, property.GetPropertyID(req.Property), req.Property, req.DeleteTime)
	if repairErr != nil {
		s.metrics.totalErr.Inc(1, "repair", kind, group)
		s.l.Error().Err(repairErr).Msg("failed to repair schema")
		return nil, repairErr
	}
	return &schemav1.RepairSchemaResponse{}, nil
}

type schemaUpdateServer struct {
	schemav1.UnimplementedSchemaUpdateServiceServer
	server  *Server
	l       *logger.Logger
	metrics *serverMetrics
}

// AggregateSchemaUpdates returns kind names of schemas that have been modified since the given revision.
// The criteria in the query should include mod_revision > sinceRevision condition.
func (s *schemaUpdateServer) AggregateSchemaUpdates(ctx context.Context, req *schemav1.AggregateSchemaUpdatesRequest) (*schemav1.AggregateSchemaUpdatesResponse, error) {
	s.metrics.totalStarted.Inc(1, "aggregate", "", "")
	start := time.Now()
	defer func() {
		s.metrics.totalFinished.Inc(1, "aggregate", "", "")
		s.metrics.totalLatency.Inc(time.Since(start).Seconds(), "aggregate", "", "")
	}()
	if req.Query == nil {
		return nil, errInvalidRequest("query is required")
	}
	results, listErr := s.server.list(ctx, req.Query)
	if listErr != nil {
		s.metrics.totalErr.Inc(1, "aggregate", "", "")
		s.l.Error().Err(listErr).Msg("failed to list schemas for updates")
		return nil, listErr
	}
	names := make([]string, 0, len(results))
	seen := make(map[string]bool)
	for _, r := range results {
		kindName := r.Property.Metadata.GetName()
		if _, exists := seen[kindName]; !exists {
			names = append(names, kindName)
			seen[kindName] = true
		}
	}
	return &schemav1.AggregateSchemaUpdatesResponse{Names: names}, nil
}

type schemaManagementClient struct {
	schemav1.SchemaManagementServiceClient
	*schemaManagementServer
}

func (s *schemaManagementClient) InsertSchema(ctx context.Context, in *schemav1.InsertSchemaRequest,
	_ ...grpc.CallOption,
) (*schemav1.InsertSchemaResponse, error) {
	return s.schemaManagementServer.InsertSchema(ctx, in)
}

func (s *schemaManagementClient) UpdateSchema(ctx context.Context, in *schemav1.UpdateSchemaRequest,
	_ ...grpc.CallOption,
) (*schemav1.UpdateSchemaResponse, error) {
	return s.schemaManagementServer.UpdateSchema(ctx, in)
}

func (s *schemaManagementClient) ListSchemas(ctx context.Context, in *schemav1.ListSchemasRequest,
	_ ...grpc.CallOption,
) (grpc.ServerStreamingClient[schemav1.ListSchemasResponse], error) {
	if in.Query == nil {
		return nil, errInvalidRequest("query is required")
	}
	results, listErr := s.server.list(ctx, in.Query)
	if listErr != nil {
		return nil, listErr
	}
	var batches []*schemav1.ListSchemasResponse
	for batchStart := 0; batchStart < len(results); batchStart += listSchemasBatchSize {
		batchEnd := batchStart + listSchemasBatchSize
		if batchEnd > len(results) {
			batchEnd = len(results)
		}
		batch := results[batchStart:batchEnd]
		props := make([]*propertyv1.Property, 0, len(batch))
		deleteTimes := make([]int64, 0, len(batch))
		for _, r := range batch {
			props = append(props, r.Property)
			deleteTimes = append(deleteTimes, r.DeleteTime)
		}
		batches = append(batches, &schemav1.ListSchemasResponse{Properties: props, DeleteTimes: deleteTimes})
	}
	return &localListSchemasStream{ctx: ctx, batches: batches}, nil
}

func (s *schemaManagementClient) DeleteSchema(ctx context.Context, in *schemav1.DeleteSchemaRequest,
	_ ...grpc.CallOption,
) (*schemav1.DeleteSchemaResponse, error) {
	return s.schemaManagementServer.DeleteSchema(ctx, in)
}

func (s *schemaManagementClient) RepairSchema(ctx context.Context, in *schemav1.RepairSchemaRequest,
	_ ...grpc.CallOption,
) (*schemav1.RepairSchemaResponse, error) {
	return s.schemaManagementServer.RepairSchema(ctx, in)
}

func (s *schemaManagementClient) ExistSchema(ctx context.Context, in *schemav1.ExistSchemaRequest,
	_ ...grpc.CallOption,
) (*schemav1.ExistSchemaResponse, error) {
	return s.schemaManagementServer.ExistSchema(ctx, in)
}

type localListSchemasStream struct {
	ctx     context.Context
	batches []*schemav1.ListSchemasResponse
	pos     int
}

func (l *localListSchemasStream) Recv() (*schemav1.ListSchemasResponse, error) {
	if l.pos >= len(l.batches) {
		return nil, io.EOF
	}
	resp := l.batches[l.pos]
	l.pos++
	return resp, nil
}

func (l *localListSchemasStream) Header() (metadata.MD, error) { return nil, nil }
func (l *localListSchemasStream) Trailer() metadata.MD         { return nil }
func (l *localListSchemasStream) CloseSend() error             { return nil }
func (l *localListSchemasStream) Context() context.Context     { return l.ctx }
func (l *localListSchemasStream) SendMsg(interface{}) error    { return nil }
func (l *localListSchemasStream) RecvMsg(interface{}) error    { return nil }

type schemaUpdateClient struct {
	schemav1.SchemaUpdateServiceClient
	*schemaUpdateServer
}

func (s *schemaUpdateClient) AggregateSchemaUpdates(ctx context.Context, in *schemav1.AggregateSchemaUpdatesRequest,
	_ ...grpc.CallOption,
) (*schemav1.AggregateSchemaUpdatesResponse, error) {
	return s.schemaUpdateServer.AggregateSchemaUpdates(ctx, in)
}

func errInvalidRequest(msg string) error {
	return &invalidRequestError{msg: msg}
}

type invalidRequestError struct {
	msg string
}

func (e *invalidRequestError) Error() string {
	return "invalid request: " + e.msg
}
