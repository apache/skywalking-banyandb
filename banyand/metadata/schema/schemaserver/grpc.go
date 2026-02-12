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
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"

	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	schemav1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/schema/v1"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/banyand/property/db"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/meter"
)

var schemaServerScope = observability.RootScope.SubScope("schema_server")

type serverMetrics struct {
	totalStarted  meter.Counter
	totalFinished meter.Counter
	totalErr      meter.Counter
	totalLatency  meter.Counter
}

func newServerMetrics(factory observability.Factory) *serverMetrics {
	return &serverMetrics{
		totalStarted:  factory.NewCounter("total_started", "method"),
		totalFinished: factory.NewCounter("total_finished", "method"),
		totalErr:      factory.NewCounter("total_err", "method"),
		totalLatency:  factory.NewCounter("total_latency", "method"),
	}
}

type schemaManagementServer struct {
	schemav1.UnimplementedSchemaManagementServiceServer
	server  *server
	l       *logger.Logger
	metrics *serverMetrics
}

// InsertSchema inserts a new schema property.
func (s *schemaManagementServer) InsertSchema(ctx context.Context, req *schemav1.InsertSchemaRequest) (*schemav1.InsertSchemaResponse, error) {
	if req.Property == nil {
		return nil, errInvalidRequest("property is required")
	}
	if req.Property.Metadata == nil {
		return nil, errInvalidRequest("metadata should not be nil")
	}
	if req.Property.Metadata.ModRevision == 0 {
		return nil, errInvalidRequest("mod_revision should be set for update")
	}
	s.metrics.totalStarted.Inc(1, "insert")
	start := time.Now()
	defer func() {
		s.metrics.totalFinished.Inc(1, "insert")
		s.metrics.totalLatency.Inc(time.Since(start).Seconds(), "insert")
	}()
	req.Property.Metadata.Group = schemaGroup
	existQuery := &propertyv1.QueryRequest{
		Groups: []string{schemaGroup},
		Name:   req.Property.Metadata.Name,
		Ids:    []string{req.Property.Id},
	}
	existing, queryErr := s.server.db.Query(ctx, existQuery)
	if queryErr != nil {
		s.metrics.totalErr.Inc(1, "insert")
		s.l.Error().Err(queryErr).Msg("failed to check schema existence")
		return nil, queryErr
	}
	for _, result := range existing {
		if result.DeleteTime() == 0 {
			s.metrics.totalErr.Inc(1, "insert")
			return nil, fmt.Errorf("schema already exists")
		}
	}
	id := db.GetPropertyID(req.Property)
	if updateErr := s.server.db.Update(ctx, defaultShardID, id, req.Property); updateErr != nil {
		s.metrics.totalErr.Inc(1, "insert")
		s.l.Error().Err(updateErr).Msg("failed to insert schema")
		return nil, updateErr
	}
	return &schemav1.InsertSchemaResponse{}, nil
}

// UpdateSchema updates an existing schema property.
func (s *schemaManagementServer) UpdateSchema(ctx context.Context, req *schemav1.UpdateSchemaRequest) (*schemav1.UpdateSchemaResponse, error) {
	if req.Property == nil {
		return nil, errInvalidRequest("property is required")
	}
	if req.Property.Metadata == nil {
		return nil, errInvalidRequest("metadata should not be nil")
	}
	if req.Property.Metadata.ModRevision == 0 {
		return nil, errInvalidRequest("mod_revision should be set for update")
	}
	s.metrics.totalStarted.Inc(1, "update")
	start := time.Now()
	defer func() {
		s.metrics.totalFinished.Inc(1, "update")
		s.metrics.totalLatency.Inc(time.Since(start).Seconds(), "update")
	}()
	req.Property.Metadata.Group = schemaGroup
	id := db.GetPropertyID(req.Property)
	if updateErr := s.server.db.Update(ctx, defaultShardID, id, req.Property); updateErr != nil {
		s.metrics.totalErr.Inc(1, "update")
		s.l.Error().Err(updateErr).Msg("failed to update schema")
		return nil, updateErr
	}
	return &schemav1.UpdateSchemaResponse{}, nil
}

const listSchemasBatchSize = 100

// ListSchemas lists schema properties via server streaming.
func (s *schemaManagementServer) ListSchemas(req *schemav1.ListSchemasRequest,
	stream grpc.ServerStreamingServer[schemav1.ListSchemasResponse],
) error {
	if req.Query == nil {
		return errInvalidRequest("query is required")
	}
	s.metrics.totalStarted.Inc(1, "list")
	start := time.Now()
	defer func() {
		s.metrics.totalFinished.Inc(1, "list")
		s.metrics.totalLatency.Inc(time.Since(start).Seconds(), "list")
	}()
	req.Query.Groups = []string{schemaGroup}
	results, queryErr := s.server.db.Query(stream.Context(), req.Query)
	if queryErr != nil {
		s.metrics.totalErr.Inc(1, "list")
		s.l.Error().Err(queryErr).Msg("failed to list schemas")
		return queryErr
	}
	for batchStart := 0; batchStart < len(results); batchStart += listSchemasBatchSize {
		batchEnd := batchStart + listSchemasBatchSize
		if batchEnd > len(results) {
			batchEnd = len(results)
		}
		batch := results[batchStart:batchEnd]
		props := make([]*propertyv1.Property, 0, len(batch))
		deleteTimes := make([]int64, 0, len(batch))
		for _, result := range batch {
			var p propertyv1.Property
			if unmarshalErr := protojson.Unmarshal(result.Source(), &p); unmarshalErr != nil {
				s.metrics.totalErr.Inc(1, "list")
				return unmarshalErr
			}
			props = append(props, &p)
			deleteTimes = append(deleteTimes, result.DeleteTime())
		}
		if sendErr := stream.Send(&schemav1.ListSchemasResponse{Properties: props, DeleteTimes: deleteTimes}); sendErr != nil {
			s.metrics.totalErr.Inc(1, "list")
			return sendErr
		}
	}
	return nil
}

// DeleteSchema deletes a schema property.
func (s *schemaManagementServer) DeleteSchema(ctx context.Context, req *schemav1.DeleteSchemaRequest) (*schemav1.DeleteSchemaResponse, error) {
	if req.Delete == nil || req.UpdateAt == nil {
		return nil, errInvalidRequest("delete request is required")
	}
	s.metrics.totalStarted.Inc(1, "delete")
	start := time.Now()
	defer func() {
		s.metrics.totalFinished.Inc(1, "delete")
		s.metrics.totalLatency.Inc(time.Since(start).Seconds(), "delete")
	}()
	query := &propertyv1.QueryRequest{
		Groups: []string{schemaGroup},
		Name:   req.Delete.Name,
	}
	if req.Delete.Id != "" {
		query.Ids = []string{req.Delete.Id}
	}
	results, queryErr := s.server.db.Query(ctx, query)
	if queryErr != nil {
		s.metrics.totalErr.Inc(1, "delete")
		s.l.Error().Err(queryErr).Msg("failed to delete schema")
		return nil, queryErr
	}
	if len(results) == 0 {
		return &schemav1.DeleteSchemaResponse{Found: false}, nil
	}
	ids := make([][]byte, 0, len(results))
	for _, result := range results {
		if result.DeleteTime() > 0 {
			continue
		}
		ids = append(ids, result.ID())
	}
	if len(ids) == 0 {
		return &schemav1.DeleteSchemaResponse{Found: false}, nil
	}
	if deleteErr := s.server.db.Delete(ctx, ids, req.UpdateAt.AsTime()); deleteErr != nil {
		s.metrics.totalErr.Inc(1, "delete")
		s.l.Error().Err(deleteErr).Msg("failed to delete schema")
		return nil, deleteErr
	}
	return &schemav1.DeleteSchemaResponse{Found: true}, nil
}

// RepairSchema repairs a schema property with the specified delete time.
func (s *schemaManagementServer) RepairSchema(ctx context.Context, req *schemav1.RepairSchemaRequest) (*schemav1.RepairSchemaResponse, error) {
	if req.Property == nil {
		return nil, errInvalidRequest("property is required")
	}
	if req.Property.Metadata == nil {
		return nil, errInvalidRequest("metadata should not be nil")
	}
	if req.Property.Metadata.ModRevision == 0 {
		return nil, errInvalidRequest("mod_revision should be set for update")
	}
	s.metrics.totalStarted.Inc(1, "repair")
	start := time.Now()
	defer func() {
		s.metrics.totalFinished.Inc(1, "repair")
		s.metrics.totalLatency.Inc(time.Since(start).Seconds(), "repair")
	}()
	req.Property.Metadata.Group = schemaGroup
	id := db.GetPropertyID(req.Property)
	if repairErr := s.server.db.Repair(ctx, id, uint64(defaultShardID), req.Property, req.DeleteTime); repairErr != nil {
		s.metrics.totalErr.Inc(1, "repair")
		s.l.Error().Err(repairErr).Msg("failed to repair schema")
		return nil, repairErr
	}
	return &schemav1.RepairSchemaResponse{}, nil
}

// ExistSchema checks if a schema property exists.
func (s *schemaManagementServer) ExistSchema(ctx context.Context, req *schemav1.ExistSchemaRequest) (*schemav1.ExistSchemaResponse, error) {
	if req.Query == nil {
		return nil, errInvalidRequest("query is required")
	}
	s.metrics.totalStarted.Inc(1, "exist")
	start := time.Now()
	defer func() {
		s.metrics.totalFinished.Inc(1, "exist")
		s.metrics.totalLatency.Inc(time.Since(start).Seconds(), "exist")
	}()
	req.Query.Groups = []string{schemaGroup}
	req.Query.Limit = 1
	results, queryErr := s.server.db.Query(ctx, req.Query)
	if queryErr != nil {
		s.metrics.totalErr.Inc(1, "exist")
		s.l.Error().Err(queryErr).Msg("failed to check schema existence")
		return nil, queryErr
	}
	return &schemav1.ExistSchemaResponse{HasSchema: len(results) > 0}, nil
}

type schemaUpdateServer struct {
	schemav1.UnimplementedSchemaUpdateServiceServer
	server  *server
	l       *logger.Logger
	metrics *serverMetrics
}

// AggregateSchemaUpdates returns distinct schema names that have been modified.
func (s *schemaUpdateServer) AggregateSchemaUpdates(ctx context.Context,
	req *schemav1.AggregateSchemaUpdatesRequest,
) (*schemav1.AggregateSchemaUpdatesResponse, error) {
	s.metrics.totalStarted.Inc(1, "aggregate")
	start := time.Now()
	defer func() {
		s.metrics.totalFinished.Inc(1, "aggregate")
		s.metrics.totalLatency.Inc(time.Since(start).Seconds(), "aggregate")
	}()
	if req.Query == nil {
		return nil, errInvalidRequest("query is required")
	}
	req.Query.Groups = []string{schemaGroup}
	results, queryErr := s.server.db.Query(ctx, req.Query)
	if queryErr != nil {
		s.metrics.totalErr.Inc(1, "aggregate")
		s.l.Error().Err(queryErr).Msg("failed to aggregate schema updates")
		return nil, queryErr
	}
	nameSet := make(map[string]struct{}, len(results))
	for _, result := range results {
		var p propertyv1.Property
		if unmarshalErr := protojson.Unmarshal(result.Source(), &p); unmarshalErr != nil {
			s.metrics.totalErr.Inc(1, "aggregate")
			return nil, unmarshalErr
		}
		if p.Metadata != nil && p.Metadata.Name != "" {
			nameSet[p.Metadata.Name] = struct{}{}
		}
	}
	names := make([]string, 0, len(nameSet))
	for name := range nameSet {
		names = append(names, name)
	}
	return &schemav1.AggregateSchemaUpdatesResponse{Names: names}, nil
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
