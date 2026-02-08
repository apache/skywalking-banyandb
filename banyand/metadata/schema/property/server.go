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

	"github.com/pkg/errors"
	"github.com/robfig/cron/v3"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	schemav1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/schema/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/banyand/property"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

const (
	defaultRepairCron   = "@every 10m"
	metadataServiceName = "metadata-property-server"
)

var metadataScope = observability.RootScope.SubScope("metadata_property")

// Server is the metadata property server that stores schema data as properties.
type Server struct {
	propertyService   property.Service
	omr               observability.MetricsRegistry
	pm                protector.Memory
	metadataSvc       metadata.HandlerRegister
	l                 *logger.Logger
	repairScheduler   *repairScheduler
	closer            *run.Closer
	schemaService     *schemaManagementServer
	updateService     *schemaUpdateServer
	repairTriggerCron string
	enabled           bool
}

// NewServer creates a new metadata property server.
func NewServer(propertyService property.Service, omr observability.MetricsRegistry, metadataSvc metadata.HandlerRegister, pm protector.Memory) *Server {
	s := &Server{
		propertyService: propertyService,
		metadataSvc:     metadataSvc,
		omr:             omr,
		pm:              pm,
		closer:          run.NewCloser(0),
	}

	grpcFactory := s.omr.With(metadataScope.SubScope("grpc"))
	sm := newServerMetrics(grpcFactory)
	s.schemaService = &schemaManagementServer{
		server:  s,
		l:       s.l,
		metrics: sm,
	}
	s.updateService = &schemaUpdateServer{
		server:  s,
		l:       s.l,
		metrics: sm,
	}
	return s
}

// Name returns the server name.
func (s *Server) Name() string {
	return metadataServiceName
}

// Role returns the server role.
func (s *Server) Role() databasev1.Role {
	if !s.enabled {
		return databasev1.Role_ROLE_UNSPECIFIED
	}
	return databasev1.Role_ROLE_META
}

// FlagSet returns the flag set for configuration.
func (s *Server) FlagSet() *run.FlagSet {
	fs := run.NewFlagSet("schema-property-server")
	fs.BoolVar(&s.enabled, "schema-property-server-enabled", false, "is enabled the metadata property server or not")
	fs.StringVar(&s.repairTriggerCron, "schema-property-server-repair-trigger-cron", defaultRepairCron, "the cron expression for schema repair trigger")
	return fs
}

// Validate validates the server configuration.
func (s *Server) Validate() error {
	if !s.enabled {
		return nil
	}
	if s.repairTriggerCron == "" {
		return errors.New("repair trigger cron is required")
	}
	if _, cronErr := cron.ParseStandard(s.repairTriggerCron); cronErr != nil {
		return errors.New("schema-property-server-repair-trigger-cron is not a valid cron expression")
	}
	return nil
}

// PreRun initializes the server.
func (s *Server) PreRun(context.Context) error {
	if !s.enabled {
		return nil
	}
	s.l = logger.GetLogger(s.Name())
	if s.propertyService == nil {
		return errors.New("property service is not set")
	}

	// Initialize repair scheduler
	repairFactory := s.omr.With(metadataScope.SubScope("repair"))
	var repairErr error
	s.repairScheduler, repairErr = newRepairScheduler(s, s.repairTriggerCron, s.l, repairFactory)
	if repairErr != nil {
		return repairErr
	}
	if s.metadataSvc != nil {
		s.metadataSvc.RegisterHandler("metadata-node-property", schema.KindNode, s.repairScheduler)
	}
	return nil
}

// Serve starts the server.
func (s *Server) Serve() run.StopNotify {
	if !s.enabled {
		return s.closer.CloseNotify()
	}
	if startErr := s.repairScheduler.Start(); startErr != nil {
		s.l.Error().Err(startErr).Msg("failed to start repair scheduler")
	}
	return s.closer.CloseNotify()
}

// GracefulStop stops the server gracefully.
func (s *Server) GracefulStop() {
	if s.repairScheduler != nil {
		s.repairScheduler.Stop()
	}
	s.closer.CloseThenWait()
}

// RegisterGRPCServices registers schema management services to gRPC server.
func (s *Server) RegisterGRPCServices(grpcServer *grpc.Server) {
	if !s.enabled {
		return
	}

	schemav1.RegisterSchemaManagementServiceServer(grpcServer, s.schemaService)
	schemav1.RegisterSchemaUpdateServiceServer(grpcServer, s.updateService)
}

// GenerateClients generates schema management clients.
func (s *Server) GenerateClients() (schemav1.SchemaManagementServiceClient, schemav1.SchemaUpdateServiceClient) {
	return &schemaManagementClient{schemaManagementServer: s.schemaService},
		&schemaUpdateClient{schemaUpdateServer: s.updateService}
}

func (s *Server) insert(ctx context.Context, prop *propertyv1.Property) error {
	if s.closer.Closed() || !s.enabled {
		return errors.New("server is closed or not enabled")
	}
	return s.propertyService.DirectInsert(ctx, prop.Metadata.Group, 0, property.GetPropertyID(prop), prop)
}

func (s *Server) update(ctx context.Context, prop *propertyv1.Property) error {
	if s.closer.Closed() || !s.enabled {
		return errors.New("server is closed or not enabled")
	}
	return s.propertyService.DirectUpdate(ctx, prop.Metadata.Group, 0, property.GetPropertyID(prop), prop)
}

// delete deletes a schema property.
func (s *Server) delete(ctx context.Context, group, name, id string, updateAt *timestamppb.Timestamp) (bool, error) {
	if s.closer.Closed() || !s.enabled {
		return false, errors.New("server is closed or not enabled")
	}
	prop, getErr := s.propertyService.DirectGet(ctx, group, name, id)
	if getErr != nil {
		return false, getErr
	}
	if prop == nil {
		return false, nil
	}
	// update the updated_at timestamp and tag before deletion
	// so that the client can be detected deleted by queries using updated_at
	for _, tag := range prop.Tags {
		if tag.Key == TagKeyUpdatedAt {
			tag.Value = &modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: updateAt.AsTime().UnixNano()}}}
			break
		}
	}
	prop.UpdatedAt = updateAt
	// update the property before deletion
	if updateErr := s.propertyService.DirectUpdate(ctx, group, 0, property.GetPropertyID(prop), prop); updateErr != nil {
		return false, updateErr
	}
	deleteErr := s.propertyService.DirectDelete(ctx, [][]byte{property.GetPropertyID(prop)})
	return deleteErr == nil, deleteErr
}

func (s *Server) get(ctx context.Context, group, name, id string) (*propertyv1.Property, error) {
	if s.closer.Closed() || !s.enabled {
		return nil, errors.New("server is closed or not enabled")
	}
	return s.propertyService.DirectGet(ctx, group, name, id)
}

func (s *Server) exist(ctx context.Context, group, name, id string) (bool, error) {
	if s.closer.Closed() || !s.enabled {
		return false, errors.New("server is closed or not enabled")
	}
	return s.propertyService.DirectExist(ctx, group, name, id)
}

func (s *Server) list(ctx context.Context, req *propertyv1.QueryRequest) ([]*property.WithDeleteTime, error) {
	if s.closer.Closed() || !s.enabled {
		return nil, errors.New("server is closed or not enabled")
	}
	return s.propertyService.DirectQuery(ctx, req)
}
