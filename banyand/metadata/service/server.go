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

// Package service implements the metadata server wrapper.
package service

import (
	"context"
	"errors"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/backup/snapshot"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema/schemaserver"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

// Service extends metadata.Service with schema server port access.
type Service interface {
	metadata.Service
	GetSchemaServerPort() *uint32
	GetSchemaGossipPort() *uint32
	SetPropertyPipelineClient(queue.Client)
	SetSnapshotPipeline(queue.Server)
}

type server struct {
	metadata.Service
	propServer         schemaserver.Server
	repairSvc          schemaserver.RepairService
	pipelineClient     queue.Client
	snapshotPipeline   queue.Server
	omr                observability.MetricsRegistry
	serviceFlags       *run.FlagSet
	closer             *run.Closer
	schemaRegistryMode string
	nodeDiscoveryMode  string
	hasMetaRole        bool
}

func (s *server) Name() string {
	return "metadata"
}

func (s *server) Role() databasev1.Role {
	if s.hasMetaRole {
		return databasev1.Role_ROLE_META
	}
	return databasev1.Role_ROLE_UNSPECIFIED
}

func (s *server) FlagSet() *run.FlagSet {
	fs := run.NewFlagSet("metadata")
	fs.StringVar(&s.schemaRegistryMode, "schema-registry-mode", metadata.RegistryModeProperty,
		"Schema registry mode: 'property' for property-based storage")
	fs.StringVar(&s.nodeDiscoveryMode, "node-discovery-mode", metadata.NodeDiscoveryModeNone,
		"Node discovery mode: 'none' for standalone, 'dns' for DNS-based, 'file' for file-based")
	fs.BoolVar(&s.hasMetaRole, "has-meta-role", true,
		"Whether this data node runs the schema server. Only effective in property schema registry mode.")
	if s.propServer == nil {
		omr := s.omr
		if omr == nil {
			omr = observability.BypassRegistry
		}
		s.propServer = schemaserver.NewServer(omr)
	}
	if s.serviceFlags == nil {
		s.serviceFlags = s.Service.FlagSet()
	}
	fs.AddFlagSet(s.propServer.FlagSet().FlagSet)
	fs.AddFlagSet(s.serviceFlags.FlagSet)
	return fs
}

func (s *server) Validate() error {
	if s.serviceFlags == nil {
		return errors.New("service flags are not initialized")
	}
	if err := s.serviceFlags.Set("schema-registry-mode", s.schemaRegistryMode); err != nil {
		return err
	}
	if err := s.serviceFlags.Set("node-discovery-mode", s.nodeDiscoveryMode); err != nil {
		return err
	}
	if validateErr := s.Service.Validate(); validateErr != nil {
		return validateErr
	}
	if s.propServer != nil {
		if propValidateErr := s.propServer.Validate(); propValidateErr != nil {
			return propValidateErr
		}
		if s.repairSvc != nil {
			return s.repairSvc.Validate()
		}
	}
	return nil
}

func (s *server) PreRun(ctx context.Context) error {
	if !s.hasMetaRole {
		s.propServer = nil
		s.repairSvc = nil
	} else {
		ctx = s.enrichContextWithSchemaAddress(ctx)
		if propPreRunErr := s.propServer.PreRun(ctx); propPreRunErr != nil {
			return propPreRunErr
		}
		if s.repairSvc != nil {
			if repairPreRunErr := s.repairSvc.PreRun(ctx); repairPreRunErr != nil {
				return repairPreRunErr
			}
		}
	}
	if s.snapshotPipeline != nil {
		if subscribeErr := s.snapshotPipeline.Subscribe(data.TopicSnapshot,
			&schemaPropertySnapshotListener{s: s}); subscribeErr != nil {
			return subscribeErr
		}
	}
	return s.Service.PreRun(ctx)
}

func (s *server) Serve() run.StopNotify {
	if s.propServer != nil {
		s.closer.AddRunning()
		go func() {
			defer s.closer.Done()
			<-s.propServer.Serve()
		}()
	}
	if s.repairSvc != nil {
		s.closer.AddRunning()
		go func() {
			defer s.closer.Done()
			<-s.repairSvc.Serve()
		}()
	}
	_ = s.Service.Serve()
	return s.closer.CloseNotify()
}

func (s *server) GracefulStop() {
	if s.repairSvc != nil {
		s.repairSvc.GracefulStop()
	}
	if s.propServer != nil {
		s.propServer.GracefulStop()
	}
	s.Service.GracefulStop()
	s.closer.CloseThenWait()
}

// NewService returns a new metadata repository Service.
func NewService() (Service, error) {
	s := &server{
		closer: run.NewCloser(0),
	}
	var clientErr error
	s.Service, clientErr = metadata.NewClient()
	if clientErr != nil {
		return nil, clientErr
	}
	return s, nil
}

// SetMetricsRegistry stores the metrics registry for lazy propServer creation.
func (s *server) SetMetricsRegistry(omr observability.MetricsRegistry) {
	s.omr = omr
	s.Service.SetMetricsRegistry(omr)
}

// GetSchemaServerPort returns the schema server gRPC port.
func (s *server) GetSchemaServerPort() *uint32 {
	if s.propServer != nil {
		return s.propServer.GetPort()
	}
	return nil
}

// SetPropertyPipelineClient injects the pipeline client used by the schema gossip repair service.
func (s *server) SetPropertyPipelineClient(client queue.Client) {
	s.pipelineClient = client
}

// SetSnapshotPipeline stores the pipeline server for subscribing to snapshot topics.
func (s *server) SetSnapshotPipeline(pipeline queue.Server) {
	s.snapshotPipeline = pipeline
}

// GetSchemaGossipPort returns the schema gossip gRPC port.
func (s *server) GetSchemaGossipPort() *uint32 {
	if s.repairSvc != nil {
		return s.repairSvc.GetGossipPort()
	}
	return nil
}

func (s *server) enrichContextWithSchemaAddress(ctx context.Context) context.Context {
	port := s.propServer.GetPort()
	if port == nil {
		return ctx
	}
	val := ctx.Value(common.ContextNodeKey)
	if val == nil {
		return ctx
	}
	node, ok := val.(common.Node)
	if !ok {
		return ctx
	}
	if node.PropertySchemaGrpcAddress != "" {
		return ctx
	}
	nodeHost := node.GrpcAddress
	if idx := strings.LastIndex(nodeHost, ":"); idx >= 0 {
		nodeHost = nodeHost[:idx]
	}
	if nodeHost == "" {
		nodeHost = "localhost"
	}
	node.PropertySchemaGrpcAddress = net.JoinHostPort(nodeHost, strconv.FormatUint(uint64(*port), 10))
	return context.WithValue(ctx, common.ContextNodeKey, node)
}

type schemaPropertySnapshotListener struct {
	*bus.UnImplementedHealthyListener
	s *server
}

func (l *schemaPropertySnapshotListener) Rev(ctx context.Context, _ bus.Message) bus.Message {
	if l.s.propServer == nil {
		return bus.NewMessage(bus.MessageID(time.Now().UnixNano()), nil)
	}
	snp := l.s.propServer.TakeSnapshot(ctx)
	if snp == nil {
		return bus.NewMessage(bus.MessageID(time.Now().UnixNano()), nil)
	}
	snp.Name = snapshot.SchemaPropertyCatalogName + "/" + snp.Name
	return bus.NewMessage(bus.MessageID(time.Now().UnixNano()), snp)
}
