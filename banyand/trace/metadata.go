// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package trace

import (
	"context"
	"time"

	"github.com/pkg/errors"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/liaison/grpc"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	resourceSchema "github.com/apache/skywalking-banyandb/pkg/schema"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

var (
	metadataScope = traceScope.SubScope("metadata")
	// ErrTraceNotFound is returned when a trace is not found.
	ErrTraceNotFound = errors.New("trace not found")
)

// SchemaService allows querying schema information.
type SchemaService interface {
	Query
	Close()
}

type schemaRepo struct {
	resourceSchema.Repository
	l        *logger.Logger
	metadata metadata.Repo
	path     string
}

func newSchemaRepo(path string, svc *standalone, nodeLabels map[string]string) schemaRepo {
	sr := schemaRepo{
		l:        svc.l,
		path:     path,
		metadata: svc.metadata,
		Repository: resourceSchema.NewRepository(
			svc.metadata,
			svc.l,
			newSupplier(path, svc, nodeLabels),
			resourceSchema.NewMetrics(svc.omr.With(metadataScope)),
		),
	}
	sr.start()
	return sr
}

func newLiaisonSchemaRepo(path string, svc *liaison, traceDataNodeRegistry grpc.NodeRegistry) schemaRepo {
	sr := schemaRepo{
		l:        svc.l,
		path:     path,
		metadata: svc.metadata,
		Repository: resourceSchema.NewRepository(
			svc.metadata,
			svc.l,
			newQueueSupplier(path, svc, traceDataNodeRegistry),
			resourceSchema.NewMetrics(svc.omr.With(metadataScope)),
		),
	}
	sr.start()
	return sr
}

func (sr *schemaRepo) start() {
	sr.l.Info().Str("path", sr.path).Msg("starting trace metadata repository")
}

func (sr *schemaRepo) LoadGroup(name string) (resourceSchema.Group, bool) {
	return sr.Repository.LoadGroup(name)
}

func (sr *schemaRepo) Trace(metadata *commonv1.Metadata) (*trace, bool) {
	sm, ok := sr.Repository.LoadResource(metadata)
	if !ok {
		return nil, false
	}
	t, ok := sm.Delegated().(*trace)
	return t, ok
}

func (sr *schemaRepo) GetRemovalSegmentsTimeRange(_ string) *timestamp.TimeRange {
	panic("not implemented")
}

// supplier is the supplier for standalone service.
type supplier struct {
	metadata   metadata.Repo
	omr        observability.MetricsRegistry
	pm         protector.Memory
	l          *logger.Logger
	schemaRepo *schemaRepo
	nodeLabels map[string]string
	path       string
	option     option
}

func newSupplier(path string, svc *standalone, nodeLabels map[string]string) *supplier {
	return &supplier{
		metadata:   svc.metadata,
		omr:        svc.omr,
		pm:         svc.pm,
		l:          svc.l,
		nodeLabels: nodeLabels,
		path:       path,
		option:     svc.option,
	}
}

func (s *supplier) OpenResource(spec resourceSchema.Resource) (resourceSchema.IndexListener, error) {
	traceSchema := spec.Schema().(*databasev1.Trace)
	return openTrace(traceSchema, s.l, s.pm, s.schemaRepo), nil
}

func (s *supplier) ResourceSchema(md *commonv1.Metadata) (resourceSchema.ResourceSchema, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.metadata.TraceRegistry().GetTrace(ctx, md)
}

func (s *supplier) OpenDB(_ *commonv1.Group) (resourceSchema.DB, error) {
	panic("not implemented")
}

// queueSupplier is the supplier for liaison service.
type queueSupplier struct {
	metadata              metadata.Repo
	omr                   observability.MetricsRegistry
	pm                    protector.Memory
	traceDataNodeRegistry grpc.NodeRegistry
	l                     *logger.Logger
	schemaRepo            *schemaRepo
	path                  string
	option                option
}

func newQueueSupplier(path string, svc *liaison, traceDataNodeRegistry grpc.NodeRegistry) *queueSupplier {
	return &queueSupplier{
		metadata:              svc.metadata,
		omr:                   svc.omr,
		pm:                    svc.pm,
		traceDataNodeRegistry: traceDataNodeRegistry,
		l:                     svc.l,
		path:                  path,
		option:                svc.option,
	}
}

func (qs *queueSupplier) OpenResource(spec resourceSchema.Resource) (resourceSchema.IndexListener, error) {
	traceSchema := spec.Schema().(*databasev1.Trace)
	return openTrace(traceSchema, qs.l, qs.pm, qs.schemaRepo), nil
}

func (qs *queueSupplier) ResourceSchema(md *commonv1.Metadata) (resourceSchema.ResourceSchema, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return qs.metadata.TraceRegistry().GetTrace(ctx, md)
}

func (qs *queueSupplier) OpenDB(_ *commonv1.Group) (resourceSchema.DB, error) {
	panic("not implemented")
}
