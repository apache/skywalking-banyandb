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

package trace

import (
	"context"
	"path"

	"github.com/pkg/errors"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/liaison/grpc"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/node"
	"github.com/apache/skywalking-banyandb/pkg/run"
	resourceSchema "github.com/apache/skywalking-banyandb/pkg/schema"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

type liaison struct {
	pm                  protector.Memory
	metadata            metadata.Repo
	pipeline            queue.Server
	omr                 observability.MetricsRegistry
	lfs                 fs.FileSystem
	dataNodeSelector    node.Selector
	l                   *logger.Logger
	schemaRepo          schemaRepo
	dataPath            string
	root                string
	option              option
	maxDiskUsagePercent int
}

var _ Service = (*liaison)(nil)

// LiaisonService returns a new liaison service.
func LiaisonService(_ context.Context) (Service, error) {
	return &liaison{}, nil
}

func (l *liaison) FlagSet() *run.FlagSet {
	fs := run.NewFlagSet("trace")
	fs.StringVar(&l.root, "trace-root-path", "/tmp", "the root path for trace data")
	fs.StringVar(&l.dataPath, "trace-data-path", "", "the path for trace data (optional)")
	fs.DurationVar(&l.option.flushTimeout, "trace-flush-timeout", defaultFlushTimeout, "the timeout for trace data flush")
	fs.IntVar(&l.maxDiskUsagePercent, "trace-max-disk-usage-percent", 95, "the maximum disk usage percentage")
	// Additional liaison-specific flags can be added here
	return fs
}

func (l *liaison) Validate() error {
	if l.root == "" {
		return errEmptyRootPath
	}
	return nil
}

func (l *liaison) Name() string {
	return "trace"
}

func (l *liaison) Role() databasev1.Role {
	return databasev1.Role_ROLE_LIAISON
}

func (l *liaison) PreRun(_ context.Context) error {
	l.l = logger.GetLogger("trace")

	// Initialize metadata
	if l.metadata == nil {
		return errors.New("metadata repo is required")
	}

	// Initialize filesystem
	if l.lfs == nil {
		l.lfs = fs.NewLocalFileSystem()
	}

	// Initialize protector
	if l.pm == nil {
		return errors.New("memory protector is required")
	}

	// Initialize pipeline
	if l.pipeline == nil {
		return errors.New("pipeline is required")
	}

	// Set up data path
	if l.dataPath == "" {
		l.dataPath = path.Join(l.root, "trace-data")
	}

	// Initialize data node registry - this would be provided by the system
	var traceDataNodeRegistry grpc.NodeRegistry

	// Initialize schema repository
	l.schemaRepo = newLiaisonSchemaRepo(l.dataPath, l, traceDataNodeRegistry)

	l.l.Info().
		Str("root", l.root).
		Str("dataPath", l.dataPath).
		Msg("trace liaison service initialized")

	return nil
}

func (l *liaison) Serve() run.StopNotify {
	// As specified in the plan, no pipeline listeners should be implemented
	l.l.Info().Msg("trace liaison service started")

	// Return a channel that never closes since this service runs indefinitely
	stopCh := make(chan struct{})
	return stopCh
}

func (l *liaison) GracefulStop() {
	if l.schemaRepo.Repository != nil {
		l.schemaRepo.Repository.Close()
	}
	l.l.Info().Msg("trace liaison service stopped")
}

func (l *liaison) LoadGroup(name string) (resourceSchema.Group, bool) {
	return l.schemaRepo.LoadGroup(name)
}

func (l *liaison) Trace(metadata *commonv1.Metadata) (Trace, error) {
	sm, ok := l.schemaRepo.Trace(metadata)
	if !ok {
		return nil, ErrTraceNotFound
	}
	return sm, nil
}

func (l *liaison) GetRemovalSegmentsTimeRange(group string) *timestamp.TimeRange {
	return l.schemaRepo.GetRemovalSegmentsTimeRange(group)
}

// SetMetadata sets the metadata repository.
func (l *liaison) SetMetadata(metadata metadata.Repo) {
	l.metadata = metadata
}

// SetObservabilityRegistry sets the observability metrics registry.
func (l *liaison) SetObservabilityRegistry(omr observability.MetricsRegistry) {
	l.omr = omr
}

// SetProtector sets the memory protector.
func (l *liaison) SetProtector(pm protector.Memory) {
	l.pm = pm
}

// SetPipeline sets the pipeline server.
func (l *liaison) SetPipeline(pipeline queue.Server) {
	l.pipeline = pipeline
}

// SetFileSystem sets the file system.
func (l *liaison) SetFileSystem(lfs fs.FileSystem) {
	l.lfs = lfs
}

// SetDataNodeSelector sets the data node selector.
func (l *liaison) SetDataNodeSelector(selector node.Selector) {
	l.dataNodeSelector = selector
}
