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
	"errors"
	"path"
	"path/filepath"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/liaison/grpc"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/bus"
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
	writeListener       bus.MessageListener
	dataNodeSelector    node.Selector
	l                   *logger.Logger
	schemaRepo          schemaRepo
	dataPath            string
	root                string
	option              option
	maxDiskUsagePercent int
}

var _ Service = (*liaison)(nil)

// NewLiaison creates a new trace liaison service with the given dependencies.
func NewLiaison(metadata metadata.Repo, pipeline queue.Server, omr observability.MetricsRegistry, pm protector.Memory,
	dataNodeSelector node.Selector, tire2Client queue.Client,
) (Service, error) {
	return &liaison{
		metadata:         metadata,
		pipeline:         pipeline,
		omr:              omr,
		pm:               pm,
		dataNodeSelector: dataNodeSelector,
		option: option{
			tire2Client: tire2Client,
		},
	}, nil
}

// LiaisonService returns a new liaison service (deprecated - use NewLiaison).
func LiaisonService(_ context.Context) (Service, error) {
	return &liaison{}, nil
}

func (l *liaison) FlagSet() *run.FlagSet {
	fs := run.NewFlagSet("trace")
	fs.StringVar(&l.root, "trace-root-path", "/tmp", "the root path for trace data")
	fs.StringVar(&l.dataPath, "trace-data-path", "", "the path for trace data (optional)")
	fs.DurationVar(&l.option.flushTimeout, "trace-flush-timeout", defaultFlushTimeout, "the timeout for trace data flush")
	fs.IntVar(&l.maxDiskUsagePercent, "trace-max-disk-usage-percent", 95, "the maximum disk usage percentage")
	fs.DurationVar(&l.option.syncInterval, "trace-sync-interval", defaultSyncInterval, "the periodic sync interval for trace data")
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

func (l *liaison) PreRun(ctx context.Context) error {
	l.l = logger.GetLogger(l.Name())
	l.l.Info().Msg("memory protector is initialized in PreRun")
	l.lfs = fs.NewLocalFileSystemWithLoggerAndLimit(l.l, l.pm.GetLimit())
	path := path.Join(l.root, l.Name())
	observability.UpdatePath(path)
	val := ctx.Value(common.ContextNodeKey)
	if val == nil {
		return errors.New("node id is empty")
	}
	if l.dataPath == "" {
		l.dataPath = filepath.Join(path, storage.DataDir)
	}

	traceDataNodeRegistry := grpc.NewClusterNodeRegistry(data.TopicTracePartSync, l.option.tire2Client, l.dataNodeSelector)
	l.schemaRepo = newLiaisonSchemaRepo(l.dataPath, l, traceDataNodeRegistry)
	l.writeListener = setUpWriteQueueCallback(l.l, &l.schemaRepo, l.maxDiskUsagePercent, l.option.tire2Client)

	// Register chunked sync handler for trace and sidx data
	l.pipeline.RegisterChunkedSyncHandler(data.TopicTracePartSync, setUpChunkedSyncCallback(l.l, &l.schemaRepo))
	l.l.Info().
		Str("root", l.root).
		Str("dataPath", l.dataPath).
		Msg("trace liaison service initialized")

	return l.pipeline.Subscribe(data.TopicTraceWrite, l.writeListener)
}

func (l *liaison) Serve() run.StopNotify {
	return l.schemaRepo.StopCh()
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
