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
	"fmt"
	"path"
	"path/filepath"
	"time"

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
	pm                    protector.Memory
	metadata              metadata.Repo
	pipeline              queue.Server
	omr                   observability.MetricsRegistry
	lfs                   fs.FileSystem
	writeListener         bus.MessageListener
	dataNodeSelector      node.Selector
	l                     *logger.Logger
	schemaRepo            schemaRepo
	handoffCtrl           *handoffController
	dataPath              string
	root                  string
	option                option
	dataNodeList          []string
	maxDiskUsagePercent   int
	handoffMaxSizePercent int
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
	fs.DurationVar(&l.option.flushTimeout, "trace-flush-timeout", 3*time.Second, "the timeout for trace data flush")
	fs.IntVar(&l.maxDiskUsagePercent, "trace-max-disk-usage-percent", 95, "the maximum disk usage percentage")
	fs.DurationVar(&l.option.syncInterval, "trace-sync-interval", defaultSyncInterval, "the periodic sync interval for trace data")
	fs.StringSliceVar(&l.dataNodeList, "data-node-list", nil, "comma-separated list of data node names to monitor for handoff")
	fs.IntVar(&l.handoffMaxSizePercent, "handoff-max-size-percent", 10,
		"percentage of BanyanDB's allowed disk usage allocated to handoff storage. "+
			"Calculated as: totalDisk * trace-max-disk-usage-percent * handoff-max-size-percent / 10000. "+
			"Example: 100GB disk with 95% max usage and 10% handoff = 9.5GB; 50% handoff = 47.5GB. "+
			"Valid range: 0-100")
	return fs
}

func (l *liaison) Validate() error {
	if l.root == "" {
		return errEmptyRootPath
	}

	// Validate handoff-max-size-percent is within valid range
	// Since handoffMaxSizePercent represents the percentage of BanyanDB's allowed disk usage
	// that goes to handoff storage, it should be between 0-100%
	if l.handoffMaxSizePercent < 0 || l.handoffMaxSizePercent > 100 {
		return fmt.Errorf("invalid handoff-max-size-percent: %d%%. Must be between 0 and 100. "+
			"This represents what percentage of BanyanDB's allowed disk usage is allocated to handoff storage. "+
			"Example: 100GB disk with 95%% max usage and 50%% handoff = 100 * 95%% * 50%% = 47.5GB for handoff",
			l.handoffMaxSizePercent)
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
	// Initialize handoff controller if data nodes are configured
	l.l.Info().Strs("dataNodeList", l.dataNodeList).Int("maxSizePercent", l.handoffMaxSizePercent).
		Msg("handoff configuration")
	if len(l.dataNodeList) > 0 && l.option.tire2Client != nil {
		// Calculate max handoff size based on percentage of disk space
		// Formula: totalDisk * maxDiskUsagePercent * handoffMaxSizePercent / 10000
		// Example: 100GB disk, 95% max usage, 10% handoff = 100 * 95 * 10 / 10000 = 9.5GB
		maxSizeMB := 0
		if l.handoffMaxSizePercent > 0 {
			l.lfs.MkdirIfNotExist(l.dataPath, storage.DirPerm)
			totalSpace := l.lfs.MustGetTotalSpace(l.dataPath)
			maxSizeBytes := totalSpace * uint64(l.maxDiskUsagePercent) * uint64(l.handoffMaxSizePercent) / 10000
			maxSizeMB = int(maxSizeBytes / 1024 / 1024)
		}

		var err error
		l.handoffCtrl, err = newHandoffController(l.lfs, l.dataPath, l.option.tire2Client, l.dataNodeList, maxSizeMB, l.l)
		if err != nil {
			return err
		}
		l.l.Info().
			Int("dataNodes", len(l.dataNodeList)).
			Int("maxSizeMB", maxSizeMB).
			Int("maxSizePercent", l.handoffMaxSizePercent).
			Int("diskUsagePercent", l.maxDiskUsagePercent).
			Msg("handoff controller initialized")
	}

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
	if l.handoffCtrl != nil {
		if err := l.handoffCtrl.close(); err != nil {
			l.l.Warn().Err(err).Msg("failed to close handoff controller")
		}
	}
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
