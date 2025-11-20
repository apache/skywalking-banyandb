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
	"sort"
	"time"

	"github.com/dustin/go-humanize"

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
	metadata                  metadata.Repo
	pipeline                  queue.Server
	omr                       observability.MetricsRegistry
	lfs                       fs.FileSystem
	writeListener             bus.MessageListener
	dataNodeSelector          node.Selector
	pm                        protector.Memory
	handoffCtrl               *handoffController
	l                         *logger.Logger
	schemaRepo                schemaRepo
	dataPath                  string
	root                      string
	dataNodeList              []string
	option                    option
	maxDiskUsagePercent       int
	handoffMaxSizePercent     int
	failedPartsMaxSizePercent int
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
	fs.IntVar(&l.failedPartsMaxSizePercent, "failed-parts-max-size-percent", 10,
		"percentage of BanyanDB's allowed disk usage allocated to failed parts storage. "+
			"Calculated as: totalDisk * trace-max-disk-usage-percent * failed-parts-max-size-percent / 10000. "+
			"Set to 0 to disable copying failed parts. Valid range: 0-100")
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

	if l.failedPartsMaxSizePercent < 0 || l.failedPartsMaxSizePercent > 100 {
		return fmt.Errorf("invalid failed-parts-max-size-percent: %d%%. Must be between 0 and 100", l.failedPartsMaxSizePercent)
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
	l.lfs.MkdirIfNotExist(l.dataPath, storage.DirPerm)

	traceDataNodeRegistry := grpc.NewClusterNodeRegistry(data.TopicTracePartSync, l.option.tire2Client, l.dataNodeSelector)
	l.option.failedPartsMaxTotalSizeBytes = 0
	if l.failedPartsMaxSizePercent > 0 {
		totalSpace := l.lfs.MustGetTotalSpace(l.dataPath)
		maxTotalSizeBytes := totalSpace * uint64(l.maxDiskUsagePercent) / 100
		maxTotalSizeBytes = maxTotalSizeBytes * uint64(l.failedPartsMaxSizePercent) / 100
		l.option.failedPartsMaxTotalSizeBytes = maxTotalSizeBytes
		l.l.Info().
			Uint64("maxFailedPartsBytes", maxTotalSizeBytes).
			Int("failedPartsMaxSizePercent", l.failedPartsMaxSizePercent).
			Int("maxDiskUsagePercent", l.maxDiskUsagePercent).
			Msg("configured failed parts storage limit")
	} else {
		l.l.Info().Msg("failed parts storage limit disabled (percent set to 0)")
	}
	// Initialize handoff controller if data nodes are configured
	l.l.Info().Strs("dataNodeList", l.dataNodeList).Int("maxSizePercent", l.handoffMaxSizePercent).
		Msg("handoff configuration")
	if len(l.dataNodeList) > 0 && l.option.tire2Client != nil && l.handoffMaxSizePercent > 0 {
		// Calculate max handoff size based on percentage of disk space
		// Formula: totalDisk * maxDiskUsagePercent * handoffMaxSizePercent / 10000
		// Example: 100GB disk, 95% max usage, 10% handoff = 100 * 95 * 10 / 10000 = 9.5GB
		var maxSizeBytes uint64
		if l.handoffMaxSizePercent > 0 {
			totalSpace := l.lfs.MustGetTotalSpace(l.dataPath)
			// Divide after each multiplication to avoid overflow with large disk capacities
			maxSizeBytes = totalSpace * uint64(l.maxDiskUsagePercent) / 100 * uint64(l.handoffMaxSizePercent) / 100
		}
		if maxSizeBytes == 0 {
			return fmt.Errorf("handoff max size is 0 because handoff-max-size-percent is 0 or not set. " +
				"Set BYDB_HANDOFF_MAX_SIZE_PERCENT environment variable or --handoff-max-size-percent flag to enable handoff storage limit")
		}
		l.l.Info().
			Str("maxSizeBytes", humanize.Bytes(maxSizeBytes)).
			Int("maxSizePercent", l.handoffMaxSizePercent).
			Int("diskUsagePercent", l.maxDiskUsagePercent).
			Msg("handoff max size")

		// nolint:contextcheck
		resolveAssignments := func(group string, shardID uint32) ([]string, error) {
			if l.metadata == nil {
				return nil, fmt.Errorf("metadata repo is not initialized")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			groupSchema, err := l.metadata.GroupRegistry().GetGroup(ctx, group)
			if err != nil {
				return nil, err
			}
			if groupSchema == nil || groupSchema.ResourceOpts == nil {
				return nil, fmt.Errorf("group %s missing resource options", group)
			}
			copies := groupSchema.ResourceOpts.Replicas + 1
			if len(l.dataNodeList) == 0 {
				return nil, fmt.Errorf("no data nodes configured for handoff")
			}
			sortedNodes := append([]string(nil), l.dataNodeList...)
			sort.Strings(sortedNodes)
			nodes := make([]string, 0, copies)
			seen := make(map[string]struct{}, copies)
			for replica := uint32(0); replica < copies; replica++ {
				nodeID := sortedNodes[(int(shardID)+int(replica))%len(sortedNodes)]
				if _, ok := seen[nodeID]; ok {
					continue
				}
				nodes = append(nodes, nodeID)
				seen[nodeID] = struct{}{}
			}
			return nodes, nil
		}

		var err error
		// nolint:contextcheck
		l.handoffCtrl, err = newHandoffController(l.lfs, l.dataPath, l.option.tire2Client, l.dataNodeList, maxSizeBytes, l.l, resolveAssignments)
		if err != nil {
			return err
		}
		l.l.Info().
			Int("dataNodes", len(l.dataNodeList)).
			Uint64("maxSize", maxSizeBytes).
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
