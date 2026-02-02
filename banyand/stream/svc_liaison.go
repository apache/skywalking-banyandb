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

package stream

import (
	"context"
	"fmt"
	"path"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/liaison/grpc"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	obsservice "github.com/apache/skywalking-banyandb/banyand/observability/services"
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
	pm                        protector.Memory
	metadata                  metadata.Repo
	pipeline                  queue.Server
	omr                       observability.MetricsRegistry
	lfs                       fs.FileSystem
	writeListener             bus.MessageListener
	dataNodeSelector          node.Selector
	l                         *logger.Logger
	schemaRepo                schemaRepo
	dataPath                  string
	root                      string
	option                    option
	maxDiskUsagePercent       int
	failedPartsMaxSizePercent int
}

func (s *liaison) Stream(metadata *commonv1.Metadata) (Stream, error) {
	sm, ok := s.schemaRepo.loadStream(metadata)
	if !ok {
		return nil, errors.WithStack(ErrStreamNotExist)
	}
	return sm, nil
}

func (s *liaison) LoadGroup(name string) (resourceSchema.Group, bool) {
	return s.schemaRepo.LoadGroup(name)
}

func (s *liaison) GetRemovalSegmentsTimeRange(group string) *timestamp.TimeRange {
	return s.schemaRepo.GetRemovalSegmentsTimeRange(group)
}

func (s *liaison) CollectDataInfo(_ context.Context, _ string) (*databasev1.DataInfo, error) {
	return nil, errors.New("collect data info is not supported on liaison node")
}

// CollectLiaisonInfo collects liaison node info.
func (s *liaison) CollectLiaisonInfo(_ context.Context, group string) (*databasev1.LiaisonInfo, error) {
	info := &databasev1.LiaisonInfo{
		PendingWriteDataCount:       0,
		PendingSyncPartCount:        0,
		PendingSyncDataSizeBytes:    0,
		PendingHandoffPartCount:     0,
		PendingHandoffDataSizeBytes: 0,
	}
	pendingWriteCount, writeErr := s.schemaRepo.collectPendingWriteInfo(group)
	if writeErr != nil {
		return nil, fmt.Errorf("failed to collect pending write info: %w", writeErr)
	}
	info.PendingWriteDataCount = pendingWriteCount
	pendingSyncPartCount, pendingSyncDataSizeBytes, syncErr := s.schemaRepo.collectPendingSyncInfo(group)
	if syncErr != nil {
		return nil, fmt.Errorf("failed to collect pending sync info: %w", syncErr)
	}
	info.PendingSyncPartCount = pendingSyncPartCount
	info.PendingSyncDataSizeBytes = pendingSyncDataSizeBytes
	return info, nil
}

func (s *liaison) FlagSet() *run.FlagSet {
	flagS := run.NewFlagSet("storage")
	flagS.StringVar(&s.root, "stream-root-path", "/tmp", "the root path of stream")
	flagS.StringVar(&s.dataPath, "stream-data-path", "", "the data directory path of stream. If not set, <stream-root-path>/stream/data will be used")
	flagS.DurationVar(&s.option.flushTimeout, "stream-flush-timeout", defaultFlushTimeout, "the memory data timeout of stream")
	flagS.IntVar(&s.maxDiskUsagePercent, "stream-max-disk-usage-percent", 95, "the maximum disk usage percentage allowed")
	flagS.DurationVar(&s.option.syncInterval, "stream-sync-interval", defaultSyncInterval, "the periodic sync interval for stream data")
	flagS.IntVar(&s.failedPartsMaxSizePercent, "failed-parts-max-size-percent", 10,
		"percentage of BanyanDB's allowed disk usage allocated to failed parts storage. "+
			"Calculated as: totalDisk * stream-max-disk-usage-percent * failed-parts-max-size-percent / 10000. "+
			"Set to 0 to disable copying failed parts. Valid range: 0-100")
	return flagS
}

func (s *liaison) Validate() error {
	if s.root == "" {
		return errEmptyRootPath
	}
	if s.maxDiskUsagePercent < 0 {
		return errors.New("stream-max-disk-usage-percent must be greater than or equal to 0")
	}
	if s.maxDiskUsagePercent > 100 {
		return errors.New("stream-max-disk-usage-percent must be less than or equal to 100")
	}
	if s.failedPartsMaxSizePercent < 0 || s.failedPartsMaxSizePercent > 100 {
		return errors.New("failed-parts-max-size-percent must be between 0 and 100")
	}
	return nil
}

func (s *liaison) Name() string {
	return "stream"
}

func (s *liaison) Role() databasev1.Role {
	return databasev1.Role_ROLE_LIAISON
}

func (s *liaison) PreRun(ctx context.Context) error {
	s.l = logger.GetLogger(s.Name())
	s.l.Info().Msg("memory protector is initialized in PreRun")
	s.lfs = fs.NewLocalFileSystemWithLoggerAndLimit(s.l, s.pm.GetLimit())
	path := path.Join(s.root, s.Name())
	obsservice.UpdatePath(path)
	val := ctx.Value(common.ContextNodeKey)
	if val == nil {
		return errors.New("node id is empty")
	}
	if s.dataPath == "" {
		s.dataPath = filepath.Join(path, storage.DataDir)
	}
	if !strings.HasPrefix(filepath.VolumeName(s.dataPath), filepath.VolumeName(path)) {
		obsservice.UpdatePath(s.dataPath)
	}
	s.lfs.MkdirIfNotExist(s.dataPath, storage.DirPerm)

	s.option.failedPartsMaxTotalSizeBytes = 0
	if s.failedPartsMaxSizePercent > 0 {
		totalSpace := s.lfs.MustGetTotalSpace(s.dataPath)
		maxTotalSizeBytes := totalSpace * uint64(s.maxDiskUsagePercent) / 100
		maxTotalSizeBytes = maxTotalSizeBytes * uint64(s.failedPartsMaxSizePercent) / 100
		s.option.failedPartsMaxTotalSizeBytes = maxTotalSizeBytes
		s.l.Info().
			Uint64("maxFailedPartsBytes", maxTotalSizeBytes).
			Int("failedPartsMaxSizePercent", s.failedPartsMaxSizePercent).
			Int("maxDiskUsagePercent", s.maxDiskUsagePercent).
			Msg("configured failed parts storage limit")
	} else {
		s.l.Info().Msg("failed parts storage limit disabled (percent set to 0)")
	}
	streamDataNodeRegistry := grpc.NewClusterNodeRegistry(data.TopicStreamPartSync, s.option.tire2Client, s.dataNodeSelector)
	s.schemaRepo = newLiaisonSchemaRepo(s.dataPath, s, streamDataNodeRegistry)
	s.writeListener = setUpWriteQueueCallback(s.l, &s.schemaRepo, s.maxDiskUsagePercent, s.option.tire2Client)

	// Register chunked sync handler for stream data
	s.pipeline.RegisterChunkedSyncHandler(data.TopicStreamPartSync, setUpChunkedSyncCallback(s.l, &s.schemaRepo))

	if metaSvc, ok := s.metadata.(metadata.Service); ok {
		metaSvc.RegisterLiaisonCollector(commonv1.Catalog_CATALOG_STREAM, s)
	}

	collectLiaisonInfoListener := &collectLiaisonInfoListener{s: s}
	if subscribeErr := s.pipeline.Subscribe(data.TopicStreamCollectLiaisonInfo, collectLiaisonInfoListener); subscribeErr != nil {
		return fmt.Errorf("failed to subscribe to collect liaison info topic: %w", subscribeErr)
	}

	return s.pipeline.Subscribe(data.TopicStreamWrite, s.writeListener)
}

func (s *liaison) Serve() run.StopNotify {
	return s.schemaRepo.StopCh()
}

func (s *liaison) GracefulStop() {
	s.schemaRepo.Close()
}

// NewLiaison creates a new stream liaison service with the given dependencies.
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

type collectLiaisonInfoListener struct {
	*bus.UnImplementedHealthyListener
	s *liaison
}

func (l *collectLiaisonInfoListener) Rev(ctx context.Context, message bus.Message) bus.Message {
	req, ok := message.Data().(*databasev1.GroupRegistryServiceInspectRequest)
	if !ok {
		return bus.NewMessage(message.ID(), common.NewError("invalid data type for collect liaison info request"))
	}
	liaisonInfo, collectErr := l.s.CollectLiaisonInfo(ctx, req.Group)
	if collectErr != nil {
		return bus.NewMessage(message.ID(), common.NewError("failed to collect liaison info: %v", collectErr))
	}
	return bus.NewMessage(message.ID(), liaisonInfo)
}
