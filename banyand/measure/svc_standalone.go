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

package measure

import (
	"context"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
	resourceSchema "github.com/apache/skywalking-banyandb/pkg/schema"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

var (
	errEmptyRootPath = errors.New("root path is empty")
	// ErrMeasureNotExist denotes a measure doesn't exist in the metadata repo.
	ErrMeasureNotExist = errors.New("measure doesn't exist")
)

// Service allows inspecting the measure data points.
type Service interface {
	run.PreRunner
	run.Config
	run.Service
	Query
}

var _ Service = (*standalone)(nil)

type standalone struct {
	lfs                fs.FileSystem
	c                  storage.Cache
	pipeline           queue.Server
	localPipeline      queue.Queue
	metricPipeline     queue.Server
	omr                observability.MetricsRegistry
	metadata           metadata.Repo
	pm                 protector.Memory
	cm                 *cacheMetrics
	l                  *logger.Logger
	schemaRepo         *schemaRepo
	diskMonitor        *storage.DiskMonitor
	root               string
	snapshotDir        string
	dataPath           string
	option             option
	retentionConfig    storage.RetentionConfig
	cc                 storage.CacheConfig
	maxFileSnapshotNum int
}

func (s *standalone) Measure(metadata *commonv1.Metadata) (Measure, error) {
	sm, ok := s.schemaRepo.loadMeasure(metadata)
	if !ok {
		return nil, errors.WithStack(ErrMeasureNotExist)
	}
	return sm, nil
}

func (s *standalone) LoadGroup(name string) (resourceSchema.Group, bool) {
	return s.schemaRepo.LoadGroup(name)
}

func (s *standalone) GetRemovalSegmentsTimeRange(group string) *timestamp.TimeRange {
	return s.schemaRepo.GetRemovalSegmentsTimeRange(group)
}

// RetentionService interface implementation

func (s *standalone) GetDataPath() string {
	return s.dataPath
}

func (s *standalone) GetSnapshotDir() string {
	return s.snapshotDir
}

func (s *standalone) LoadAllGroups() []resourceSchema.Group {
	return s.schemaRepo.LoadAllGroups()
}

func (s *standalone) DeleteOldestSegmentInGroup(group string) (bool, error) {
	// TODO: Implement actual segment deletion when storage APIs are ready
	// For now, return false to indicate no deletion performed
	s.l.Debug().Str("group", group).Msg("DeleteOldestSegmentInGroup not yet implemented")
	return false, nil
}

func (s *standalone) CleanupOldSnapshots(maxAge time.Duration) error {
	if s.snapshotDir == "" {
		return nil
	}
	// DeleteStaleSnapshots doesn't return an error, it just deletes stale snapshots
	storage.DeleteStaleSnapshots(s.snapshotDir, s.maxFileSnapshotNum, s.lfs)
	return nil
}

func (s *standalone) GetServiceName() string {
	return serviceName
}

func (s *standalone) FlagSet() *run.FlagSet {
	flagS := run.NewFlagSet("storage")
	flagS.StringVar(&s.root, "measure-root-path", "/tmp", "the root path of measure")
	flagS.StringVar(&s.dataPath, "measure-data-path", "", "the data directory path of measure. If not set, <measure-root-path>/measure/data will be used")
	flagS.DurationVar(&s.option.flushTimeout, "measure-flush-timeout", defaultFlushTimeout, "the memory data timeout of measure")
	s.option.mergePolicy = newDefaultMergePolicy()
	flagS.VarP(&s.option.mergePolicy.maxFanOutSize, "measure-max-fan-out-size", "", "the upper bound of a single file size after merge of measure")
	s.option.seriesCacheMaxSize = run.Bytes(32 << 20)
	flagS.VarP(&s.option.seriesCacheMaxSize, "measure-series-cache-max-size", "", "the max size of series cache in each group")

	// Retention configuration flags
	flagS.Float64Var(&s.retentionConfig.HighWatermark, "measure-retention-high-watermark", 95.0, "disk usage high watermark percentage that triggers forced retention cleanup")
	flagS.Float64Var(&s.retentionConfig.LowWatermark, "measure-retention-low-watermark", 85.0, "disk usage low watermark percentage where forced retention cleanup stops")
	flagS.DurationVar(&s.retentionConfig.CheckInterval, "measure-retention-check-interval", 5*time.Minute, "interval for checking disk usage")
	flagS.DurationVar(&s.retentionConfig.Cooldown, "measure-retention-cooldown", 30*time.Second, "cooldown period between forced segment deletions")

	flagS.IntVar(&s.maxFileSnapshotNum, "measure-max-file-snapshot-num", 10, "the maximum number of file snapshots allowed")
	s.cc.MaxCacheSize = run.Bytes(100 * 1024 * 1024)
	flagS.VarP(&s.cc.MaxCacheSize, "service-cache-max-size", "", "maximum service cache size (e.g., 100M)")
	flagS.DurationVar(&s.cc.CleanupInterval, "service-cache-cleanup-interval", 30*time.Second, "service cache cleanup interval")
	flagS.DurationVar(&s.cc.IdleTimeout, "service-cache-idle-timeout", 2*time.Minute, "service cache entry idle timeout")
	return flagS
}

func (s *standalone) Validate() error {
	if s.root == "" {
		return errEmptyRootPath
	}

	// Validate retention configuration
	if s.retentionConfig.HighWatermark < 0 || s.retentionConfig.HighWatermark > 100 {
		return errors.New("measure-retention-high-watermark must be between 0 and 100")
	}
	if s.retentionConfig.LowWatermark < 0 || s.retentionConfig.LowWatermark > 100 {
		return errors.New("measure-retention-low-watermark must be between 0 and 100")
	}
	if s.retentionConfig.LowWatermark >= s.retentionConfig.HighWatermark {
		return errors.New("measure-retention-low-watermark must be less than measure-retention-high-watermark")
	}
	if s.retentionConfig.CheckInterval <= 0 {
		return errors.New("measure-retention-check-interval must be greater than 0")
	}
	if s.retentionConfig.Cooldown <= 0 {
		return errors.New("measure-retention-cooldown must be greater than 0")
	}

	if s.cc.MaxCacheSize < 0 {
		return errors.New("service-cache-max-size must be greater than or equal to 0")
	}
	if s.cc.CleanupInterval <= 0 {
		return errors.New("service-cache-cleanup-interval must be greater than 0")
	}
	if s.cc.IdleTimeout <= 0 {
		return errors.New("service-cache-idle-timeout must be greater than 0")
	}
	return nil
}

func (s *standalone) Name() string {
	return serviceName
}

func (s *standalone) Role() databasev1.Role {
	return databasev1.Role_ROLE_DATA
}

func (s *standalone) PreRun(ctx context.Context) error {
	s.l = logger.GetLogger(s.Name())
	s.l.Info().Msg("memory protector is initialized in PreRun")
	s.lfs = fs.NewLocalFileSystemWithLoggerAndLimit(s.l, s.pm.GetLimit())
	path := path.Join(s.root, s.Name())
	s.snapshotDir = filepath.Join(path, storage.SnapshotsDir)
	observability.UpdatePath(path)
	if s.dataPath == "" {
		s.dataPath = filepath.Join(path, storage.DataDir)
	}
	if !strings.HasPrefix(filepath.VolumeName(s.dataPath), filepath.VolumeName(path)) {
		observability.UpdatePath(s.dataPath)
	}
	s.localPipeline = queue.Local()
	val := ctx.Value(common.ContextNodeKey)
	if val == nil {
		return errors.New("node id is empty")
	}
	if s.cc.MaxCacheSize == 0 {
		s.c = storage.NewBypassCache()
	} else {
		s.c = storage.NewServiceCacheWithConfig(s.cc)
	}
	node := val.(common.Node)
	s.schemaRepo = newSchemaRepo(s.dataPath, s, node.Labels, node.NodeID)

	s.cm = newCacheMetrics(s.omr)
	observability.MetricsCollector.Register("measure_cache", s.collectCacheMetrics)

	if s.pipeline == nil {
		return nil
	}

	if err := s.createNativeObservabilityGroup(ctx); err != nil {
		return err
	}

	if err := s.pipeline.Subscribe(data.TopicSnapshot, &snapshotListener{s: s}); err != nil {
		return err
	}

	// Initialize disk monitor for forced retention
	s.diskMonitor = storage.NewDiskMonitor(s, s.retentionConfig, s.omr)
	s.diskMonitor.Start(ctx)

	// For now, keep the original write throttling behavior based on high watermark
	writeListener := setUpWriteCallback(s.l, s.schemaRepo, int(s.retentionConfig.HighWatermark))
	// only subscribe metricPipeline for data node
	if s.metricPipeline != nil {
		err := s.metricPipeline.Subscribe(data.TopicMeasureWrite, writeListener)
		if err != nil {
			return err
		}
	}
	err := s.pipeline.Subscribe(data.TopicMeasureWrite, writeListener)
	if err != nil {
		return err
	}
	// Register chunked sync handler for measure data.
	s.pipeline.RegisterChunkedSyncHandler(data.TopicMeasurePartSync, setUpChunkedSyncCallback(s.l, s.schemaRepo))
	return s.localPipeline.Subscribe(data.TopicMeasureWrite, writeListener)
}

func (s *standalone) Serve() run.StopNotify {
	return s.schemaRepo.StopCh()
}

func (s *standalone) GracefulStop() {
	// Stop disk monitor
	if s.diskMonitor != nil {
		s.diskMonitor.Stop()
	}

	observability.MetricsCollector.Unregister("measure_cache")
	s.schemaRepo.Close()
	s.c.Close()
	if s.localPipeline != nil {
		s.localPipeline.GracefulStop()
	}
}

func (s *standalone) collectCacheMetrics() {
	if s.cm == nil || s.c == nil {
		return
	}

	requests := s.c.Requests()
	misses := s.c.Misses()
	length := s.c.Entries()
	size := s.c.Size()
	var hitRatio float64
	if requests > 0 {
		hitRatio = float64(requests-misses) / float64(requests)
	}

	s.cm.requests.Set(float64(requests))
	s.cm.misses.Set(float64(misses))
	s.cm.hitRatio.Set(hitRatio)
	s.cm.entries.Set(float64(length))
	s.cm.size.Set(float64(size))
}

// NewStandalone returns a new standalone service.
func NewStandalone(metadata metadata.Repo, pipeline queue.Server, metricPipeline queue.Server, omr observability.MetricsRegistry, pm protector.Memory) (Service, error) {
	return &standalone{
		metadata:       metadata,
		pipeline:       pipeline,
		metricPipeline: metricPipeline,
		omr:            omr,
		pm:             pm,
	}, nil
}
