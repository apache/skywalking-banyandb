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
	// ErrTraceNotExist denotes a trace doesn't exist in the metadata repo.
	ErrTraceNotExist = errors.New("trace doesn't exist")
)

var _ Service = (*standalone)(nil)

type standalone struct {
	pm                 protector.Memory
	pipeline           queue.Server
	localPipeline      queue.Queue
	omr                observability.MetricsRegistry
	lfs                fs.FileSystem
	metadata           metadata.Repo
	l                  *logger.Logger
	diskMonitor        *storage.DiskMonitor
	schemaRepo         schemaRepo
	snapshotDir        string
	root               string
	dataPath           string
	option             option
	retentionConfig    storage.RetentionConfig
	maxFileSnapshotNum int
}

func (s *standalone) FlagSet() *run.FlagSet {
	fs := run.NewFlagSet("trace")
	fs.StringVar(&s.root, "trace-root-path", "/tmp", "the root path for trace data")
	fs.StringVar(&s.dataPath, "trace-data-path", "", "the path for trace data (optional)")
	fs.DurationVar(&s.option.flushTimeout, "trace-flush-timeout", defaultFlushTimeout, "the timeout for trace data flush")

	// Retention configuration flags
	fs.Float64Var(&s.retentionConfig.HighWatermark, "trace-retention-high-watermark", 95.0, "disk usage high watermark percentage that triggers forced retention cleanup")
	fs.Float64Var(&s.retentionConfig.LowWatermark, "trace-retention-low-watermark", 85.0, "disk usage low watermark percentage where forced retention cleanup stops")
	fs.DurationVar(&s.retentionConfig.CheckInterval, "trace-retention-check-interval", 5*time.Minute, "interval for checking disk usage")
	fs.DurationVar(&s.retentionConfig.Cooldown, "trace-retention-cooldown", 30*time.Second, "cooldown period between forced segment deletions")
	fs.BoolVar(&s.retentionConfig.ForceCleanupEnabled, "trace-retention-force-cleanup-enabled", false,
		"enable forced retention cleanup when disk usage exceeds high watermark")

	fs.IntVar(&s.maxFileSnapshotNum, "trace-max-file-snapshot-num", 2, "the maximum number of file snapshots")
	s.option.mergePolicy = newDefaultMergePolicy()
	fs.VarP(&s.option.mergePolicy.maxFanOutSize, "trace-max-fan-out-size", "", "the upper bound of a single file size after merge of trace")
	// Additional flags can be added here
	return fs
}

func (s *standalone) Validate() error {
	if s.root == "" {
		return errEmptyRootPath
	}

	// Validate retention configuration
	if s.retentionConfig.HighWatermark < 0 || s.retentionConfig.HighWatermark > 100 {
		return errors.New("trace-retention-high-watermark must be between 0 and 100")
	}
	if s.retentionConfig.LowWatermark < 0 || s.retentionConfig.LowWatermark > 100 {
		return errors.New("trace-retention-low-watermark must be between 0 and 100")
	}
	if s.retentionConfig.LowWatermark > s.retentionConfig.HighWatermark {
		return errors.New("trace-retention-low-watermark must be less than trace-retention-high-watermark")
	}
	if s.retentionConfig.CheckInterval <= 0 && s.retentionConfig.ForceCleanupEnabled {
		return errors.New("trace-retention-check-interval must be greater than 0 when force cleanup is enabled")
	}
	if s.retentionConfig.Cooldown <= 0 {
		return errors.New("trace-retention-cooldown must be greater than 0")
	}

	return nil
}

func (s *standalone) Name() string {
	return "trace"
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
	val := ctx.Value(common.ContextNodeKey)
	if val == nil {
		return errors.New("node id is empty")
	}
	node := val.(common.Node)
	if s.dataPath == "" {
		s.dataPath = filepath.Join(path, storage.DataDir)
	}
	if !strings.HasPrefix(filepath.VolumeName(s.dataPath), filepath.VolumeName(path)) {
		observability.UpdatePath(s.dataPath)
	}
	s.schemaRepo = newSchemaRepo(s.dataPath, s, node.Labels)

	// Initialize snapshot directory
	s.snapshotDir = filepath.Join(s.dataPath, "snapshot")

	// Initialize disk monitor for forced retention
	s.diskMonitor = storage.NewDiskMonitor(s, s.retentionConfig, s.omr)
	s.diskMonitor.Start()

	// Set up write callback handler
	if s.pipeline != nil {
		// For now, keep the original write throttling behavior based on high watermark
		writeListener := setUpWriteCallback(s.l, &s.schemaRepo, int(s.retentionConfig.HighWatermark))
		err := s.pipeline.Subscribe(data.TopicTraceWrite, writeListener)
		if err != nil {
			return err
		}
	}

	s.l.Info().
		Str("root", s.root).
		Str("dataPath", s.dataPath).
		Str("snapshotDir", s.snapshotDir).
		Msg("trace standalone service initialized")

	return nil
}

func (s *standalone) Serve() run.StopNotify {
	return s.schemaRepo.StopCh()
}

func (s *standalone) GracefulStop() {
	// Stop disk monitor
	if s.diskMonitor != nil {
		s.diskMonitor.Stop()
	}

	if s.schemaRepo.Repository != nil {
		s.schemaRepo.Repository.Close()
	}
	s.l.Info().Msg("trace standalone service stopped")
}

func (s *standalone) LoadGroup(name string) (resourceSchema.Group, bool) {
	return s.schemaRepo.LoadGroup(name)
}

func (s *standalone) Trace(metadata *commonv1.Metadata) (Trace, error) {
	sm, ok := s.schemaRepo.Trace(metadata)
	if !ok {
		return nil, ErrTraceNotFound
	}
	return sm, nil
}

func (s *standalone) GetRemovalSegmentsTimeRange(group string) *timestamp.TimeRange {
	return s.schemaRepo.GetRemovalSegmentsTimeRange(group)
}

// RetentionService interface implementation.

func (s *standalone) GetDataPath() string {
	return s.dataPath
}

func (s *standalone) GetSnapshotDir() string {
	return s.snapshotDir
}

func (s *standalone) LoadAllGroups() []resourceSchema.Group {
	return s.schemaRepo.LoadAllGroups()
}

func (s *standalone) PeekOldestSegmentEndTimeInGroup(group string) (time.Time, bool) {
	g, ok := s.schemaRepo.LoadGroup(group)
	if !ok {
		return time.Time{}, false
	}

	db := g.SupplyTSDB()
	if db == nil {
		return time.Time{}, false
	}

	// Type assert to the storage interface that has PeekOldestSegmentEndTime
	if dbWithPeek, ok := db.(interface{ PeekOldestSegmentEndTime() (time.Time, bool) }); ok {
		return dbWithPeek.PeekOldestSegmentEndTime()
	}

	return time.Time{}, false
}

func (s *standalone) DeleteOldestSegmentInGroup(group string) (bool, error) {
	g, ok := s.schemaRepo.LoadGroup(group)
	if !ok {
		return false, nil
	}

	db := g.SupplyTSDB()
	if db == nil {
		return false, nil
	}

	// Type assert to the storage interface that has DeleteOldestSegment
	if dbWithDelete, ok := db.(interface{ DeleteOldestSegment() (bool, error) }); ok {
		return dbWithDelete.DeleteOldestSegment()
	}

	s.l.Debug().Str("group", group).Msg("database does not support DeleteOldestSegment")
	return false, nil
}

func (s *standalone) CleanupOldSnapshots(maxAge time.Duration) error {
	if s.snapshotDir == "" {
		return nil
	}
	// Use age-based cleanup during forced retention cleanup
	storage.DeleteOldSnapshots(s.snapshotDir, maxAge, s.lfs)
	return nil
}

func (s *standalone) GetServiceName() string {
	return s.Name()
}

// NewService returns a new service.
func NewService(metadata metadata.Repo, pipeline queue.Server, omr observability.MetricsRegistry, pm protector.Memory) (Service, error) {
	return &standalone{
		metadata: metadata,
		pipeline: pipeline,
		omr:      omr,
		pm:       pm,
	}, nil
}
