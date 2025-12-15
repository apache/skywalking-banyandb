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
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/multierr"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/bus"
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
	s.snapshotDir = filepath.Join(path, "snapshots")

	// Initialize disk monitor for forced retention
	s.diskMonitor = storage.NewDiskMonitor(s, s.retentionConfig, s.omr)
	s.diskMonitor.Start()

	// Set up write callback handler. For now, keep the original write throttling behavior based on high watermark
	writeListener := setUpWriteCallback(s.l, &s.schemaRepo, int(s.retentionConfig.HighWatermark))
	err := s.pipeline.Subscribe(data.TopicTraceWrite, writeListener)
	if err != nil {
		return err
	}
	err = s.pipeline.Subscribe(data.TopicSnapshot, &standaloneSnapshotListener{s: s})
	if err != nil {
		return err
	}
	err = s.pipeline.Subscribe(data.TopicDeleteExpiredTraceSegments, &standaloneDeleteTraceSegmentsListener{s: s})
	if err != nil {
		return err
	}
	s.pipeline.RegisterChunkedSyncHandler(data.TopicTracePartSync, setUpChunkedSyncCallback(s.l, &s.schemaRepo))
	s.pipeline.RegisterChunkedSyncHandler(data.TopicTraceSeriesSync, setUpSeriesSyncCallback(s.l, &s.schemaRepo))
	err = s.pipeline.Subscribe(data.TopicTraceSidxSeriesWrite, setUpSidxSeriesIndexCallback(s.l, &s.schemaRepo))
	if err != nil {
		return err
	}

	s.localPipeline = queue.Local()
	err = s.localPipeline.Subscribe(data.TopicTraceWrite, writeListener)
	if err != nil {
		return err
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

func (s *standalone) takeGroupSnapshot(dstDir string, groupName string) error {
	group, ok := s.schemaRepo.LoadGroup(groupName)
	if !ok {
		return errors.Errorf("group %s not found", groupName)
	}
	db := group.SupplyTSDB()
	if db == nil {
		return errors.Errorf("group %s has no tsdb", group.GetSchema().Metadata.Name)
	}
	tsdb := db.(storage.TSDB[*tsTable, option])
	if err := tsdb.TakeFileSnapshot(dstDir); err != nil {
		return errors.WithMessagef(err, "snapshot %s fail to take file snapshot for group %s", dstDir, group.GetSchema().Metadata.Name)
	}
	return nil
}

// collectSegDirs walks a directory tree and collects all seg-* directory paths.
// It only collects directories matching the "seg-*" pattern and ignores all files.
// Returns a map of relative paths to seg-* directories.
func collectSegDirs(rootDir string) (map[string]bool, error) {
	segDirs := make(map[string]bool)

	walkErr := filepath.Walk(rootDir, func(currentPath string, info os.FileInfo, err error) error {
		if err != nil {
			// If we can't read a directory, log but continue
			return nil
		}

		// Skip files, only process directories
		if !info.IsDir() {
			return nil
		}

		// Get the directory name
		dirName := filepath.Base(currentPath)

		// Check if this is a seg-* directory
		if strings.HasPrefix(dirName, "seg-") {
			// Get relative path from root
			relPath, relErr := filepath.Rel(rootDir, currentPath)
			if relErr != nil {
				return fmt.Errorf("failed to get relative path for %s: %w", currentPath, relErr)
			}

			// Add to our collection
			segDirs[relPath] = true

			// Don't recurse into seg-* directories
			return filepath.SkipDir
		}

		return nil
	})

	if walkErr != nil {
		return nil, fmt.Errorf("failed to walk directory %s: %w", rootDir, walkErr)
	}

	return segDirs, nil
}

// compareSegDirs compares two sets of seg-* directories and returns three slices:
// matched (in both), onlyInSnapshot (only in snapshot), onlyInData (only in data).
func compareSegDirs(snapshotDirs, dataDirs map[string]bool) (matched, onlyInSnapshot, onlyInData []string) {
	// Find directories in both sets (matched)
	for dir := range snapshotDirs {
		if dataDirs[dir] {
			matched = append(matched, dir)
		} else {
			onlyInSnapshot = append(onlyInSnapshot, dir)
		}
	}

	// Find directories only in data
	for dir := range dataDirs {
		if !snapshotDirs[dir] {
			onlyInData = append(onlyInData, dir)
		}
	}

	// Sort for consistent output
	sort.Strings(matched)
	sort.Strings(onlyInSnapshot)
	sort.Strings(onlyInData)

	return matched, onlyInSnapshot, onlyInData
}

// compareSnapshotWithData compares the snapshot directory with the data directory
// to verify that seg-* directories are consistent. Only logs differences, does not return errors.
func (d *standaloneSnapshotListener) compareSnapshotWithData(snapshotDir, dataDir, groupName string) {
	// Collect seg-* directories from snapshot
	snapshotDirs, snapshotErr := collectSegDirs(snapshotDir)
	if snapshotErr != nil {
		d.s.l.Warn().Err(snapshotErr).
			Str("group", groupName).
			Str("snapshotDir", snapshotDir).
			Msg("failed to collect seg-* directories from snapshot, skipping comparison")
		return
	}

	// Collect seg-* directories from data
	dataDirs, dataErr := collectSegDirs(dataDir)
	if dataErr != nil {
		d.s.l.Warn().Err(dataErr).
			Str("group", groupName).
			Str("dataDir", dataDir).
			Msg("failed to collect seg-* directories from data, skipping comparison")
		return
	}

	// Compare the directories
	matched, onlyInSnapshot, onlyInData := compareSegDirs(snapshotDirs, dataDirs)

	// Log consolidated comparison results at Info level
	d.s.l.Info().
		Str("group", groupName).
		Int("matched", len(matched)).
		Int("onlyInSnapshot", len(onlyInSnapshot)).
		Int("onlyInData", len(onlyInData)).
		Strs("matchedDirs", matched).
		Strs("onlyInSnapshotDirs", onlyInSnapshot).
		Strs("onlyInDataDirs", onlyInData).
		Msgf("snapshot comparison for group %s, data dir: %s, snapshot dir: %s",
			groupName, dataDir, snapshotDir)
}

type standaloneSnapshotListener struct {
	*bus.UnImplementedHealthyListener
	s           *standalone
	snapshotSeq uint64
	snapshotMux sync.Mutex
}

func (d *standaloneSnapshotListener) Rev(ctx context.Context, message bus.Message) bus.Message {
	groups := message.Data().([]*databasev1.SnapshotRequest_Group)
	var gg []resourceSchema.Group
	if len(groups) == 0 {
		gg = d.s.schemaRepo.LoadAllGroups()
	} else {
		for _, g := range groups {
			if g.Catalog != commonv1.Catalog_CATALOG_TRACE {
				continue
			}
			group, ok := d.s.schemaRepo.LoadGroup(g.Group)
			if !ok {
				continue
			}
			gg = append(gg, group)
		}
	}
	if len(gg) == 0 {
		return bus.NewMessage(bus.MessageID(time.Now().UnixNano()), nil)
	}
	d.snapshotMux.Lock()
	defer d.snapshotMux.Unlock()
	storage.DeleteStaleSnapshots(d.s.snapshotDir, d.s.maxFileSnapshotNum, d.s.lfs)
	sn := d.snapshotName()
	var err error
	for _, g := range gg {
		select {
		case <-ctx.Done():
			return bus.NewMessage(bus.MessageID(time.Now().UnixNano()), nil)
		default:
		}
		groupName := g.GetSchema().Metadata.Name
		snapshotPath := filepath.Join(d.s.snapshotDir, sn, groupName)
		if errGroup := d.s.takeGroupSnapshot(snapshotPath, groupName); errGroup != nil {
			d.s.l.Error().Err(errGroup).Str("group", groupName).Msg("fail to take group snapshot")
			err = multierr.Append(err, errGroup)
			continue
		}

		// Compare snapshot with data directory to verify consistency
		dataPath := filepath.Join(d.s.dataPath, groupName)
		d.compareSnapshotWithData(snapshotPath, dataPath, groupName)
	}
	snp := &databasev1.Snapshot{
		Name:    sn,
		Catalog: commonv1.Catalog_CATALOG_TRACE,
	}
	if err != nil {
		snp.Error = err.Error()
	}
	return bus.NewMessage(bus.MessageID(time.Now().UnixNano()), snp)
}

func (d *standaloneSnapshotListener) snapshotName() string {
	d.snapshotSeq++
	return fmt.Sprintf("%s-%08X", time.Now().UTC().Format("20060102150405"), d.snapshotSeq)
}

type standaloneDeleteTraceSegmentsListener struct {
	*bus.UnImplementedHealthyListener
	s *standalone
}

func (s *standaloneDeleteTraceSegmentsListener) Rev(_ context.Context, message bus.Message) bus.Message {
	req := message.Data().(*tracev1.DeleteExpiredSegmentsRequest)
	if req == nil {
		return bus.NewMessage(bus.MessageID(time.Now().UnixNano()), int64(0))
	}

	db, err := s.s.schemaRepo.loadTSDB(req.Group)
	if err != nil {
		s.s.l.Error().Err(err).Str("group", req.Group).Msg("fail to load tsdb")
		return bus.NewMessage(bus.MessageID(time.Now().UnixNano()), int64(0))
	}
	s.s.l.Info().Msg("test")
	deleted := db.DeleteExpiredSegments(req.SegmentSuffixes)
	return bus.NewMessage(bus.MessageID(time.Now().UnixNano()), deleted)
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
