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
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
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
	// ErrStreamNotExist denotes a stream doesn't exist in the metadata repo.
	ErrStreamNotExist = errors.New("stream doesn't exist")
)

// Service allows inspecting the stream elements.
type Service interface {
	run.PreRunner
	run.Config
	run.Service
	Query
}

var _ Service = (*standalone)(nil)

type standalone struct {
	pm                  protector.Memory
	pipeline            queue.Server
	localPipeline       queue.Queue
	omr                 observability.MetricsRegistry
	lfs                 fs.FileSystem
	metadata            metadata.Repo
	l                   *logger.Logger
	schemaRepo          schemaRepo
	snapshotDir         string
	root                string
	dataPath            string
	option              option
	maxDiskUsagePercent int
	maxFileSnapshotNum  int
}

func (s *standalone) Stream(metadata *commonv1.Metadata) (Stream, error) {
	sm, ok := s.schemaRepo.loadStream(metadata)
	if !ok {
		return nil, errors.WithStack(ErrStreamNotExist)
	}
	return sm, nil
}

func (s *standalone) LoadGroup(name string) (resourceSchema.Group, bool) {
	return s.schemaRepo.LoadGroup(name)
}

func (s *standalone) GetRemovalSegmentsTimeRange(group string) *timestamp.TimeRange {
	return s.schemaRepo.GetRemovalSegmentsTimeRange(group)
}

func (s *standalone) FlagSet() *run.FlagSet {
	flagS := run.NewFlagSet("storage")
	flagS.StringVar(&s.root, "stream-root-path", "/tmp", "the root path of stream")
	flagS.StringVar(&s.dataPath, "stream-data-path", "", "the data directory path of stream. If not set, <stream-root-path>/stream/data will be used")
	flagS.DurationVar(&s.option.flushTimeout, "stream-flush-timeout", defaultFlushTimeout, "the memory data timeout of stream")
	flagS.DurationVar(&s.option.elementIndexFlushTimeout, "element-index-flush-timeout", defaultFlushTimeout, "the elementIndex timeout of stream")
	s.option.mergePolicy = newDefaultMergePolicy()
	flagS.VarP(&s.option.mergePolicy.maxFanOutSize, "stream-max-fan-out-size", "", "the upper bound of a single file size after merge of stream")
	s.option.seriesCacheMaxSize = run.Bytes(32 << 20)
	flagS.VarP(&s.option.seriesCacheMaxSize, "stream-series-cache-max-size", "", "the max size of series cache in each group")
	flagS.IntVar(&s.maxDiskUsagePercent, "stream-max-disk-usage-percent", 95, "the maximum disk usage percentage allowed")
	flagS.IntVar(&s.maxFileSnapshotNum, "stream-max-file-snapshot-num", 2, "the maximum number of file snapshots allowed")
	return flagS
}

func (s *standalone) Validate() error {
	if s.root == "" {
		return errEmptyRootPath
	}
	if s.maxDiskUsagePercent < 0 {
		return errors.New("stream-max-disk-usage-percent must be greater than or equal to 0")
	}
	if s.maxDiskUsagePercent > 100 {
		return errors.New("stream-max-disk-usage-percent must be less than or equal to 100")
	}
	return nil
}

func (s *standalone) Name() string {
	return "stream"
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
	if s.pipeline == nil {
		return nil
	}

	s.localPipeline = queue.Local()
	if err := s.pipeline.Subscribe(data.TopicSnapshot, &snapshotListener{s: s}); err != nil {
		return err
	}
	if err := s.pipeline.Subscribe(data.TopicDeleteExpiredStreamSegments, &deleteStreamSegmentsListener{s: s}); err != nil {
		return err
	}
	writeListener := setUpWriteCallback(s.l, &s.schemaRepo, s.maxDiskUsagePercent)
	err := s.pipeline.Subscribe(data.TopicStreamWrite, writeListener)
	if err != nil {
		return err
	}
	s.pipeline.RegisterChunkedSyncHandler(data.TopicStreamPartSync, setUpChunkedSyncCallback(s.l, &s.schemaRepo))
	// Register chunked sync handler for stream series index
	s.pipeline.RegisterChunkedSyncHandler(data.TopicStreamSeriesSync, setUpSyncSeriesCallback(s.l, &s.schemaRepo))
	// Register chunked sync handler for stream element index
	s.pipeline.RegisterChunkedSyncHandler(data.TopicStreamElementIndexSync, setUpSyncElementIndexCallback(s.l, &s.schemaRepo))

	err = s.pipeline.Subscribe(data.TopicStreamSeriesIndexWrite, setUpSeriesIndexCallback(s.l, &s.schemaRepo))
	if err != nil {
		return err
	}
	err = s.pipeline.Subscribe(data.TopicStreamLocalIndexWrite, setUpLocalIndexCallback(s.l, &s.schemaRepo))
	if err != nil {
		return err
	}
	return s.localPipeline.Subscribe(data.TopicStreamWrite, writeListener)
}

func (s *standalone) Serve() run.StopNotify {
	return s.schemaRepo.StopCh()
}

func (s *standalone) GracefulStop() {
	s.schemaRepo.Close()
	if s.localPipeline != nil {
		s.localPipeline.GracefulStop()
	}
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

// NewReadonlyService returns a new readonly service.
func NewReadonlyService(metadata metadata.Repo, omr observability.MetricsRegistry, pm protector.Memory) (Service, error) {
	return &standalone{
		metadata: metadata,
		omr:      omr,
		pm:       pm,
	}, nil
}

type deleteStreamSegmentsListener struct {
	*bus.UnImplementedHealthyListener
	s *standalone
}

func (d *deleteStreamSegmentsListener) Rev(_ context.Context, message bus.Message) bus.Message {
	req := message.Data().(*streamv1.DeleteExpiredSegmentsRequest)
	if req == nil {
		return bus.NewMessage(bus.MessageID(time.Now().UnixNano()), int64(0))
	}

	db, err := d.s.schemaRepo.loadTSDB(req.Group)
	if err != nil {
		d.s.l.Error().Err(err).Str("group", req.Group).Msg("failed to load tsdb")
		return bus.NewMessage(bus.MessageID(time.Now().UnixNano()), int64(0))
	}
	deleted := db.DeleteExpiredSegments(timestamp.NewSectionTimeRange(req.TimeRange.Begin.AsTime(), req.TimeRange.End.AsTime()))
	return bus.NewMessage(bus.MessageID(time.Now().UnixNano()), deleted)
}
