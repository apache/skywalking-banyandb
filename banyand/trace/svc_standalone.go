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

func (s *standalone) FlagSet() *run.FlagSet {
	fs := run.NewFlagSet("trace")
	fs.StringVar(&s.root, "trace-root-path", "/tmp", "the root path for trace data")
	fs.StringVar(&s.dataPath, "trace-data-path", "", "the path for trace data (optional)")
	fs.DurationVar(&s.option.flushTimeout, "trace-flush-timeout", defaultFlushTimeout, "the timeout for trace data flush")
	fs.IntVar(&s.maxDiskUsagePercent, "trace-max-disk-usage-percent", 95, "the maximum disk usage percentage")
	fs.IntVar(&s.maxFileSnapshotNum, "trace-max-file-snapshot-num", 2, "the maximum number of file snapshots")
	// Additional flags can be added here
	return fs
}

func (s *standalone) Validate() error {
	if s.root == "" {
		return errEmptyRootPath
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

	// Set up write callback handler
	if s.pipeline != nil {
		writeListener := setUpWriteCallback(s.l, &s.schemaRepo, s.maxDiskUsagePercent)
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

// NewService returns a new service.
func NewService(metadata metadata.Repo, pipeline queue.Server, omr observability.MetricsRegistry, pm protector.Memory) (Service, error) {
	return &standalone{
		metadata: metadata,
		pipeline: pipeline,
		omr:      omr,
		pm:       pm,
	}, nil
}
