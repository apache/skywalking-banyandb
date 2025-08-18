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

	"github.com/pkg/errors"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
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

// StandaloneService returns a new standalone service.
func StandaloneService(_ context.Context) (Service, error) {
	return &standalone{}, nil
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

func (s *standalone) PreRun(_ context.Context) error {
	s.l = logger.GetLogger("trace")

	// Initialize metadata
	if s.metadata == nil {
		return errors.New("metadata repo is required")
	}

	// Initialize filesystem
	if s.lfs == nil {
		s.lfs = fs.NewLocalFileSystem()
	}

	// Initialize protector
	if s.pm == nil {
		return errors.New("memory protector is required")
	}

	// Initialize pipeline
	if s.pipeline == nil {
		return errors.New("pipeline is required")
	}

	// Set up data path
	if s.dataPath == "" {
		s.dataPath = path.Join(s.root, "trace-data")
	}

	// Initialize schema repository
	var nodeLabels map[string]string
	s.schemaRepo = newSchemaRepo(s.dataPath, s, nodeLabels)

	// Initialize snapshot directory
	s.snapshotDir = filepath.Join(s.dataPath, "snapshot")

	s.l.Info().
		Str("root", s.root).
		Str("dataPath", s.dataPath).
		Str("snapshotDir", s.snapshotDir).
		Msg("trace standalone service initialized")

	return nil
}

func (s *standalone) Serve() run.StopNotify {
	// As specified in the plan, no pipeline listeners should be implemented
	s.l.Info().Msg("trace standalone service started")

	// Return a channel that never closes since this service runs indefinitely
	stopCh := make(chan struct{})
	return stopCh
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

// SetMetadata sets the metadata repository.
func (s *standalone) SetMetadata(metadata metadata.Repo) {
	s.metadata = metadata
}

// SetObservabilityRegistry sets the observability metrics registry.
func (s *standalone) SetObservabilityRegistry(omr observability.MetricsRegistry) {
	s.omr = omr
}

// SetProtector sets the memory protector.
func (s *standalone) SetProtector(pm protector.Memory) {
	s.pm = pm
}

// SetPipeline sets the pipeline server.
func (s *standalone) SetPipeline(pipeline queue.Server) {
	s.pipeline = pipeline
}

// SetLocalPipeline sets the local pipeline queue.
func (s *standalone) SetLocalPipeline(localPipeline queue.Queue) {
	s.localPipeline = localPipeline
}

// SetFileSystem sets the file system.
func (s *standalone) SetFileSystem(lfs fs.FileSystem) {
	s.lfs = lfs
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
