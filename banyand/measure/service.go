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

	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
	resourceSchema "github.com/apache/skywalking-banyandb/pkg/schema"
)

const dataDir = "data"

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

var _ Service = (*service)(nil)

type service struct {
	writeListener       bus.MessageListener
	lfs                 fs.FileSystem
	pipeline            queue.Server
	localPipeline       queue.Queue
	metricPipeline      queue.Server
	omr                 observability.MetricsRegistry
	metadata            metadata.Repo
	pm                  *protector.Memory
	schemaRepo          *schemaRepo
	l                   *logger.Logger
	root                string
	snapshotDir         string
	option              option
	maxDiskUsagePercent int
	maxFileSnapshotNum  int
}

func (s *service) Measure(metadata *commonv1.Metadata) (Measure, error) {
	sm, ok := s.schemaRepo.loadMeasure(metadata)
	if !ok {
		return nil, errors.WithStack(ErrMeasureNotExist)
	}
	return sm, nil
}

func (s *service) LoadGroup(name string) (resourceSchema.Group, bool) {
	return s.schemaRepo.LoadGroup(name)
}

func (s *service) FlagSet() *run.FlagSet {
	flagS := run.NewFlagSet("storage")
	flagS.StringVar(&s.root, "measure-root-path", "/tmp", "the root path of database")
	flagS.DurationVar(&s.option.flushTimeout, "measure-flush-timeout", defaultFlushTimeout, "the memory data timeout of measure")
	s.option.mergePolicy = newDefaultMergePolicy()
	flagS.VarP(&s.option.mergePolicy.maxFanOutSize, "measure-max-fan-out-size", "", "the upper bound of a single file size after merge of measure")
	s.option.seriesCacheMaxSize = run.Bytes(32 << 20)
	flagS.VarP(&s.option.seriesCacheMaxSize, "measure-series-cache-max-size", "", "the max size of series cache in each group")
	flagS.IntVar(&s.maxDiskUsagePercent, "measure-max-disk-usage-percent", 95, "the maximum disk usage percentage allowed")
	flagS.IntVar(&s.maxFileSnapshotNum, "measure-max-file-snapshot-num", 10, "the maximum number of file snapshots allowed")
	return flagS
}

func (s *service) Validate() error {
	if s.root == "" {
		return errEmptyRootPath
	}
	if s.maxDiskUsagePercent < 0 {
		return errors.New("measure-max-disk-usage-percen must be greater than or equal to 0")
	}
	if s.maxDiskUsagePercent > 100 {
		return errors.New("measure-max-disk-usage-percen must be less than or equal to 100")
	}
	return nil
}

func (s *service) Name() string {
	return "measure"
}

func (s *service) Role() databasev1.Role {
	return databasev1.Role_ROLE_DATA
}

func (s *service) PreRun(ctx context.Context) error {
	s.l = logger.GetLogger(s.Name())
	s.lfs = fs.NewLocalFileSystemWithLogger(s.l)
	path := path.Join(s.root, s.Name())
	s.snapshotDir = filepath.Join(path, snapshotsDir)
	observability.UpdatePath(path)
	s.localPipeline = queue.Local()
	s.schemaRepo = newSchemaRepo(filepath.Join(path, dataDir), s)

	if err := s.createNativeObservabilityGroup(ctx); err != nil {
		return err
	}

	if err := s.pipeline.Subscribe(data.TopicSnapshot, &snapshotListener{s: s}); err != nil {
		return err
	}

	s.writeListener = setUpWriteCallback(s.l, s.schemaRepo, s.maxDiskUsagePercent)
	// only subscribe metricPipeline for data node
	if s.metricPipeline != nil {
		err := s.metricPipeline.Subscribe(data.TopicMeasureWrite, s.writeListener)
		if err != nil {
			return err
		}
	}
	err := s.pipeline.Subscribe(data.TopicMeasureWrite, s.writeListener)
	if err != nil {
		return err
	}
	return s.localPipeline.Subscribe(data.TopicMeasureWrite, s.writeListener)
}

func (s *service) Serve() run.StopNotify {
	return s.schemaRepo.StopCh()
}

func (s *service) GracefulStop() {
	s.localPipeline.GracefulStop()
	s.schemaRepo.Close()
}

// NewService returns a new service.
func NewService(metadata metadata.Repo, pipeline queue.Server, metricPipeline queue.Server, omr observability.MetricsRegistry, pm *protector.Memory) (Service, error) {
	return &service{
		metadata:       metadata,
		pipeline:       pipeline,
		metricPipeline: metricPipeline,
		omr:            omr,
		pm:             pm,
	}, nil
}
