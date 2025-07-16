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

	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/liaison/grpc"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/node"
	"github.com/apache/skywalking-banyandb/pkg/run"
	resourceSchema "github.com/apache/skywalking-banyandb/pkg/schema"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

type liaison struct {
	pm                  protector.Memory
	metadata            metadata.Repo
	pipeline            queue.Server
	omr                 observability.MetricsRegistry
	lfs                 fs.FileSystem
	dataNodeSelector    node.Selector
	l                   *logger.Logger
	schemaRepo          *schemaRepo
	dataPath            string
	root                string
	option              option
	maxDiskUsagePercent int
}

func (s *liaison) Measure(metadata *commonv1.Metadata) (Measure, error) {
	sm, ok := s.schemaRepo.loadMeasure(metadata)
	if !ok {
		return nil, errors.WithStack(ErrMeasureNotExist)
	}
	return sm, nil
}

func (s *liaison) LoadGroup(name string) (resourceSchema.Group, bool) {
	return s.schemaRepo.LoadGroup(name)
}

func (s *liaison) GetRemovalSegmentsTimeRange(group string) *timestamp.TimeRange {
	return s.schemaRepo.GetRemovalSegmentsTimeRange(group)
}

func (s *liaison) FlagSet() *run.FlagSet {
	flagS := run.NewFlagSet("storage")
	flagS.StringVar(&s.root, "measure-root-path", "/tmp", "the root path of measure")
	flagS.StringVar(&s.dataPath, "measure-data-path", "", "the data directory path of measure. If not set, <measure-root-path>/measure/data will be used")
	flagS.DurationVar(&s.option.flushTimeout, "measure-flush-timeout", defaultFlushTimeout, "the memory data timeout of measure")
	flagS.IntVar(&s.maxDiskUsagePercent, "measure-max-disk-usage-percent", 95, "the maximum disk usage percentage allowed")
	return flagS
}

func (s *liaison) Validate() error {
	if s.root == "" {
		return errEmptyRootPath
	}
	if s.maxDiskUsagePercent < 0 {
		return errors.New("measure-max-disk-usage-percent must be greater than or equal to 0")
	}
	if s.maxDiskUsagePercent > 100 {
		return errors.New("measure-max-disk-usage-percent must be less than or equal to 100")
	}
	return nil
}

func (s *liaison) Name() string {
	return serviceName
}

func (s *liaison) Role() databasev1.Role {
	return databasev1.Role_ROLE_LIAISON
}

func (s *liaison) PreRun(ctx context.Context) error {
	s.l = logger.GetLogger(s.Name())
	s.l.Info().Msg("memory protector is initialized in PreRun")
	s.lfs = fs.NewLocalFileSystemWithLoggerAndLimit(s.l, s.pm.GetLimit())
	path := path.Join(s.root, s.Name())
	observability.UpdatePath(path)
	val := ctx.Value(common.ContextNodeKey)
	if val == nil {
		return errors.New("node id is empty")
	}
	if s.dataPath == "" {
		s.dataPath = filepath.Join(path, storage.DataDir)
	}
	if !strings.HasPrefix(filepath.VolumeName(s.dataPath), filepath.VolumeName(path)) {
		observability.UpdatePath(s.dataPath)
	}
	topNResultPipeline := queue.Local()
	measureDataNodeRegistry := grpc.NewClusterNodeRegistry(data.TopicMeasurePartSync, s.option.tire2Client, s.dataNodeSelector)
	s.schemaRepo = newLiaisonSchemaRepo(s.dataPath, s, measureDataNodeRegistry, topNResultPipeline)
	writeListener := setUpWriteQueueCallback(s.l, s.schemaRepo, s.maxDiskUsagePercent, s.option.tire2Client)
	if err := s.pipeline.Subscribe(data.TopicMeasureWrite, writeListener); err != nil {
		return err
	}
	return topNResultPipeline.Subscribe(data.TopicMeasureWrite, writeListener)
}

func (s *liaison) Serve() run.StopNotify {
	return s.schemaRepo.StopCh()
}

func (s *liaison) GracefulStop() {
	s.schemaRepo.Close()
}

func (s *liaison) InFlow(stm *databasev1.Measure, seriesID uint64, shardID uint32, entityValues []*modelv1.TagValue, dp *measurev1.DataPointValue) {
	if s.schemaRepo == nil {
		s.l.Error().Msg("schema repository is not initialized")
		return
	}
	s.schemaRepo.inFlow(stm, seriesID, shardID, entityValues, dp)
}

// NewLiaison creates a new measure liaison service with the given dependencies.
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
