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

	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
	resourceSchema "github.com/apache/skywalking-banyandb/pkg/schema"
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

var _ Service = (*service)(nil)

type service struct {
	schemaRepo             schemaRepo
	writeListener          bus.MessageListener
	metadata               metadata.Repo
	pipeline               queue.Server
	localPipeline          queue.Queue
	l                      *logger.Logger
	root                   string
	dbOpts                 tsdb.DatabaseOpts
	BlockEncoderBufferSize run.Bytes
	BlockBufferSize        run.Bytes
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
	s.BlockEncoderBufferSize = 12 << 20
	s.BlockBufferSize = 4 << 20
	s.dbOpts.SeriesMemSize = 1 << 20
	flagS.Var(&s.BlockEncoderBufferSize, "measure-encoder-buffer-size", "block fields buffer size")
	flagS.Var(&s.BlockBufferSize, "measure-buffer-size", "block buffer size")
	flagS.Var(&s.dbOpts.SeriesMemSize, "measure-seriesmeta-mem-size", "series metadata memory size")
	flagS.Int64Var(&s.dbOpts.BlockInvertedIndex.BatchWaitSec, "measure-idx-batch-wait-sec", 1, "index batch wait in second")
	flagS.BoolVar(&s.dbOpts.EnableWAL, "measure-enable-wal", true, "enable write ahead log")
	return flagS
}

func (s *service) Validate() error {
	if s.root == "" {
		return errEmptyRootPath
	}
	return nil
}

func (s *service) Name() string {
	return "measure"
}

func (s *service) Role() databasev1.Role {
	return databasev1.Role_ROLE_DATA
}

func (s *service) PreRun(_ context.Context) error {
	s.l = logger.GetLogger(s.Name())
	path := path.Join(s.root, s.Name())
	observability.UpdatePath(path)
	s.localPipeline = queue.Local()
	s.schemaRepo = newSchemaRepo(path, s.metadata, s.dbOpts,
		s.l, s.localPipeline, int64(s.BlockEncoderBufferSize), int64(s.BlockBufferSize))
	// run a serial watcher

	s.writeListener = setUpWriteCallback(s.l, &s.schemaRepo)
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
func NewService(_ context.Context, metadata metadata.Repo, pipeline queue.Server) (Service, error) {
	return &service{
		metadata: metadata,
		pipeline: pipeline,
		dbOpts: tsdb.DatabaseOpts{
			IndexGranularity: tsdb.IndexGranularitySeries,
		},
	}, nil
}
