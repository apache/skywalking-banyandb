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

	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

var (
	errEmptyRootPath  = errors.New("root path is empty")
	errStreamNotExist = errors.New("stream doesn't exist")
)

// Service allows inspecting the stream elements.
type Service interface {
	run.PreRunner
	run.Config
	run.Service
	Query
}

var _ Service = (*service)(nil)

type service struct {
	schemaRepo      schemaRepo
	metadata        metadata.Repo
	pipeline        queue.Server
	writeListener   *writeCallback
	l               *logger.Logger
	root            string
	dbOpts          tsdb.DatabaseOpts
	blockBufferSize run.Bytes
}

func (s *service) Stream(metadata *commonv1.Metadata) (Stream, error) {
	return s.schemaRepo.Stream(metadata)
}

func (s *service) FlagSet() *run.FlagSet {
	flagS := run.NewFlagSet("storage")
	s.blockBufferSize = 8 << 20
	s.dbOpts.SeriesMemSize = 1 << 20
	s.dbOpts.GlobalIndexMemSize = 2 << 20
	flagS.StringVar(&s.root, "stream-root-path", "/tmp", "the root path of database")
	flagS.Var(&s.blockBufferSize, "stream-block-buffer-size", "block buffer size")
	flagS.Var(&s.dbOpts.SeriesMemSize, "stream-seriesmeta-mem-size", "series metadata memory size")
	flagS.Var(&s.dbOpts.GlobalIndexMemSize, "stream-global-index-mem-size", "global index memory size")
	flagS.Int64Var(&s.dbOpts.BlockInvertedIndex.BatchWaitSec, "stream-idx-batch-wait-sec", 1, "index batch wait in second")
	return flagS
}

func (s *service) Validate() error {
	if s.root == "" {
		return errEmptyRootPath
	}
	return nil
}

func (s *service) Name() string {
	return "stream"
}

func (s *service) Role() databasev1.Role {
	return databasev1.Role_ROLE_DATA
}

func (s *service) PreRun(_ context.Context) error {
	s.l = logger.GetLogger(s.Name())
	path := path.Join(s.root, s.Name())
	observability.UpdatePath(path)
	s.schemaRepo = newSchemaRepo(path, s.metadata, int64(s.blockBufferSize), s.dbOpts, s.l)
	s.writeListener = setUpWriteCallback(s.l, &s.schemaRepo)

	errWrite := s.pipeline.Subscribe(data.TopicStreamWrite, s.writeListener)
	if errWrite != nil {
		return errWrite
	}
	return nil
}

func (s *service) Serve() run.StopNotify {
	return s.schemaRepo.StopCh()
}

func (s *service) GracefulStop() {
	s.schemaRepo.Close()
}

// NewService returns a new service.
func NewService(_ context.Context, metadata metadata.Repo, pipeline queue.Server) (Service, error) {
	return &service{
		metadata: metadata,
		pipeline: pipeline,
		dbOpts: tsdb.DatabaseOpts{
			EnableGlobalIndex: true,
			IndexGranularity:  tsdb.IndexGranularityBlock,
		},
	}, nil
}
