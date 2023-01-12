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
	"time"

	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/banyand/discovery"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
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
	schemaRepo    schemaRepo
	metadata      metadata.Repo
	pipeline      queue.Queue
	repo          discovery.ServiceRepo
	writeListener *writeCallback
	l             *logger.Logger
	stopCh        chan struct{}
	root          string
	dbOpts        tsdb.DatabaseOpts
}

func (s *service) Stream(metadata *commonv1.Metadata) (Stream, error) {
	sm, ok := s.schemaRepo.loadStream(metadata)
	if !ok {
		return nil, errors.WithStack(errStreamNotExist)
	}
	return sm, nil
}

func (s *service) FlagSet() *run.FlagSet {
	flagS := run.NewFlagSet("storage")
	flagS.StringVar(&s.root, "stream-root-path", "/tmp", "the root path of database")
	flagS.Int64Var(&s.dbOpts.BlockMemSize, "stream-block-mem-size", 8<<20, "block memory size")
	flagS.Int64Var(&s.dbOpts.SeriesMemSize, "stream-seriesmeta-mem-size", 1<<20, "series metadata memory size")
	flagS.Int64Var(&s.dbOpts.GlobalIndexMemSize, "stream-global-index-mem-size", 2<<20, "global index memory size")
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

func (s *service) PreRun() error {
	s.l = logger.GetLogger(s.Name())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	groups, err := s.metadata.GroupRegistry().ListGroup(ctx)
	cancel()
	if err != nil {
		return err
	}
	s.schemaRepo = newSchemaRepo(path.Join(s.root, s.Name()), s.metadata, s.repo, s.dbOpts, s.l)
	for _, g := range groups {
		if g.Catalog != commonv1.Catalog_CATALOG_STREAM {
			continue
		}
		gp, err := s.schemaRepo.StoreGroup(g.Metadata)
		if err != nil {
			return err
		}
		ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
		schemas, err := s.metadata.StreamRegistry().ListStream(ctx, schema.ListOpt{Group: gp.GetSchema().GetMetadata().Name})
		cancel()
		if err != nil {
			return err
		}
		for _, sa := range schemas {
			if _, innerErr := gp.StoreResource(sa); innerErr != nil {
				return innerErr
			}
		}
	}

	s.writeListener = setUpWriteCallback(s.l, &s.schemaRepo)

	errWrite := s.pipeline.Subscribe(data.TopicStreamWrite, s.writeListener)
	if errWrite != nil {
		return errWrite
	}
	return nil
}

func (s *service) Serve() run.StopNotify {
	_ = s.schemaRepo.NotifyAll()
	// run a serial watcher
	s.schemaRepo.Watcher()

	s.metadata.StreamRegistry().RegisterHandler(schema.KindGroup|schema.KindStream|schema.KindIndexRuleBinding|schema.KindIndexRule,
		&s.schemaRepo)
	return s.stopCh
}

func (s *service) GracefulStop() {
	s.schemaRepo.Close()
	if s.stopCh != nil {
		close(s.stopCh)
	}
}

// NewService returns a new service.
func NewService(_ context.Context, metadata metadata.Repo, repo discovery.ServiceRepo, pipeline queue.Queue) (Service, error) {
	return &service{
		metadata: metadata,
		repo:     repo,
		pipeline: pipeline,
		dbOpts: tsdb.DatabaseOpts{
			EnableGlobalIndex: true,
		},
		stopCh: make(chan struct{}),
	}, nil
}
