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
	"time"

	"github.com/pkg/errors"
	"go.uber.org/multierr"

	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/discovery"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
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
	pipeline               queue.Queue
	repo                   discovery.ServiceRepo
	l                      *logger.Logger
	stopCh                 chan struct{}
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

func (s *service) PreRun() error {
	s.l = logger.GetLogger(s.Name())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	groups, err := s.metadata.GroupRegistry().ListGroup(ctx)
	cancel()
	if err != nil {
		return err
	}
	path := path.Join(s.root, s.Name())
	observability.UpdatePath(path)
	s.schemaRepo = newSchemaRepo(path, s.metadata, s.repo, s.dbOpts,
		s.l, s.pipeline, int64(s.BlockEncoderBufferSize), int64(s.BlockBufferSize))
	for _, g := range groups {
		if g.Catalog != commonv1.Catalog_CATALOG_MEASURE {
			continue
		}
		gp, innerErr := s.schemaRepo.StoreGroup(g.Metadata)
		if innerErr != nil {
			return innerErr
		}
		ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
		allMeasureSchemas, innerErr := s.metadata.MeasureRegistry().
			ListMeasure(ctx, schema.ListOpt{Group: gp.GetSchema().GetMetadata().GetName()})
		cancel()
		if innerErr != nil {
			return innerErr
		}
		for _, measureSchema := range allMeasureSchemas {
			// sanity check before calling StoreResource
			// since StoreResource may be called inside the event loop
			if checkErr := s.sanityCheck(gp, measureSchema); checkErr != nil {
				return checkErr
			}
			if _, innerErr := gp.StoreResource(measureSchema); innerErr != nil {
				return innerErr
			}
		}
	}

	s.writeListener = setUpWriteCallback(s.l, &s.schemaRepo)
	err = s.pipeline.Subscribe(data.TopicMeasureWrite, s.writeListener)
	if err != nil {
		return err
	}
	return nil
}

func (s *service) sanityCheck(group resourceSchema.Group, measureSchema *databasev1.Measure) error {
	var topNAggrs []*databasev1.TopNAggregation
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	topNAggrs, err := s.metadata.MeasureRegistry().TopNAggregations(ctx, measureSchema.GetMetadata())
	if err != nil || len(topNAggrs) == 0 {
		return err
	}

	for _, topNAggr := range topNAggrs {
		topNMeasure, innerErr := createOrUpdateTopNMeasure(s.metadata.MeasureRegistry(), topNAggr)
		err = multierr.Append(err, innerErr)
		if topNMeasure != nil {
			_, storeErr := group.StoreResource(topNMeasure)
			if storeErr != nil {
				err = multierr.Append(err, storeErr)
			}
		}
	}

	return err
}

func (s *service) Serve() run.StopNotify {
	_ = s.schemaRepo.NotifyAll()
	// run a serial watcher
	go s.schemaRepo.Watcher()

	s.metadata.MeasureRegistry().
		RegisterHandler(schema.KindGroup|schema.KindMeasure|schema.KindIndexRuleBinding|schema.KindIndexRule|schema.KindTopNAggregation,
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
		stopCh:   make(chan struct{}),
	}, nil
}
