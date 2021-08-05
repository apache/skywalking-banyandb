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

package index

import (
	"context"
	"sync"

	"github.com/pkg/errors"
	"go.uber.org/multierr"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/event"
	apiv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/v1"
	"github.com/apache/skywalking-banyandb/banyand/discovery"
	"github.com/apache/skywalking-banyandb/banyand/index/tsdb"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/posting"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

var (
	ErrShardNotFound       = errors.New("series doesn't exist")
	ErrTraceSeriesNotFound = errors.New("trace series not found")
)

type Condition struct {
	Key    string
	Values [][]byte
	Op     apiv1.PairQuery_BinaryOp
}

type Field struct {
	ChunkID common.ChunkID
	Name    string
	Value   []byte
}

//go:generate mockgen -destination=./index_mock.go -package=index . Service
type Repo interface {
	Search(seriesMeta common.Metadata, shardID uint, startTime, endTime uint64, indexObjectName string, conditions []Condition) (posting.List, error)
	Insert(seriesMeta common.Metadata, shardID uint, fields *Field) error
}

type Builder interface {
	run.PreRunner
	run.Service
}

type ReadyOption func(map[string]*series) bool

func MetaExists(group, name string) ReadyOption {
	seriesID := &apiv1.Metadata{
		Name:  "sw",
		Group: "default",
	}
	return func(m map[string]*series) bool {
		if _, ok := m[compositeSeriesID(seriesID)]; ok {
			return true
		}
		return false
	}
}

type Service interface {
	Repo
	Builder
	Ready(context.Context, ...ReadyOption) bool
}

type series struct {
	repo map[uint]*shard
}

type shard struct {
	meta  map[string][]*apiv1.IndexObject
	store tsdb.GlobalStore
}

type service struct {
	meta              *indexMeta
	log               *logger.Logger
	repo              discovery.ServiceRepo
	stopCh            chan struct{}
	indexRuleListener *indexRuleListener
}

func NewService(_ context.Context, repo discovery.ServiceRepo) (Service, error) {
	svc := &service{
		repo:              repo,
		indexRuleListener: &indexRuleListener{},
	}
	svc.meta = &indexMeta{
		meta: make(map[string]*series),
	}
	svc.indexRuleListener.indexMeta = svc.meta
	svc.indexRuleListener.closeFunc = func() {
		svc.stopCh <- struct{}{}
	}
	return svc, nil
}

func (s *service) Insert(series common.Metadata, shardID uint, field *Field) error {
	sd, err := s.getShard(series, shardID)
	if err != nil {
		return err
	}
	objects, ok := sd.meta[field.Name]
	if !ok {
		return nil
	}
	for _, object := range objects {
		err = multierr.Append(err, sd.store.Insert(&tsdb.Field{
			Name:  []byte(compositeFieldID(object.GetName(), field.Name)),
			Value: field.Value,
		}, field.ChunkID))
	}
	return err
}

func (s *service) getShard(series common.Metadata, shardID uint) (*shard, error) {
	ss := s.meta.get(series.Spec)
	if ss == nil {
		return nil, errors.Wrapf(ErrTraceSeriesNotFound, "identify:%s", compositeSeriesID(series.Spec))
	}
	sd, existSearcher := ss.repo[shardID]
	if !existSearcher {
		return nil, errors.Wrapf(ErrShardNotFound, "shardID:%d", shardID)
	}
	return sd, nil
}

func (s *service) Name() string {
	return "index"
}

func (s *service) PreRun() error {
	//TODO: listen to written data
	s.log = logger.GetLogger("index")
	s.indexRuleListener.log = s.log
	return s.repo.Subscribe(event.TopicIndexRule, s.indexRuleListener)
}

func (s *service) Serve() error {
	s.stopCh = make(chan struct{})
	<-s.stopCh
	return nil
}

func (s *service) GracefulStop() {
	if s.stopCh != nil {
		close(s.stopCh)
	}
}

func (s *service) Ready(ctx context.Context, options ...ReadyOption) bool {
	options = append(options, func(m map[string]*series) bool {
		return len(m) > 0
	})

	for {
		select {
		case <-ctx.Done():
			return ctx.Err() == nil
		default:
			allMatches := true
			for _, opt := range options {
				allMatches = allMatches && opt(s.meta.meta)
			}
			if !allMatches {
				continue
			}
			return true
		}
	}
}

type indexMeta struct {
	meta map[string]*series
	sync.RWMutex
}

func (i *indexMeta) get(series *apiv1.Metadata) *series {
	i.RWMutex.RLock()
	defer i.RWMutex.RUnlock()
	s, ok := i.meta[compositeSeriesID(series)]
	if ok {
		return s
	}
	return nil
}

type indexRuleListener struct {
	log       *logger.Logger
	indexMeta *indexMeta
	closeFunc func()
}

func (i *indexRuleListener) Rev(message bus.Message) (resp bus.Message) {
	indexRuleEvent, ok := message.Data().(*apiv1.IndexRuleEvent)
	if !ok {
		i.log.Warn().Msg("invalid event data type")
		return
	}
	i.log.Info().
		Str("action", apiv1.Action_name[int32(indexRuleEvent.Action)]).
		Str("series-name", indexRuleEvent.Series.Name).
		Str("series-group", indexRuleEvent.Series.Group).
		Msg("received an index rule")
	i.indexMeta.Lock()
	defer i.indexMeta.Unlock()
	switch indexRuleEvent.Action {
	case apiv1.Action_ACTION_PUT:
		seriesID := compositeSeriesID(indexRuleEvent.Series)
		newSeries := &series{
			repo: make(map[uint]*shard),
		}
		for _, rule := range indexRuleEvent.Rules {
			store := tsdb.NewStore(indexRuleEvent.Series.Name, indexRuleEvent.Series.Group, uint(rule.ShardId))
			fields := make([]tsdb.FieldSpec, 0, len(rule.Rules))
			meta := make(map[string][]*apiv1.IndexObject)
			for _, indexRule := range rule.GetRules() {
				for _, object := range indexRule.Objects {
					fieldsSize := len(object.Fields)
					if fieldsSize > 1 {
						//TODO: to support composited index
						i.log.Error().Str("name", object.Name).
							Msg("index module doesn't support composited index object")
						i.closeFunc()
					} else if fieldsSize < 1 {
						continue
					}
					field := object.Fields[0]
					fieldSpec := tsdb.FieldSpec{
						Name: compositeFieldID(object.Name, field),
					}
					fields = append(fields, fieldSpec)
					objects, existed := meta[field]
					if !existed {
						objects = make([]*apiv1.IndexObject, 0, 1)
					}
					objects = append(objects, object)
					meta[field] = objects
				}
			}
			err := store.Initialize(fields)
			if err != nil {
				i.log.Warn().Err(err).Msg("failed to initialize index getShard")
			}
			newSeries.repo[uint(rule.ShardId)] = &shard{
				store: store,
				meta:  meta,
			}
		}
		i.indexMeta.meta[seriesID] = newSeries
	default:
		i.log.Warn().Msg("unsupported action")
	}
	return
}

func compositeFieldID(indexObjectName, field string) string {
	return indexObjectName + ":" + field
}

func compositeSeriesID(series *apiv1.Metadata) string {
	return series.Name + "-" + series.Group
}
