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
	"sync"
	"time"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/data"
	"github.com/apache/skywalking-banyandb/api/event"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/discovery"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

var (
	ErrEmptyRootPath  = errors.New("root path is empty")
	ErrStreamNotExist = errors.New("stream doesn't exist")
)

type Service interface {
	run.PreRunner
	run.Config
	run.Service
	Query
}

var _ Service = (*service)(nil)

type service struct {
	schemaMap     sync.Map
	writeListener *writeCallback
	l             *logger.Logger
	metadata      metadata.Repo
	root          string
	pipeline      queue.Queue
	repo          discovery.ServiceRepo
	stopCh        chan struct{}
}

func (s *service) Stream(metadata *commonv1.Metadata) (Stream, error) {
	sID := formatStreamID(metadata.GetName(), metadata.GetGroup())
	sm, ok := s.schemaMap.Load(sID)
	if !ok {
		return nil, errors.WithStack(ErrStreamNotExist)
	}
	return sm.(*stream), nil
}

func (s *service) FlagSet() *run.FlagSet {
	flagS := run.NewFlagSet("storage")
	flagS.StringVar(&s.root, "root-path", "/tmp", "the root path of database")
	return flagS
}

func (s *service) Validate() error {
	if s.root == "" {
		return ErrEmptyRootPath
	}
	return nil
}

func (s *service) Name() string {
	return "stream"
}

func (s *service) PreRun() error {
	schemas, err := s.metadata.StreamRegistry().ListStream(context.TODO(), schema.ListOpt{})
	if err != nil {
		return err
	}

	s.schemaMap = sync.Map{}
	s.l = logger.GetLogger(s.Name())
	for _, sa := range schemas {
		if _, innerErr := s.initStream(sa); innerErr != nil {
			s.l.Error().Err(innerErr).Msg("fail to initialize stream")
		}
	}
	s.writeListener = setUpWriteCallback(s.l, &s.schemaMap)
	return err
}

func (s *service) Serve() error {
	s.schemaMap.Range(func(key, value interface{}) bool {
		s.l.Debug().Str("streamID", key.(string)).Msg("serve stream")
		s.serveStream(value.(*stream))
		return true
	})

	errWrite := s.pipeline.Subscribe(data.TopicStreamWrite, s.writeListener)
	if errWrite != nil {
		return errWrite
	}

	s.metadata.StreamRegistry().RegisterHandler(schema.KindStream|schema.KindIndexRuleBinding, s)

	s.stopCh = make(chan struct{})
	<-s.stopCh
	return nil
}

func (s *service) OnAddOrUpdate(m schema.Metadata) {
	switch m.Kind {
	case schema.KindStream:
		s.reloadStream(m.Spec.(*databasev1.Stream))
	case schema.KindIndexRuleBinding:
		if m.Spec.(*databasev1.IndexRuleBinding).GetSubject().Catalog == commonv1.Catalog_CATALOG_STREAM {
			stm, err := s.metadata.StreamRegistry().GetStream(context.TODO(), &commonv1.Metadata{
				Name:  m.Name,
				Group: m.Group,
			})
			if err != nil {
				s.l.Error().Err(err).Msg("fail to get subject")
				return
			}
			s.reloadStream(stm)
		}
	default:
		panic("we are not interested in this kind")
	}
}

func (s *service) OnDelete(m schema.Metadata) {
	switch m.Kind {
	case schema.KindStream:
		s.removeStream(&commonv1.Metadata{
			Name:  m.Name,
			Group: m.Group,
		})
	case schema.KindIndexRuleBinding:
		// TODO: we only have metadata(group and name) here, what shall we remove?
	default:
		panic("we are not interested in this kind")
	}
}

// initStream initializes the given Stream definition
// 1. Prepare underlying storage layer with all belonging indexRules
// 2. Save the storage object into the cache
func (s *service) initStream(sa *databasev1.Stream) (*stream, error) {
	iRules, errIndexRules := s.metadata.IndexRules(context.TODO(), sa.Metadata)
	if errIndexRules != nil {
		return nil, errIndexRules
	}
	sm, errTS := openStream(s.root, streamSpec{
		schema:     sa,
		indexRules: iRules,
	}, s.l)
	if errTS != nil {
		return nil, errTS
	}
	id := formatStreamID(sm.name, sm.group)
	s.schemaMap.Store(id, sm)
	s.l.Info().Str("id", id).Msg("initialize stream")
	return sm, nil
}

func (s *service) serveStream(sMeta *stream) {
	now := time.Now()
	nowPb := timestamppb.New(now)
	locator := make([]*databasev1.EntityEvent_TagLocator, 0, len(sMeta.entityLocator))
	for _, tagLocator := range sMeta.entityLocator {
		locator = append(locator, &databasev1.EntityEvent_TagLocator{
			FamilyOffset: uint32(tagLocator.FamilyOffset),
			TagOffset:    uint32(tagLocator.TagOffset),
		})
	}
	_, err := s.repo.Publish(event.StreamTopicEntityEvent, bus.NewMessage(bus.MessageID(now.UnixNano()), &databasev1.EntityEvent{
		Subject: &commonv1.Metadata{
			Name:  sMeta.name,
			Group: sMeta.group,
		},
		EntityLocator: locator,
		Time:          nowPb,
		Action:        databasev1.Action_ACTION_PUT,
	}))
	if err != nil {
		s.l.Error().Err(err).Msg("fail to publish stream topic")
		return
	}
	for i := 0; i < int(sMeta.schema.GetOpts().GetShardNum()); i++ {
		_, errShard := s.repo.Publish(event.StreamTopicShardEvent, bus.NewMessage(bus.MessageID(now.UnixNano()), &databasev1.ShardEvent{
			Shard: &databasev1.Shard{
				Id:    uint64(i),
				Total: sMeta.schema.GetOpts().GetShardNum(),
				Metadata: &commonv1.Metadata{
					Name:  sMeta.name,
					Group: sMeta.group,
				},
				Node: &databasev1.Node{
					Id:        s.repo.NodeID(),
					CreatedAt: nowPb,
					UpdatedAt: nowPb,
					Addr:      "localhost",
				},
				UpdatedAt: nowPb,
				CreatedAt: nowPb,
			},
			Time:   nowPb,
			Action: databasev1.Action_ACTION_PUT,
		}))
		if errShard != nil {
			s.l.Error().Err(err).Msg("fail to publish shard")
			return
		}
	}
}

func (s *service) removeStream(metadata *commonv1.Metadata) {
	streamID := formatStreamID(metadata.GetName(), metadata.GetGroup())
	if oldStm, deleted := s.schemaMap.LoadAndDelete(streamID); deleted {
		now := time.Now()
		nowPb := timestamppb.New(now)
		// first withdraw registration from discovery
		for i := 0; i < int(oldStm.(*stream).schema.GetOpts().GetShardNum()); i++ {
			f, shardErr := s.repo.Publish(event.StreamTopicShardEvent, bus.NewMessage(bus.MessageID(now.UnixNano()), &databasev1.ShardEvent{
				Shard: &databasev1.Shard{
					Id:    uint64(i),
					Total: oldStm.(*stream).schema.GetOpts().GetShardNum(),
					Metadata: &commonv1.Metadata{
						Name:  oldStm.(*stream).name,
						Group: oldStm.(*stream).group,
					},
					Node: &databasev1.Node{
						Id:        s.repo.NodeID(),
						CreatedAt: nowPb,
						UpdatedAt: nowPb,
						Addr:      "localhost",
					},
					UpdatedAt: nowPb,
					CreatedAt: nowPb,
				},
				Time:   nowPb,
				Action: databasev1.Action_ACTION_DELETE,
			}))

			if shardErr != nil {
				s.l.Error().Err(shardErr).Int("shard", i).Msg("fail to withdraw shard")
				continue
			}

			// await result
			_, _ = f.Get()
		}

		f, err := s.repo.Publish(event.StreamTopicEntityEvent, bus.NewMessage(bus.MessageID(now.UnixNano()), &databasev1.EntityEvent{
			Subject: &commonv1.Metadata{
				Name:  oldStm.(*stream).name,
				Group: oldStm.(*stream).group,
			},
			Time:   nowPb,
			Action: databasev1.Action_ACTION_DELETE,
		}))

		if err != nil {
			s.l.Error().Err(err).Msg("fail to withdraw stream topic")
		} else {
			_, _ = f.Get()
		}

		// then close the underlying storage
		if err := oldStm.(*stream).Close(); err != nil {
			s.l.Error().Err(err).Msg("fail to close the old stream")
		}
	}
}

func (s *service) reloadStream(streamSchema *databasev1.Stream) {
	// first find existing *stream in the schemaMap
	s.removeStream(streamSchema.GetMetadata())

	stm, err := s.initStream(streamSchema)
	if err != nil {
		s.l.Error().Err(err).Msg("fail to init stream")
	}

	// incremental serve the changed stream
	s.serveStream(stm)
}

func (s *service) GracefulStop() {
	s.schemaMap.Range(func(key, value interface{}) bool {
		if sMeta, ok := value.(*stream); ok {
			_ = sMeta.Close()
		}

		return true
	})
	if s.stopCh != nil {
		close(s.stopCh)
	}
}

//NewService returns a new service
func NewService(_ context.Context, metadata metadata.Repo, repo discovery.ServiceRepo, pipeline queue.Queue) (Service, error) {
	return &service{
		metadata: metadata,
		repo:     repo,
		pipeline: pipeline,
	}, nil
}
