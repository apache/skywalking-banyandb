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
	schemaMap     map[string]*stream
	writeListener *writeCallback
	l             *logger.Logger
	metadata      metadata.Repo
	root          string
	pipeline      queue.Queue
	repo          discovery.ServiceRepo
	stopCh        chan struct{}
}

func (s *service) Stream(stream *commonv1.Metadata) (Stream, error) {
	sID := formatStreamID(stream.GetName(), stream.GetGroup())
	sm, ok := s.schemaMap[sID]
	if !ok {
		return nil, errors.WithStack(ErrStreamNotExist)
	}
	return sm, nil
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

	s.schemaMap = make(map[string]*stream, len(schemas))
	s.l = logger.GetLogger(s.Name())
	for _, sa := range schemas {
		iRules, errIndexRules := s.metadata.IndexRules(context.TODO(), sa.Metadata)
		if errIndexRules != nil {
			return errIndexRules
		}
		sm, errTS := openStream(s.root, streamSpec{
			schema:     sa,
			indexRules: iRules,
		}, s.l)
		if errTS != nil {
			return errTS
		}
		id := formatStreamID(sm.name, sm.group)
		s.schemaMap[id] = sm
		s.l.Info().Str("id", id).Msg("initialize stream")
	}
	s.writeListener = setUpWriteCallback(s.l, s.schemaMap)
	return err
}

func (s *service) Serve() error {
	t := time.Now()
	now := time.Now().UnixNano()
	nowBp := timestamppb.New(t)
	for _, sMeta := range s.schemaMap {
		locator := make([]*databasev1.EntityEvent_TagLocator, 0, len(sMeta.entityLocator))
		for _, tagLocator := range sMeta.entityLocator {
			locator = append(locator, &databasev1.EntityEvent_TagLocator{
				FamilyOffset: uint32(tagLocator.FamilyOffset),
				TagOffset:    uint32(tagLocator.TagOffset),
			})
		}
		_, err := s.repo.Publish(event.StreamTopicEntityEvent, bus.NewMessage(bus.MessageID(now), &databasev1.EntityEvent{
			Subject: &commonv1.Metadata{
				Name:  sMeta.name,
				Group: sMeta.group,
			},
			EntityLocator: locator,
			Time:          nowBp,
			Action:        databasev1.Action_ACTION_PUT,
		}))
		if err != nil {
			return err
		}
		for i := 0; i < int(sMeta.schema.GetOpts().GetShardNum()); i++ {
			_, errShard := s.repo.Publish(event.StreamTopicShardEvent, bus.NewMessage(bus.MessageID(now), &databasev1.ShardEvent{
				Shard: &databasev1.Shard{
					Id:    uint64(i),
					Total: sMeta.schema.GetOpts().GetShardNum(),
					Metadata: &commonv1.Metadata{
						Name:  sMeta.name,
						Group: sMeta.group,
					},
					Node: &databasev1.Node{
						Id:        s.repo.NodeID(),
						CreatedAt: nowBp,
						UpdatedAt: nowBp,
						Addr:      "localhost",
					},
					UpdatedAt: nowBp,
					CreatedAt: nowBp,
				},
				Time:   nowBp,
				Action: databasev1.Action_ACTION_PUT,
			}))
			if errShard != nil {
				return errShard
			}
		}
	}
	errWrite := s.pipeline.Subscribe(data.TopicStreamWrite, s.writeListener)
	if errWrite != nil {
		return errWrite
	}
	s.stopCh = make(chan struct{})
	<-s.stopCh
	return nil
}

func (s *service) GracefulStop() {
	for _, sm := range s.schemaMap {
		_ = sm.Close()
	}
	if s.stopCh != nil {
		close(s.stopCh)
	}
}

// NewService returns a new service
func NewService(_ context.Context, metadata metadata.Repo, repo discovery.ServiceRepo, pipeline queue.Queue) (Service, error) {
	return &service{
		metadata: metadata,
		repo:     repo,
		pipeline: pipeline,
	}, nil
}
