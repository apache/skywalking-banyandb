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
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	"github.com/apache/skywalking-banyandb/api/event"
	v1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/v1"
	"github.com/apache/skywalking-banyandb/banyand/discovery"
	"github.com/apache/skywalking-banyandb/banyand/index"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/banyand/series"
	"github.com/apache/skywalking-banyandb/banyand/series/schema"
	"github.com/apache/skywalking-banyandb/banyand/storage"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/pb"
)

var _ series.Service = (*service)(nil)

type service struct {
	db            storage.Database
	schemaMap     map[string]*traceSeries
	l             *logger.Logger
	repo          discovery.ServiceRepo
	stopCh        chan struct{}
	idx           index.Service
	writeListener *writeCallback
	pipeline      queue.Queue
}

//NewService returns a new service
func NewService(_ context.Context, db storage.Database, repo discovery.ServiceRepo, idx index.Service, pipeline queue.Queue) (series.Service, error) {
	return &service{
		db:            db,
		repo:          repo,
		idx:           idx,
		pipeline:      pipeline,
		writeListener: &writeCallback{},
	}, nil
}

func (s *service) Name() string {
	return "trace-series"
}

func (s *service) PreRun() error {
	schemas, err := s.TraceSeries().List(context.Background(), schema.ListOpt{})
	if err != nil {
		return err
	}
	s.schemaMap = make(map[string]*traceSeries, len(schemas))
	s.writeListener.schemaMap = make(map[string]*traceSeries, len(schemas))
	s.l = logger.GetLogger(s.Name())
	s.writeListener.l = logger.GetLogger(s.Name())
	for _, sa := range schemas {
		ts, errTS := newTraceSeries(sa, s.l, s.idx)
		if errTS != nil {
			return errTS
		}
		s.db.Register(ts)
		id := formatTraceSeriesID(ts.name, ts.group)
		s.schemaMap[id] = ts
		s.writeListener.schemaMap[id] = ts
		s.l.Info().Str("id", id).Msg("initialize Trace series")
	}
	return err
}

func (s *service) Serve() error {
	now := time.Now().UnixNano()
	for _, sMeta := range s.schemaMap {
		e := pb.NewSeriesEventBuilder().
			SeriesMetadata(sMeta.group, sMeta.name).
			FieldNames(sMeta.fieldsNamesCompositeSeriesID...).
			Time(time.Now()).
			Action(v1.Action_ACTION_PUT).
			Build()
		_, err := s.repo.Publish(event.TopicSeriesEvent, bus.NewMessage(bus.MessageID(now), e))
		if err != nil {
			return err
		}
		seriesObj := &v1.Series{
			Series: &v1.Metadata{
				Name:  sMeta.name,
				Group: sMeta.group,
			},
			Catalog: v1.Series_CATALOG_TRACE,
		}
		rules, errGetRules := s.IndexRules(context.Background(), seriesObj, nil)
		if errGetRules != nil {
			return errGetRules
		}
		shardedRuleIndex := make([]*v1.IndexRuleEvent_ShardedIndexRule, 0, len(rules)*int(sMeta.shardNum))
		for i := 0; i < int(sMeta.shardNum); i++ {
			t := time.Now()
			e := pb.NewShardEventBuilder().Action(v1.Action_ACTION_PUT).Time(t).
				Shard(
					pb.NewShardBuilder().
						ID(uint64(i)).Total(sMeta.shardNum).SeriesMetadata(sMeta.group, sMeta.name).UpdatedAt(t).CreatedAt(t).
						Node(pb.NewNodeBuilder().
							ID(s.repo.NodeID()).CreatedAt(t).UpdatedAt(t).Addr("localhost").
							Build()).
						Build()).
				Build()
			_, errShard := s.repo.Publish(event.TopicShardEvent, bus.NewMessage(bus.MessageID(now), e))
			if errShard != nil {
				return errShard
			}
			shardedRuleIndex = append(shardedRuleIndex, &v1.IndexRuleEvent_ShardedIndexRule{
				ShardId: uint64(i),
				Rules:   rules,
			})
		}

		indexRule := &v1.IndexRuleEvent{
			Series: seriesObj.Series,
			Rules:  shardedRuleIndex,
			Action: v1.Action_ACTION_PUT,
			Time:   timestamppb.New(time.Now()),
		}
		_, errPublishRules := s.repo.Publish(event.TopicIndexRule, bus.NewMessage(bus.MessageID(now), indexRule))
		if errPublishRules != nil {
			return errPublishRules
		}
	}
	errWrite := s.pipeline.Subscribe(data.TopicWriteEvent, s.writeListener)
	if errWrite != nil {
		return errWrite
	}
	s.stopCh = make(chan struct{})
	<-s.stopCh
	return nil
}

func (s *service) GracefulStop() {
	if s.stopCh != nil {
		close(s.stopCh)
	}
}

type writeCallback struct {
	l         *logger.Logger
	schemaMap map[string]*traceSeries
}

func (w *writeCallback) Rev(message bus.Message) (resp bus.Message) {
	writeEvent, ok := message.Data().(data.TraceWriteDate)
	if !ok {
		w.l.Warn().Msg("invalid event data type")
		return
	}
	entityValue := writeEvent.WriteRequest.GetEntity()
	ts := writeEvent.WriteRequest.GetMetadata()
	id := formatTraceSeriesID(ts.GetName(), ts.GetGroup())
	_, err := w.schemaMap[id].Write(common.SeriesID(writeEvent.SeriesID), writeEvent.ShardID, data.EntityValue{
		EntityValue: entityValue,
	})
	if err != nil {
		w.l.Warn().Err(err)
	}
	return
}
