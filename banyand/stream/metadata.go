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
//
package stream

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/skywalking-banyandb/api/event"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/discovery"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/pkg/errors"
	"go.uber.org/multierr"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type eventType uint8

const (
	eventAddOrUpdate eventType = iota
	eventDelete
)

type metadataEvent struct {
	typ      eventType
	kind     schema.Kind
	metadata *commonv1.Metadata
}

type schemaRepo struct {
	sync.RWMutex
	path     string
	metadata metadata.Repo
	repo     discovery.ServiceRepo
	l        *logger.Logger
	data     map[string]*group

	// stop channel for the inner worker
	workerStopCh chan struct{}
	eventCh      chan metadataEvent
}

func newSchemaRepo(path string, metadata metadata.Repo, repo discovery.ServiceRepo, l *logger.Logger) schemaRepo {
	return schemaRepo{
		path:         path,
		metadata:     metadata,
		repo:         repo,
		l:            l,
		data:         make(map[string]*group),
		eventCh:      make(chan metadataEvent),
		workerStopCh: make(chan struct{}),
	}
}

func (sr *schemaRepo) OnAddOrUpdate(m schema.Metadata) {
	switch m.Kind {
	case schema.KindGroup:
		sr.eventCh <- metadataEvent{
			typ:      eventAddOrUpdate,
			kind:     schema.KindGroup,
			metadata: m.Spec.(*commonv1.Group).GetMetadata(),
		}
	case schema.KindStream:
		sr.eventCh <- metadataEvent{
			typ:      eventAddOrUpdate,
			kind:     schema.KindStream,
			metadata: m.Spec.(*databasev1.Stream).GetMetadata(),
		}
	case schema.KindIndexRuleBinding:
		irb, ok := m.Spec.(*databasev1.IndexRuleBinding)
		if !ok {
			sr.l.Warn().Msg("fail to convert message to IndexRuleBinding")
			return
		}
		if irb.GetSubject().Catalog == commonv1.Catalog_CATALOG_STREAM {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			stm, err := sr.metadata.StreamRegistry().GetStream(ctx, &commonv1.Metadata{
				Name:  irb.GetSubject().GetName(),
				Group: m.Group,
			})
			cancel()
			if err != nil {
				sr.l.Error().Err(err).Msg("fail to get subject")
				return
			}
			sr.eventCh <- metadataEvent{
				typ:      eventAddOrUpdate,
				kind:     schema.KindStream,
				metadata: stm.GetMetadata(),
			}
		}
	case schema.KindIndexRule:
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		subjects, err := sr.metadata.Subjects(ctx, m.Spec.(*databasev1.IndexRule), commonv1.Catalog_CATALOG_STREAM)
		cancel()
		if err != nil {
			sr.l.Error().Err(err).Msg("fail to get subjects(stream)")
			return
		}
		for _, sub := range subjects {
			sr.eventCh <- metadataEvent{
				typ:      eventAddOrUpdate,
				kind:     schema.KindStream,
				metadata: sub.(*databasev1.Stream).GetMetadata(),
			}
		}
	default:
	}
}

func (sr *schemaRepo) OnDelete(m schema.Metadata) {
	switch m.Kind {
	case schema.KindGroup:
		sr.eventCh <- metadataEvent{
			typ:      eventDelete,
			kind:     schema.KindGroup,
			metadata: m.Spec.(*commonv1.Group).GetMetadata(),
		}
	case schema.KindStream:
		sr.eventCh <- metadataEvent{
			typ:      eventDelete,
			kind:     schema.KindStream,
			metadata: m.Spec.(*databasev1.Stream).GetMetadata(),
		}
	case schema.KindIndexRuleBinding:
		if m.Spec.(*databasev1.IndexRuleBinding).GetSubject().Catalog == commonv1.Catalog_CATALOG_STREAM {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			stm, err := sr.metadata.StreamRegistry().GetStream(ctx, &commonv1.Metadata{
				Name:  m.Name,
				Group: m.Group,
			})
			cancel()
			if err != nil {
				sr.l.Error().Err(err).Msg("fail to get subject")
				return
			}
			sr.eventCh <- metadataEvent{
				typ:      eventDelete,
				kind:     schema.KindStream,
				metadata: stm.GetMetadata(),
			}
		}
	case schema.KindIndexRule:
	default:
	}
}

func (sr *schemaRepo) watcher() {
	defer func() {
		if err := recover(); err != nil {
			sr.l.Warn().Interface("err", err).Msg("watching the events")
		}
	}()
	for {
		select {
		case evt := <-sr.eventCh:
			var err error
			switch evt.typ {
			case eventAddOrUpdate:
				switch evt.kind {
				case schema.KindGroup:
					_, err = sr.storeGroup(evt.metadata)
				case schema.KindStream:
					_, err = sr.storeStream(evt.metadata)
				}
			case eventDelete:
				switch evt.kind {
				case schema.KindGroup:
					err = sr.deleteGroup(evt.metadata)
				case schema.KindStream:
					err = sr.deleteStream(evt.metadata)
				}
			}
			if err != nil {
				sr.l.Err(err).Interface("event", evt).Msg("fail to handle the metadata event. retry...")
				sr.eventCh <- evt
			}
		case <-sr.workerStopCh:
			return
		}
	}
}

func (sr *schemaRepo) storeGroup(groupMeta *commonv1.Metadata) (*group, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	groupSchema, err := sr.metadata.GroupRegistry().GetGroup(ctx, groupMeta.GetName())
	cancel()
	if err != nil {
		return nil, err
	}
	name := groupSchema.GetMetadata().GetName()
	sr.Lock()
	defer sr.Unlock()
	g, ok := sr.getGroup(name)
	if !ok {
		sr.l.Info().Str("group", name).Msg("creating a tsdb")
		db, err := sr.openDB(groupSchema)
		if err != nil {
			return nil, err
		}
		g = newGroup(groupSchema, sr.repo, sr.metadata, db, sr.l)
		sr.data[name] = g
		return g, sr.notify(groupSchema, databasev1.Action_ACTION_PUT)
	}
	prevGroupSchema := g.groupSchema
	if groupSchema.GetMetadata().GetModRevision() <= prevGroupSchema.Metadata.ModRevision {
		return g, nil
	}
	sr.l.Info().Str("group", name).Msg("closing the previous tsdb")
	db := g.SupplyTSDB()
	db.Close()
	err = sr.notify(prevGroupSchema, databasev1.Action_ACTION_DELETE)
	if err != nil {
		return nil, err
	}
	sr.l.Info().Str("group", name).Msg("creating a new tsdb")
	newDB, err := sr.openDB(groupSchema)
	if err != nil {
		return nil, err
	}
	g.setDB(newDB)
	err = sr.notify(groupSchema, databasev1.Action_ACTION_PUT)
	if err != nil {
		return nil, err
	}
	g.groupSchema = groupSchema
	return g, nil
}

func (sr *schemaRepo) deleteGroup(groupMeta *commonv1.Metadata) error {
	name := groupMeta.GetName()
	sr.Lock()
	defer sr.Unlock()
	var ok bool
	g, ok := sr.getGroup(name)
	if !ok {
		return nil
	}
	err := g.close()
	if err != nil {
		return err
	}
	sr.notify(g.groupSchema, databasev1.Action_ACTION_DELETE)
	delete(sr.data, name)
	return nil
}

func (sr *schemaRepo) openDB(groupSchema *commonv1.Group) (tsdb.Database, error) {
	return tsdb.OpenDatabase(
		context.TODO(),
		tsdb.DatabaseOpts{
			Location: sr.path,
			ShardNum: groupSchema.ResourceOpts.ShardNum,
			EncodingMethod: tsdb.EncodingMethod{
				EncoderPool: encoding.NewPlainEncoderPool(chunkSize),
				DecoderPool: encoding.NewPlainDecoderPool(chunkSize),
			},
		})
}

func (sr *schemaRepo) getGroup(name string) (*group, bool) {
	g := sr.data[name]
	if g == nil {
		return nil, false
	}
	return g, true
}

func (sr *schemaRepo) loadGroup(name string) (*group, bool) {
	sr.RLock()
	defer sr.RUnlock()
	return sr.getGroup(name)
}

func (sr *schemaRepo) loadStream(metadata *commonv1.Metadata) (*stream, bool) {
	g, ok := sr.loadGroup(metadata.Group)
	if !ok {
		return nil, false
	}
	return g.loadStream(metadata.Name)
}

func (sr *schemaRepo) storeStream(metadata *commonv1.Metadata) (*stream, error) {
	group, ok := sr.loadGroup(metadata.Group)
	if !ok {
		return nil, errors.Errorf("unknown group")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	stm, err := sr.metadata.StreamRegistry().GetStream(ctx, metadata)
	cancel()
	if err != nil {
		return nil, errors.Errorf("fails to get the stream")
	}
	return group.storeStream(stm)
}

func (sr *schemaRepo) deleteStream(metadata *commonv1.Metadata) error {
	group, ok := sr.loadGroup(metadata.Group)
	if !ok {
		return nil
	}
	return group.deleteStream(metadata)
}

func (sr *schemaRepo) notify(groupSchema *commonv1.Group, action databasev1.Action) (err error) {
	now := time.Now()
	nowPb := timestamppb.New(now)
	shardNum := groupSchema.GetResourceOpts().GetShardNum()
	for i := 0; i < int(shardNum); i++ {
		_, errInternal := sr.repo.Publish(event.StreamTopicShardEvent, bus.NewMessage(bus.MessageID(now.UnixNano()), &databasev1.ShardEvent{
			Shard: &databasev1.Shard{
				Id:    uint64(i),
				Total: shardNum,
				Metadata: &commonv1.Metadata{
					Name: groupSchema.GetMetadata().GetName(),
				},
				Node: &databasev1.Node{
					Id:        sr.repo.NodeID(),
					CreatedAt: nowPb,
					UpdatedAt: nowPb,
					Addr:      "localhost",
				},
				UpdatedAt: nowPb,
				CreatedAt: nowPb,
			},
			Time:   nowPb,
			Action: action,
		}))
		if errors.Is(errInternal, bus.ErrTopicNotExist) {
			return nil
		}
		if errInternal != nil {
			err = multierr.Append(err, errInternal)
		}
	}
	return err
}

func (sr *schemaRepo) notifyAll() (err error) {
	for _, g := range sr.getMap() {
		err = multierr.Append(err, sr.notify(g.groupSchema, databasev1.Action_ACTION_PUT))
		for _, s := range g.getMap() {
			multierr.Append(err, g.notify(s, databasev1.Action_ACTION_PUT))
		}
	}
	return err
}

func (sr *schemaRepo) close() {
	defer func() {
		if err := recover(); err != nil {
			sr.l.Warn().Interface("err", err).Msg("closing stream")
		}
	}()
	if sr.eventCh != nil {
		close(sr.eventCh)
	}
	if sr.workerStopCh != nil {
		close(sr.workerStopCh)
	}
	for _, g := range sr.getMap() {
		err := g.close()
		if err != nil {
			sr.l.Err(err).Stringer("group", g.groupSchema.Metadata).Msg("closing")
		}
	}
}

func (sr *schemaRepo) getMap() map[string]*group {
	sr.RLock()
	defer sr.RUnlock()
	return sr.data
}

type group struct {
	groupSchema *commonv1.Group
	l           *logger.Logger
	repo        discovery.ServiceRepo
	metadata    metadata.Repo
	db          atomic.Value
	mapMutex    sync.RWMutex
	schemaMap   map[string]*stream
}

func newGroup(groupSchema *commonv1.Group, repo discovery.ServiceRepo, metadata metadata.Repo, db tsdb.Database, l *logger.Logger) *group {
	g := &group{
		groupSchema: groupSchema,
		repo:        repo,
		metadata:    metadata,
		l:           l,
		schemaMap:   make(map[string]*stream),
	}
	g.db.Store(db)
	return g
}

func (g *group) SupplyTSDB() tsdb.Database {
	return g.db.Load().(tsdb.Database)
}

func (g *group) setDB(db tsdb.Database) {
	g.db.Store(db)
}

func (g *group) storeStream(streamSchema *databasev1.Stream) (*stream, error) {
	g.mapMutex.Lock()
	defer g.mapMutex.Unlock()
	key := streamSchema.GetMetadata().GetName()
	preStream := g.schemaMap[key]
	if preStream != nil &&
		streamSchema.GetMetadata().GetModRevision() <= preStream.schema.GetMetadata().GetModRevision() {
		// we only need to check the max modifications revision observed for index rules
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		idxRules, errIndexRules := g.metadata.IndexRules(ctx, streamSchema.GetMetadata())
		cancel()
		if errIndexRules != nil {
			return nil, errIndexRules
		}
		if len(idxRules) == len(preStream.indexRules) {
			maxModRevision := parseMaxModRevision(idxRules)
			if preStream.maxObservedModRevision >= maxModRevision {
				return preStream, nil
			}
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	idxRules, errIndexRules := g.metadata.IndexRules(ctx, streamSchema.GetMetadata())
	cancel()
	if errIndexRules != nil {
		return nil, errIndexRules
	}
	sm, errTS := openStream(g.groupSchema.GetResourceOpts().ShardNum, g, streamSpec{
		schema:     streamSchema,
		indexRules: idxRules,
	}, g.l)
	if errTS != nil {
		return nil, errTS
	}
	if err := g.notify(sm, databasev1.Action_ACTION_PUT); err != nil {
		return nil, err
	}
	g.schemaMap[key] = sm
	if preStream != nil {
		_ = preStream.Close()
	}
	return sm, nil
}

func (g *group) deleteStream(streamMetadata *commonv1.Metadata) error {
	g.mapMutex.Lock()
	defer g.mapMutex.Unlock()
	key := streamMetadata.GetName()
	preStream := g.schemaMap[key]
	if preStream == nil {
		return nil
	}
	if err := g.notify(preStream, databasev1.Action_ACTION_DELETE); err != nil {
		return err
	}
	delete(g.schemaMap, key)
	_ = preStream.Close()
	return nil
}

func (g *group) loadStream(name string) (*stream, bool) {
	data := g.getMap()
	s := data[name]
	if s == nil {
		return nil, false
	}
	return s, true
}

func (g *group) notify(stream *stream, action databasev1.Action) error {
	now := time.Now()
	nowPb := timestamppb.New(now)
	locator := make([]*databasev1.EntityEvent_TagLocator, 0, len(stream.entityLocator))
	for _, tagLocator := range stream.entityLocator {
		locator = append(locator, &databasev1.EntityEvent_TagLocator{
			FamilyOffset: uint32(tagLocator.FamilyOffset),
			TagOffset:    uint32(tagLocator.TagOffset),
		})
	}
	_, err := g.repo.Publish(event.StreamTopicEntityEvent, bus.NewMessage(bus.MessageID(now.UnixNano()), &databasev1.EntityEvent{
		Subject: &commonv1.Metadata{
			Name:  stream.name,
			Group: stream.group,
		},
		EntityLocator: locator,
		Time:          nowPb,
		Action:        action,
	}))
	if errors.Is(err, bus.ErrTopicNotExist) {
		return nil
	}
	return err
}

func (g *group) close() (err error) {
	for _, s := range g.getMap() {
		err = multierr.Append(err, s.Close())
	}
	return multierr.Append(err, g.SupplyTSDB().Close())
}

func (g *group) getMap() map[string]*stream {
	g.mapMutex.RLock()
	defer g.mapMutex.RUnlock()
	return g.schemaMap
}
