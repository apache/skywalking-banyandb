// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package schema

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/multierr"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// GroupGetter provides method to get group metadata.
type GroupGetter interface {
	GetGroup(ctx context.Context, group string) (*commonv1.Group, error)
}

// GroupDropHandler handles dropping group physical data files on the local node.
type GroupDropHandler interface {
	DropGroup(ctx context.Context, group string) error
}

// InfoCollectorRegistry manages data and liaison info collectors.
type InfoCollectorRegistry struct {
	groupGetter        GroupGetter
	dataCollectors     map[commonv1.Catalog]DataInfoCollector
	liaisonCollectors  map[commonv1.Catalog]LiaisonInfoCollector
	dropHandlers       map[commonv1.Catalog]GroupDropHandler
	dataBroadcaster    bus.Broadcaster
	liaisonBroadcaster bus.Broadcaster
	l                  *logger.Logger
	mux                sync.RWMutex
}

// NewInfoCollectorRegistry creates a new InfoCollectorRegistry.
func NewInfoCollectorRegistry(l *logger.Logger, groupGetter GroupGetter) *InfoCollectorRegistry {
	return &InfoCollectorRegistry{
		groupGetter:       groupGetter,
		dataCollectors:    make(map[commonv1.Catalog]DataInfoCollector),
		liaisonCollectors: make(map[commonv1.Catalog]LiaisonInfoCollector),
		dropHandlers:      make(map[commonv1.Catalog]GroupDropHandler),
		l:                 l,
	}
}

// CollectDataInfo collects data information from both local and remote data nodes.
func (icr *InfoCollectorRegistry) CollectDataInfo(ctx context.Context, group string) ([]*databasev1.DataInfo, error) {
	g, getErr := icr.groupGetter.GetGroup(ctx, group)
	if getErr != nil {
		return nil, getErr
	}
	localInfo, localErr := icr.collectDataInfoLocal(ctx, g.Catalog, group)
	if localErr != nil {
		return nil, localErr
	}
	localInfoList := []*databasev1.DataInfo{}
	if localInfo != nil {
		localInfoList = []*databasev1.DataInfo{localInfo}
	}
	if icr.dataBroadcaster == nil {
		return localInfoList, nil
	}

	var topic bus.Topic
	switch g.Catalog {
	case commonv1.Catalog_CATALOG_MEASURE:
		topic = data.TopicMeasureCollectDataInfo
	case commonv1.Catalog_CATALOG_STREAM:
		topic = data.TopicStreamCollectDataInfo
	case commonv1.Catalog_CATALOG_TRACE:
		topic = data.TopicTraceCollectDataInfo
	default:
		return nil, fmt.Errorf("unsupported catalog type: %v", g.Catalog)
	}
	remoteInfo := icr.broadcastCollectDataInfo(topic, group)
	return append(localInfoList, remoteInfo...), nil
}

func (icr *InfoCollectorRegistry) broadcastCollectDataInfo(topic bus.Topic, group string) []*databasev1.DataInfo {
	message := bus.NewMessage(bus.MessageID(time.Now().UnixNano()), &databasev1.GroupRegistryServiceInspectRequest{Group: group})
	futures, broadcastErr := icr.dataBroadcaster.Broadcast(5*time.Second, topic, message)
	if broadcastErr != nil {
		icr.l.Warn().Err(broadcastErr).Str("group", group).Msg("failed to broadcast collect data info request")
		return []*databasev1.DataInfo{}
	}

	dataInfoList := make([]*databasev1.DataInfo, 0, len(futures))
	for _, future := range futures {
		msg, getErr := future.Get()
		if getErr != nil {
			icr.l.Warn().Err(getErr).Str("group", group).Msg("failed to get collect data info response")
			continue
		}
		msgData := msg.Data()
		switch d := msgData.(type) {
		case *databasev1.DataInfo:
			if d != nil {
				dataInfoList = append(dataInfoList, d)
			}
		case *common.Error:
			icr.l.Warn().Str("error", d.Error()).Str("group", group).Msg("error collecting data info from node")
		}
	}
	return dataInfoList
}

func (icr *InfoCollectorRegistry) collectDataInfoLocal(ctx context.Context, catalog commonv1.Catalog, group string) (*databasev1.DataInfo, error) {
	icr.mux.RLock()
	collector, hasCollector := icr.dataCollectors[catalog]
	icr.mux.RUnlock()
	if hasCollector && collector != nil {
		return collector.CollectDataInfo(ctx, group)
	}
	return nil, nil
}

// CollectLiaisonInfo collects liaison information from both local and remote liaison nodes.
func (icr *InfoCollectorRegistry) CollectLiaisonInfo(ctx context.Context, group string) ([]*databasev1.LiaisonInfo, error) {
	g, getErr := icr.groupGetter.GetGroup(ctx, group)
	if getErr != nil {
		return nil, getErr
	}
	localInfo, localErr := icr.collectLiaisonInfoLocal(ctx, g.Catalog, group)
	if localErr != nil {
		return nil, localErr
	}
	localInfoList := []*databasev1.LiaisonInfo{}
	if localInfo != nil {
		localInfoList = []*databasev1.LiaisonInfo{localInfo}
	}
	if icr.liaisonBroadcaster == nil {
		return localInfoList, nil
	}

	var topic bus.Topic
	switch g.Catalog {
	case commonv1.Catalog_CATALOG_MEASURE:
		topic = data.TopicMeasureCollectLiaisonInfo
	case commonv1.Catalog_CATALOG_STREAM:
		topic = data.TopicStreamCollectLiaisonInfo
	case commonv1.Catalog_CATALOG_TRACE:
		topic = data.TopicTraceCollectLiaisonInfo
	default:
		return nil, fmt.Errorf("unsupported catalog type: %v", g.Catalog)
	}
	remoteInfo := icr.broadcastCollectLiaisonInfo(topic, group)
	return append(localInfoList, remoteInfo...), nil
}

func (icr *InfoCollectorRegistry) broadcastCollectLiaisonInfo(topic bus.Topic, group string) []*databasev1.LiaisonInfo {
	message := bus.NewMessage(bus.MessageID(time.Now().UnixNano()), &databasev1.GroupRegistryServiceInspectRequest{Group: group})
	futures, broadcastErr := icr.liaisonBroadcaster.Broadcast(5*time.Second, topic, message)
	if broadcastErr != nil {
		icr.l.Warn().Err(broadcastErr).Str("group", group).Msg("failed to broadcast collect liaison info request")
		return []*databasev1.LiaisonInfo{}
	}

	liaisonInfoList := make([]*databasev1.LiaisonInfo, 0, len(futures))
	for _, future := range futures {
		msg, getErr := future.Get()
		if getErr != nil {
			icr.l.Warn().Err(getErr).Str("group", group).Msg("failed to get collect liaison info response")
			continue
		}
		msgData := msg.Data()
		switch d := msgData.(type) {
		case *databasev1.LiaisonInfo:
			if d != nil {
				liaisonInfoList = append(liaisonInfoList, d)
			}
		case *common.Error:
			icr.l.Warn().Str("error", d.Error()).Str("group", group).Msg("error collecting liaison info from node")
		}
	}
	return liaisonInfoList
}

func (icr *InfoCollectorRegistry) collectLiaisonInfoLocal(ctx context.Context, catalog commonv1.Catalog, group string) (*databasev1.LiaisonInfo, error) {
	icr.mux.RLock()
	collector, hasCollector := icr.liaisonCollectors[catalog]
	icr.mux.RUnlock()
	if hasCollector && collector != nil {
		return collector.CollectLiaisonInfo(ctx, group)
	}
	return nil, nil
}

// DropGroup drops the group data files on all nodes.
func (icr *InfoCollectorRegistry) DropGroup(ctx context.Context, catalog commonv1.Catalog, group string) error {
	if localErr := icr.dropGroupLocal(ctx, catalog, group); localErr != nil {
		return fmt.Errorf("failed to drop group locally: %w", localErr)
	}
	icr.mux.RLock()
	dataBroadcaster := icr.dataBroadcaster
	liaisonBroadcaster := icr.liaisonBroadcaster
	icr.mux.RUnlock()

	var topic bus.Topic
	switch catalog {
	case commonv1.Catalog_CATALOG_MEASURE:
		topic = data.TopicMeasureDropGroup
	case commonv1.Catalog_CATALOG_STREAM:
		topic = data.TopicStreamDropGroup
	case commonv1.Catalog_CATALOG_TRACE:
		topic = data.TopicTraceDropGroup
	default:
		return nil
	}

	var errs []error
	if dataBroadcaster != nil {
		if broadcastErr := icr.broadcastDropGroup(dataBroadcaster, topic, group); broadcastErr != nil {
			errs = append(errs, fmt.Errorf("data nodes: %w", broadcastErr))
		}
	}
	if liaisonBroadcaster != nil {
		if broadcastErr := icr.broadcastDropGroup(liaisonBroadcaster, topic, group); broadcastErr != nil {
			errs = append(errs, fmt.Errorf("liaison nodes: %w", broadcastErr))
		}
	}
	return multierr.Combine(errs...)
}

func (icr *InfoCollectorRegistry) broadcastDropGroup(broadcaster bus.Broadcaster, topic bus.Topic, group string) error {
	message := bus.NewMessage(bus.MessageID(time.Now().UnixNano()), &databasev1.GroupRegistryServiceDeleteRequest{Group: group})
	futures, broadcastErr := broadcaster.Broadcast(30*time.Second, topic, message)
	if broadcastErr != nil {
		return fmt.Errorf("failed to broadcast drop group request: %w", broadcastErr)
	}
	var errs []error
	for _, future := range futures {
		msg, getErr := future.Get()
		if getErr != nil {
			errs = append(errs, getErr)
			continue
		}
		if errMsg, ok := msg.Data().(*common.Error); ok {
			errs = append(errs, fmt.Errorf("node reported error dropping group: %s", errMsg.Error()))
		}
	}
	return multierr.Combine(errs...)
}

func (icr *InfoCollectorRegistry) dropGroupLocal(ctx context.Context, catalog commonv1.Catalog, group string) error {
	icr.mux.RLock()
	handler, hasHandler := icr.dropHandlers[catalog]
	icr.mux.RUnlock()
	if hasHandler && handler != nil {
		return handler.DropGroup(ctx, group)
	}
	return nil
}

// RegisterDataCollector registers a data info collector for a specific catalog.
func (icr *InfoCollectorRegistry) RegisterDataCollector(catalog commonv1.Catalog, collector DataInfoCollector) {
	icr.mux.Lock()
	defer icr.mux.Unlock()
	icr.dataCollectors[catalog] = collector
}

// RegisterLiaisonCollector registers a liaison info collector for a specific catalog.
func (icr *InfoCollectorRegistry) RegisterLiaisonCollector(catalog commonv1.Catalog, collector LiaisonInfoCollector) {
	icr.mux.Lock()
	defer icr.mux.Unlock()
	icr.liaisonCollectors[catalog] = collector
}

// RegisterGroupDropHandler registers a group drop handler for a specific catalog.
func (icr *InfoCollectorRegistry) RegisterGroupDropHandler(catalog commonv1.Catalog, handler GroupDropHandler) {
	icr.mux.Lock()
	defer icr.mux.Unlock()
	icr.dropHandlers[catalog] = handler
}

// SetDataBroadcaster sets the broadcaster for data info collection.
func (icr *InfoCollectorRegistry) SetDataBroadcaster(broadcaster bus.Broadcaster) {
	icr.mux.Lock()
	defer icr.mux.Unlock()
	icr.dataBroadcaster = broadcaster
}

// SetLiaisonBroadcaster sets the broadcaster for liaison info collection.
func (icr *InfoCollectorRegistry) SetLiaisonBroadcaster(broadcaster bus.Broadcaster) {
	icr.mux.Lock()
	defer icr.mux.Unlock()
	icr.liaisonBroadcaster = broadcaster
}
