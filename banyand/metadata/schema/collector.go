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

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

type infoCollectorRegistry struct {
	dataCollectors     map[commonv1.Catalog]DataInfoCollector
	liaisonCollectors  map[commonv1.Catalog]LiaisonInfoCollector
	dataBroadcaster    bus.Broadcaster
	liaisonBroadcaster bus.Broadcaster
	l                  *logger.Logger
	mux                sync.RWMutex
}

func newInfoCollectorRegistry(l *logger.Logger) *infoCollectorRegistry {
	return &infoCollectorRegistry{
		dataCollectors:    make(map[commonv1.Catalog]DataInfoCollector),
		liaisonCollectors: make(map[commonv1.Catalog]LiaisonInfoCollector),
		l:                 l,
	}
}

// CollectDataInfo collects data information from both local and remote data nodes.
func (icr *infoCollectorRegistry) CollectDataInfo(ctx context.Context, catalog commonv1.Catalog, group string) ([]*databasev1.DataInfo, error) {
	localInfo, localErr := icr.collectDataInfoLocal(ctx, catalog, group)
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
	switch catalog {
	case commonv1.Catalog_CATALOG_MEASURE:
		topic = data.TopicMeasureCollectDataInfo
	case commonv1.Catalog_CATALOG_STREAM:
		topic = data.TopicStreamCollectDataInfo
	case commonv1.Catalog_CATALOG_TRACE:
		topic = data.TopicTraceCollectDataInfo
	default:
		return nil, fmt.Errorf("unsupported catalog type: %v", catalog)
	}
	remoteInfo := icr.broadcastCollectDataInfo(topic, group)
	return append(localInfoList, remoteInfo...), nil
}

func (icr *infoCollectorRegistry) broadcastCollectDataInfo(topic bus.Topic, group string) []*databasev1.DataInfo {
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

func (icr *infoCollectorRegistry) collectDataInfoLocal(ctx context.Context, catalog commonv1.Catalog, group string) (*databasev1.DataInfo, error) {
	icr.mux.RLock()
	collector, hasCollector := icr.dataCollectors[catalog]
	icr.mux.RUnlock()
	if hasCollector && collector != nil {
		return collector.CollectDataInfo(ctx, group)
	}
	return nil, nil
}

// CollectLiaisonInfo collects liaison information from both local and remote liaison nodes.
func (icr *infoCollectorRegistry) CollectLiaisonInfo(ctx context.Context, catalog commonv1.Catalog, group string) ([]*databasev1.LiaisonInfo, error) {
	localInfo, localErr := icr.collectLiaisonInfoLocal(ctx, catalog, group)
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
	switch catalog {
	case commonv1.Catalog_CATALOG_MEASURE:
		topic = data.TopicMeasureCollectLiaisonInfo
	case commonv1.Catalog_CATALOG_STREAM:
		topic = data.TopicStreamCollectLiaisonInfo
	case commonv1.Catalog_CATALOG_TRACE:
		topic = data.TopicTraceCollectLiaisonInfo
	default:
		return nil, fmt.Errorf("unsupported catalog type: %v", catalog)
	}
	remoteInfo := icr.broadcastCollectLiaisonInfo(topic, group)
	return append(localInfoList, remoteInfo...), nil
}

func (icr *infoCollectorRegistry) broadcastCollectLiaisonInfo(topic bus.Topic, group string) []*databasev1.LiaisonInfo {
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

func (icr *infoCollectorRegistry) collectLiaisonInfoLocal(ctx context.Context, catalog commonv1.Catalog, group string) (*databasev1.LiaisonInfo, error) {
	icr.mux.RLock()
	collector, hasCollector := icr.liaisonCollectors[catalog]
	icr.mux.RUnlock()
	if hasCollector && collector != nil {
		return collector.CollectLiaisonInfo(ctx, group)
	}
	return nil, nil
}

// RegisterDataCollector registers a data info collector for a specific catalog.
func (icr *infoCollectorRegistry) RegisterDataCollector(catalog commonv1.Catalog, collector DataInfoCollector) {
	icr.mux.Lock()
	defer icr.mux.Unlock()
	icr.dataCollectors[catalog] = collector
}

// RegisterLiaisonCollector registers a liaison info collector for a specific catalog.
func (icr *infoCollectorRegistry) RegisterLiaisonCollector(catalog commonv1.Catalog, collector LiaisonInfoCollector) {
	icr.mux.Lock()
	defer icr.mux.Unlock()
	icr.liaisonCollectors[catalog] = collector
}

// SetDataBroadcaster sets the broadcaster for data info collection.
func (icr *infoCollectorRegistry) SetDataBroadcaster(broadcaster bus.Broadcaster) {
	icr.mux.Lock()
	defer icr.mux.Unlock()
	icr.dataBroadcaster = broadcaster
}

// SetLiaisonBroadcaster sets the broadcaster for liaison info collection.
func (icr *infoCollectorRegistry) SetLiaisonBroadcaster(broadcaster bus.Broadcaster) {
	icr.mux.Lock()
	defer icr.mux.Unlock()
	icr.liaisonBroadcaster = broadcaster
}
