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

package property

import (
	"context"
	"fmt"
	"path"
	"path/filepath"
	"runtime/debug"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/query"
)

var (
	_ bus.MessageListener = (*updateListener)(nil)
	_ bus.MessageListener = (*deleteListener)(nil)
	_ bus.MessageListener = (*queryListener)(nil)
	_ bus.MessageListener = (*repairListener)(nil)
)

type updateListener struct {
	s                   *service
	l                   *logger.Logger
	path                string
	maxDiskUsagePercent int
}

func (h *updateListener) CheckHealth() *common.Error {
	if h.maxDiskUsagePercent < 1 {
		return common.NewErrorWithStatus(modelv1.Status_STATUS_DISK_FULL, "property is readonly because \"property-max-disk-usage-percent\" is 0")
	}
	diskPercent := observability.GetPathUsedPercent(h.path)
	if diskPercent < h.maxDiskUsagePercent {
		return nil
	}
	h.l.Warn().Int("maxPercent", h.maxDiskUsagePercent).Int("diskPercent", diskPercent).Msg("disk usage is too high, stop writing")
	return common.NewErrorWithStatus(modelv1.Status_STATUS_DISK_FULL, "disk usage is too high, stop writing")
}

func (h *updateListener) Rev(ctx context.Context, message bus.Message) (resp bus.Message) {
	n := time.Now()
	now := n.UnixNano()
	var protoReq proto.Message
	defer func() {
		if err := recover(); err != nil {
			h.s.l.Error().Interface("err", err).RawJSON("req", logger.Proto(protoReq)).Str("stack", string(debug.Stack())).Msg("panic")
			resp = bus.NewMessage(bus.MessageID(time.Now().UnixNano()), common.NewError("panic: %v", err))
		}
	}()
	d := message.Data().(*propertyv1.InternalUpdateRequest)
	if d == nil {
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("request is nil"))
		return
	}
	protoReq = d
	if d.Property == nil {
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("property is nil"))
		return
	}
	if d.Property.Tags == nil {
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("tags is nil"))
		return
	}
	if len(d.Id) == 0 {
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("id is empty"))
		return
	}
	err := h.s.db.update(ctx, common.ShardID(d.ShardId), d.Id, d.Property)
	if err != nil {
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("fail to update property: %v", err))
		return
	}
	resp = bus.NewMessage(bus.MessageID(now), &propertyv1.ApplyResponse{
		Created: true,
		TagsNum: uint32(len(d.Property.Tags)),
	})
	return
}

type deleteListener struct {
	*bus.UnImplementedHealthyListener
	s *service
}

func (h *deleteListener) Rev(ctx context.Context, message bus.Message) (resp bus.Message) {
	n := time.Now()
	now := n.UnixNano()
	var protoReq proto.Message
	defer func() {
		if err := recover(); err != nil {
			h.s.l.Error().Interface("err", err).RawJSON("req", logger.Proto(protoReq)).Str("stack", string(debug.Stack())).Msg("panic")
			resp = bus.NewMessage(bus.MessageID(time.Now().UnixNano()), common.NewError("panic: %v", err))
		}
	}()
	d := message.Data().(*propertyv1.InternalDeleteRequest)
	if d == nil {
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("request is nil"))
		return
	}
	protoReq = d
	if len(d.Ids) == 0 {
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("id is empty"))
		return
	}
	err := h.s.db.delete(ctx, d.Ids)
	if err != nil {
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("fail to delete property: %v", err))
		return
	}
	resp = bus.NewMessage(bus.MessageID(now), &propertyv1.DeleteResponse{
		Deleted: true,
	})
	return
}

type queryListener struct {
	*bus.UnImplementedHealthyListener
	s *service
}

func (h *queryListener) Rev(ctx context.Context, message bus.Message) (resp bus.Message) {
	n := time.Now()
	now := n.UnixNano()
	var protoReq proto.Message
	defer func() {
		if err := recover(); err != nil {
			h.s.l.Error().Interface("err", err).RawJSON("req", logger.Proto(protoReq)).Str("stack", string(debug.Stack())).Msg("panic")
			resp = bus.NewMessage(bus.MessageID(time.Now().UnixNano()), common.NewError("panic: %v", err))
		}
	}()
	d := message.Data().(*propertyv1.QueryRequest)
	if d == nil {
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("request is nil"))
		return
	}
	protoReq = d
	if len(d.Groups) == 0 {
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("groups is empty"))
		return
	}
	if d.Limit == 0 {
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("limit is 0"))
		return
	}
	var tracer *query.Tracer
	var span *query.Span
	if d.Trace {
		tracer, ctx = query.NewTracer(ctx, n.Format(time.RFC3339Nano))
		span, ctx = tracer.StartSpan(ctx, "data-%s", h.s.nodeID)
		span.Tag("req", string(logger.Proto(protoReq)))
		defer func() {
			span.Stop()
		}()
	}
	properties, err := h.s.db.query(ctx, d)
	if err != nil {
		if tracer != nil {
			span.Error(err)
			resp = bus.NewMessage(bus.MessageID(now), &propertyv1.InternalQueryResponse{
				Trace: tracer.ToProto(),
			})
			return
		}
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("fail to query property: %v", err))
		return
	}
	qResp := &propertyv1.InternalQueryResponse{}
	for _, p := range properties {
		qResp.Sources = append(qResp.Sources, p.source)
		qResp.Deletes = append(qResp.Deletes, p.deleteTime)
	}
	if tracer != nil {
		qResp.Trace = tracer.ToProto()
	}
	resp = bus.NewMessage(bus.MessageID(now), qResp)
	return
}

type snapshotListener struct {
	*bus.UnImplementedHealthyListener
	s           *service
	snapshotSeq uint64
	snapshotMux sync.Mutex
}

// Rev takes a snapshot of the database.
func (s *snapshotListener) Rev(ctx context.Context, message bus.Message) bus.Message {
	groups := message.Data().([]*databasev1.SnapshotRequest_Group)
	var toTake bool
	if len(groups) == 0 {
		toTake = true
	} else {
		for _, g := range groups {
			if g.Catalog != commonv1.Catalog_CATALOG_PROPERTY {
				toTake = true
				break
			}
		}
	}
	if !toTake {
		return bus.NewMessage(bus.MessageID(time.Now().UnixNano()), nil)
	}
	s.snapshotMux.Lock()
	defer s.snapshotMux.Unlock()
	storage.DeleteStaleSnapshots(s.s.snapshotDir, s.s.maxFileSnapshotNum, s.s.lfs)
	sn := s.snapshotName()
	shardsRef := s.s.db.sLst.Load()
	if shardsRef == nil {
		return bus.NewMessage(bus.MessageID(time.Now().UnixNano()), nil)
	}
	shards := *shardsRef
	for _, shard := range shards {
		select {
		case <-ctx.Done():
			return bus.NewMessage(bus.MessageID(time.Now().UnixNano()), nil)
		default:
		}
		snpDir := path.Join(s.s.snapshotDir, sn, storage.DataDir, filepath.Base(shard.location))
		lfs.MkdirPanicIfExist(snpDir, storage.DirPerm)
		err := shard.store.TakeFileSnapshot(snpDir)
		if err != nil {
			s.s.l.Error().Err(err).Str("shard", filepath.Base(shard.location)).Msg("fail to take shard snapshot")
			return bus.NewMessage(bus.MessageID(time.Now().UnixNano()), &databasev1.Snapshot{
				Name:    sn,
				Catalog: commonv1.Catalog_CATALOG_PROPERTY,
				Error:   err.Error(),
			})
		}
	}

	return bus.NewMessage(bus.MessageID(time.Now().UnixNano()), &databasev1.Snapshot{
		Name:    sn,
		Catalog: commonv1.Catalog_CATALOG_PROPERTY,
	})
}

func (s *snapshotListener) snapshotName() string {
	s.snapshotSeq++
	return fmt.Sprintf("%s-%08X", time.Now().UTC().Format("20060102150405"), s.snapshotSeq)
}

type repairListener struct {
	*bus.UnImplementedHealthyListener
	s *service
}

func (r *repairListener) Rev(ctx context.Context, message bus.Message) (resp bus.Message) {
	n := time.Now()
	now := n.UnixNano()
	var protoReq proto.Message
	defer func() {
		if err := recover(); err != nil {
			r.s.l.Error().Interface("err", err).RawJSON("req", logger.Proto(protoReq)).Str("stack", string(debug.Stack())).Msg("panic")
			resp = bus.NewMessage(bus.MessageID(time.Now().UnixNano()), common.NewError("panic: %v", err))
		}
	}()
	d := message.Data().(*propertyv1.InternalRepairRequest)
	if d == nil {
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("request is nil"))
		return
	}
	if d.Id == nil {
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("id is nil"))
		return
	}
	if d.Property == nil {
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("property is nil"))
		return
	}
	protoReq = d
	if err := r.s.db.repair(ctx, d.Id, d.ShardId, d.Property, d.DeleteTime); err != nil {
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("fail to delete property: %v", err))
		return
	}
	resp = bus.NewMessage(bus.MessageID(now), &propertyv1.InternalRepairResponse{})
	return
}
