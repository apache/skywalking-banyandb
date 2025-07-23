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

package schema

import (
	"context"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/api/validate"
)

var traceKeyPrefix = "/traces/"

func (e *etcdSchemaRegistry) GetTrace(ctx context.Context, metadata *commonv1.Metadata) (*databasev1.Trace, error) {
	var trace databasev1.Trace
	if err := e.get(ctx, formatTraceKey(metadata), &trace); err != nil {
		return nil, err
	}
	return &trace, nil
}

func (e *etcdSchemaRegistry) ListTrace(ctx context.Context, opt ListOpt) ([]*databasev1.Trace, error) {
	messages, err := e.listWithPrefix(ctx, listPrefixesForEntity(opt.Group, traceKeyPrefix), KindTrace)
	if err != nil {
		return nil, err
	}
	traces := make([]*databasev1.Trace, len(messages))
	for i, message := range messages {
		traces[i] = message.(*databasev1.Trace)
	}
	return traces, nil
}

func (e *etcdSchemaRegistry) CreateTrace(ctx context.Context, trace *databasev1.Trace) (int64, error) {
	if trace.UpdatedAt != nil {
		trace.UpdatedAt = timestamppb.Now()
	}
	if err := validate.Trace(trace); err != nil {
		return 0, err
	}
	group := trace.Metadata.GetGroup()
	g, err := e.GetGroup(ctx, group)
	if err != nil {
		return 0, err
	}
	if err := validate.GroupForStreamOrMeasure(g); err != nil {
		return 0, err
	}
	return e.create(ctx, Metadata{
		TypeMeta: TypeMeta{
			Kind:  KindTrace,
			Group: trace.GetMetadata().GetGroup(),
			Name:  trace.GetMetadata().GetName(),
		},
		Spec: trace,
	})
}

func (e *etcdSchemaRegistry) UpdateTrace(ctx context.Context, trace *databasev1.Trace) (int64, error) {
	if trace.UpdatedAt != nil {
		trace.UpdatedAt = timestamppb.Now()
	}
	if err := validate.Trace(trace); err != nil {
		return 0, err
	}
	group := trace.Metadata.GetGroup()
	g, err := e.GetGroup(ctx, group)
	if err != nil {
		return 0, err
	}
	if err = validate.GroupForStreamOrMeasure(g); err != nil {
		return 0, err
	}
	prev, err := e.GetTrace(ctx, trace.GetMetadata())
	if err != nil {
		return 0, err
	}
	if prev == nil {
		return 0, errors.WithMessagef(ErrGRPCResourceNotFound, "trace %s not found", trace.GetMetadata().GetName())
	}
	return e.update(ctx, Metadata{
		TypeMeta: TypeMeta{
			Kind:        KindTrace,
			Group:       trace.GetMetadata().GetGroup(),
			Name:        trace.GetMetadata().GetName(),
			ModRevision: trace.GetMetadata().GetModRevision(),
		},
		Spec: trace,
	})
}

func (e *etcdSchemaRegistry) DeleteTrace(ctx context.Context, metadata *commonv1.Metadata) (bool, error) {
	return e.delete(ctx, Metadata{
		TypeMeta: TypeMeta{
			Kind:  KindTrace,
			Group: metadata.GetGroup(),
			Name:  metadata.GetName(),
		},
	})
}

func formatTraceKey(metadata *commonv1.Metadata) string {
	return formatKey(traceKeyPrefix, metadata)
}
