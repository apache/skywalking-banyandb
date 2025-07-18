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

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
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
	return e.update(ctx, Metadata{
		TypeMeta: TypeMeta{
			Kind:  KindTrace,
			Group: trace.GetMetadata().GetGroup(),
			Name:  trace.GetMetadata().GetName(),
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
