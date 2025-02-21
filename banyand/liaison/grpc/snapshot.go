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

package grpc

import (
	"context"
	"errors"
	"fmt"

	"github.com/apache/skywalking-banyandb/api/data"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

func (s *server) Snapshot(ctx context.Context, req *databasev1.SnapshotRequest) (*databasev1.SnapshotResponse, error) {
	fs, err := s.pipeline.Publish(ctx, data.TopicSnapshot, bus.NewMessage(bus.MessageID(0), req.Groups))
	if errors.Is(err, bus.ErrTopicNotExist) {
		return nil, fmt.Errorf("this server does not support taking snapshot")
	}
	mm, err := fs.GetAll()
	if err != nil {
		return nil, err
	}
	var result []*databasev1.Snapshot
	for _, m := range mm {
		data := m.Data()
		if data != nil {
			snp, ok := data.(*databasev1.Snapshot)
			if !ok {
				logger.Panicf("invalid data type %T", data)
			}
			if snp != nil {
				result = append(result, snp)
			}
		}
	}
	return &databasev1.SnapshotResponse{Snapshots: result}, nil
}
