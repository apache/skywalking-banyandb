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

package sub

import (
	"context"

	"github.com/apache/skywalking-banyandb/api/data"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

func (s *server) Snapshot(ctx context.Context, req *databasev1.SnapshotRequest) (*databasev1.SnapshotResponse, error) {
	s.listenersLock.RLock()
	defer s.listenersLock.RUnlock()
	ll := s.getListeners(data.TopicSnapshot)
	if len(ll) == 0 {
		logger.Panicf("no listener found for topic %s", data.TopicSnapshot)
	}
	var result []*databasev1.Snapshot
	for _, l := range ll {
		message := l.Rev(ctx, bus.NewMessage(bus.MessageID(0), req.Groups))
		data := message.Data()
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
