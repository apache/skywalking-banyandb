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

	clusterv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/cluster/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

func (s *server) HealthCheck(_ context.Context, req *clusterv1.HealthCheckRequest) (*clusterv1.HealthCheckResponse, error) {
	if t, ok := s.topicMap[req.ServiceName]; ok {
		ll := s.listeners[t]
		for _, l := range ll {
			if err := l.CheckHealth(); err != nil {
				return &clusterv1.HealthCheckResponse{
					ServiceName: req.ServiceName,
					Status:      err.Status(),
					Error:       err.Error(),
				}, nil
			}
		}
		return &clusterv1.HealthCheckResponse{
			ServiceName: req.ServiceName,
			Status:      modelv1.Status_STATUS_SUCCEED,
		}, nil
	}
	return &clusterv1.HealthCheckResponse{
		ServiceName: req.ServiceName,
		Status:      modelv1.Status_STATUS_NOT_FOUND,
	}, nil
}
