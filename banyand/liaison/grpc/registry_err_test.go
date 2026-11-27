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
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/observability/services"
	"github.com/apache/skywalking-banyandb/pkg/meter/prom"
)

// TestRegistryErrExcludesNotFound checks that a lookup of a missing resource, which is a normal answer to a client,
// does not count in total_registry_err, while every other failure does.
func TestRegistryErrExcludesNotFound(t *testing.T) {
	meta := &commonv1.Metadata{Group: "g", Name: "s"}
	tests := []struct {
		call    func(rs *streamRegistryServer) error
		want    map[string]float64
		err     error
		name    string
		wantErr bool
	}{
		{
			name: "get of a missing stream", err: schema.ErrGRPCResourceNotFound,
			call: func(rs *streamRegistryServer) error {
				_, err := rs.Get(context.Background(), &databasev1.StreamRegistryServiceGetRequest{Metadata: meta})
				return err
			},
			want: map[string]float64{}, wantErr: true,
		},
		{
			name: "exist of a missing stream", err: schema.ErrGRPCResourceNotFound,
			call: func(rs *streamRegistryServer) error {
				_, err := rs.Exist(context.Background(), &databasev1.StreamRegistryServiceExistRequest{Metadata: meta})
				return err
			},
			want: map[string]float64{},
		},
		{
			name: "get of a missing stream through a remote status", err: status.Error(codes.NotFound, "banyandb: resource not found"),
			call: func(rs *streamRegistryServer) error {
				_, err := rs.Get(context.Background(), &databasev1.StreamRegistryServiceGetRequest{Metadata: meta})
				return err
			},
			want: map[string]float64{}, wantErr: true,
		},
		{
			name: "get that fails", err: status.Error(codes.Unavailable, "no schema server"),
			call: func(rs *streamRegistryServer) error {
				_, err := rs.Get(context.Background(), &databasev1.StreamRegistryServiceGetRequest{Metadata: meta})
				return err
			},
			want: map[string]float64{"stream/get": 1}, wantErr: true,
		},
		{
			name: "update that is invalid", err: status.Error(codes.InvalidArgument, "bad stream"),
			call: func(rs *streamRegistryServer) error {
				_, err := rs.Update(context.Background(), &databasev1.StreamRegistryServiceUpdateRequest{Stream: &databasev1.Stream{Metadata: meta}})
				return err
			},
			want: map[string]float64{"stream/update": 1}, wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			streams := schema.NewMockStream(ctrl)
			streams.EXPECT().GetStream(gomock.Any(), gomock.Any()).Return(nil, tt.err).AnyTimes()
			streams.EXPECT().UpdateStream(gomock.Any(), gomock.Any()).Return(int64(0), tt.err).AnyTimes()
			groups := schema.NewMockGroup(ctrl)
			groups.EXPECT().GetGroup(gomock.Any(), "g").Return(&commonv1.Group{}, nil).AnyTimes()
			repo := metadata.NewMockRepo(ctrl)
			repo.EXPECT().StreamRegistry().Return(streams).AnyTimes()
			repo.EXPECT().GroupRegistry().Return(groups).AnyTimes()
			reg := prometheus.NewRegistry()
			rs := &streamRegistryServer{
				schemaRegistry: repo,
				metrics:        newMetrics(services.NewFactory(prom.NewProvider(liaisonGrpcScope, reg), nil, nil)),
			}
			require.Equal(t, tt.wantErr, tt.call(rs) != nil)
			got := map[string]float64{}
			for key, value := range counterSeries(t, reg, "total_registry_err") {
				if value > 0 {
					got[key] = value
				}
			}
			require.Equal(t, tt.want, got)
		})
	}
}
