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

package trace

import (
	"testing"
	"time"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/partition"
	v1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

func Test_traceSeries_Write(t *testing.T) {
	ts, stopFunc := setup(t)
	defer stopFunc()

	tests := []struct {
		name    string
		args    seriesEntity
		wantErr bool
	}{
		{
			name: "golden path",
			args: seriesEntity{
				seriesID: "webapp_10.0.0.1",
				entity: getEntity("1231.dfd.123123ssf", []byte{12},
					"trace_id-xxfff.111323",
					0,
					"webapp_id",
					"10.0.0.1_id",
					"/home_id",
					300,
					1622933202000000000,
				),
			},
		},
		{
			name: "minimal",
			args: seriesEntity{
				seriesID: "webapp_10.0.0.1",
				entity: getEntity("1231.dfd.123123ssf", []byte{12},
					"trace_id-xxfff.111323",
					1,
				),
			},
		},
		{
			name: "http",
			args: seriesEntity{
				seriesID: "webapp_10.0.0.1",
				entity: getEntity("1231.dfd.123123ssf", []byte{12},
					"trace_id-xxfff.111323",
					0,
					"webapp_id",
					"10.0.0.1_id",
					"/home_id",
					300,
					1622933202000000000,
					"GET",
					"200",
				),
			},
		},
		{
			name: "database",
			args: seriesEntity{
				seriesID: "webapp_10.0.0.1",
				entity: getEntity("1231.dfd.123123ssf", []byte{12},
					"trace_id-xxfff.111323",
					0,
					"webapp_id",
					"10.0.0.1_id",
					"/home_id",
					300,
					1622933202000000000,
					nil,
					nil,
					"MySQL",
					"10.1.1.2",
				),
			},
		},
		{
			name: "mq",
			args: seriesEntity{
				seriesID: "webapp_10.0.0.1",
				entity: getEntity("1231.dfd.123123ssf", []byte{12},
					"trace_id-xxfff.111323",
					1,
					"webapp_id",
					"10.0.0.1_id",
					"/home_id",
					300,
					1622933202000000000,
					nil,
					nil,
					nil,
					nil,
					"test_topic",
					"10.0.0.1",
				),
			},
		},
		{
			name: "absent Trace id",
			args: seriesEntity{
				seriesID: "webapp_10.0.0.1",
				entity: getEntity("1231.dfd.123123ssf", []byte{12},
					nil,
					0,
				),
			},
			wantErr: true,
		},
		{
			name: "invalid Trace id",
			args: seriesEntity{
				seriesID: "webapp_10.0.0.1",
				entity: getEntity("1231.dfd.123123ssf", []byte{12},
					1212323,
					1,
				),
			},
			wantErr: true,
		},
		{
			name: "absent State",
			args: seriesEntity{
				seriesID: "webapp_10.0.0.1",
				entity: getEntity("1231.dfd.123123ssf", []byte{12},
					"trace_id-xxfff.111323",
					nil,
				),
			},
			wantErr: true,
		},
		{
			name: "invalid State",
			args: seriesEntity{
				seriesID: "webapp_10.0.0.1",
				entity: getEntity("1231.dfd.123123ssf", []byte{12},
					"trace_id-xxfff.111323",
					6,
				),
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			seriesID := []byte(tt.args.seriesID)
			shardID, shardIDError := partition.ShardID(seriesID, 2)
			if shardIDError != nil {
				return
			}
			ev := v1.NewEntityValueBuilder().
				DataBinary(tt.args.entity.binary).
				EntityID(tt.args.entity.id).
				Fields(tt.args.entity.items...).
				Timestamp(time.Now()).
				Build()
			got, err := ts.Write(common.SeriesID(convert.Hash(seriesID)), shardID, data.EntityValue{
				EntityValue: ev,
			})
			if (err != nil) != tt.wantErr {
				t.Errorf("write() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err == nil && got < 1 {
				t.Error("write() got empty chunkID")
			}
		})
	}
}
