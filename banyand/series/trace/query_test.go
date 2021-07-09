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
	"math"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/series"
)

func Test_traceSeries_FetchEntity(t *testing.T) {
	type args struct {
		chunkIDIndices []int
		chunkIDs       []string
		opt            series.ScanOptions
	}
	tests := []struct {
		name         string
		args         args
		wantEntities []wantEntity
		wantErr      bool
	}{
		{
			name: "golden path",
			args: args{
				chunkIDIndices: []int{0, 1, 2, 3, 4, 5, 6},
				opt:            series.ScanOptions{Projection: []string{"trace_id", "data_binary"}},
			},
			wantEntities: []wantEntity{
				{entityID: "1", dataBinary: []byte{11}, fieldsSize: 1},
				{entityID: "2", dataBinary: []byte{12}, fieldsSize: 1},
				{entityID: "3", dataBinary: []byte{13}, fieldsSize: 1},
				{entityID: "4", dataBinary: []byte{14}, fieldsSize: 1},
				{entityID: "5", dataBinary: []byte{15}, fieldsSize: 1},
				{entityID: "6", dataBinary: []byte{16}, fieldsSize: 1},
				{entityID: "7", dataBinary: []byte{17}, fieldsSize: 1},
			},
		},
		{
			name: "multiple fields",
			args: args{
				chunkIDIndices: []int{0, 1, 2, 3, 4, 5, 6},
				opt:            series.ScanOptions{Projection: []string{"trace_id", "service_name", "state", "duration", "mq.queue"}},
			},
			wantEntities: []wantEntity{
				{entityID: "1", fieldsSize: 4},
				{entityID: "2", fieldsSize: 2},
				{entityID: "3", fieldsSize: 4},
				{entityID: "4", fieldsSize: 4},
				{entityID: "5", fieldsSize: 5},
				{entityID: "6", fieldsSize: 4},
				{entityID: "7", fieldsSize: 5},
			},
		},
		{
			name: "data binary",
			args: args{
				chunkIDIndices: []int{0, 1, 2, 3, 4, 5, 6},
				opt:            series.ScanOptions{Projection: []string{"data_binary"}},
			},
			wantEntities: []wantEntity{
				{entityID: "1", dataBinary: []byte{11}},
				{entityID: "2", dataBinary: []byte{12}},
				{entityID: "3", dataBinary: []byte{13}},
				{entityID: "4", dataBinary: []byte{14}},
				{entityID: "5", dataBinary: []byte{15}},
				{entityID: "6", dataBinary: []byte{16}},
				{entityID: "7", dataBinary: []byte{17}},
			},
		},
		{
			name: "invalid chunk ids",
			args: args{
				chunkIDs: []string{"x"},
				opt:      series.ScanOptions{Projection: []string{"trace_id", "data_binary"}},
			},
			wantErr: true,
		},
		{
			name: "mix up invalid/valid ids",
			args: args{
				chunkIDs:       []string{"undefined"},
				chunkIDIndices: []int{0, 1},
				opt:            series.ScanOptions{Projection: []string{"trace_id", "data_binary"}},
			},
			wantEntities: []wantEntity{
				{entityID: "1", dataBinary: []byte{11}, fieldsSize: 1},
				{entityID: "2", dataBinary: []byte{12}, fieldsSize: 1},
			},
			wantErr: true,
		},
		{
			name: "absent scan opt",
			args: args{
				chunkIDIndices: []int{0, 1},
			},
			wantErr: true,
		},
		{
			name: "invalid opt absent",
			args: args{
				chunkIDIndices: []int{0, 1},
				opt:            series.ScanOptions{Projection: []string{"trace_id", "undefined"}},
			},
			wantErr: true,
		},
	}
	ts, stopFunc := setup(t)
	defer stopFunc()
	chunkIDs := setUpTestData(t, ts, testData(uint64(time.Now().UnixNano())))
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			chunkIDCriteria := make([]common.ChunkID, 0, len(tt.args.chunkIDIndices))
			for i := range tt.args.chunkIDIndices {
				chunkIDCriteria = append(chunkIDCriteria, chunkIDs[i])
			}
			for _, id := range tt.args.chunkIDs {
				chunkIDCriteria = append(chunkIDCriteria, common.ChunkID(id))
			}
			entities, err := ts.FetchEntity(chunkIDCriteria, tt.args.opt)
			if (err != nil) != tt.wantErr {
				t.Errorf("Write() error = %v, wantErr %v", err, tt.wantErr)
			}
			assert.Equal(t, len(tt.wantEntities), len(entities))
			for i, e := range entities {
				assert.EqualValues(t, tt.wantEntities[i].entityID, e.EntityId())
				assert.Equal(t, tt.wantEntities[i].dataBinary, e.DataBinaryBytes())
				assert.Equal(t, tt.wantEntities[i].fieldsSize, e.FieldsLength())
			}
		})
	}
}

func Test_traceSeries_FetchTrace(t *testing.T) {
	type args struct {
		traceID string
	}
	tests := []struct {
		name         string
		args         args
		wantEntities []wantEntity
		wantErr      bool
	}{
		{
			name: "golden path",
			args: args{
				traceID: "trace_id-xxfff.111323",
			},
			wantEntities: []wantEntity{
				{entityID: "1", dataBinary: []byte{11}, fieldsSize: 1},
				{entityID: "2", dataBinary: []byte{12}, fieldsSize: 1},
				{entityID: "3", dataBinary: []byte{13}, fieldsSize: 1},
				{entityID: "4", dataBinary: []byte{14}, fieldsSize: 1},
			},
		},
		{
			name: "found nothing",
			args: args{
				traceID: "not_existed",
			},
		},
		{
			name:    "absent Trace id",
			args:    args{},
			wantErr: true,
		},
	}
	ts, stopFunc := setup(t)
	defer stopFunc()
	setUpTestData(t, ts, testData(uint64(time.Now().UnixNano())))
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			traceData, err := ts.FetchTrace(tt.args.traceID, series.ScanOptions{Projection: []string{"data_binary", "trace_id"}})
			if (err != nil) != tt.wantErr {
				t.Errorf("Write() error = %v, wantErr %v", err, tt.wantErr)
			}
			var entities ByEntityID = traceData.Entities
			assert.Equal(t, len(tt.wantEntities), len(entities))
			sort.Sort(entities)
			for i, e := range entities {
				assert.EqualValues(t, tt.wantEntities[i].entityID, e.EntityId())
				assert.Equal(t, tt.wantEntities[i].dataBinary, e.DataBinaryBytes())
				assert.Equal(t, tt.wantEntities[i].fieldsSize, e.FieldsLength())
			}
		})
	}
}

func Test_traceSeries_ScanEntity(t *testing.T) {
	type args struct {
		start uint64
		end   uint64
	}
	baseTS := uint64(time.Now().UnixNano())
	tests := []struct {
		name         string
		args         args
		wantEntities []wantEntity
		wantErr      bool
	}{
		{
			name: "scan all",
			args: args{
				start: 0,
				end:   math.MaxUint64,
			},
			wantEntities: []wantEntity{
				{entityID: "1", dataBinary: []byte{11}, fieldsSize: 1},
				{entityID: "2", dataBinary: []byte{12}, fieldsSize: 1},
				{entityID: "3", dataBinary: []byte{13}, fieldsSize: 1},
				{entityID: "4", dataBinary: []byte{14}, fieldsSize: 1},
				{entityID: "5", dataBinary: []byte{15}, fieldsSize: 1},
				{entityID: "6", dataBinary: []byte{16}, fieldsSize: 1},
				{entityID: "7", dataBinary: []byte{17}, fieldsSize: 1},
			},
		},
		{
			name: "scan range",
			args: args{
				start: baseTS,
				end:   baseTS + 6*gap,
			},
			wantEntities: []wantEntity{
				{entityID: "1", dataBinary: []byte{11}, fieldsSize: 1},
				{entityID: "2", dataBinary: []byte{12}, fieldsSize: 1},
				{entityID: "3", dataBinary: []byte{13}, fieldsSize: 1},
				{entityID: "4", dataBinary: []byte{14}, fieldsSize: 1},
				{entityID: "5", dataBinary: []byte{15}, fieldsSize: 1},
				{entityID: "6", dataBinary: []byte{16}, fieldsSize: 1},
				{entityID: "7", dataBinary: []byte{17}, fieldsSize: 1},
			},
		},
		{
			name: "scan slice",
			args: args{
				start: baseTS + gap + 1,
				end:   baseTS + 5*gap - 2,
			},
			wantEntities: []wantEntity{
				{entityID: "3", dataBinary: []byte{13}, fieldsSize: 1},
				{entityID: "4", dataBinary: []byte{14}, fieldsSize: 1},
				{entityID: "5", dataBinary: []byte{15}, fieldsSize: 1},
			},
		},
		{
			name: "single result",
			args: args{
				start: baseTS,
				end:   baseTS,
			},
			wantEntities: []wantEntity{
				{entityID: "1", dataBinary: []byte{11}, fieldsSize: 1},
			},
		},
		{
			name: "found nothing",
			args: args{},
		},
	}
	ts, stopFunc := setup(t)
	defer stopFunc()
	setUpTestData(t, ts, testData(baseTS))
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var entities ByEntityID
			var err error
			entities, err = ts.ScanEntity(tt.args.start, tt.args.end, series.ScanOptions{Projection: []string{"data_binary", "trace_id"}})
			if (err != nil) != tt.wantErr {
				t.Errorf("Write() error = %v, wantErr %v", err, tt.wantErr)
			}
			assert.Equal(t, len(tt.wantEntities), len(entities))
			sort.Sort(entities)
			for i, e := range entities {
				assert.Equal(t, tt.wantEntities[i].entityID, string(e.EntityId()))
				assert.Equal(t, tt.wantEntities[i].dataBinary, e.DataBinaryBytes())
				assert.Equal(t, tt.wantEntities[i].fieldsSize, e.FieldsLength())
			}
		})
	}
}
