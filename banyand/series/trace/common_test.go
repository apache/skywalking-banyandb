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
	"context"
	"os"
	"path"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	googleUUID "github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	v1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/banyand/index"
	"github.com/apache/skywalking-banyandb/banyand/storage"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/partition"
	v12 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

var _ sort.Interface = (ByEntityID)(nil)

type ByEntityID []data.Entity

func (b ByEntityID) Len() int {
	return len(b)
}

func (b ByEntityID) Less(i, j int) bool {
	return strings.Compare(b[i].GetEntityId(), b[j].GetEntityId()) < 0
}

func (b ByEntityID) Swap(i, j int) {
	b[i], b[j] = b[j], b[i]
}

func setup(t *testing.T) (*traceSeries, func()) {
	_ = logger.Bootstrap()
	db, err := storage.NewDB(context.TODO(), nil)
	assert.NoError(t, err)
	uuid, err := googleUUID.NewUUID()
	assert.NoError(t, err)
	rootPath := path.Join(os.TempDir(), "banyandb-"+uuid.String())
	assert.NoError(t, db.FlagSet().Parse([]string{"--root-path=" + rootPath}))
	ctrl := gomock.NewController(t)
	mockIndex := index.NewMockService(ctrl)
	mockIndex.EXPECT().Insert(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()
	svc, err := NewService(context.TODO(), db, nil, mockIndex, nil)
	assert.NoError(t, err)
	assert.NoError(t, svc.PreRun())
	assert.NoError(t, db.PreRun())
	traceSVC := svc.(*service)
	ts, err := traceSVC.getSeries(common.Metadata{
		Spec: &v1.Metadata{
			Name:  "sw",
			Group: "default",
		},
	})
	assert.NoError(t, err)
	return ts, func() {
		db.GracefulStop()
		_ = os.RemoveAll(rootPath)
	}
}

type seriesEntity struct {
	seriesID string
	entity   entity
}

type wantEntity struct {
	entityID   string
	dataBinary []byte
	fieldsSize int
}

var interval = 500 * time.Millisecond

func testData(t time.Time) []seriesEntity {
	return []seriesEntity{
		{
			seriesID: "webapp_10.0.0.1",
			entity: getEntityWithTS("1", []byte{11}, t,
				"trace_id-xxfff.111323",
				0,
				"webapp_id",
				"10.0.0.1_id",
				"/home_id",
				300,
				1622933202000000000,
			),
		},
		{
			seriesID: "gateway_10.0.0.2",
			entity: getEntityWithTS("2", []byte{12}, t.Add(interval),
				"trace_id-xxfff.111323a",
				1,
			),
		},
		{
			seriesID: "httpserver_10.0.0.3",
			entity: getEntityWithTS("3", []byte{13}, t.Add(interval*2),
				"trace_id-xxfff.111323",
				1,
				"httpserver_id",
				"10.0.0.3_id",
				"/home_id",
				300,
				1622933202000000000,
				"GET",
				"200",
			),
		},
		{
			seriesID: "database_10.0.0.4",
			entity: getEntityWithTS("4", []byte{14}, t.Add(interval*3),
				"trace_id-xxfff.111323",
				0,
				"database_id",
				"10.0.0.4_id",
				"/home_id",
				300,
				1622933202000000000,
				nil,
				nil,
				"MySQL",
				"10.1.1.4",
			),
		},
		{
			seriesID: "mq_10.0.0.5",
			entity: getEntityWithTS("5", []byte{15}, t.Add(interval*4),
				"trace_id-zzpp.111323",
				0,
				"mq_id",
				"10.0.0.5_id",
				"/home_id",
				300,
				1622933202000000000,
				nil,
				nil,
				nil,
				nil,
				"test_topic",
				"10.0.0.5",
			),
		},
		{
			seriesID: "database_10.0.0.6",
			entity: getEntityWithTS("6", []byte{16}, t.Add(interval*5),
				"trace_id-zzpp.111323",
				1,
				"database_id",
				"10.0.0.6_id",
				"/home_id",
				300,
				1622933202000000000,
				nil,
				nil,
				"MySQL",
				"10.1.1.6",
			),
		},
		{
			seriesID: "mq_10.0.0.7",
			entity: getEntityWithTS("7", []byte{17}, t.Add(interval*6),
				"trace_id-zzpp.111323",
				0,
				"nq_id",
				"10.0.0.7_id",
				"/home_id",
				300,
				1622933202000000000,
				nil,
				nil,
				nil,
				nil,
				"test_topic",
				"10.0.0.7",
			),
		},
	}
}

type entity struct {
	id     string
	binary []byte
	t      time.Time
	items  []interface{}
}

func getEntity(id string, binary []byte, items ...interface{}) entity {
	return entity{
		id:     id,
		binary: binary,
		items:  items,
	}
}

func getEntityWithTS(id string, binary []byte, t time.Time, items ...interface{}) entity {
	return entity{
		id:     id,
		binary: binary,
		t:      t,
		items:  items,
	}
}

func setupTestData(t *testing.T, ts *traceSeries, seriesEntities []seriesEntity) (results []idWithShard) {
	results = make([]idWithShard, 0, len(seriesEntities))
	for _, se := range seriesEntities {
		seriesID := []byte(se.seriesID)
		ev := v12.NewEntityValueBuilder().
			DataBinary(se.entity.binary).
			EntityID(se.entity.id).
			Timestamp(se.entity.t).
			Fields(se.entity.items...).
			Build()
		shardID, _ := partition.ShardID(seriesID, 2)
		got, err := ts.Write(common.SeriesID(convert.Hash(seriesID)), shardID, data.EntityValue{
			EntityValue: ev,
		})
		if err != nil {
			t.Error("write() got error")
		}
		if got < 1 {
			t.Error("write() got empty chunkID")
		}
		results = append(results, idWithShard{
			id:      got,
			shardID: shardID,
		})
	}
	return results
}
