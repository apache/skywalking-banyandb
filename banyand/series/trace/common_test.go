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
	"bytes"
	"context"
	"sort"
	"testing"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/stretchr/testify/assert"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	v1 "github.com/apache/skywalking-banyandb/api/fbs/v1"
	"github.com/apache/skywalking-banyandb/banyand/storage"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/fb"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/partition"
)

var _ sort.Interface = (ByEntityID)(nil)

type ByEntityID []data.Entity

func (b ByEntityID) Len() int {
	return len(b)
}

func (b ByEntityID) Less(i, j int) bool {
	return bytes.Compare(b[i].EntityId(), b[j].EntityId()) < 0
}

func (b ByEntityID) Swap(i, j int) {
	b[i], b[j] = b[j], b[i]
}

func setup(t testing.TB) (*traceSeries, func()) {
	_ = logger.Init(logger.Logging{
		Env:   "test",
		Level: "warn",
	})
	db, err := storage.NewDB(context.TODO(), nil)
	assert.NoError(t, err)
	assert.NoError(t, db.FlagSet().Parse(nil))
	svc, err := NewService(context.TODO(), db, nil)
	assert.NoError(t, err)
	assert.NoError(t, svc.PreRun())
	assert.NoError(t, db.PreRun())
	traceSVC := svc.(*service)
	b := flatbuffers.NewBuilder(0)
	name := b.CreateString("sw")
	group := b.CreateString("default")
	v1.MetadataStart(b)
	v1.MetadataAddName(b, name)
	v1.MetadataAddGroup(b, group)
	b.Finish(v1.MetadataEnd(b))
	meta := new(v1.Metadata)
	meta.Init(b.Bytes, b.Offset())
	ts, err := traceSVC.getSeries(common.Metadata{
		Spec: v1.GetRootAsMetadata(b.FinishedBytes(), 0),
	})
	assert.NoError(t, err)
	return ts, db.GracefulStop
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

var gap = uint64(500 * time.Millisecond)

func testData(ts uint64) []seriesEntity {

	return []seriesEntity{
		{
			seriesID: "webapp_10.0.0.1",
			entity: getEntityWithTS("1", []byte{11}, ts,
				"trace_id-xxfff.111323",
				0,
				"webapp_id",
				"10.0.0.1_id",
				"/home_id",
				"webapp",
				"10.0.0.1",
				"/home",
				300,
				1622933202000000000,
			),
		},
		{
			seriesID: "gateway_10.0.0.2",
			entity: getEntityWithTS("2", []byte{12}, ts+gap,
				"trace_id-xxfff.111323a",
				1,
			),
		},
		{
			seriesID: "httpserver_10.0.0.3",
			entity: getEntityWithTS("3", []byte{13}, ts+2*gap,
				"trace_id-xxfff.111323",
				1,
				"httpserver_id",
				"10.0.0.3_id",
				"/home_id",
				"httpserver",
				"10.0.0.3",
				"/home",
				300,
				1622933202000000000,
				"GET",
				"200",
			),
		},
		{
			seriesID: "database_10.0.0.4",
			entity: getEntityWithTS("4", []byte{14}, ts+3*gap,
				"trace_id-xxfff.111323",
				0,
				"database_id",
				"10.0.0.4_id",
				"/home_id",
				"database",
				"10.0.0.4",
				"/home",
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
			entity: getEntityWithTS("5", []byte{15}, ts+4*gap,
				"trace_id-zzpp.111323",
				0,
				"mq_id",
				"10.0.0.5_id",
				"/home_id",
				"mq",
				"10.0.0.5",
				"/home",
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
			entity: getEntityWithTS("6", []byte{16}, ts+5*gap,
				"trace_id-zzpp.111323",
				1,
				"database_id",
				"10.0.0.6_id",
				"/home_id",
				"database",
				"10.0.0.6",
				"/home",
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
			entity: getEntityWithTS("7", []byte{17}, ts+6*gap,
				"trace_id-zzpp.111323",
				0,
				"nq_id",
				"10.0.0.7_id",
				"/home_id",
				"mq",
				"10.0.0.7",
				"/home",
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
	ts     uint64
	items  []interface{}
}

func getEntity(id string, binary []byte, items ...interface{}) entity {
	return entity{
		id:     id,
		binary: binary,
		items:  items,
	}
}

func getEntityWithTS(id string, binary []byte, ts uint64, items ...interface{}) entity {
	return entity{
		id:     id,
		binary: binary,
		ts:     ts,
		items:  items,
	}
}

func setUpTestData(t testing.TB, ts *traceSeries, seriesEntities []seriesEntity) (chunkIDs []common.ChunkID) {
	chunkIDs = make([]common.ChunkID, 0, len(seriesEntities))
	for _, se := range seriesEntities {
		seriesID := []byte(se.seriesID)
		b := fb.NewWriteEntityBuilder()
		items := make([]fb.ComponentBuilderFunc, 0, 2)
		items = append(items, b.BuildMetaData("default", "sw"))
		timestamp := se.entity.ts
		items = append(items, b.BuildEntityWithTS(se.entity.id, se.entity.binary, timestamp, se.entity.items...))
		builder, err := b.BuildWriteEntity(
			items...,
		)
		assert.NoError(t, err)
		we := v1.GetRootAsWriteEntity(builder.FinishedBytes(), 0)
		got, err := ts.Write(common.SeriesID(convert.Hash(seriesID)), partition.ShardID(seriesID, 2), data.EntityValue{
			EntityValue: we.Entity(nil),
		})
		if err != nil {
			t.Error("Write() got error")
		}
		if got < 1 {
			t.Error("Write() got empty chunkID")
		}
		chunkIDs = append(chunkIDs, got)
	}
	return chunkIDs
}
