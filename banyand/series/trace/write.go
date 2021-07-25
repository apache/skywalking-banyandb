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
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	v1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/v1"
	bydb_bytes "github.com/apache/skywalking-banyandb/pkg/bytes"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/partition"
)

//TODO: To add WAL to handle crashing
//TODO: To replace timestamp with monotonic ID in case of server time reset
func (t *traceSeries) Write(seriesID common.SeriesID, shardID uint, entity data.EntityValue) (common.ChunkID, error) {
	entityVal := entity.EntityValue
	traceID, err := t.getTraceID(entityVal)
	if err != nil {
		return 0, err
	}
	state, fieldsStoreName, dataStoreName, errGetState := t.getState(entityVal)
	if errGetState != nil {
		return 0, errGetState
	}
	stateBytes := []byte{byte(state)}
	tts := uint64(entity.GetTimestamp().AsTime().UnixNano())
	chunkID := t.idGen.Next(tts)
	ts, errParseTS := t.idGen.ParseTS(chunkID)
	if errParseTS != nil {
		return 0, errors.Wrap(errParseTS, "failed to parse timestamp from chunk id")
	}
	tsBytes := convert.Uint64ToBytes(ts)
	intSeriesID := uint64(seriesID)
	seriesIDBytes := convert.Uint64ToBytes(intSeriesID)
	wp := t.writePoint(ts)

	err = wp.TimeSeriesWriter(shardID, dataStoreName).Put(seriesIDBytes, entityVal.GetDataBinary(), ts)
	if err != nil {
		return 0, errors.Wrap(err, "fail to write traceSeries data")
	}

	byteVal, err := proto.Marshal(copyEntityValueWithoutDataBinary(entityVal))
	if err != nil {
		return 0, errors.Wrap(err, "fail to serialize EntityValue to []byte")
	}
	err = wp.TimeSeriesWriter(shardID, fieldsStoreName).Put(seriesIDBytes, byteVal, ts)
	if err != nil {
		return 0, errors.Wrap(err, "failed to write traceSeries fields")
	}

	chunkIDBytes := convert.Uint64ToBytes(chunkID)
	if err = wp.Writer(shardID, chunkIDMapping).Put(chunkIDBytes, bydb_bytes.Join(stateBytes, seriesIDBytes, tsBytes)); err != nil {
		return 0, errors.Wrap(err, "failed to write chunkID index")
	}
	traceIDShardID := partition.ShardID(traceID, t.shardNum)
	if err = wp.TimeSeriesWriter(traceIDShardID, traceIndex).
		Put(traceID, bydb_bytes.Join(convert.Uint16ToBytes(uint16(shardID)), chunkIDBytes), tts); err != nil {
		return 0, errors.Wrap(err, "failed to Trace index")
	}
	err = wp.Writer(shardID, startTimeIndex).Put(bydb_bytes.Join(stateBytes, tsBytes, chunkIDBytes), nil)
	if err != nil {
		return 0, errors.Wrap(err, "failed to write start time index")
	}
	t.l.Debug().Uint64("chunk_id", chunkID).
		Uint64("series_id", intSeriesID).
		Time("ts", time.Unix(0, int64(ts))).
		Uint64("ts_int", ts).
		Int("data_size", len(entityVal.GetDataBinary())).
		Int("fields_num", len(entityVal.GetFields())).
		Hex("trace_id", traceID).
		Uint("trace_shard_id", traceIDShardID).
		Uint("shard_id", shardID).
		Msg("written to Trace series")
	return common.ChunkID(chunkID), err
}

// copyEntityValueWithoutDataBinary copies all fields without DataBinary
func copyEntityValueWithoutDataBinary(ev *v1.EntityValue) *v1.EntityValue {
	return &v1.EntityValue{
		EntityId:   ev.GetEntityId(),
		Timestamp:  ev.GetTimestamp(),
		DataBinary: nil,
		Fields:     ev.GetFields(),
	}
}
