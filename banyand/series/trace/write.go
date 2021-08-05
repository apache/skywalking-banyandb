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
	"go.uber.org/multierr"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	v1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/v1"
	"github.com/apache/skywalking-banyandb/banyand/index"
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
	entityTs := uint64(entity.GetTimestamp().AsTime().UnixNano())
	chunkID := t.idGen.Next(entityTs)
	wallTs, errParseTS := t.idGen.ParseTS(chunkID)
	if errParseTS != nil {
		return 0, errors.Wrap(errParseTS, "failed to parse timestamp from chunk id")
	}
	wallTsBytes := convert.Uint64ToBytes(wallTs)
	intSeriesID := uint64(seriesID)
	seriesIDBytes := convert.Uint64ToBytes(intSeriesID)
	wp := t.writePoint(wallTs)

	err = wp.TimeSeriesWriter(shardID, dataStoreName).Put(seriesIDBytes, entityVal.GetDataBinary(), wallTs)
	if err != nil {
		return 0, errors.Wrap(err, "fail to write traceSeries data")
	}

	byteVal, err := proto.Marshal(copyEntityValueWithoutDataBinary(entityVal))
	if err != nil {
		return 0, errors.Wrap(err, "fail to serialize EntityValue to []byte")
	}
	err = wp.TimeSeriesWriter(shardID, fieldsStoreName).Put(seriesIDBytes, byteVal, wallTs)
	if err != nil {
		return 0, errors.Wrap(err, "failed to write traceSeries fields")
	}

	chunkIDBytes := convert.Uint64ToBytes(chunkID)
	if err = wp.Writer(shardID, chunkIDMapping).Put(chunkIDBytes, bydb_bytes.Join(stateBytes, seriesIDBytes, wallTsBytes)); err != nil {
		return 0, errors.Wrap(err, "failed to write chunkID index")
	}
	traceIDShardID := partition.ShardID(traceID, t.shardNum)
	if err = wp.TimeSeriesWriter(traceIDShardID, traceIndex).
		Put(traceID, bydb_bytes.Join(convert.Uint16ToBytes(uint16(shardID)), chunkIDBytes), entityTs); err != nil {
		return 0, errors.Wrap(err, "failed to Trace index")
	}
	//entityTsBytes := convert.Uint64ToBytes(entityTs)
	err = wp.Writer(shardID, startTimeIndex).Put(bydb_bytes.Join(stateBytes, wallTsBytes, chunkIDBytes), nil)
	if err != nil {
		return 0, errors.Wrap(err, "failed to write start time index")
	}
	t.l.Debug().Uint64("chunk_id", chunkID).
		Uint64("series_id", intSeriesID).
		Time("wallTs", time.Unix(0, int64(wallTs))).
		Uint64("wallTs_int", wallTs).
		Int("data_size", len(entityVal.GetDataBinary())).
		Int("fields_num", len(entityVal.GetFields())).
		Hex("trace_id", traceID).
		Uint("trace_shard_id", traceIDShardID).
		Uint("shard_id", shardID).
		Msg("written to Trace series")
	id := common.ChunkID(chunkID)
	for i, field := range entityVal.GetFields() {
		fieldSpec := t.schema.Spec.GetFields()[i]
		fieldName := fieldSpec.GetName()
		switch x := field.ValueType.(type) {
		case *v1.Field_Str:
			err = multierr.Append(err, t.writeStrToIndex(shardID, id, fieldName, x.Str.GetValue()))
		case *v1.Field_Int:
			err = multierr.Append(err, t.writeIntToIndex(shardID, id, fieldName, x.Int.GetValue()))
		case *v1.Field_StrArray:
			for _, s := range x.StrArray.GetValue() {
				err = multierr.Append(err, t.writeStrToIndex(shardID, id, fieldName, s))
			}
		case *v1.Field_IntArray:
			for _, integer := range x.IntArray.GetValue() {
				err = multierr.Append(err, t.writeIntToIndex(shardID, id, fieldName, integer))
			}
		default:
			continue
		}
	}
	return common.ChunkID(chunkID), err
}

func (t *traceSeries) writeIntToIndex(shardID uint, id common.ChunkID, name string, value int64) error {
	return t.writeIndex(shardID, id, name, convert.Int64ToBytes(value))
}

func (t *traceSeries) writeStrToIndex(shardID uint, id common.ChunkID, name string, value string) error {
	return t.writeIndex(shardID, id, name, []byte(value))
}

func (t *traceSeries) writeIndex(shardID uint, id common.ChunkID, name string, value []byte) error {
	return t.idx.Insert(*common.NewMetadata(&v1.Metadata{
		Name:  t.name,
		Group: t.group,
	}),
		shardID,
		&index.Field{
			ChunkID: id,
			Name:    name,
			Value:   value,
		},
	)
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
