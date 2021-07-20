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
	"encoding/hex"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/pkg/errors"
	"go.uber.org/multierr"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	v1 "github.com/apache/skywalking-banyandb/api/fbs/v1"
	"github.com/apache/skywalking-banyandb/banyand/kv"
	"github.com/apache/skywalking-banyandb/banyand/series"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/fb"
	"github.com/apache/skywalking-banyandb/pkg/partition"
	"github.com/apache/skywalking-banyandb/pkg/posting"
	"github.com/apache/skywalking-banyandb/pkg/posting/roaring"
)

func (t *traceSeries) FetchTrace(traceID string, opt series.ScanOptions) (trace data.Trace, err error) {
	if traceID == "" {
		return trace, ErrInvalidTraceID
	}
	traceIDBytes := []byte(traceID)
	traceIDShardID := partition.ShardID(traceIDBytes, t.shardNum)
	bb, errTraceID := t.reader.TimeSeriesReader(traceIDShardID, traceIndex, 0, 0).GetAll(traceIDBytes)
	if errTraceID != nil {
		return trace, errTraceID
	}
	t.l.Debug().Uint("shard_id", traceIDShardID).
		Str("trace_id", traceID).
		Hex("trace_id_bytes", traceIDBytes).
		Int("chunk_num", len(bb)).Msg("fetch Trace by trace_id")
	if len(bb) < 1 {
		return trace, nil
	}
	dataMap := make(map[uint]posting.List)
	for _, b := range bb {
		id := idWithShard{
			id:      common.ChunkID(convert.BytesToUint64(b[2:])),
			shardID: uint(convert.BytesToUint16(b[:2])),
		}
		placeID(dataMap, id)
	}
	var entities []data.Entity
	for s, c := range dataMap {
		ee, errEntity := t.FetchEntity(c, s, opt)
		if errEntity != nil {
			err = multierr.Append(err, errEntity)
			continue
		}
		entities = append(entities, ee...)
	}
	return data.Trace{
		KindVersion: data.TraceKindVersion,
		Entities:    entities,
	}, err
}

func (t *traceSeries) ScanEntity(startTime, endTime uint64, opt series.ScanOptions) ([]data.Entity, error) {
	total := opt.Limit
	if total < 1 {
		total = 10
	}
	states := make([]byte, 0, 2)
	switch opt.State {
	case series.TraceStateSuccess:
		states = append(states, StateSuccess)
	case series.TraceStateError:
		states = append(states, StateError)
	case series.TraceStateDefault:
		states = append(states, StateSuccess, StateError)
	}
	seekKeys := make([][]byte, 0, len(states))
	startTimeBytes := convert.Uint64ToBytes(startTime)
	for _, state := range states {
		key := make([]byte, 8+1)
		key[0] = state
		copy(key[1:], startTimeBytes)
		seekKeys = append(seekKeys, key)
	}
	entities := make([]data.Entity, 0, total)
	var num uint32
	opts := kv.DefaultScanOpts
	opts.PrefetchValues = false
	opts.PrefetchSize = int(total)
	var errAll error
	for i := uint(0); i < t.shardNum; i++ {
		chunkIDs := roaring.NewPostingList()
		for _, seekKey := range seekKeys {
			state := seekKey[0]
			err := t.reader.Reader(i, startTimeIndex, startTime, endTime).Scan(
				seekKey,
				opts,
				func(shardID int, key []byte, _ func() ([]byte, error)) error {
					if len(key) <= 9 {
						return errors.Wrapf(ErrInvalidKey, "key:%s", hex.EncodeToString(key))
					}
					if key[0] != state {
						return kv.ErrStopScan
					}
					ts := convert.BytesToUint64(key[1 : 8+1])
					if ts > endTime {
						return nil
					}
					chunk := make([]byte, len(key)-8-1)
					copy(chunk, key[8+1:])
					chunkIDs.Insert(common.ChunkID(convert.BytesToUint64(chunk)))
					num++
					if num > total {
						return kv.ErrStopScan
					}
					return nil
				})
			if err != nil {
				errAll = multierr.Append(errAll, err)
			}
		}
		if chunkIDs.IsEmpty() {
			continue
		}
		ee, err := t.FetchEntity(chunkIDs, i, opt)
		if err != nil {
			errAll = multierr.Append(errAll, err)
			continue
		}
		entities = append(entities, ee...)
	}
	return entities, errAll
}

func (t *traceSeries) FetchEntity(chunkIDs posting.List, shardID uint, opt series.ScanOptions) (entities []data.Entity, err error) {
	chunkIDsLen := chunkIDs.Len()
	if chunkIDsLen < 1 {
		return nil, ErrChunkIDsEmpty
	}
	entities = make([]data.Entity, 0, chunkIDsLen)
	fetchDataBinary, fetchFieldsIndices, errInfo := t.parseFetchInfo(opt)
	if errInfo != nil {
		return nil, errInfo
	}
	if !fetchDataBinary && len(fetchFieldsIndices) < 1 {
		return nil, ErrProjectionEmpty
	}

	for iter := chunkIDs.Iterator(); iter.Next(); {
		id := iter.Current()
		chunkID := uint64(id)
		ts, errParseTS := t.idGen.ParseTS(chunkID)
		if errParseTS != nil {
			err = multierr.Append(err, errParseTS)
		}
		ref, chunkErr := t.reader.Reader(shardID, chunkIDMapping, ts, ts).Get(convert.Uint64ToBytes(chunkID))
		if chunkErr != nil {
			err = multierr.Append(err, chunkErr)
			continue
		}
		sRef := ref[:len(ref)-8]
		seriesID := sRef[1:]
		state := sRef[0]

		t.l.Debug().
			Uint64("chunk_id", chunkID).
			Hex("id", ref).
			Uint64("series_id", convert.BytesToUint64(seriesID)).
			Uint("shard_id", shardID).
			Time("ts", time.Unix(0, int64(ts))).
			Uint64("ts_int", ts).
			Msg("fetch internal id by chunk_id")
		entity, errGet := t.getEntityByInternalRef(seriesID, State(state), fetchDataBinary, fetchFieldsIndices, shardID, ts)
		if errGet != nil {
			err = multierr.Append(err, errGet)
			continue
		}
		t.l.Debug().
			Hex("entity_id", entity.EntityId()).
			Int("fields_num", entity.FieldsLength()).
			Int("data_binary_size_bytes", entity.DataBinaryLength()).
			Msg("fetch entity")
		entities = append(entities, entity)
	}
	return entities, err
}

func (t *traceSeries) parseFetchInfo(opt series.ScanOptions) (fetchDataBinary bool, fetchFieldsIndices []fb.FieldEntry, err error) {
	fetchFieldsIndices = make([]fb.FieldEntry, 0)
	for _, p := range opt.Projection {
		if p == common.DataBinaryFieldName {
			fetchDataBinary = true
			t.l.Debug().Msg("to fetch data binary")
			continue
		}
		index, ok := t.fieldIndex[p]
		if !ok {
			return false, nil, errors.Wrapf(ErrFieldNotFound, "field name:%s", p)
		}
		fetchFieldsIndices = append(fetchFieldsIndices, fb.FieldEntry{
			Key:   p,
			Index: int(index),
		})
		t.l.Debug().Str("name", p).Uint("index", index).Msg("to fetch the field")
	}
	return fetchDataBinary, fetchFieldsIndices, nil
}

func (t *traceSeries) getEntityByInternalRef(seriesID []byte, state State, fetchDataBinary bool,
	fetchFieldsIndices []fb.FieldEntry, shardID uint, ts uint64) (data.Entity, error) {
	fieldsStore, dataStore, err := getStoreName(state)
	if err != nil {
		return data.Entity{}, err
	}
	b := flatbuffers.NewBuilder(0)
	var fieldsOffset flatbuffers.UOffsetT
	val, getErr := t.reader.TimeSeriesReader(shardID, fieldsStore, ts, ts).Get(seriesID, ts)
	if getErr != nil {
		return data.Entity{}, getErr
	}
	entityVal := v1.GetRootAsEntityValue(val, 0)
	entityIDOffset := b.CreateByteString(entityVal.EntityId())
	timestamp := entityVal.TimestampNanoseconds()
	if len(fetchFieldsIndices) > 0 {
		fieldsOffset = fb.Transform(entityVal, fetchFieldsIndices, b)
	}
	var dataBinary flatbuffers.UOffsetT
	if fetchDataBinary {
		val, getErr = t.reader.TimeSeriesReader(shardID, dataStore, ts, ts).Get(seriesID, ts)
		if getErr != nil {
			return data.Entity{}, getErr
		}
		dataBinary = b.CreateByteVector(val)
	}
	v1.EntityValueStart(b)
	v1.EntityAddEntityId(b, entityIDOffset)
	v1.EntityAddTimestampNanoseconds(b, timestamp)
	if fieldsOffset > 0 {
		v1.EntityAddFields(b, fieldsOffset)
	}
	if dataBinary > 0 {
		v1.EntityAddDataBinary(b, dataBinary)
	}
	b.Finish(v1.EntityValueEnd(b))
	return data.Entity{
		Entity: v1.GetRootAsEntity(b.FinishedBytes(), 0),
	}, nil
}

type idWithShard struct {
	id      common.ChunkID
	shardID uint
}

func placeID(chunkIDCriteria map[uint]posting.List, data idWithShard) {
	list, ok := chunkIDCriteria[data.shardID]
	if ok {
		list.Insert(data.id)
		return
	}
	list = roaring.NewPostingList()
	list.Insert(data.id)
	chunkIDCriteria[data.shardID] = list
}
