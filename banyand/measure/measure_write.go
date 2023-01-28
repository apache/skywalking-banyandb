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

package measure

import (
	"bytes"
	"context"
	"time"

	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/api/common"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/banyand/tsdb/index"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

var errMalformedElement = errors.New("element is malformed")

func (s *measure) write(shardID common.ShardID, entity []byte, entityValues tsdb.EntityValues,
	value *measurev1.DataPointValue,
) error {
	t := value.GetTimestamp().AsTime().Local()
	if err := timestamp.Check(t); err != nil {
		return errors.WithMessage(err, "writing stream")
	}
	fLen := len(value.GetTagFamilies())
	if fLen < 1 {
		return errors.Wrap(errMalformedElement, "no tag family")
	}
	if fLen > len(s.schema.GetTagFamilies()) {
		return errors.Wrap(errMalformedElement, "tag family number is more than expected")
	}
	shard, err := s.databaseSupplier.SupplyTSDB().Shard(shardID)
	if err != nil {
		return err
	}
	series, err := shard.Series().Get(entity, entityValues)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	wp, err := series.Create(ctx, t)
	if err != nil {
		if wp != nil {
			_ = wp.Close()
		}
		return err
	}
	writeFn := func() (tsdb.Writer, error) {
		builder := wp.WriterBuilder().Time(t)
		for fi, family := range value.GetTagFamilies() {
			spec := s.schema.GetTagFamilies()[fi]
			bb, errMarshal := pbv1.EncodeFamily(spec, family)
			if errMarshal != nil {
				return nil, errMarshal
			}
			builder.Family(familyIdentity(spec.GetName(), pbv1.TagFlag), bb)
		}
		if len(value.GetFields()) > len(s.schema.GetFields()) {
			return nil, errors.Wrap(errMalformedElement, "fields number is more than expected")
		}
		for fi, fieldValue := range value.GetFields() {
			fieldSpec := s.schema.GetFields()[fi]
			fType, isNull := pbv1.FieldValueTypeConv(fieldValue)
			if isNull {
				s.l.Warn().RawJSON("written", logger.Proto(value)).Msg("ignore null field")
				continue
			}
			if fType != fieldSpec.GetFieldType() {
				return nil, errors.Wrapf(errMalformedElement, "field %s type is unexpected", fieldSpec.GetName())
			}
			data := encodeFieldValue(fieldValue)
			if data == nil {
				s.l.Warn().RawJSON("written", logger.Proto(value)).Msg("ignore unknown field")
				continue
			}
			builder.Family(familyIdentity(s.schema.GetFields()[fi].GetName(), pbv1.EncoderFieldFlag(fieldSpec, s.interval)), data)
		}
		writer, errWrite := builder.Build()
		if errWrite != nil {
			return nil, errWrite
		}
		_, errWrite = writer.Write()
		if e := s.l.Named(s.schema.GetMetadata().GetGroup(), s.schema.GetMetadata().GetName()).Debug(); e.Enabled() {
			e.Time("ts", t).
				Int("ts_nano", t.Nanosecond()).
				RawJSON("data", logger.Proto(value)).
				Uint64("series_id", uint64(series.ID())).
				Stringer("series", series).
				Uint64("item_id", uint64(writer.ItemID().ID)).
				Int("shard_id", int(shardID)).
				Msg("write measure")
		}
		return writer, errWrite
	}
	writer, err := writeFn()
	if err != nil {
		_ = wp.Close()
		return err
	}
	m := index.Message{
		LocalWriter: writer,
		Value: index.Value{
			TagFamilies: value.GetTagFamilies(),
			Timestamp:   t,
		},
		BlockCloser: wp,
	}
	s.indexWriter.Write(m)
	if s.processorManager != nil {
		s.processorManager.onMeasureWrite(&measurev1.WriteRequest{
			Metadata:  s.GetMetadata(),
			DataPoint: value,
		})
	}
	return err
}

type writeCallback struct {
	l          *logger.Logger
	schemaRepo *schemaRepo
}

func setUpWriteCallback(l *logger.Logger, schemaRepo *schemaRepo) bus.MessageListener {
	return &writeCallback{
		l:          l,
		schemaRepo: schemaRepo,
	}
}

func (w *writeCallback) Rev(message bus.Message) (resp bus.Message) {
	writeEvent, ok := message.Data().(*measurev1.InternalWriteRequest)
	if !ok {
		w.l.Warn().Msg("invalid event data type")
		return
	}

	stm, ok := w.schemaRepo.loadMeasure(writeEvent.GetRequest().GetMetadata())
	if !ok {
		w.l.Warn().Msg("cannot find measure definition")
		return
	}
	err := stm.write(common.ShardID(writeEvent.GetShardId()),
		writeEvent.SeriesHash, tsdb.DecodeEntityValues(writeEvent.GetEntityValues()), writeEvent.GetRequest().GetDataPoint())
	if err != nil {
		w.l.Error().Err(err).RawJSON("written", logger.Proto(writeEvent)).Msg("fail to write entity")
	}
	return
}

func familyIdentity(name string, flag []byte) []byte {
	return bytes.Join([][]byte{tsdb.Hash([]byte(name)), flag}, nil)
}

func encodeFieldValue(fieldValue *modelv1.FieldValue) []byte {
	switch fieldValue.GetValue().(type) {
	case *modelv1.FieldValue_Int:
		return convert.Int64ToBytes(fieldValue.GetInt().GetValue())
	case *modelv1.FieldValue_Float:
		return convert.Float64ToBytes(fieldValue.GetFloat().GetValue())
	case *modelv1.FieldValue_Str:
		return []byte(fieldValue.GetStr().Value)
	case *modelv1.FieldValue_BinaryData:
		return fieldValue.GetBinaryData()
	}
	return nil
}
