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

package stream

import (
	"bytes"
	"strings"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	modelv2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v2"
	streamv2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v2"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

var (
	ErrMalformedElement            = errors.New("element is malformed")
	ErrUnsupportedTagTypeAsEntry   = errors.New("the tag type can not be as an entry in an entity")
	ErrUnsupportedTagForIndexField = errors.New("the tag type(for example, null) can not be as the index field value")
)

func (s *stream) write(shardID common.ShardID, value *streamv2.ElementValue) (*tsdb.GlobalItemID, error) {
	sm := s.schema
	fLen := len(value.GetTagFamilies())
	if fLen < 1 {
		return nil, errors.Wrap(ErrMalformedElement, "no tag family")
	}
	if fLen > len(sm.TagFamilies) {
		return nil, errors.Wrap(ErrMalformedElement, "tag family number is more than expected")
	}
	shard, err := s.db.Shard(shardID)
	if err != nil {
		return nil, err
	}
	entity, err := s.buildEntity(value)
	if err != nil {
		return nil, err
	}
	series, err := shard.Series().Get(entity)
	if err != nil {
		return nil, err
	}
	t := value.GetTimestamp().AsTime()
	wp, err := series.Span(tsdb.NewTimeRangeDuration(t, 0))
	if err != nil {
		if wp != nil {
			_ = wp.Close()
		}
		return nil, err
	}
	writeFn := func() (tsdb.Writer, error) {
		builder := wp.WriterBuilder().Time(t)
		for fi, family := range value.GetTagFamilies() {
			familySpec := sm.GetTagFamilies()[fi]
			if len(family.GetTags()) > len(familySpec.GetTags()) {
				return nil, errors.Wrap(ErrMalformedElement, "tag number is more than expected")
			}
			for ti, tag := range family.GetTags() {
				tagSpec := familySpec.GetTags()[ti]
				tType, isNull := tagValueTypeConv(tag)
				if isNull {
					continue
				}
				if tType != tagSpec.GetType() {
					return nil, errors.Wrapf(ErrMalformedElement, "tag %s type is unexpected", tagSpec.GetName())
				}
			}
			bb, errMarshal := proto.Marshal(family)
			if errMarshal != nil {
				return nil, errMarshal
			}
			builder.Family(sm.GetTagFamilies()[fi].GetName(), bb)
		}
		writer, errWrite := builder.Build()
		if errWrite != nil {
			return nil, errWrite
		}
		_, errWrite = writer.Write()
		s.l.Debug().
			Time("ts", t).
			Int("ts_nano", t.Nanosecond()).
			Interface("data", value).
			Uint64("series_id", uint64(series.ID())).
			Msg("write stream")
		return writer, errWrite
	}
	writer, err := writeFn()
	if err != nil {
		_ = wp.Close()
		return nil, err
	}
	m := indexMessage{
		localWriter: writer,
		value:       value,
		blockCloser: wp,
	}
	go func(m indexMessage) {
		defer func() {
			if recover() != nil {
				_ = m.blockCloser.Close()
			}
		}()
		s.indexCh <- m
	}(m)
	itemID := writer.ItemID()
	return &itemID, err
}

func getIndexValue(ruleIndex indexRule, value *streamv2.ElementValue) (val []byte, err error) {
	val = make([]byte, 0, len(ruleIndex.tagIndices))
	for _, tIndex := range ruleIndex.tagIndices {
		tag, err := getTagByOffset(value, tIndex.family, tIndex.tag)
		if err != nil {
			return nil, errors.WithMessagef(err, "index rule:%v", ruleIndex.rule.Metadata)
		}
		v, err := marshalIndexFieldValue(tag)
		if err != nil {
			return nil, err
		}
		val = append(val, v...)
	}
	return val, nil
}

func marshalIndexFieldValue(tagValue *modelv2.TagValue) ([]byte, error) {
	switch x := tagValue.GetValue().(type) {
	case *modelv2.TagValue_Str:
		return []byte(x.Str.GetValue()), nil
	case *modelv2.TagValue_Int:
		return convert.Int64ToBytes(x.Int.GetValue()), nil
	case *modelv2.TagValue_StrArray:
		return []byte(strings.Join(x.StrArray.GetValue(), strDelimiter)), nil
	case *modelv2.TagValue_IntArray:
		buf := bytes.NewBuffer(nil)
		for _, i := range x.IntArray.GetValue() {
			buf.Write(convert.Int64ToBytes(i))
		}
		return buf.Bytes(), nil
	case *modelv2.TagValue_BinaryData:
		return x.BinaryData, nil
	}
	return nil, ErrUnsupportedTagForIndexField
}

func (s *stream) buildEntity(value *streamv2.ElementValue) (tsdb.Entity, error) {
	entity := make(tsdb.Entity, len(s.entityIndex))
	for i, index := range s.entityIndex {
		tag, err := getTagByOffset(value, index.family, index.tag)
		if err != nil {
			return nil, err
		}
		e, errMarshal := marshalIndexFieldValue(tag)
		if errMarshal != nil {
			return nil, errMarshal
		}
		entity[i] = e
	}
	return entity, nil
}

func getTagByOffset(value *streamv2.ElementValue, fIndex, tIndex int) (*modelv2.TagValue, error) {
	if fIndex >= len(value.TagFamilies) {
		return nil, errors.Wrap(ErrMalformedElement, "tag family offset is invalid")
	}
	family := value.GetTagFamilies()[fIndex]
	if tIndex >= len(family.GetTags()) {
		return nil, errors.Wrap(ErrMalformedElement, "tag offset is invalid")
	}
	return family.GetTags()[tIndex], nil
}

type writeCallback struct {
	l         *logger.Logger
	schemaMap map[string]*stream
}

func setUpWriteCallback(l *logger.Logger, schemaMap map[string]*stream) *writeCallback {
	wcb := &writeCallback{
		l:         l,
		schemaMap: schemaMap,
	}
	return wcb
}

func (w *writeCallback) Rev(message bus.Message) (resp bus.Message) {
	writeEvent, ok := message.Data().(data.StreamWriteData)
	if !ok {
		w.l.Warn().Msg("invalid event data type")
		return
	}
	sm := writeEvent.WriteRequest.GetMetadata()
	id := formatStreamID(sm.GetName(), sm.GetGroup())
	_, err := w.schemaMap[id].write(common.ShardID(writeEvent.ShardID), writeEvent.WriteRequest.GetElement())
	if err != nil {
		w.l.Debug().Err(err)
	}
	return
}
