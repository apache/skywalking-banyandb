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
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"

	"github.com/apache/skywalking-banyandb/api/data"
	modelv2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v2"
	streamv2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v2"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

var (
	ErrMalformedElement          = errors.New("element is malformed")
	ErrUnsupportedTagTypeAsEntry = errors.New("the tag type can not be as an entry in an entity")
)

func (s *stream) Write(shardID uint, value *streamv2.ElementValue) error {
	sm := s.schema
	fLen := len(value.GetTagFamilies())
	if fLen < 1 {
		return errors.Wrap(ErrMalformedElement, "no tag family")
	}
	if fLen > len(sm.TagFamilies) {
		return errors.Wrap(ErrMalformedElement, "tag family number is more than expected")
	}
	shard, err := s.db.Shard(shardID)
	if err != nil {
		return err
	}
	entity, err := s.buildEntity(value)
	if err != nil {
		return err
	}
	series, err := shard.Series().Get(entity)
	if err != nil {
		return err
	}
	t := value.GetTimestamp().AsTime()
	wp, err := series.Span(tsdb.TimeRange{
		Start:    t,
		Duration: 0,
	})
	if err != nil {
		return err
	}
	defer func() {
		_ = wp.Close()
	}()
	builder := wp.WriterBuilder().Time(t)
	for fi, family := range value.GetTagFamilies() {
		familySpec := sm.GetTagFamilies()[fi]
		if len(family.GetTags()) > len(familySpec.GetTags()) {
			return errors.Wrap(ErrMalformedElement, "tag number is more than expected")
		}
		for ti, tag := range family.GetTags() {
			tagSpec := familySpec.GetTags()[ti]
			tType, isNull := tagValueTypeConv(tag)
			if isNull {
				continue
			}
			if tType != tagSpec.GetType() {
				return errors.Wrapf(ErrMalformedElement, "tag %s type is unexpected", tagSpec.GetName())
			}
		}
		bb, errMarshal := proto.Marshal(family)
		if errMarshal != nil {
			return errMarshal
		}
		builder.Family(sm.GetTagFamilies()[fi].GetName(), bb)
	}
	writer, err := builder.Build()
	if err != nil {
		return err
	}
	_, err = writer.Write()
	return err
}

func (s *stream) buildEntity(value *streamv2.ElementValue) (entity tsdb.Entity, err error) {
	for _, index := range s.entityIndex {
		family := value.GetTagFamilies()[index.family]
		if index.tag >= len(family.GetTags()) {
			return nil, errors.Wrap(ErrMalformedElement, "the tag which composite the entity doesn't exist ")
		}
		entry, err := tagConvEntry(family.GetTags()[index.tag])
		if err != nil {
			return nil, err
		}
		entity = append(entity, entry)
	}
	return entity, nil
}

func tagConvEntry(tag *modelv2.Tag) (tsdb.Entry, error) {
	switch tag.GetValueType().(type) {
	case *modelv2.Tag_Str:
		return tsdb.Entry(tag.GetStr().GetValue()), nil
	case *modelv2.Tag_Int:
		return convert.Int64ToBytes(tag.GetInt().GetValue()), nil
	default:
		return nil, ErrUnsupportedTagTypeAsEntry
	}
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
	err := w.schemaMap[id].Write(writeEvent.ShardID, writeEvent.WriteRequest.GetElement())
	if err != nil {
		w.l.Debug().Err(err)
	}
	return
}
