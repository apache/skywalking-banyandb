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

	"github.com/apache/skywalking-banyandb/api/common"
	streamv2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v2"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/partition"
	pbv2 "github.com/apache/skywalking-banyandb/pkg/pb/v2"
)

var (
	ErrMalformedElement            = errors.New("element is malformed")
	ErrUnsupportedTagForIndexField = errors.New("the tag type(for example, null) can not be as the index field value")
)

func (s *stream) Write(value *streamv2.ElementValue) error {
	entity, shardID, err := s.entityLocator.Locate(value.GetTagFamilies(), s.schema.GetShardNum())
	if err != nil {
		return err
	}
	waitCh := make(chan struct{})
	err = s.write(shardID, tsdb.HashEntity(entity), value, func() {
		close(waitCh)
	})
	if err != nil {
		close(waitCh)
		return err
	}
	<-waitCh
	return nil
}

func (s *stream) write(shardID common.ShardID, seriesHashKey []byte, value *streamv2.ElementValue, cb callbackFn) error {
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
	series, err := shard.Series().GetByHashKey(seriesHashKey)
	if err != nil {
		return err
	}
	t := value.GetTimestamp().AsTime()
	wp, err := series.Span(tsdb.NewTimeRangeDuration(t, 0))
	if err != nil {
		if wp != nil {
			_ = wp.Close()
		}
		return err
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
		builder.Val([]byte(value.GetElementId()))
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
			Int("shard_id", int(shardID)).
			Msg("write stream")
		return writer, errWrite
	}
	writer, err := writeFn()
	if err != nil {
		_ = wp.Close()
		return err
	}
	m := indexMessage{
		localWriter: writer,
		value:       value,
		blockCloser: wp,
		cb:          cb,
	}
	go func(m indexMessage) {
		defer func() {
			if recover() != nil {
				_ = m.blockCloser.Close()
			}
		}()
		s.indexCh <- m
	}(m)
	return err
}

func getIndexValue(ruleIndex indexRule, value *streamv2.ElementValue) (val []byte, isInt bool, err error) {
	val = make([]byte, 0, len(ruleIndex.tagIndices))
	var existInt bool
	for _, tIndex := range ruleIndex.tagIndices {
		tag, err := partition.GetTagByOffset(value.GetTagFamilies(), tIndex.FamilyOffset, tIndex.TagOffset)
		if err != nil {
			return nil, false, errors.WithMessagef(err, "index rule:%v", ruleIndex.rule.Metadata)
		}
		if tag.GetInt() != nil {
			existInt = true
		}
		v, err := pbv2.MarshalIndexFieldValue(tag)
		if err != nil {
			return nil, false, err
		}
		val = append(val, v...)
	}
	if len(ruleIndex.tagIndices) == 1 && existInt {
		return val, true, nil
	}
	return val, false, nil
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
	writeEvent, ok := message.Data().(*streamv2.InternalWriteRequest)
	if !ok {
		w.l.Warn().Msg("invalid event data type")
		return
	}
	sm := writeEvent.GetRequest().GetMetadata()
	id := formatStreamID(sm.GetName(), sm.GetGroup())
	err := w.schemaMap[id].write(common.ShardID(writeEvent.GetShardId()), writeEvent.GetSeriesHash(), writeEvent.GetRequest().GetElement(), nil)
	if err != nil {
		w.l.Debug().Err(err)
	}
	return
}
