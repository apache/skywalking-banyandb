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
	"sync"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"

	"github.com/apache/skywalking-banyandb/api/common"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/banyand/tsdb/index"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

var (
	ErrMalformedElement = errors.New("element is malformed")
)

func (s *stream) Write(value *streamv1.ElementValue) error {
	entity, shardID, err := s.entityLocator.Locate(value.GetTagFamilies(), s.schema.GetOpts().GetShardNum())
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

func (s *stream) write(shardID common.ShardID, seriesHashKey []byte, value *streamv1.ElementValue, cb index.CallbackFn) error {
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
				tType, isNull := pbv1.TagValueTypeConv(tag)
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
			builder.Family([]byte(sm.GetTagFamilies()[fi].GetName()), bb)
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
			Msg("write measure")
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
			Timestamp:   value.GetTimestamp().AsTime(),
		},
		BlockCloser: wp,
		Cb:          cb,
	}
	s.indexWriter.Write(m)
	return err
}

type writeCallback struct {
	l         *logger.Logger
	schemaMap *sync.Map
}

func setUpWriteCallback(l *logger.Logger, schemaMap *sync.Map) *writeCallback {
	wcb := &writeCallback{
		l:         l,
		schemaMap: schemaMap,
	}
	return wcb
}

func (w *writeCallback) Rev(message bus.Message) (resp bus.Message) {
	writeEvent, ok := message.Data().(*streamv1.InternalWriteRequest)
	if !ok {
		w.l.Warn().Msg("invalid event data type")
		return
	}
	sm := writeEvent.GetRequest().GetMetadata()
	id := formatStreamID(sm.GetName(), sm.GetGroup())
	val, ok := w.schemaMap.Load(id)
	if !ok {
		w.l.Warn().Msg("cannot find stream definition")
		return
	}

	if stm, ok := val.(*stream); ok {
		err := stm.write(common.ShardID(writeEvent.GetShardId()), writeEvent.GetSeriesHash(), writeEvent.GetRequest().GetElement(), nil)
		if err != nil {
			w.l.Debug().Err(err).Msg("fail to write entity")
		}
	}

	return
}
