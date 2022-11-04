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
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/apache/skywalking-banyandb/api/common"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/banyand/tsdb/index"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

var (
	ErrMalformedElement = errors.New("element is malformed")
	writtenBytes        *prometheus.CounterVec
)

func init() {
	labels := []string{"group"}
	writtenBytes = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "banyand_written_stream_bytes",
			Help: "written stream in bytes",
		},
		labels,
	)
}

func (s *stream) Write(value *streamv1.ElementValue) error {
	entity, shardID, err := s.entityLocator.Locate(s.name, value.GetTagFamilies(), s.shardNum)
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
	tp := value.GetTimestamp().AsTime()
	if err := timestamp.Check(tp); err != nil {
		return errors.WithMessage(err, "writing stream")
	}
	sm := s.schema
	fLen := len(value.GetTagFamilies())
	if fLen < 1 {
		return errors.Wrap(ErrMalformedElement, "no tag family")
	}
	if fLen > len(sm.TagFamilies) {
		return errors.Wrap(ErrMalformedElement, "tag family number is more than expected")
	}
	shard, err := s.db.SupplyTSDB().Shard(shardID)
	if err != nil {
		return err
	}
	series, err := shard.Series().GetByHashKey(seriesHashKey)
	if err != nil {
		return err
	}
	t := timestamp.MToN(tp)
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
		size := 0
		for fi, family := range value.GetTagFamilies() {
			spec := sm.GetTagFamilies()[fi]
			bb, errMarshal := pbv1.EncodeFamily(spec, family)
			if errMarshal != nil {
				return nil, errMarshal
			}
			builder.Family(tsdb.Hash([]byte(spec.GetName())), bb)
			size += len(bb)
		}
		builder.Val([]byte(value.GetElementId()))
		writer, errWrite := builder.Build()
		if errWrite != nil {
			return nil, errWrite
		}
		_, errWrite = writer.Write()
		writtenBytes.WithLabelValues(s.group).Add(float64(size))
		s.l.Debug().
			Time("ts", t).
			Int("ts_nano", t.Nanosecond()).
			Interface("data", value).
			Uint64("series_id", uint64(series.ID())).
			Uint64("item_id", uint64(writer.ItemID().ID)).
			Int("shard_id", int(shardID)).
			Str("stream", sm.Metadata.GetName()).
			Msg("write stream")
		return writer, errWrite
	}
	writer, err := writeFn()
	if err != nil {
		_ = wp.Close()
		return err
	}
	m := index.Message{
		Scope:       tsdb.Entry(s.name),
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
	l          *logger.Logger
	schemaRepo *schemaRepo
}

func setUpWriteCallback(l *logger.Logger, schemaRepo *schemaRepo) *writeCallback {
	wcb := &writeCallback{
		l:          l,
		schemaRepo: schemaRepo,
	}
	return wcb
}

func (w *writeCallback) Rev(message bus.Message) (resp bus.Message) {
	writeEvent, ok := message.Data().(*streamv1.InternalWriteRequest)
	if !ok {
		w.l.Warn().Msg("invalid event data type")
		return
	}
	stm, ok := w.schemaRepo.loadStream(writeEvent.GetRequest().GetMetadata())
	if !ok {
		w.l.Warn().Msg("cannot find stream definition")
		return
	}
	err := stm.write(common.ShardID(writeEvent.GetShardId()), writeEvent.GetSeriesHash(), writeEvent.GetRequest().GetElement(), nil)
	if err != nil {
		w.l.Error().Err(err).Msg("fail to write entity")
	}
	return
}
