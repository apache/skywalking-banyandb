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

package index

import (
	"context"
	"io"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/multierr"

	"github.com/apache/skywalking-banyandb/api/common"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/partition"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

type CallbackFn func()

type Message struct {
	Scope       tsdb.Entry
	Value       Value
	LocalWriter tsdb.Writer
	BlockCloser io.Closer
	Cb          CallbackFn
}

type Value struct {
	TagFamilies []*modelv1.TagFamilyForWrite
	Timestamp   time.Time
}

type WriterOptions struct {
	ShardNum   uint32
	Families   []*databasev1.TagFamilySpec
	IndexRules []*databasev1.IndexRule
	DB         tsdb.Supplier
}

type Writer struct {
	l              *logger.Logger
	db             tsdb.Supplier
	shardNum       uint32
	ch             *run.Chan[Message]
	indexRuleIndex []*partition.IndexRuleLocator
}

func NewWriter(ctx context.Context, options WriterOptions) *Writer {
	w := new(Writer)
	parentLogger := ctx.Value(logger.ContextKey)
	if parentLogger != nil {
		if pl, ok := parentLogger.(*logger.Logger); ok {
			w.l = pl.Named("index-writer")
		}
	}
	w.shardNum = options.ShardNum
	w.db = options.DB
	w.indexRuleIndex = partition.ParseIndexRuleLocators(options.Families, options.IndexRules)
	w.ch = run.NewChan[Message](make(chan Message))
	w.bootIndexGenerator()
	return w
}

func (s *Writer) Write(value Message) {
	go func(m Message) {
		s.ch.Write(m)
	}(value)
}

func (s *Writer) Close() error {
	return s.ch.Close()
}

func (s *Writer) bootIndexGenerator() {
	go func() {
		for {
			m, more := s.ch.Read()
			if !more {
				return
			}
			var err error
			for _, ruleIndex := range s.indexRuleIndex {
				rule := ruleIndex.Rule
				switch rule.GetLocation() {
				case databasev1.IndexRule_LOCATION_SERIES:
					err = multierr.Append(err, writeLocalIndex(m.LocalWriter, ruleIndex, m.Value))
				case databasev1.IndexRule_LOCATION_GLOBAL:
					err = multierr.Append(err, s.writeGlobalIndex(m.Scope, ruleIndex, m.LocalWriter.ItemID(), m.Value))
				}
			}
			err = multierr.Append(err, m.BlockCloser.Close())
			if err != nil {
				s.l.Error().Err(err).Msg("encounter some errors when generating indices")
			}
			if m.Cb != nil {
				m.Cb()
			}
		}
	}()
}

// TODO: should listen to pipeline in a distributed cluster
func (s *Writer) writeGlobalIndex(scope tsdb.Entry, ruleIndex *partition.IndexRuleLocator, ref tsdb.GlobalItemID, value Value) error {
	values, _, err := getIndexValue(ruleIndex, value)
	if err != nil {
		return err
	}
	if values == nil {
		return nil
	}
	var errWriting error
	for _, val := range values {
		indexShardID, err := partition.ShardID(val, s.shardNum)
		if err != nil {
			return err
		}
		shard, err := s.db.SupplyTSDB().Shard(common.ShardID(indexShardID))
		if err != nil {
			return err
		}
		builder := shard.Index().WriterBuilder()
		indexWriter, err := builder.
			Scope(scope).
			GlobalItemID(ref).
			Time(value.Timestamp).
			Build()
		if err != nil {
			return err
		}
		rule := ruleIndex.Rule
		switch rule.GetType() {
		case databasev1.IndexRule_TYPE_INVERTED:
			errWriting = multierr.Append(errWriting, indexWriter.WriteInvertedIndex(index.Field{
				Key: index.FieldKey{
					IndexRuleID: rule.GetMetadata().GetId(),
				},
				Term: val,
			}))
		case databasev1.IndexRule_TYPE_TREE:
			errWriting = multierr.Append(errWriting, indexWriter.WriteLSMIndex(index.Field{
				Key: index.FieldKey{
					IndexRuleID: rule.GetMetadata().GetId(),
				},
				Term: val,
			}))
		}
	}
	return errWriting
}

func writeLocalIndex(writer tsdb.Writer, ruleIndex *partition.IndexRuleLocator, value Value) (err error) {
	values, _, err := getIndexValue(ruleIndex, value)
	if err != nil {
		return err
	}
	if values == nil {
		return nil
	}
	var errWriting error
	for _, val := range values {
		rule := ruleIndex.Rule
		switch rule.GetType() {
		case databasev1.IndexRule_TYPE_INVERTED:
			errWriting = multierr.Append(errWriting, writer.WriteInvertedIndex(index.Field{
				Key: index.FieldKey{
					IndexRuleID: rule.GetMetadata().GetId(),
				},
				Term: val,
			}))
		case databasev1.IndexRule_TYPE_TREE:
			errWriting = multierr.Append(errWriting, writer.WriteLSMIndex(index.Field{
				Key: index.FieldKey{
					IndexRuleID: rule.GetMetadata().GetId(),
				},
				Term: val,
			}))
		}
	}

	return errWriting
}

var ErrUnsupportedIndexType = errors.New("unsupported index type")

func getIndexValue(ruleIndex *partition.IndexRuleLocator, value Value) (val [][]byte, isInt bool, err error) {
	val = make([][]byte, 0)
	var existInt bool
	if len(ruleIndex.TagIndices) != 1 {
		return nil, false, errors.Wrap(ErrUnsupportedIndexType, "the index rule didn't support composited tags")
	}
	tIndex := ruleIndex.TagIndices[0]
	tag, err := partition.GetTagByOffset(value.TagFamilies, tIndex.FamilyOffset, tIndex.TagOffset)

	if errors.Is(err, partition.ErrMalformedElement) {
		return val, false, nil
	}
	if err != nil {
		return nil, false, errors.WithMessagef(err, "index rule:%v", ruleIndex.Rule.Metadata)
	}
	if tag.GetInt() != nil {
		existInt = true
	}
	fv, err := pbv1.ParseIndexFieldValue(tag)
	if errors.Is(err, pbv1.ErrNullValue) {
		return nil, existInt, nil
	}
	if err != nil {
		return nil, false, err
	}
	v := fv.GetValue()
	if v != nil {
		val = append(val, v)
		return val, existInt, nil
	}
	arr := fv.GetArr()
	if arr != nil {
		val = append(val, arr...)
	}
	return val, existInt, nil
}
