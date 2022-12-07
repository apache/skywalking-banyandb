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

// Package index implements transferring data to indices.
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
)

// Message wraps value and other info to generate relative indices.
type Message struct {
	Value       Value
	LocalWriter tsdb.Writer
	BlockCloser io.Closer
	Scope       tsdb.Entry
}

// Value represents the input data for generating indices.
type Value struct {
	Timestamp   time.Time
	TagFamilies []*modelv1.TagFamilyForWrite
}

// WriterOptions wrap all options to create an index writer.
type WriterOptions struct {
	DB                tsdb.Supplier
	Families          []*databasev1.TagFamilySpec
	IndexRules        []*databasev1.IndexRule
	ShardNum          uint32
	EnableGlobalIndex bool
}

const (
	local = 1 << iota
	global
	inverted
	tree
)

// Writer generates indices based on index rules.
type Writer struct {
	db                tsdb.Supplier
	l                 *logger.Logger
	invertRuleIndex   map[byte][]*partition.IndexRuleLocator
	shardNum          uint32
	enableGlobalIndex bool
}

// NewWriter returns a new Writer with WriterOptions.
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
	w.enableGlobalIndex = options.EnableGlobalIndex
	w.invertRuleIndex = make(map[byte][]*partition.IndexRuleLocator, 4)
	for _, ruleIndex := range partition.ParseIndexRuleLocators(options.Families, options.IndexRules) {
		rule := ruleIndex.Rule
		var key byte
		switch rule.GetLocation() {
		case databasev1.IndexRule_LOCATION_SERIES:
			key |= local
		case databasev1.IndexRule_LOCATION_GLOBAL:
			if !w.enableGlobalIndex {
				w.l.Warn().RawJSON("index-rule", logger.Proto(ruleIndex.Rule)).Msg("global index is disabled")
				continue
			}
			key |= global
		case databasev1.IndexRule_LOCATION_UNSPECIFIED:
			w.l.Warn().RawJSON("index-rule", logger.Proto(ruleIndex.Rule)).Msg("invalid:unspecified location")
			continue
		}
		switch rule.Type {
		case databasev1.IndexRule_TYPE_INVERTED:
			key |= inverted
		case databasev1.IndexRule_TYPE_TREE:
			key |= tree
		case databasev1.IndexRule_TYPE_UNSPECIFIED:
			w.l.Warn().RawJSON("index-rule", logger.Proto(ruleIndex.Rule)).Msg("invalid:unspecified type")
			continue
		}
		rules := w.invertRuleIndex[key]
		rules = append(rules, ruleIndex)
		w.invertRuleIndex[key] = rules
	}
	return w
}

func (s *Writer) Write(m Message) {
	err := multierr.Combine(
		s.writeLocalIndex(m.LocalWriter, m.Value),
		s.writeGlobalIndex(m.Scope, m.LocalWriter.ItemID(), m.Value),
		m.BlockCloser.Close(),
	)
	if err != nil {
		s.l.Error().Err(err).Msg("encounter some errors when generating indices")
	}
}

// TODO: should listen to pipeline in a distributed cluster.
func (s *Writer) writeGlobalIndex(scope tsdb.Entry, ref tsdb.GlobalItemID, value Value) error {
	collect := func(ruleIndexes []*partition.IndexRuleLocator, fn func(indexWriter tsdb.IndexWriter, fields []index.Field) error) error {
		fields := make(map[uint][]index.Field)
		for _, ruleIndex := range ruleIndexes {
			values, err := getIndexValue(ruleIndex, value)
			if err != nil {
				return err
			}
			if values == nil {
				continue
			}
			for _, val := range values {
				indexShardID, err := partition.ShardID(val, s.shardNum)
				if err != nil {
					return err
				}
				rule := ruleIndex.Rule
				rr := fields[indexShardID]
				rr = append(rr, index.Field{
					Key: index.FieldKey{
						IndexRuleID: rule.GetMetadata().GetId(),
						Analyzer:    rule.Analyzer,
					},
					Term: val,
				})
				fields[indexShardID] = rr
			}
		}
		for shardID, rules := range fields {
			shard, err := s.db.SupplyTSDB().Shard(common.ShardID(shardID))
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
			err = fn(indexWriter, rules)
			if err != nil {
				return err
			}
		}
		return nil
	}
	return multierr.Combine(
		collect(s.invertRuleIndex[global|inverted], func(indexWriter tsdb.IndexWriter, fields []index.Field) error {
			return indexWriter.WriteInvertedIndex(fields)
		}),
		collect(s.invertRuleIndex[global|tree], func(indexWriter tsdb.IndexWriter, fields []index.Field) error {
			return indexWriter.WriteLSMIndex(fields)
		}),
	)
}

func (s *Writer) writeLocalIndex(writer tsdb.Writer, value Value) (err error) {
	collect := func(ruleIndexes []*partition.IndexRuleLocator, fn func(fields []index.Field) error) error {
		fields := make([]index.Field, 0)
		for _, ruleIndex := range ruleIndexes {
			values, err := getIndexValue(ruleIndex, value)
			if err != nil {
				return err
			}
			if values == nil {
				continue
			}
			for _, val := range values {
				rule := ruleIndex.Rule
				fields = append(fields, index.Field{
					Key: index.FieldKey{
						IndexRuleID: rule.GetMetadata().GetId(),
						Analyzer:    rule.Analyzer,
					},
					Term: val,
				})
			}
		}
		return fn(fields)
	}
	return multierr.Combine(
		collect(s.invertRuleIndex[local|inverted], func(fields []index.Field) error {
			return writer.WriteInvertedIndex(fields)
		}),
		collect(s.invertRuleIndex[local|tree], func(fields []index.Field) error {
			return writer.WriteLSMIndex(fields)
		}),
	)
}

var errUnsupportedIndexType = errors.New("unsupported index type")

func getIndexValue(ruleIndex *partition.IndexRuleLocator, value Value) (val [][]byte, err error) {
	val = make([][]byte, 0)
	if len(ruleIndex.TagIndices) != 1 {
		return nil, errors.WithMessagef(errUnsupportedIndexType,
			"the index rule %s(%v) didn't support composited tags",
			ruleIndex.Rule.Metadata.Name, ruleIndex.Rule.Tags)
	}
	tIndex := ruleIndex.TagIndices[0]
	tag, err := partition.GetTagByOffset(value.TagFamilies, tIndex.FamilyOffset, tIndex.TagOffset)

	if errors.Is(err, partition.ErrMalformedElement) {
		return val, nil
	}
	if err != nil {
		return nil, errors.WithMessagef(err, "index rule:%v", ruleIndex.Rule.Metadata)
	}
	fv, err := pbv1.ParseTagValue(tag)
	if err != nil {
		return nil, err
	}
	if fv.GetValue() == nil && fv.GetArr() == nil {
		return nil, nil
	}
	v := fv.GetValue()
	if v != nil {
		val = append(val, v)
		return val, nil
	}
	arr := fv.GetArr()
	if arr != nil {
		val = append(val, arr...)
	}
	return val, nil
}
