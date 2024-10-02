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

// Package inverted implements an inverted index repository.
package inverted

import (
	"bytes"
	"context"
	"io"
	"log"
	"math"
	"time"

	"github.com/blugelabs/bluge"
	"github.com/blugelabs/bluge/analysis"
	"github.com/blugelabs/bluge/analysis/analyzer"
	blugeIndex "github.com/blugelabs/bluge/index"
	"github.com/blugelabs/bluge/search"
	"github.com/pkg/errors"
	"go.uber.org/multierr"

	"github.com/apache/skywalking-banyandb/api/common"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/posting"
	"github.com/apache/skywalking-banyandb/pkg/index/posting/roaring"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/pool"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

const (
	docIDField     = "_id"
	batchSize      = 1024
	seriesIDField  = "series_id"
	entityField    = "entity"
	idField        = "id"
	timestampField = "timestamp"
)

var (
	defaultUpper            = convert.Uint64ToBytes(math.MaxUint64)
	defaultLower            = convert.Uint64ToBytes(0)
	defaultRangePreloadSize = 1000
	defaultProjection       = []string{docIDField}
)

// Analyzers is a map that associates each IndexRule_Analyzer type with a corresponding Analyzer.
var Analyzers map[string]*analysis.Analyzer

func init() {
	Analyzers = map[string]*analysis.Analyzer{
		index.AnalyzerKeyword:  analyzer.NewKeywordAnalyzer(),
		index.AnalyzerSimple:   analyzer.NewSimpleAnalyzer(),
		index.AnalyzerStandard: analyzer.NewStandardAnalyzer(),
		index.AnalyzerURL:      newURLAnalyzer(),
	}
}

var _ index.Store = (*store)(nil)

// StoreOpts wraps options to create an inverted index repository.
type StoreOpts struct {
	Logger       *logger.Logger
	Metrics      *Metrics
	Path         string
	BatchWaitSec int64
}

type store struct {
	writer  *bluge.Writer
	closer  *run.Closer
	l       *logger.Logger
	metrics *Metrics
}

var batchPool = pool.Register[*blugeIndex.Batch]("index-bluge-batch")

func generateBatch() *blugeIndex.Batch {
	b := batchPool.Get()
	if b == nil {
		return bluge.NewBatch()
	}
	return b
}

func releaseBatch(b *blugeIndex.Batch) {
	b.Reset()
	batchPool.Put(b)
}

func (s *store) Batch(batch index.Batch) error {
	if !s.closer.AddRunning() {
		return nil
	}
	defer s.closer.Done()
	b := generateBatch()
	defer releaseBatch(b)
	for _, d := range batch.Documents {
		var fk *index.FieldKey
		if len(d.Fields) > 0 {
			fk = &d.Fields[0].Key
		}
		doc := bluge.NewDocument(convert.BytesToString(convert.Uint64ToBytes(d.DocID)))
		for _, f := range d.Fields {
			tf := bluge.NewKeywordFieldBytes(f.Key.Marshal(), f.Term)
			if !f.NoSort {
				tf.StoreValue().Sortable()
			}
			if f.Store {
				tf.StoreValue()
			}
			if f.Key.Analyzer != index.AnalyzerUnspecified {
				tf = tf.WithAnalyzer(Analyzers[f.Key.Analyzer])
			}
			doc.AddField(tf)
		}

		if d.EntityValues != nil {
			doc.AddField(bluge.NewKeywordFieldBytes(entityField, d.EntityValues).StoreValue())
		} else if fk != nil && fk.HasSeriesID() {
			doc.AddField(bluge.NewKeywordFieldBytes(seriesIDField, fk.SeriesID.Marshal()).StoreValue())
		}
		if d.Timestamp > 0 {
			doc.AddField(bluge.NewDateTimeField(timestampField, time.Unix(0, d.Timestamp)).StoreValue())
		}
		b.Update(doc.ID(), doc)
	}
	return s.writer.Batch(b)
}

// NewStore create a new inverted index repository.
func NewStore(opts StoreOpts) (index.SeriesStore, error) {
	if opts.Logger == nil {
		opts.Logger = logger.GetLogger("inverted")
	}
	indexConfig := blugeIndex.DefaultConfig(opts.Path)
	if opts.BatchWaitSec > 0 {
		indexConfig = indexConfig.WithUnsafeBatches().
			WithPersisterNapTimeMSec(int(opts.BatchWaitSec * 1000))
	}
	config := bluge.DefaultConfigWithIndexConfig(indexConfig)
	config.DefaultSearchAnalyzer = Analyzers[index.AnalyzerKeyword]
	config.Logger = log.New(opts.Logger, opts.Logger.Module(), 0)
	w, err := bluge.OpenWriter(config)
	if err != nil {
		return nil, err
	}
	s := &store{
		writer:  w,
		l:       opts.Logger,
		closer:  run.NewCloser(1),
		metrics: opts.Metrics,
	}
	return s, nil
}

func (s *store) Close() error {
	s.closer.Done()
	s.closer.CloseThenWait()
	return s.writer.Close()
}

func (s *store) Iterator(ctx context.Context, fieldKey index.FieldKey, termRange index.RangeOpts, order modelv1.Sort,
	preLoadSize int, indexQuery index.Query, fieldKeys []index.FieldKey,
) (iter index.FieldIterator[*index.DocumentResult], err error) {
	if termRange.Lower != nil &&
		termRange.Upper != nil &&
		bytes.Compare(termRange.Lower, termRange.Upper) > 0 {
		return index.DummyFieldIterator, nil
	}
	if !s.closer.AddRunning() {
		return nil, nil
	}

	reader, err := s.writer.Reader()
	if err != nil {
		return nil, err
	}
	fk := fieldKey.Marshal()
	rangeQuery := bluge.NewBooleanQuery()
	rangeNode := newMustNode()
	addRange := func(query *bluge.BooleanQuery, termRange index.RangeOpts) *bluge.BooleanQuery {
		if termRange.Upper == nil {
			termRange.Upper = defaultUpper
		}
		if termRange.Lower == nil {
			termRange.Lower = defaultLower
		}
		query.AddMust(bluge.NewTermRangeInclusiveQuery(
			string(termRange.Lower),
			string(termRange.Upper),
			termRange.IncludesLower,
			termRange.IncludesUpper,
		).
			SetField(fk))
		rangeNode.Append(newTermRangeInclusiveNode(string(termRange.Lower), string(termRange.Upper), termRange.IncludesLower, termRange.IncludesUpper, nil))
		return query
	}

	if fieldKey.HasSeriesID() {
		rangeQuery = rangeQuery.AddMust(bluge.NewTermQuery(string(fieldKey.SeriesID.Marshal())).
			SetField(seriesIDField))
		rangeNode.Append(newTermNode(string(fieldKey.SeriesID.Marshal()), nil))
		if termRange.Lower != nil || termRange.Upper != nil {
			rangeQuery = addRange(rangeQuery, termRange)
		}
	} else {
		rangeQuery = addRange(rangeQuery, termRange)
	}

	sortedKey := fk
	if order == modelv1.Sort_SORT_DESC {
		sortedKey = "-" + sortedKey
	}
	query := bluge.NewBooleanQuery().AddMust(rangeQuery)
	node := newMustNode()
	node.Append(rangeNode)
	if indexQuery != nil && indexQuery.(*queryNode).query != nil {
		query.AddMust(indexQuery.(*queryNode).query)
		node.Append(indexQuery.(*queryNode).node)
	}
	fields := make([]string, 0, len(fieldKeys))
	for i := range fieldKeys {
		fields = append(fields, fieldKeys[i].Marshal())
	}
	result := &sortIterator{
		query:     &queryNode{query, node},
		fields:    fields,
		reader:    reader,
		sortedKey: sortedKey,
		size:      preLoadSize,
		closer:    s.closer,
		ctx:       ctx,
	}
	return result, nil
}

func (s *store) MatchField(fieldKey index.FieldKey) (list posting.List, err error) {
	return s.Range(fieldKey, index.RangeOpts{})
}

func (s *store) MatchTerms(field index.Field) (list posting.List, err error) {
	reader, err := s.writer.Reader()
	if err != nil {
		return nil, err
	}
	fk := field.Key.Marshal()
	query := bluge.NewBooleanQuery().
		AddMust(bluge.NewTermQuery(string(field.Term)).SetField(fk))
	if field.Key.HasSeriesID() {
		query = query.AddMust(bluge.NewTermQuery(string(field.Key.SeriesID.Marshal())).
			SetField(seriesIDField))
	}
	documentMatchIterator, err := reader.Search(context.Background(), bluge.NewAllMatches(query))
	if err != nil {
		return nil, err
	}
	iter := newBlugeMatchIterator(documentMatchIterator, reader, defaultProjection)
	defer func() {
		err = multierr.Append(err, iter.Close())
	}()
	list = roaring.NewPostingList()
	for iter.Next() {
		list.Insert(iter.Val().DocID)
	}
	return list, err
}

func (s *store) Match(fieldKey index.FieldKey, matches []string, opts *modelv1.Condition_MatchOption) (posting.List, error) {
	if len(matches) == 0 || fieldKey.Analyzer == index.AnalyzerUnspecified {
		return roaring.DummyPostingList, nil
	}
	reader, err := s.writer.Reader()
	if err != nil {
		return nil, err
	}
	analyzer, operator := getMatchOptions(fieldKey.Analyzer, opts)
	fk := fieldKey.Marshal()
	query := bluge.NewBooleanQuery()
	if fieldKey.HasSeriesID() {
		query.AddMust(bluge.NewTermQuery(string(fieldKey.SeriesID.Marshal())).SetField(seriesIDField))
	}
	for _, m := range matches {
		query.AddMust(bluge.NewMatchQuery(m).SetField(fk).
			SetAnalyzer(analyzer).SetOperator(operator))
	}
	documentMatchIterator, err := reader.Search(context.Background(), bluge.NewAllMatches(query))
	if err != nil {
		return nil, err
	}
	iter := newBlugeMatchIterator(documentMatchIterator, reader, defaultProjection)
	defer func() {
		err = multierr.Append(err, iter.Close())
	}()
	list := roaring.NewPostingList()
	for iter.Next() {
		list.Insert(iter.Val().DocID)
	}
	return list, err
}

func getMatchOptions(analyzerOnIndexRule string, opts *modelv1.Condition_MatchOption) (*analysis.Analyzer, bluge.MatchQueryOperator) {
	analyzer := Analyzers[analyzerOnIndexRule]
	operator := bluge.MatchQueryOperatorOr
	if opts != nil {
		if opts.Analyzer != index.AnalyzerUnspecified {
			analyzer = Analyzers[opts.Analyzer]
		}
		if opts.Operator != modelv1.Condition_MatchOption_OPERATOR_UNSPECIFIED {
			if opts.Operator == modelv1.Condition_MatchOption_OPERATOR_AND {
				operator = bluge.MatchQueryOperatorAnd
			}
		}
	}
	return analyzer, bluge.MatchQueryOperator(operator)
}

func (s *store) Range(fieldKey index.FieldKey, opts index.RangeOpts) (list posting.List, err error) {
	iter, err := s.Iterator(context.TODO(), fieldKey, opts, modelv1.Sort_SORT_ASC, defaultRangePreloadSize, nil, nil)
	if err != nil {
		return roaring.DummyPostingList, err
	}
	list = roaring.NewPostingList()
	for iter.Next() {
		list.Insert(iter.Val().DocID)
	}
	err = multierr.Append(err, iter.Close())
	return
}

type blugeMatchIterator struct {
	delegated        search.DocumentMatchIterator
	err              error
	closer           io.Closer
	needToLoadFields []string
	current          index.DocumentResult
	hit              int
}

func newBlugeMatchIterator(delegated search.DocumentMatchIterator, closer io.Closer,
	needToLoadFields []string,
) blugeMatchIterator {
	bmi := blugeMatchIterator{
		delegated:        delegated,
		closer:           closer,
		needToLoadFields: needToLoadFields,
		current:          index.DocumentResult{Values: make(map[string][]byte, len(needToLoadFields))},
	}
	for _, f := range needToLoadFields {
		bmi.current.Values[f] = nil
	}
	return bmi
}

func (bmi *blugeMatchIterator) Next() bool {
	var match *search.DocumentMatch
	match, bmi.err = bmi.delegated.Next()
	if bmi.err != nil {
		bmi.err = errors.WithMessagef(bmi.err, "failed to get next document, hit: %d", bmi.hit)
		return false
	}
	if match == nil {
		bmi.err = io.EOF
		return false
	}
	bmi.hit = match.HitNumber
	for i := range bmi.current.Values {
		bmi.current.Values[i] = nil
	}
	bmi.current.DocID = 0
	bmi.current.SeriesID = 0
	bmi.current.Timestamp = 0
	bmi.current.SortedValue = nil
	if len(match.SortValue) > 0 {
		bmi.current.SortedValue = match.SortValue[0]
	}
	err := match.VisitStoredFields(func(field string, value []byte) bool {
		switch field {
		case entityField:
			bmi.current.EntityValues = value
		case docIDField:
			bmi.current.DocID = convert.BytesToUint64(value)
		case seriesIDField:
			bmi.current.SeriesID = common.SeriesID(convert.BytesToUint64(value))
		case timestampField:
			ts, err := bluge.DecodeDateTime(value)
			if err != nil {
				bmi.err = err
				return false
			}
			bmi.current.Timestamp = ts.UnixNano()
		default:
			if _, ok := bmi.current.Values[field]; ok {
				bmi.current.Values[field] = bytes.Clone(value)
			}
		}
		return true
	})
	bmi.err = errors.WithMessagef(err, "visit stored fields, hit: %d", bmi.hit)
	return bmi.err == nil
}

func (bmi *blugeMatchIterator) Val() index.DocumentResult {
	return bmi.current
}

func (bmi *blugeMatchIterator) Close() error {
	if bmi.closer == nil {
		if errors.Is(bmi.err, io.EOF) {
			return nil
		}
		return bmi.err
	}
	err := bmi.closer.Close()
	if errors.Is(bmi.err, io.EOF) {
		return err
	}
	return multierr.Combine(bmi.err, bmi.closer.Close())
}
