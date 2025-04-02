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
	"context"
	"io"
	"log"
	"strconv"
	"time"

	"github.com/blugelabs/bluge"
	"github.com/blugelabs/bluge/analysis"
	"github.com/blugelabs/bluge/analysis/analyzer"
	blugeIndex "github.com/blugelabs/bluge/index"
	"github.com/blugelabs/bluge/numeric"
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
	seriesIDField  = "_series_id"
	timestampField = "_timestamp"
	versionField   = "_version"
	sourceField    = "_source"
)

var (
	defaultRangePreloadSize = 1000
	defaultProjection       = []string{docIDField, timestampField}
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
	Logger        *logger.Logger
	Metrics       *Metrics
	Path          string
	BatchWaitSec  int64
	CacheMaxBytes int
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
		doc := bluge.NewDocument(convert.BytesToString(convert.Uint64ToBytes(d.DocID)))
		for i, f := range d.Fields {
			var tf *bluge.TermField
			switch f.GetTerm().(type) {
			case *index.BytesTermValue:
				tf = bluge.NewKeywordFieldBytes(f.Key.Marshal(), f.GetBytes())
			case *index.FloatTermValue:
				tf = bluge.NewNumericField(f.Key.Marshal(), f.GetFloat())
			default:
				return errors.Errorf("unexpected field type: %T", f.GetTerm())
			}
			if !f.NoSort {
				tf.Sortable()
			}
			if f.Store {
				tf.StoreValue()
			}
			if f.Key.Analyzer != index.AnalyzerUnspecified {
				tf = tf.WithAnalyzer(Analyzers[f.Key.Analyzer])
			}
			doc.AddField(tf)
			if i == 0 {
				doc.AddField(bluge.NewKeywordFieldBytes(seriesIDField, f.Key.SeriesID.Marshal()).StoreValue())
			}
		}

		if d.Timestamp > 0 {
			doc.AddField(bluge.NewDateTimeField(timestampField, time.Unix(0, d.Timestamp)).StoreValue())
		}
		b.Insert(doc)
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
	indexConfig.CacheMaxBytes = opts.CacheMaxBytes
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

func (s *store) Reset() {
	s.writer.ResetCache()
}

func (s *store) Iterator(ctx context.Context, fieldKey index.FieldKey, termRange index.RangeOpts, order modelv1.Sort,
	preLoadSize int,
) (iter index.FieldIterator[*index.DocumentResult], err error) {
	if !termRange.IsEmpty() && !termRange.Valid() {
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

	rangeQuery = rangeQuery.AddMust(bluge.NewTermQuery(string(fieldKey.SeriesID.Marshal())).
		SetField(seriesIDField))
	rangeNode.Append(newTermNode(string(fieldKey.SeriesID.Marshal()), nil))
	if !termRange.IsEmpty() && termRange.Valid() {
		switch lower := termRange.Lower.(type) {
		case *index.BytesTermValue:
			upper := termRange.Upper.(*index.BytesTermValue)
			rangeQuery.AddMust(bluge.NewTermRangeInclusiveQuery(
				string(lower.Value),
				string(upper.Value),
				termRange.IncludesLower,
				termRange.IncludesUpper,
			).
				SetField(fk))
			rangeNode.Append(newTermRangeInclusiveNode(
				string(lower.Value),
				string(upper.Value),
				termRange.IncludesLower,
				termRange.IncludesUpper, nil,
				false,
			))

		case *index.FloatTermValue:
			upper := termRange.Upper.(*index.FloatTermValue)
			rangeQuery.AddMust(bluge.NewNumericRangeInclusiveQuery(
				lower.Value,
				upper.Value,
				termRange.IncludesLower,
				termRange.IncludesUpper,
			).
				SetField(fk))
			rangeNode.Append(newTermRangeInclusiveNode(
				strconv.FormatFloat(lower.Value, 'f', -1, 64),
				strconv.FormatFloat(upper.Value, 'f', -1, 64),
				termRange.IncludesLower,
				termRange.IncludesUpper,
				nil,
				false,
			))
		default:
			logger.Panicf("unexpected field type: %T", lower)
		}
	}
	if n := appendTimeRangeToQuery(rangeQuery, fieldKey); n != nil {
		rangeNode.Append(n)
	}

	sortedKey := fk
	if order == modelv1.Sort_SORT_DESC {
		sortedKey = "-" + sortedKey
	}
	result := &sortIterator{
		query:       &queryNode{rangeQuery, rangeNode},
		reader:      reader,
		sortedKey:   sortedKey,
		size:        preLoadSize,
		closer:      s.closer,
		ctx:         ctx,
		newIterator: newBlugeMatchIterator,
	}
	return result, nil
}

func appendTimeRangeToQuery(query *bluge.BooleanQuery, fieldKey index.FieldKey) node {
	if fieldKey.TimeRange == nil || !fieldKey.TimeRange.Valid() {
		return nil
	}
	lower := numeric.Float64ToInt64(fieldKey.TimeRange.Lower.(*index.FloatTermValue).Value)
	upper := numeric.Float64ToInt64(fieldKey.TimeRange.Upper.(*index.FloatTermValue).Value)
	query.AddMust(bluge.NewDateRangeInclusiveQuery(
		time.Unix(0, lower),
		time.Unix(0, upper),
		fieldKey.TimeRange.IncludesLower,
		fieldKey.TimeRange.IncludesUpper,
	).SetField(timestampField))
	return newTermRangeInclusiveNode(
		strconv.FormatInt(lower, 10),
		strconv.FormatInt(upper, 10),
		fieldKey.TimeRange.IncludesLower,
		fieldKey.TimeRange.IncludesUpper,
		nil,
		true,
	)
}

func (s *store) MatchField(fieldKey index.FieldKey) (posting.List, posting.List, error) {
	return s.Range(fieldKey, index.RangeOpts{})
}

func (s *store) MatchTerms(field index.Field) (list posting.List, timestamps posting.List, err error) {
	reader, err := s.writer.Reader()
	if err != nil {
		return nil, nil, err
	}

	var query *bluge.BooleanQuery
	switch field.GetTerm().(type) {
	case *index.BytesTermValue:
		query = bluge.NewBooleanQuery()
		query.AddMust(bluge.NewTermQuery(string(field.GetBytes())).SetField(field.Key.Marshal()))
	case *index.FloatTermValue:
		query = bluge.NewBooleanQuery()
		query.AddMust(bluge.NewTermQuery(
			strconv.FormatFloat(field.GetFloat(), 'f', -1, 64)).
			SetField(field.Key.Marshal()))
	case nil:
		return roaring.DummyPostingList, roaring.DummyPostingList, nil
	default:
		return nil, nil, errors.Errorf("unexpected field type: %T", field.GetTerm())
	}
	query.AddMust(bluge.NewTermQuery(string(field.Key.SeriesID.Marshal())).
		SetField(seriesIDField))
	_ = appendTimeRangeToQuery(query, field.Key)

	documentMatchIterator, err := reader.Search(context.Background(), bluge.NewAllMatches(query))
	if err != nil {
		return nil, nil, err
	}
	iter := newBlugeMatchIterator(documentMatchIterator, reader, defaultProjection)
	defer func() {
		err = multierr.Append(err, iter.Close())
	}()
	list, timestamps = roaring.NewPostingList(), roaring.NewPostingList()
	for iter.Next() {
		list.Insert(iter.Val().DocID)
		timestamps.Insert(uint64(iter.Val().Timestamp))
	}
	return list, timestamps, err
}

func (s *store) Match(fieldKey index.FieldKey, matches []string, opts *modelv1.Condition_MatchOption) (posting.List, posting.List, error) {
	if len(matches) == 0 || fieldKey.Analyzer == index.AnalyzerUnspecified {
		return roaring.DummyPostingList, roaring.DummyPostingList, nil
	}
	reader, err := s.writer.Reader()
	if err != nil {
		return nil, nil, err
	}
	analyzer, operator := getMatchOptions(fieldKey.Analyzer, opts)
	fk := fieldKey.Marshal()
	query := bluge.NewBooleanQuery()
	query.AddMust(bluge.NewTermQuery(string(fieldKey.SeriesID.Marshal())).SetField(seriesIDField))
	for _, m := range matches {
		query.AddMust(bluge.NewMatchQuery(m).SetField(fk).
			SetAnalyzer(analyzer).SetOperator(operator))
	}
	_ = appendTimeRangeToQuery(query, fieldKey)
	documentMatchIterator, err := reader.Search(context.Background(), bluge.NewAllMatches(query))
	if err != nil {
		return nil, nil, err
	}
	iter := newBlugeMatchIterator(documentMatchIterator, reader, defaultProjection)
	defer func() {
		err = multierr.Append(err, iter.Close())
	}()
	list, timestamps := roaring.NewPostingList(), roaring.NewPostingList()
	for iter.Next() {
		list.Insert(iter.Val().DocID)
		timestamps.Insert(uint64(iter.Val().Timestamp))
	}
	return list, timestamps, err
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

func (s *store) Range(fieldKey index.FieldKey, opts index.RangeOpts) (list posting.List, timestamps posting.List, err error) {
	iter, err := s.Iterator(context.TODO(), fieldKey, opts, modelv1.Sort_SORT_ASC, defaultRangePreloadSize)
	if err != nil {
		return roaring.DummyPostingList, roaring.DummyPostingList, err
	}
	list, timestamps = roaring.NewPostingList(), roaring.NewPostingList()
	for iter.Next() {
		list.Insert(iter.Val().DocID)
		timestamps.Insert(uint64(iter.Val().Timestamp))
	}
	err = multierr.Append(err, iter.Close())
	return
}

func (s *store) TakeFileSnapshot(dst string) error {
	reader, err := s.writer.Reader()
	if err != nil {
		return err
	}
	defer reader.Close()
	return reader.Backup(dst, nil)
}

type blugeMatchIterator struct {
	delegated     search.DocumentMatchIterator
	err           error
	closer        io.Closer
	ctx           *search.Context
	loadDocValues []string
	current       index.DocumentResult
	hit           int
}

func newBlugeMatchIterator(delegated search.DocumentMatchIterator, closer io.Closer,
	loadDocValues []string,
) blugeIterator {
	bmi := &blugeMatchIterator{
		delegated:     delegated,
		closer:        closer,
		current:       index.DocumentResult{},
		ctx:           search.NewSearchContext(1, 0),
		loadDocValues: loadDocValues,
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
	if len(bmi.loadDocValues) == 0 {
		err := match.VisitStoredFields(bmi.setVal)
		bmi.err = multierr.Combine(bmi.err, err)
		return bmi.err == nil
	}
	if err := match.LoadDocumentValues(bmi.ctx, bmi.loadDocValues); err != nil {
		bmi.err = multierr.Combine(bmi.err, err)
		return false
	}
	for _, dv := range bmi.loadDocValues {
		vv := match.DocValues(dv)
		if len(vv) == 0 {
			continue
		}
		bmi.setVal(dv, vv[0])
	}
	return true
}

func (bmi *blugeMatchIterator) setVal(field string, value []byte) bool {
	switch field {
	case docIDField:
		bmi.current.DocID = convert.BytesToUint64(value)
	case seriesIDField:
		bmi.current.SeriesID = common.SeriesID(convert.BytesToUint64(value))
	case timestampField:
		ts, errTime := bluge.DecodeDateTime(value)
		if errTime != nil {
			bmi.err = errTime
			return false
		}
		bmi.current.Timestamp = ts.UnixNano()
	default:
		bmi.err = errors.Errorf("unexpected field: %s", field)
	}
	return true
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
