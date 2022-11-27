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

package inverted

import (
	"bytes"
	"context"
	"errors"
	"log"
	"math"
	"time"

	"github.com/blugelabs/bluge"
	"github.com/blugelabs/bluge/analysis"
	"github.com/blugelabs/bluge/analysis/analyzer"
	"github.com/blugelabs/bluge/search"
	"github.com/dgraph-io/badger/v3/y"
	"go.uber.org/multierr"

	"github.com/apache/skywalking-banyandb/api/common"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/posting"
	"github.com/apache/skywalking-banyandb/pkg/index/posting/roaring"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

const (
	docID     = "_id"
	batchSize = 1024
)

var analyzers map[databasev1.IndexRule_Analyzer]*analysis.Analyzer

func init() {
	analyzers = map[databasev1.IndexRule_Analyzer]*analysis.Analyzer{
		databasev1.IndexRule_ANALYZER_KEYWORD:  analyzer.NewKeywordAnalyzer(),
		databasev1.IndexRule_ANALYZER_SIMPLE:   analyzer.NewSimpleAnalyzer(),
		databasev1.IndexRule_ANALYZER_STANDARD: analyzer.NewStandardAnalyzer(),
	}
}

var _ index.Store = (*store)(nil)

type StoreOpts struct {
	Path   string
	Logger *logger.Logger
}

type doc struct {
	fields []index.Field
	itemID common.ItemID
}

type flushEvent struct {
	onComplete chan struct{}
}

type store struct {
	writer *bluge.Writer
	ch     chan any
	closer *run.Closer
	l      *logger.Logger
}

func NewStore(opts StoreOpts) (index.Store, error) {
	config := bluge.DefaultConfig(opts.Path)
	config.DefaultSearchAnalyzer = analyzers[databasev1.IndexRule_ANALYZER_KEYWORD]
	config.Logger = log.New(opts.Logger, opts.Logger.Module(), 0)
	w, err := bluge.OpenWriter(config)
	if err != nil {
		return nil, err
	}
	s := &store{
		writer: w,
		l:      opts.Logger,
		ch:     make(chan any, batchSize),
		closer: run.NewCloser(1),
	}
	s.run()
	return s, nil
}

func (s *store) Stats() observability.Statistics {
	return observability.Statistics{}
}

func (s *store) Close() error {
	s.closer.CloseThenWait()
	return s.writer.Close()
}

func (s *store) Write(fields []index.Field, itemID common.ItemID) error {
	if !s.closer.AddRunning() {
		return nil
	}
	defer s.closer.Done()
	select {
	case <-s.closer.CloseNotify():
	case s.ch <- doc{
		fields: fields,
		itemID: itemID,
	}:
	}
	return nil
}

func (s *store) Iterator(fieldKey index.FieldKey, termRange index.RangeOpts, order modelv1.Sort) (iter index.FieldIterator, err error) {
	if termRange.Lower != nil &&
		termRange.Upper != nil &&
		bytes.Compare(termRange.Lower, termRange.Upper) > 0 {
		return index.EmptyFieldIterator, nil
	}
	reader, err := s.writer.Reader()
	if err != nil {
		return nil, err
	}
	fk := fieldKey.MarshalToStr()
	query := bluge.NewTermRangeInclusiveQuery(
		string(termRange.Lower),
		string(termRange.Upper),
		termRange.IncludesLower,
		termRange.IncludesUpper,
	).
		SetField(fk)
	sortedKey := fk
	if order == modelv1.Sort_SORT_DESC {
		sortedKey = "-" + sortedKey
	}
	documentMatchIterator, err := reader.Search(context.Background(), bluge.NewTopNSearch(math.MaxInt64, query).SortBy([]string{sortedKey}))
	if err != nil {
		return nil, err
	}
	result := newBlugeMatchIterator(documentMatchIterator, fk)
	return &result, nil
}

func (s *store) MatchField(fieldKey index.FieldKey) (list posting.List, err error) {
	return s.Range(fieldKey, index.RangeOpts{})
}

func (s *store) MatchTerms(field index.Field) (list posting.List, err error) {
	reader, err := s.writer.Reader()
	if err != nil {
		return nil, err
	}
	fk := field.Key.MarshalToStr()
	query := bluge.NewTermQuery(string(field.Term)).SetField(fk)
	documentMatchIterator, err := reader.Search(context.Background(), bluge.NewAllMatches(query))
	if err != nil {
		return nil, err
	}
	iter := newBlugeMatchIterator(documentMatchIterator, fk)
	list = roaring.NewPostingList()
	for iter.Next() {
		err = multierr.Append(err, list.Union(iter.Val().Value))
	}
	err = multierr.Append(err, iter.Close())
	return list, err
}

func (s *store) Match(fieldKey index.FieldKey, matches []string) (posting.List, error) {
	if len(matches) == 0 {
		return roaring.EmptyPostingList, nil
	}
	reader, err := s.writer.Reader()
	if err != nil {
		return nil, err
	}
	fk := fieldKey.MarshalToStr()
	var query bluge.Query
	getMatchQuery := func(match string) bluge.Query {
		q := bluge.NewMatchQuery(match).SetField(fk)
		if fieldKey.Analyzer != databasev1.IndexRule_ANALYZER_UNSPECIFIED {
			q.SetAnalyzer(analyzers[fieldKey.Analyzer])
		}
		return q
	}
	if len(matches) == 1 {
		query = getMatchQuery(matches[0])
	} else {
		bq := bluge.NewBooleanQuery()
		for _, m := range matches {
			bq.AddMust(getMatchQuery(m))
		}
		query = bq
	}
	documentMatchIterator, err := reader.Search(context.Background(), bluge.NewAllMatches(query))
	if err != nil {
		return nil, err
	}
	iter := newBlugeMatchIterator(documentMatchIterator, fk)
	list := roaring.NewPostingList()
	for iter.Next() {
		err = multierr.Append(err, list.Union(iter.Val().Value))
	}
	err = multierr.Append(err, iter.Close())
	return list, err
}

func (s *store) Range(fieldKey index.FieldKey, opts index.RangeOpts) (list posting.List, err error) {
	iter, err := s.Iterator(fieldKey, opts, modelv1.Sort_SORT_ASC)
	if err != nil {
		return roaring.EmptyPostingList, err
	}
	list = roaring.NewPostingList()
	for iter.Next() {
		err = multierr.Append(err, list.Union(iter.Val().Value))
	}
	err = multierr.Append(err, iter.Close())
	return
}

func (s *store) run() {
	go func() {
		defer s.closer.Done()
		size := 0
		batch := bluge.NewBatch()
		flush := func() {
			if size < 1 {
				return
			}
			if err := s.writer.Batch(batch); err != nil {
				s.l.Error().Err(err).Msg("write to the inverted index")
			}
			batch.Reset()
			size = 0
		}
		for {
			timer := time.NewTimer(time.Second)
			select {
			case <-s.closer.CloseNotify():
				return
			case event, more := <-s.ch:
				if !more {
					return
				}
				switch d := event.(type) {
				case flushEvent:
					flush()
					close(d.onComplete)
				case doc:
					doc := bluge.NewDocument(string(convert.Uint64ToBytes(uint64(d.itemID))))
					for _, f := range d.fields {
						field := bluge.NewKeywordFieldBytes(f.Key.MarshalToStr(), f.Term).StoreValue().Sortable()
						if f.Key.Analyzer != databasev1.IndexRule_ANALYZER_UNSPECIFIED {
							field.WithAnalyzer(analyzers[f.Key.Analyzer])
						}
						doc.AddField(field)
					}
					size++
					if size >= batchSize {
						flush()
					} else {
						batch.Insert(doc)
					}
				}

			case <-timer.C:
				flush()
			}
		}
	}()
}

func (s *store) flush() {
	if !s.closer.AddRunning() {
		return
	}
	defer s.closer.Done()
	onComplete := make(chan struct{})
	select {
	case <-s.closer.CloseNotify():
	case s.ch <- flushEvent{onComplete: onComplete}:
	}
	<-onComplete
}

type blugeMatchIterator struct {
	delegated search.DocumentMatchIterator
	fieldKey  string

	current *index.PostingValue
	agg     *index.PostingValue

	closed bool
	err    error
}

func newBlugeMatchIterator(delegated search.DocumentMatchIterator, fieldKey string) blugeMatchIterator {
	return blugeMatchIterator{
		delegated: delegated,
		fieldKey:  fieldKey,
	}
}

func (bmi *blugeMatchIterator) Next() bool {
	if bmi.err != nil || bmi.closed {
		return false
	}
	for bmi.nextTerm() {
	}
	if bmi.err != nil || bmi.closed {
		return false
	}
	return true
}

func (bmi *blugeMatchIterator) nextTerm() bool {
	var match *search.DocumentMatch
	match, bmi.err = bmi.delegated.Next()
	if bmi.err != nil {
		return false
	}
	if match == nil {
		if bmi.agg == nil {
			bmi.closed = true
		} else {
			bmi.current = bmi.agg
			bmi.agg = nil
		}
		return false
	}
	i := 0
	var itemID common.ItemID
	var term []byte
	bmi.err = match.VisitStoredFields(func(field string, value []byte) bool {
		if field == docID {
			id := convert.BytesToUint64(value)
			itemID = common.ItemID(id)
			i++
		}
		if field == bmi.fieldKey {
			term = y.Copy(value)
			i++
		}
		return i < 2
	})
	if i != 2 {
		bmi.err = errors.New("less fields")
		return false
	}
	if bmi.err != nil {
		return false
	}
	if bmi.agg == nil {
		bmi.agg = &index.PostingValue{
			Term:  term,
			Value: roaring.NewPostingListWithInitialData(uint64(itemID)),
		}
		return true
	}
	if bytes.Equal(bmi.agg.Term, term) {
		bmi.agg.Value.Insert(itemID)
		return true
	}
	bmi.current = bmi.agg
	bmi.agg = &index.PostingValue{
		Term:  term,
		Value: roaring.NewPostingListWithInitialData(uint64(itemID)),
	}
	return false
}

func (bmi *blugeMatchIterator) Val() *index.PostingValue {
	return bmi.current
}

func (bmi *blugeMatchIterator) Close() error {
	bmi.closed = true
	return bmi.err
}
