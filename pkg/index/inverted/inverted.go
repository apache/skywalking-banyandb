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

// Package inverted implements a inverted index repository.
package inverted

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"math"
	"sync"
	"time"

	"github.com/blugelabs/bluge"
	"github.com/blugelabs/bluge/analysis"
	"github.com/blugelabs/bluge/analysis/analyzer"
	blugeIndex "github.com/blugelabs/bluge/index"
	"github.com/blugelabs/bluge/search"
	"go.uber.org/multierr"

	"github.com/apache/skywalking-banyandb/api/common"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/posting"
	"github.com/apache/skywalking-banyandb/pkg/index/posting/roaring"
	"github.com/apache/skywalking-banyandb/pkg/logger"
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

// StoreOpts wraps options to create a inverted index repository.
type StoreOpts struct {
	Logger       *logger.Logger
	Path         string
	BatchWaitSec int64
}

type store struct {
	writer *bluge.Writer
	closer *run.Closer
	l      *logger.Logger
}

var batchPool sync.Pool

func generateBatch() *blugeIndex.Batch {
	b := batchPool.Get()
	if b == nil {
		return bluge.NewBatch()
	}
	return b.(*blugeIndex.Batch)
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
			if f.Key.Analyzer != databasev1.IndexRule_ANALYZER_UNSPECIFIED {
				tf = tf.WithAnalyzer(analyzers[f.Key.Analyzer])
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
	config.DefaultSearchAnalyzer = analyzers[databasev1.IndexRule_ANALYZER_KEYWORD]
	config.Logger = log.New(opts.Logger, opts.Logger.Module(), 0)
	w, err := bluge.OpenWriter(config)
	if err != nil {
		return nil, err
	}
	s := &store{
		writer: w,
		l:      opts.Logger,
		closer: run.NewCloser(1),
	}
	return s, nil
}

func (s *store) Close() error {
	s.closer.Done()
	s.closer.CloseThenWait()
	return s.writer.Close()
}

func (s *store) Iterator(fieldKey index.FieldKey, termRange index.RangeOpts, order modelv1.Sort, preLoadSize int) (iter index.FieldIterator[*index.ItemRef], err error) {
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
	query := bluge.NewBooleanQuery()
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
		return query
	}

	if fieldKey.HasSeriesID() {
		query = query.AddMust(bluge.NewTermQuery(string(fieldKey.SeriesID.Marshal())).
			SetField(seriesIDField))
		if termRange.Lower != nil || termRange.Upper != nil {
			query = addRange(query, termRange)
		}
	} else {
		query = addRange(query, termRange)
	}

	sortedKey := fk
	if order == modelv1.Sort_SORT_DESC {
		sortedKey = "-" + sortedKey
	}
	result := &sortIterator{
		query:     query,
		reader:    reader,
		sortedKey: sortedKey,
		size:      preLoadSize,
		sid:       fieldKey.SeriesID,
		closer:    s.closer,
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
	iter := newBlugeMatchIterator(documentMatchIterator, reader, nil)
	defer func() {
		err = multierr.Append(err, iter.Close())
	}()
	list = roaring.NewPostingList()
	for iter.Next() {
		list.Insert(iter.Val().docID)
	}
	return list, err
}

func (s *store) Match(fieldKey index.FieldKey, matches []string) (posting.List, error) {
	if len(matches) == 0 || fieldKey.Analyzer == databasev1.IndexRule_ANALYZER_UNSPECIFIED {
		return roaring.DummyPostingList, nil
	}
	reader, err := s.writer.Reader()
	if err != nil {
		return nil, err
	}
	analyzer := analyzers[fieldKey.Analyzer]
	fk := fieldKey.Marshal()
	query := bluge.NewBooleanQuery()
	if fieldKey.HasSeriesID() {
		query.AddMust(bluge.NewTermQuery(string(fieldKey.SeriesID.Marshal())).SetField(seriesIDField))
	}
	for _, m := range matches {
		query.AddMust(bluge.NewMatchQuery(m).SetField(fk).
			SetAnalyzer(analyzer))
	}
	documentMatchIterator, err := reader.Search(context.Background(), bluge.NewAllMatches(query))
	if err != nil {
		return nil, err
	}
	iter := newBlugeMatchIterator(documentMatchIterator, reader, nil)
	defer func() {
		err = multierr.Append(err, iter.Close())
	}()
	list := roaring.NewPostingList()
	for iter.Next() {
		list.Insert(iter.Val().docID)
	}
	return list, err
}

func (s *store) Range(fieldKey index.FieldKey, opts index.RangeOpts) (list posting.List, err error) {
	iter, err := s.Iterator(fieldKey, opts, modelv1.Sort_SORT_ASC, defaultRangePreloadSize)
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

func (s *store) SizeOnDisk() int64 {
	_, bytes := s.writer.DirectoryStats()
	return int64(bytes)
}

type searchResult struct {
	values    map[string][]byte
	seriesID  common.SeriesID
	docID     uint64
	timestamp int64
}

type blugeMatchIterator struct {
	delegated search.DocumentMatchIterator
	err       error
	closer    io.Closer
	current   searchResult
}

func newBlugeMatchIterator(delegated search.DocumentMatchIterator, closer io.Closer, needToLoadFields []string) blugeMatchIterator {
	bmi := blugeMatchIterator{
		delegated: delegated,
		closer:    closer,
		current:   searchResult{values: make(map[string][]byte, len(needToLoadFields))},
	}
	for i := range needToLoadFields {
		bmi.current.values[needToLoadFields[i]] = nil
	}
	return bmi
}

func (bmi *blugeMatchIterator) Next() bool {
	var match *search.DocumentMatch
	match, bmi.err = bmi.delegated.Next()
	if bmi.err != nil {
		return false
	}
	if match == nil {
		bmi.err = io.EOF
		return false
	}
	for i := range bmi.current.values {
		bmi.current.values[i] = nil
	}
	err := match.VisitStoredFields(func(field string, value []byte) bool {
		switch field {
		case docIDField:
			bmi.current.docID = convert.BytesToUint64(value)
		case seriesIDField:
			bmi.current.seriesID = common.SeriesID(convert.BytesToUint64(value))
		case timestampField:
			ts, err := bluge.DecodeDateTime(value)
			if err != nil {
				bmi.err = err
				return false
			}
			bmi.current.timestamp = ts.UnixNano()
		default:
			if _, ok := bmi.current.values[field]; ok {
				bmi.current.values[field] = bytes.Clone(value)
			}
		}
		return true
	})
	bmi.err = multierr.Combine(bmi.err, err)
	return bmi.err == nil
}

func (bmi *blugeMatchIterator) Val() searchResult {
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
	return errors.Join(bmi.err, bmi.closer.Close())
}
