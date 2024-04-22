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
	"sync/atomic"
	"time"

	"github.com/blugelabs/bluge"
	"github.com/blugelabs/bluge/analysis"
	"github.com/blugelabs/bluge/analysis/analyzer"
	blugeIndex "github.com/blugelabs/bluge/index"
	"github.com/blugelabs/bluge/search"
	"go.uber.org/multierr"

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
	docIDField    = "_id"
	batchSize     = 1024
	seriesIDField = "series_id"
	entityField   = "entity"
	idField       = "id"
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

type flushEvent struct {
	onComplete chan struct{}
}

type store struct {
	writer        *bluge.Writer
	ch            chan any
	closer        *run.Closer
	l             *logger.Logger
	errClosing    atomic.Pointer[error]
	batchInterval time.Duration
}

func (s *store) Batch(batch index.Batch) error {
	if !s.closer.AddRunning() {
		return nil
	}
	defer s.closer.Done()
	select {
	case <-s.closer.CloseNotify():
	case s.ch <- batch:
	}
	return nil
}

// NewStore create a new inverted index repository.
func NewStore(opts StoreOpts) (index.SeriesStore, error) {
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
	sec := opts.BatchWaitSec
	if sec < 1 {
		sec = 1
	}
	s := &store{
		writer:        w,
		batchInterval: time.Duration(sec * int64(time.Second)),
		l:             opts.Logger,
		ch:            make(chan any, batchSize),
		closer:        run.NewCloser(1),
	}
	s.run()
	return s, nil
}

func (s *store) Close() error {
	s.flush()
	s.closer.CloseThenWait()
	if s.errClosing.Load() != nil {
		return *s.errClosing.Load()
	}
	return nil
}

func (s *store) Write(fields []index.Field, docID uint64) error {
	if len(fields) < 1 {
		return nil
	}
	if !s.closer.AddRunning() {
		return nil
	}
	defer s.closer.Done()
	select {
	case <-s.closer.CloseNotify():
	case s.ch <- index.Document{
		Fields: fields,
		DocID:  docID,
	}:
	}
	return nil
}

func (s *store) Iterator(fieldKey index.FieldKey, termRange index.RangeOpts, order modelv1.Sort, preLoadSize int) (iter index.FieldIterator, err error) {
	if termRange.Lower != nil &&
		termRange.Upper != nil &&
		bytes.Compare(termRange.Lower, termRange.Upper) > 0 {
		return index.DummyFieldIterator, nil
	}
	if termRange.Upper == nil {
		termRange.Upper = defaultUpper
	}
	if termRange.Lower == nil {
		termRange.Lower = defaultLower
	}
	reader, err := s.writer.Reader()
	if err != nil {
		return nil, err
	}
	fk := fieldKey.MarshalIndexRule()
	var query bluge.Query
	if fieldKey.Analyzer == databasev1.IndexRule_ANALYZER_UNSPECIFIED {
		query = bluge.NewTermRangeInclusiveQuery(
			index.FieldStr(fieldKey, termRange.Lower),
			index.FieldStr(fieldKey, termRange.Upper),
			termRange.IncludesLower,
			termRange.IncludesUpper,
		).
			SetField(fk)
	} else {
		bQuery := bluge.NewBooleanQuery().
			AddMust(bluge.NewTermRangeInclusiveQuery(
				string(termRange.Lower),
				string(termRange.Upper),
				termRange.IncludesLower,
				termRange.IncludesUpper,
			).
				SetField(fk))
		if fieldKey.HasSeriesID() {
			bQuery.AddMust(bluge.NewTermQuery(string(fieldKey.SeriesID.Marshal())).SetField(seriesIDField))
		}
		query = bQuery
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
	fk := field.Key.MarshalIndexRule()
	var query bluge.Query
	if field.Key.Analyzer == databasev1.IndexRule_ANALYZER_UNSPECIFIED {
		query = bluge.NewTermQuery(string(field.Marshal())).SetField(fk)
	} else {
		bQuery := bluge.NewBooleanQuery().
			AddMust(bluge.NewTermQuery(string(field.Term)).SetField(fk))
		if field.Key.HasSeriesID() {
			bQuery.AddMust(bluge.NewTermQuery(string(field.Key.SeriesID.Marshal())).SetField(seriesIDField))
		}
		query = bQuery
	}
	documentMatchIterator, err := reader.Search(context.Background(), bluge.NewAllMatches(query))
	if err != nil {
		return nil, err
	}
	iter := newBlugeMatchIterator(documentMatchIterator, reader)
	defer func() {
		err = multierr.Append(err, iter.Close())
	}()
	list = roaring.NewPostingList()
	for iter.Next() {
		list.Insert(iter.Val())
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
	fk := fieldKey.MarshalIndexRule()
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
	iter := newBlugeMatchIterator(documentMatchIterator, reader)
	defer func() {
		err = multierr.Append(err, iter.Close())
	}()
	list := roaring.NewPostingList()
	for iter.Next() {
		list.Insert(iter.Val())
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
		list.Insert(iter.Val())
	}
	err = multierr.Append(err, iter.Close())
	return
}

func (s *store) SizeOnDisk() int64 {
	_, bytes := s.writer.DirectoryStats()
	return int64(bytes)
}

func (s *store) run() {
	go func() {
		defer func() {
			if err := s.writer.Close(); err != nil {
				s.errClosing.Store(&err)
			}
			s.closer.Done()
		}()
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
		defer flush()
		var docIDBuffer bytes.Buffer
		for {
			timer := time.NewTimer(s.batchInterval)
			select {
			case <-s.closer.CloseNotify():
				timer.Stop()
				return
			case event, more := <-s.ch:
				if !more {
					timer.Stop()
					return
				}
				switch d := event.(type) {
				case flushEvent:
					flush()
					close(d.onComplete)
				case index.Document, index.Batch:
					var docs []index.Document
					var isBatch bool
					var applied chan struct{}
					switch v := d.(type) {
					case index.Document:
						docs = []index.Document{v}
					case index.Batch:
						docs = v.Documents
						applied = v.Applied
						isBatch = true
					}

					for _, d := range docs {
						// TODO: generate a segment directly.
						var fk *index.FieldKey
						if len(d.Fields) > 0 {
							fk = &d.Fields[0].Key
						}
						docIDBuffer.Reset()
						if fk != nil && fk.HasSeriesID() {
							docIDBuffer.Write(fk.SeriesID.Marshal())
						}
						docIDBuffer.Write(convert.Uint64ToBytes(d.DocID))
						doc := bluge.NewDocument(docIDBuffer.String())
						toAddSeriesIDField := false
						for _, f := range d.Fields {
							if f.Key.Analyzer == databasev1.IndexRule_ANALYZER_UNSPECIFIED {
								doc.AddField(bluge.NewKeywordFieldBytes(f.Key.MarshalIndexRule(), f.Marshal()).Sortable())
							} else {
								toAddSeriesIDField = true
								doc.AddField(bluge.NewKeywordFieldBytes(f.Key.MarshalIndexRule(), f.Term).Sortable().
									WithAnalyzer(analyzers[f.Key.Analyzer]))
							}
						}
						if fk != nil && toAddSeriesIDField && fk.HasSeriesID() {
							doc.AddField(bluge.NewKeywordFieldBytes(seriesIDField, fk.SeriesID.Marshal()))
						}

						if d.EntityValues != nil {
							doc.AddField(bluge.NewKeywordFieldBytes(entityField, d.EntityValues).StoreValue())
						}

						size++
						batch.Update(doc.ID(), doc)
					}
					if isBatch || size >= batchSize {
						flush()
						if applied != nil {
							close(applied)
						}
					}
				}
			case <-timer.C:
				flush()
			}
			timer.Stop()
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
	err       error
	closer    io.Closer
	docID     uint64
}

func newBlugeMatchIterator(delegated search.DocumentMatchIterator, closer io.Closer) blugeMatchIterator {
	return blugeMatchIterator{
		delegated: delegated,
		closer:    closer,
	}
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
	bmi.err = match.VisitStoredFields(func(field string, value []byte) bool {
		if field == docIDField {
			if len(value) == 8 {
				bmi.docID = convert.BytesToUint64(value)
			} else if len(value) == 16 {
				// value = seriesID(8bytes)+docID(8bytes)
				bmi.docID = convert.BytesToUint64(value[8:])
			}
		}
		return true
	})
	return bmi.err == nil
}

func (bmi *blugeMatchIterator) Val() uint64 {
	return bmi.docID
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
