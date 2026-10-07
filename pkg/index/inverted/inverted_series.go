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
	"io"
	"time"

	"github.com/blugelabs/bluge"
	segment "github.com/blugelabs/bluge_segment_api"
	"github.com/pkg/errors"
	"go.uber.org/multierr"

	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/analyzer"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

func (s *store) InsertSeriesBatch(batch index.Batch) error {
	if len(batch.Documents) == 0 {
		return nil
	}
	if !s.closer.AddRunning() {
		if batch.PersistentCallback != nil {
			batch.PersistentCallback(errors.New("store is closed"))
		}
		return nil
	}
	defer s.closer.Done()
	b := generateBatch()
	if batch.PersistentCallback != nil {
		b.SetPersistedCallback(batch.PersistentCallback)
	}
	defer releaseBatch(b)
	for _, d := range batch.Documents {
		doc, ff := toDoc(d, true)
		b.InsertIfAbsent(doc.ID(), ff, doc)
	}
	return s.writer.Batch(b)
}

func (s *store) UpdateSeriesBatch(batch index.Batch) error {
	if len(batch.Documents) == 0 {
		return nil
	}
	if !s.closer.AddRunning() {
		if batch.PersistentCallback != nil {
			batch.PersistentCallback(errors.New("store is closed"))
		}
		return nil
	}
	defer s.closer.Done()
	b := generateBatch()
	defer releaseBatch(b)
	if batch.PersistentCallback != nil {
		b.SetPersistedCallback(batch.PersistentCallback)
	}
	for _, d := range batch.Documents {
		doc, _ := toDoc(d, false)
		b.Update(doc.ID(), doc)
	}
	return s.writer.Batch(b)
}

func (s *store) Delete(docID [][]byte) error {
	if !s.closer.AddRunning() {
		return nil
	}
	defer s.closer.Done()
	batch := generateBatch()
	defer releaseBatch(batch)
	for _, id := range docID {
		batch.Delete(bluge.Identifier(id))
	}
	return s.writer.Batch(batch)
}

func toDoc(d index.Document, toParseFieldNames bool) (*bluge.Document, []string) {
	doc := bluge.NewDocument(convert.BytesToString(d.EntityValues))
	var fieldNames []string
	if toParseFieldNames && len(d.Fields) > 0 {
		fieldNames = make([]string, 0, len(d.Fields))
	}
	for _, f := range d.Fields {
		var tf *bluge.TermField
		k := f.Key.Marshal()
		if f.Index {
			tf = bluge.NewKeywordFieldBytes(k, f.GetBytes())
			if f.Store {
				tf.StoreValue()
			}
			if !f.NoSort {
				tf.Sortable()
			}
			if f.Key.Analyzer != index.AnalyzerUnspecified {
				tf = tf.WithAnalyzer(analyzer.Analyzers[f.Key.Analyzer])
			}
		} else {
			tf = bluge.NewStoredOnlyField(k, f.GetBytes())
		}
		doc.AddField(tf)
		if fieldNames != nil {
			fieldNames = append(fieldNames, k)
		}
	}

	if d.Timestamp > 0 {
		doc.AddField(bluge.NewDateTimeField(timestampField, time.Unix(0, d.Timestamp)).StoreValue())
	}
	if d.Version > 0 {
		vf := bluge.NewStoredOnlyField(versionField, convert.Int64ToBytes(d.Version))
		doc.AddField(vf)
	}
	return doc, fieldNames
}

// errRetiredSeriesQuery reports a call to a series-index query method of this
// retired store. The series index is served by pkg/index/native since the
// NIDX-03 cutover; the only production user left, the Stream element index's
// offline migration tool (NIDX-04, banyand/stream/migration_element_index.go),
// writes through Batch and Close and never queries.
var errRetiredSeriesQuery = errors.New("inverted: series-index queries are retired; " +
	"the NIDX-04 element migration tool only writes through Batch")

// BuildQuery implements index.SeriesStore. It is retired and always fails:
// nothing calls it any more, and it exists only because index.SeriesStore
// declares it.
func (s *store) BuildQuery([]index.SeriesMatcher, index.Query, *timestamp.TimeRange) (index.Query, error) {
	return nil, errors.WithMessage(errRetiredSeriesQuery, "BuildQuery")
}

// Search implements index.SeriesStore. It is retired and always fails; see
// BuildQuery.
func (s *store) Search(context.Context, []index.FieldKey, index.Query, int) ([]index.SeriesDocument, error) {
	return nil, errors.WithMessage(errRetiredSeriesQuery, "Search")
}

// StoredFields implements index.SeriesStore.
func (s *store) StoredFields(ctx context.Context, docID []byte, projection ...index.FieldKey) (map[string][][]byte, error) {
	reader, err := s.writer.Reader()
	if err != nil {
		return nil, err
	}
	defer func() {
		if r := recover(); r != nil {
			_ = reader.Close()
			panic(r)
		}
		_ = reader.Close()
	}()

	q := bluge.NewTermQuery(convert.BytesToString(docID))
	q.SetField(docIDField)
	dmi, err := reader.Search(ctx, bluge.NewAllMatches(q))
	if err != nil {
		return nil, err
	}
	match, err := dmi.Next()
	if err != nil {
		return nil, err
	}
	if match == nil {
		return nil, nil
	}
	var want map[string]struct{}
	if len(projection) > 0 {
		want = make(map[string]struct{}, len(projection))
		for i := range projection {
			want[projection[i].Marshal()] = struct{}{}
		}
	}
	fields := make(map[string][][]byte)
	if visitErr := match.VisitStoredFields(func(field string, value []byte) bool {
		switch field {
		case docIDField, seriesIDField, timestampField, versionField:
			return true // always skip internal bookkeeping fields, even if projected
		}
		if want != nil {
			if _, ok := want[field]; !ok {
				return true // not in the requested projection
			}
		}
		fields[field] = append(fields[field], bytes.Clone(value))
		return true
	}); visitErr != nil {
		return nil, visitErr
	}
	return fields, nil
}

// SeriesSort implements index.SeriesStore. It is retired and always fails;
// see BuildQuery.
func (s *store) SeriesSort(context.Context, index.Query, *index.OrderBy, int, []index.FieldKey) (index.FieldIterator[*index.DocumentResult], error) {
	return nil, errors.WithMessage(errRetiredSeriesQuery, "SeriesSort")
}

func (s *store) SeriesIterator(ctx context.Context) (index.FieldIterator[index.Series], error) {
	reader, err := s.writer.Reader()
	if err != nil {
		return nil, err
	}

	dict, err := reader.DictionaryIterator(docIDField, nil, nil, nil)
	if err != nil {
		_ = reader.Close()
		return nil, err
	}
	return &dictIterator{dict: dict, ctx: ctx, closer: reader}, nil
}

//nolint:govet // reader ownership is kept beside the dictionary lifecycle state.
type dictIterator struct {
	dict     segment.DictionaryIterator
	ctx      context.Context
	err      error
	series   index.Series
	i        int
	closer   io.Closer
	closed   bool
	closeErr error
}

func (d *dictIterator) Next() bool {
	if d.err != nil {
		return false
	}
	if d.i%1000 == 0 {
		select {
		case <-d.ctx.Done():
			d.err = d.ctx.Err()
			_ = d.Close()
			return false
		default:
		}
	}
	de, err := d.dict.Next()
	if err != nil {
		d.err = err
		_ = d.Close()
		return false
	}
	if de == nil {
		_ = d.Close()
		return false
	}
	d.series = index.Series{
		EntityValues: convert.StringToBytes(de.Term()),
	}
	d.i++
	return true
}

func (d *dictIterator) Query() index.Query {
	return nil
}

func (d *dictIterator) Val() index.Series {
	return d.series
}

func (d *dictIterator) Close() error {
	if d.closed {
		return d.closeErr
	}
	d.closed = true
	if d.closer == nil {
		d.closeErr = multierr.Combine(d.err, d.dict.Close())
		return d.closeErr
	}
	d.closeErr = multierr.Combine(d.err, d.dict.Close(), d.closer.Close())
	return d.closeErr
}
