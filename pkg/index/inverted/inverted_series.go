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
	"github.com/blugelabs/bluge/search"
	segment "github.com/blugelabs/bluge_segment_api"
	"github.com/pkg/errors"
	"go.uber.org/multierr"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/analyzer"
	querypkg "github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

var emptySeries = make([]index.SeriesDocument, 0)

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

// BuildQuery implements index.SeriesStore.
func (s *store) BuildQuery(seriesMatchers []index.SeriesMatcher, secondaryQuery index.Query, timeRange *timestamp.TimeRange) (index.Query, error) {
	if len(seriesMatchers) == 0 && timeRange == nil {
		return secondaryQuery, nil
	}

	query := bluge.NewBooleanQuery()
	rootNode := newMustNode()
	if len(seriesMatchers) > 0 {
		qs := make([]bluge.Query, len(seriesMatchers))
		matcherNodes := make([]node, len(seriesMatchers))
		for i := range seriesMatchers {
			switch seriesMatchers[i].Type {
			case index.SeriesMatcherTypeExact:
				match := convert.BytesToString(seriesMatchers[i].Match)
				q := bluge.NewTermQuery(match)
				q.SetField(docIDField)
				qs[i] = q
				matcherNodes = append(matcherNodes, newTermNode(match, nil))
			case index.SeriesMatcherTypePrefix:
				match := convert.BytesToString(seriesMatchers[i].Match)
				q := bluge.NewPrefixQuery(match)
				q.SetField(docIDField)
				qs[i] = q
				matcherNodes = append(matcherNodes, newPrefixNode(match))
			case index.SeriesMatcherTypeWildcard:
				match := convert.BytesToString(seriesMatchers[i].Match)
				q := bluge.NewWildcardQuery(match)
				q.SetField(docIDField)
				qs[i] = q
				matcherNodes = append(matcherNodes, newWildcardNode(match))
			default:
				return nil, errors.Errorf("unsupported series matcher type: %v", seriesMatchers[i].Type)
			}
		}
		var primaryQuery bluge.Query
		var primaryNode node
		if len(qs) > 1 {
			bq := bluge.NewBooleanQuery()
			bq.AddShould(qs...)
			bq.SetMinShould(1)
			primaryQuery = bq
			primaryNode = newShouldNode()
			for i := range matcherNodes {
				primaryNode.(*shouldNode).Append(matcherNodes[i])
			}
		} else {
			primaryQuery = qs[0]
			primaryNode = matcherNodes[0]
		}
		query.AddMust(primaryQuery)
		rootNode.Append(primaryNode)
	}
	if secondaryQuery != nil && secondaryQuery.(*queryNode).query != nil {
		query.AddMust(secondaryQuery.(*queryNode).query)
		rootNode.Append(secondaryQuery.(*queryNode).node)
	}
	if timeRange != nil {
		q := bluge.NewDateRangeInclusiveQuery(timeRange.Start, timeRange.End, timeRange.IncludeStart, timeRange.IncludeEnd)
		q.SetField(timestampField)
		query.AddMust(q)
		rootNode.Append(newTimeRangeNode(timeRange))
	}
	return &queryNode{query, rootNode}, nil
}

// Search implements index.SeriesStore.
func (s *store) Search(ctx context.Context,
	projection []index.FieldKey, query index.Query, limit int,
) ([]index.SeriesDocument, error) {
	reader, err := s.writer.Reader()
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := recover(); err != nil {
			_ = reader.Close()
			panic(err)
		}
		_ = reader.Close()
	}()

	dmi, err := reader.Search(ctx, bluge.NewAllMatches(query.(*queryNode).query))
	if err != nil {
		return nil, err
	}
	return parseResult(ctx, dmi, projection, limit)
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

func parseResult(ctx context.Context, dmi search.DocumentMatchIterator, loadedFields []index.FieldKey, limit int) ([]index.SeriesDocument, error) {
	if chargeErr := querypkg.Charge(ctx, 1024+uint64(len(loadedFields))*64); chargeErr != nil {
		return nil, chargeErr
	}
	result := make([]index.SeriesDocument, 0, 10)
	fields := make([]string, 0, len(loadedFields))
	for _, loadedField := range loadedFields {
		fields = append(fields, loadedField.Marshal())
	}
	for {
		if contextErr := ctx.Err(); contextErr != nil {
			return nil, contextErr
		}
		next, nextErr := dmi.Next()
		if nextErr != nil {
			return nil, errors.WithMessage(nextErr, "iterate document match iterator")
		}
		if next == nil {
			return result, nil
		}
		doc, readErr := readSeriesDocument(ctx, next, fields)
		if readErr != nil {
			return nil, readErr
		}
		if len(doc.Key.EntityValues) > 0 {
			result = append(result, doc)
		}
		if limit > 0 && len(result) >= limit {
			return result, nil
		}
	}
}

func readSeriesDocument(ctx context.Context, match *search.DocumentMatch, fields []string) (index.SeriesDocument, error) {
	var doc index.SeriesDocument
	// Charge the result container and projected-field map before retaining this hit.
	if chargeErr := querypkg.ChargeResult(ctx, 256+uint64(len(fields))*64); chargeErr != nil {
		return doc, chargeErr
	}
	if len(fields) > 0 {
		doc.Fields = make(map[string][]byte, len(fields))
		for _, fieldName := range fields {
			doc.Fields[fieldName] = nil
		}
	}
	var fieldErr error
	visitErr := match.VisitStoredFields(func(field string, value []byte) bool {
		switch field {
		case docIDField:
			if fieldErr = querypkg.Charge(ctx, uint64(len(value))); fieldErr != nil {
				return false
			}
			doc.Key.EntityValues = bytes.Clone(value)
		case timestampField:
			var ts time.Time
			ts, fieldErr = bluge.DecodeDateTime(value)
			if fieldErr != nil {
				return false
			}
			doc.Timestamp = ts.UnixNano()
		case versionField:
			doc.Version = convert.BytesToInt64(value)
		default:
			if _, ok := doc.Fields[field]; ok {
				if fieldErr = querypkg.Charge(ctx, uint64(len(value))); fieldErr != nil {
					return false
				}
				doc.Fields[field] = bytes.Clone(value)
			}
		}
		return true
	})
	if readErr := multierr.Combine(visitErr, fieldErr); readErr != nil {
		return index.SeriesDocument{}, errors.WithMessagef(readErr, "visit stored fields, hit: %d", match.HitNumber)
	}
	return doc, nil
}

func (s *store) SeriesSort(ctx context.Context, indexQuery index.Query, orderBy *index.OrderBy,
	preLoadSize int, fieldKeys []index.FieldKey,
) (iter index.FieldIterator[*index.DocumentResult], err error) {
	var sortedKey string
	switch orderBy.Type {
	case index.OrderByTypeTime:
		sortedKey = timestampField
	case index.OrderByTypeIndex:
		fieldKey := index.FieldKey{
			IndexRuleID: orderBy.Index.Metadata.Id,
		}
		sortedKey = fieldKey.Marshal()
	default:
		return nil, errors.Errorf("unsupported order by type: %v", orderBy.Type)
	}
	if orderBy.Sort == modelv1.Sort_SORT_DESC {
		sortedKey = "-" + sortedKey
	}
	fields := make([]string, 0, len(fieldKeys))
	for i := range fieldKeys {
		fields = append(fields, fieldKeys[i].Marshal())
	}

	if !s.closer.AddRunning() {
		return nil, nil
	}
	reader, err := s.writer.Reader()
	if err != nil {
		return nil, err
	}

	return &sortIterator{
		query:       indexQuery,
		fields:      fields,
		reader:      reader,
		sortedKey:   sortedKey,
		size:        preLoadSize,
		closer:      s.closer,
		ctx:         ctx,
		newIterator: newSeriesIterator,
	}, nil
}

type seriesIterator struct {
	*blugeMatchIterator
}

func newSeriesIterator(delegated search.DocumentMatchIterator, closer io.Closer,
	needToLoadFields []string,
) blugeIterator {
	si := &seriesIterator{
		blugeMatchIterator: &blugeMatchIterator{
			delegated: delegated,
			closer:    closer,
			ctx:       search.NewSearchContext(1, 0),
			current:   index.DocumentResult{Values: make(map[string][]byte, len(needToLoadFields))},
		},
	}
	for _, f := range needToLoadFields {
		si.current.Values[f] = nil
	}
	return si
}

func (si *seriesIterator) Next() bool {
	var match *search.DocumentMatch
	match, si.err = si.delegated.Next()
	if si.err != nil {
		si.err = errors.WithMessagef(si.err, "failed to get next document, hit: %d", si.hit)
		return false
	}
	if match == nil {
		si.err = io.EOF
		return false
	}
	si.hit = match.HitNumber
	for i := range si.current.Values {
		si.current.Values[i] = nil
	}
	si.current.DocID = 0
	si.current.Timestamp = 0
	si.current.SortedValue = nil
	if len(match.SortValue) > 0 {
		si.current.SortedValue = match.SortValue[0]
	}

	err := match.VisitStoredFields(si.setVal)
	si.err = multierr.Combine(si.err, err)
	if si.err != nil {
		return false
	}
	return si.err == nil
}

func (si *seriesIterator) setVal(field string, value []byte) bool {
	switch field {
	case docIDField:
		si.current.EntityValues = value
	case timestampField:
		ts, errTime := bluge.DecodeDateTime(value)
		if errTime != nil {
			si.err = errTime
			return false
		}
		si.current.Timestamp = ts.UnixNano()
	case versionField:
		si.current.Version = convert.BytesToInt64(value)
	default:
		if _, ok := si.current.Values[field]; ok {
			si.current.Values[field] = bytes.Clone(value)
		}
	}
	return true
}

func (s *store) SeriesIterator(ctx context.Context) (index.FieldIterator[index.Series], error) {
	reader, err := s.writer.Reader()
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = reader.Close()
	}()

	dict, err := reader.DictionaryIterator(docIDField, nil, nil, nil)
	if err != nil {
		return nil, err
	}
	return &dictIterator{dict: dict, ctx: ctx}, nil
}

type dictIterator struct {
	dict   segment.DictionaryIterator
	ctx    context.Context
	err    error
	series index.Series
	i      int
}

func (d *dictIterator) Next() bool {
	if d.err != nil {
		return false
	}
	if d.i%1000 == 0 {
		select {
		case <-d.ctx.Done():
			d.err = d.ctx.Err()
			return false
		default:
		}
	}
	de, err := d.dict.Next()
	if err != nil {
		d.err = err
		return false
	}
	if de == nil {
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
	return multierr.Combine(d.err, d.dict.Close())
}
