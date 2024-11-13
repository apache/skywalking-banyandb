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

	"github.com/apache/skywalking-banyandb/api/common"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
)

var emptySeries = make([]index.SeriesDocument, 0)

func (s *store) SeriesBatch(batch index.Batch) error {
	if len(batch.Documents) == 0 {
		return nil
	}
	if !s.closer.AddRunning() {
		return nil
	}
	defer s.closer.Done()
	b := generateBatch()
	defer releaseBatch(b)
	for _, d := range batch.Documents {
		doc := bluge.NewDocument(convert.BytesToString(convert.Uint64ToBytes(d.DocID)))
		for _, f := range d.Fields {
			tf := bluge.NewKeywordFieldBytes(f.Key.Marshal(), f.Term)
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
		}

		doc.AddField(bluge.NewKeywordFieldBytes(entityField, d.EntityValues).StoreValue())
		if d.Timestamp > 0 {
			doc.AddField(bluge.NewDateTimeField(timestampField, time.Unix(0, d.Timestamp)).StoreValue())
		}
		if len(d.Fields) == 0 {
			b.InsertIfAbsent(doc.ID(), doc)
		} else {
			b.Update(doc.ID(), doc)
		}
	}
	return s.writer.Batch(b)
}

// BuildQuery implements index.SeriesStore.
func (s *store) BuildQuery(seriesMatchers []index.SeriesMatcher, secondaryQuery index.Query) (index.Query, error) {
	if len(seriesMatchers) == 0 {
		return secondaryQuery, nil
	}

	qs := make([]bluge.Query, len(seriesMatchers))
	nodes := make([]node, len(seriesMatchers))
	for i := range seriesMatchers {
		switch seriesMatchers[i].Type {
		case index.SeriesMatcherTypeExact:
			match := convert.BytesToString(seriesMatchers[i].Match)
			q := bluge.NewTermQuery(match)
			q.SetField(entityField)
			qs[i] = q
			nodes = append(nodes, newTermNode(match, nil))
		case index.SeriesMatcherTypePrefix:
			match := convert.BytesToString(seriesMatchers[i].Match)
			q := bluge.NewPrefixQuery(match)
			q.SetField(entityField)
			qs[i] = q
			nodes = append(nodes, newPrefixNode(match))
		case index.SeriesMatcherTypeWildcard:
			match := convert.BytesToString(seriesMatchers[i].Match)
			q := bluge.NewWildcardQuery(match)
			q.SetField(entityField)
			qs[i] = q
			nodes = append(nodes, newWildcardNode(match))
		default:
			return nil, errors.Errorf("unsupported series matcher type: %v", seriesMatchers[i].Type)
		}
	}
	var primaryQuery bluge.Query
	var n node
	if len(qs) > 1 {
		bq := bluge.NewBooleanQuery()
		bq.AddShould(qs...)
		bq.SetMinShould(1)
		primaryQuery = bq
		n = newShouldNode()
		for i := range nodes {
			n.(*shouldNode).Append(nodes[i])
		}
	} else {
		primaryQuery = qs[0]
		n = nodes[0]
	}

	query := bluge.NewBooleanQuery().AddMust(primaryQuery)
	node := newMustNode()
	node.Append(n)
	if secondaryQuery != nil && secondaryQuery.(*queryNode).query != nil {
		query.AddMust(secondaryQuery.(*queryNode).query)
		node.Append(secondaryQuery.(*queryNode).node)
	}
	return &queryNode{query, node}, nil
}

// Search implements index.SeriesStore.
func (s *store) Search(ctx context.Context,
	projection []index.FieldKey, query index.Query,
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
	return parseResult(dmi, projection)
}

func parseResult(dmi search.DocumentMatchIterator, loadedFields []index.FieldKey) ([]index.SeriesDocument, error) {
	result := make([]index.SeriesDocument, 0, 10)
	next, err := dmi.Next()
	if err != nil {
		return nil, errors.WithMessage(err, "iterate document match iterator")
	}
	docIDMap := make(map[uint64]struct{})
	fields := make([]string, 0, len(loadedFields))
	for i := range loadedFields {
		fields = append(fields, loadedFields[i].Marshal())
	}
	var hitNumber int
	for err == nil && next != nil {
		hitNumber = next.HitNumber
		var doc index.SeriesDocument
		if len(loadedFields) > 0 {
			doc.Fields = make(map[string][]byte)
			for i := range loadedFields {
				doc.Fields[fields[i]] = nil
			}
		}
		err = next.VisitStoredFields(func(field string, value []byte) bool {
			switch field {
			case docIDField:
				id := convert.BytesToUint64(value)
				if _, ok := docIDMap[id]; !ok {
					doc.Key.ID = common.SeriesID(convert.BytesToUint64(value))
					docIDMap[id] = struct{}{}
				}
			case entityField:
				doc.Key.EntityValues = value
			default:
				if _, ok := doc.Fields[field]; ok {
					doc.Fields[field] = bytes.Clone(value)
				}
			}
			return true
		})
		if err != nil {
			return nil, errors.WithMessagef(err, "visit stored fields, hit: %d", hitNumber)
		}
		if doc.Key.ID > 0 {
			result = append(result, doc)
		}
		next, err = dmi.Next()
	}
	if err != nil {
		return nil, errors.WithMessagef(err, "iterate document match iterator, hit: %d", hitNumber)
	}
	return result, nil
}

func (s *store) SeriesSort(ctx context.Context, fieldKey index.FieldKey, termRange index.RangeOpts, order modelv1.Sort,
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

	if termRange.Upper == nil {
		termRange.Upper = defaultUpper
	}
	if termRange.Lower == nil {
		termRange.Lower = defaultLower
	}
	rangeQuery := bluge.NewBooleanQuery()
	rangeQuery.AddMust(bluge.NewTermRangeInclusiveQuery(
		string(termRange.Lower),
		string(termRange.Upper),
		termRange.IncludesLower,
		termRange.IncludesUpper,
	).
		SetField(fk))
	rangeNode := newMustNode()
	rangeNode.Append(newTermRangeInclusiveNode(string(termRange.Lower), string(termRange.Upper), termRange.IncludesLower, termRange.IncludesUpper, nil))

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
		query:       &queryNode{query, node},
		fields:      fields,
		reader:      reader,
		sortedKey:   sortedKey,
		size:        preLoadSize,
		closer:      s.closer,
		ctx:         ctx,
		newIterator: newSeriesIterator,
	}
	return result, nil
}

type seriesIterator struct {
	*blugeMatchIterator
	needToLoadFields []string
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
		needToLoadFields: append(needToLoadFields, entityField, docIDField, seriesIDField, timestampField),
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
	si.current.SeriesID = 0
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
	case entityField:
		si.current.EntityValues = value
	case docIDField:
		si.current.DocID = convert.BytesToUint64(value)
	case timestampField:
		ts, errTime := bluge.DecodeDateTime(value)
		if errTime != nil {
			si.err = errTime
			return false
		}
		si.current.Timestamp = ts.UnixNano()
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
	dict, err := reader.DictionaryIterator(entityField, nil, nil, nil)
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
