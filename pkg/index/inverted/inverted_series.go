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

	"github.com/blugelabs/bluge"
	"github.com/blugelabs/bluge/search"
	segment "github.com/blugelabs/bluge_segment_api"
	"github.com/pkg/errors"
	"go.uber.org/multierr"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
)

var emptySeries = make([]index.SeriesDocument, 0)

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
