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
	"github.com/pkg/errors"

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
	for i := range seriesMatchers {
		switch seriesMatchers[i].Type {
		case index.SeriesMatcherTypeExact:
			q := bluge.NewTermQuery(convert.BytesToString(seriesMatchers[i].Match))
			q.SetField(entityField)
			qs[i] = q
		case index.SeriesMatcherTypePrefix:
			q := bluge.NewPrefixQuery(convert.BytesToString(seriesMatchers[i].Match))
			q.SetField(entityField)
			qs[i] = q
		case index.SeriesMatcherTypeWildcard:
			q := bluge.NewWildcardQuery(convert.BytesToString(seriesMatchers[i].Match))
			q.SetField(entityField)
			qs[i] = q
		default:
			return nil, errors.Errorf("unsupported series matcher type: %v", seriesMatchers[i].Type)
		}
	}
	var primaryQuery bluge.Query
	if len(qs) > 1 {
		bq := bluge.NewBooleanQuery()
		bq.AddShould(qs...)
		bq.SetMinShould(1)
		primaryQuery = bq
	} else {
		primaryQuery = qs[0]
	}

	query := bluge.NewBooleanQuery().AddMust(primaryQuery)
	if secondaryQuery != nil && secondaryQuery.(*Query).query != nil {
		query.AddMust(secondaryQuery.(*Query).query)
	}
	return &Query{query: query}, nil
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

	dmi, err := reader.Search(ctx, bluge.NewAllMatches(query.(*Query).query))
	if err != nil {
		return nil, err
	}
	return parseResult(dmi, projection)
}

func parseResult(dmi search.DocumentMatchIterator, loadedFields []index.FieldKey) ([]index.SeriesDocument, error) {
	result := make([]index.SeriesDocument, 0, 10)
	next, err := dmi.Next()
	docIDMap := make(map[uint64]struct{})
	fields := make([]string, 0, len(loadedFields))
	for i := range loadedFields {
		fields = append(fields, loadedFields[i].Marshal())
	}
	for err == nil && next != nil {
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
			return nil, errors.WithMessage(err, "visit stored fields")
		}
		if doc.Key.ID > 0 {
			result = append(result, doc)
		}
		next, err = dmi.Next()
	}
	if err != nil {
		return nil, errors.WithMessage(err, "iterate document match iterator")
	}
	return result, nil
}
