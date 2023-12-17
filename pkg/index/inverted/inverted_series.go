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
	"context"

	"github.com/blugelabs/bluge"
	"github.com/blugelabs/bluge/search"
	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
)

// Create implements index.SeriesStore.
func (s *store) Create(series index.Series) error {
	if !s.closer.AddRunning() {
		return nil
	}
	defer s.closer.Done()
	select {
	case <-s.closer.CloseNotify():
	case s.ch <- series:
	}
	return nil
}

// Search implements index.SeriesStore.
func (s *store) Search(term []byte) (common.SeriesID, error) {
	reader, err := s.writer.Reader()
	if err != nil {
		return 0, err
	}
	query := bluge.NewTermQuery(convert.BytesToString(term)).SetField(entityField)
	dmi, err := reader.Search(context.Background(), bluge.NewAllMatches(query))
	if err != nil {
		return 0, err
	}
	var result common.SeriesID
	next, err := dmi.Next()
	if err == nil && next != nil {
		err = next.VisitStoredFields(func(field string, value []byte) bool {
			if field == docIDField {
				result = common.SeriesID(convert.BytesToUint64(value))
				return false
			}
			return true
		})
		if err != nil {
			return 0, errors.WithMessage(err, "visit stored fields")
		}
	}
	if err != nil {
		return 0, errors.WithMessage(err, "iterate document match iterator")
	}
	return result, nil
}

// SearchPrefix implements index.SeriesStore.
func (s *store) SearchPrefix(prefix []byte) ([]index.Series, error) {
	reader, err := s.writer.Reader()
	if err != nil {
		return nil, err
	}
	query := bluge.NewPrefixQuery(convert.BytesToString(prefix)).SetField(entityField)
	dmi, err := reader.Search(context.Background(), bluge.NewAllMatches(query))
	if err != nil {
		return nil, err
	}
	return parseResult(dmi)
}

// SearchWildcard implements index.SeriesStore.
func (s *store) SearchWildcard(wildcard []byte) ([]index.Series, error) {
	reader, err := s.writer.Reader()
	if err != nil {
		return nil, err
	}
	query := bluge.NewWildcardQuery(convert.BytesToString(wildcard)).SetField(entityField)
	dmi, err := reader.Search(context.Background(), bluge.NewAllMatches(query))
	if err != nil {
		return nil, err
	}
	return parseResult(dmi)
}

func parseResult(dmi search.DocumentMatchIterator) ([]index.Series, error) {
	result := make([]index.Series, 0, 10)
	next, err := dmi.Next()
	docIDMap := make(map[uint64]struct{})
	for err == nil && next != nil {
		var series index.Series
		err = next.VisitStoredFields(func(field string, value []byte) bool {
			if field == docIDField {
				id := convert.BytesToUint64(value)
				if _, ok := docIDMap[id]; !ok {
					series.ID = common.SeriesID(convert.BytesToUint64(value))
					docIDMap[id] = struct{}{}
				}
			}
			if field == entityField {
				series.EntityValues = value
			}
			return true
		})
		if err != nil {
			return nil, errors.WithMessage(err, "visit stored fields")
		}
		if series.ID > 0 {
			result = append(result, series)
		}
		next, err = dmi.Next()
	}
	if err != nil {
		return nil, errors.WithMessage(err, "iterate document match iterator")
	}
	return result, nil
}
