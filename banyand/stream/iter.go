// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package stream

import (
	"errors"
	"io"

	"go.uber.org/multierr"

	"github.com/apache/skywalking-banyandb/api/common"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/partition"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

type searcherIterator struct {
	fieldIterator     index.FieldIterator
	err               error
	tagSpecIndex      map[string]*databasev1.TagSpec
	timeFilter        filterFn
	table             *tsTable
	l                 *logger.Logger
	indexFilter       map[common.SeriesID]filterFn
	tagProjIndex      map[string]partition.TagLocator
	sidToIndex        map[common.SeriesID]int
	entityMap         map[string]int
	tagProjection     []pbv1.TagProjection
	seriesList        pbv1.SeriesList
	currItem          item
	sortedTagLocation tagLocation
}

func newSearcherIterator(l *logger.Logger, fieldIterator index.FieldIterator, table *tsTable,
	indexFilter map[common.SeriesID]filterFn, timeFilter filterFn, tagProjection []pbv1.TagProjection,
	sortedTagLocation tagLocation, tagSpecIndex map[string]*databasev1.TagSpec,
	tagProjIndex map[string]partition.TagLocator, sidToIndex map[common.SeriesID]int,
	seriesList pbv1.SeriesList, entityMap map[string]int,
) *searcherIterator {
	return &searcherIterator{
		fieldIterator:     fieldIterator,
		table:             table,
		indexFilter:       indexFilter,
		timeFilter:        timeFilter,
		l:                 l,
		tagProjection:     tagProjection,
		sortedTagLocation: sortedTagLocation,
		tagSpecIndex:      tagSpecIndex,
		tagProjIndex:      tagProjIndex,
		sidToIndex:        sidToIndex,
		seriesList:        seriesList,
		entityMap:         entityMap,
	}
}

func (s *searcherIterator) Next() bool {
	if s.err != nil {
		return false
	}
	if !s.fieldIterator.Next() {
		s.err = io.EOF
		return false
	}
	itemID, seriesID, _ := s.fieldIterator.Val()
	if !s.timeFilter(itemID) {
		return s.Next()
	}
	if s.indexFilter != nil {
		if f, ok := s.indexFilter[seriesID]; ok && !f(itemID) {
			return s.Next()
		}
	}
	e, c, err := s.table.getElement(seriesID, int64(itemID), s.tagProjection)
	if err != nil {
		s.err = err
		return false
	}
	if len(s.tagProjIndex) != 0 {
		for entity, offset := range s.tagProjIndex {
			tagSpec := s.tagSpecIndex[entity]
			if tagSpec.IndexedOnly {
				continue
			}
			index, ok := s.sidToIndex[seriesID]
			if !ok {
				continue
			}
			series := s.seriesList[index]
			entityPos := s.entityMap[entity] - 1
			e.tagFamilies[offset.FamilyOffset].tags[offset.TagOffset] = tag{
				name:      entity,
				values:    mustEncodeTagValue(entity, tagSpec.GetType(), series.EntityValues[entityPos], c),
				valueType: pbv1.MustTagValueToValueType(series.EntityValues[entityPos]),
			}
		}
	}
	sv, err := s.sortedTagLocation.getTagValue(e)
	if err != nil {
		s.err = err
		return false
	}
	s.currItem = item{
		element:        e,
		count:          c,
		sortedTagValue: sv,
		seriesID:       seriesID,
	}
	return true
}

func (s *searcherIterator) Val() item {
	return s.currItem
}

func (s *searcherIterator) Close() error {
	if errors.Is(s.err, io.EOF) {
		return s.fieldIterator.Close()
	}
	return multierr.Combine(s.err, s.fieldIterator.Close())
}

type item struct {
	element        *element
	sortedTagValue []byte
	count          int
	seriesID       common.SeriesID
}

func (i item) SortedField() []byte {
	return i.sortedTagValue
}
