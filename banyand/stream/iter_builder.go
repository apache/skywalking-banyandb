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
	"bytes"
	"fmt"

	"github.com/apache/skywalking-banyandb/api/common"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/posting"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

var rangeOpts = index.RangeOpts{}

type filterFn func(itemID uint64) bool

type iterBuilder struct {
	indexFilter         index.Filter
	timeRange           *timestamp.TimeRange
	tableWrappers       []storage.TSTableWrapper[*tsTable]
	indexRuleForSorting *databasev1.IndexRule
	l                   *logger.Logger
	tagProjection       []pbv1.TagProjection
	seriesID            common.SeriesID
	order               modelv1.Sort
	preloadSize         int
}

func newIterBuilder(tableWrappers []storage.TSTableWrapper[*tsTable], id common.SeriesID, sso pbv1.StreamSortOptions) *iterBuilder {
	return &iterBuilder{
		indexFilter:         sso.Filter,
		timeRange:           sso.TimeRange,
		tableWrappers:       tableWrappers,
		indexRuleForSorting: sso.Order.Index,
		l:                   logger.GetLogger("seeker-builder"),
		tagProjection:       sso.TagProjection,
		seriesID:            id,
		order:               sso.Order.Sort,
		preloadSize:         sso.MaxElementSize,
	}
}

func buildSeriesByIndex(s *iterBuilder) (series []*searcherIterator, err error) {
	timeFilter := func(itemID uint64) bool {
		return s.timeRange.Contains(int64(itemID))
	}
	if len(s.indexRuleForSorting.Tags) != 1 {
		return nil, fmt.Errorf("only support one tag for sorting, but got %d", len(s.indexRuleForSorting.Tags))
	}
	sortedTag := s.indexRuleForSorting.Tags[0]
	tl := newTagLocation()
	for i := range s.tagProjection {
		for j := range s.tagProjection[i].Names {
			if s.tagProjection[i].Names[j] == sortedTag {
				tl.familyIndex, tl.tagIndex = i, j
			}
		}
	}
	if !tl.valid() {
		return nil, fmt.Errorf("sorted tag %s not found in tag projection", sortedTag)
	}
	for _, tw := range s.tableWrappers {
		var indexFilter filterFn
		if s.indexFilter != nil {
			var pl posting.List
			pl, err = s.indexFilter.Execute(func(ruleType databasev1.IndexRule_Type) (index.Searcher, error) {
				return tw.Table().Index().store, nil
			}, s.seriesID)
			if err != nil {
				return nil, err
			}
			indexFilter = func(itemID uint64) bool {
				if pl == nil {
					return true
				}
				return pl.Contains(itemID)
			}
		}

		var inner index.FieldIterator
		fieldKey := index.FieldKey{
			SeriesID:    s.seriesID,
			IndexRuleID: s.indexRuleForSorting.GetMetadata().GetId(),
			Analyzer:    s.indexRuleForSorting.GetAnalyzer(),
		}
		inner, err = tw.Table().Index().Iterator(fieldKey, rangeOpts, s.order, s.preloadSize)
		if err != nil {
			return nil, err
		}

		if inner != nil {
			series = append(series, newSearcherIterator(s.l, inner, tw.Table(),
				s.seriesID, indexFilter, timeFilter, s.tagProjection, tl))
		}
	}
	return
}

type tagLocation struct {
	familyIndex int
	tagIndex    int
}

func newTagLocation() tagLocation {
	return tagLocation{
		familyIndex: -1,
		tagIndex:    -1,
	}
}

func (t tagLocation) valid() bool {
	return t.familyIndex != -1 && t.tagIndex != -1
}

func (t tagLocation) getTagValue(e *element) ([]byte, error) {
	if len(e.tagFamilies) <= t.familyIndex {
		return nil, fmt.Errorf("tag family index %d out of range", t.familyIndex)
	}
	if len(e.tagFamilies[t.familyIndex].tags) <= t.tagIndex {
		return nil, fmt.Errorf("tag index %d out of range", t.tagIndex)
	}
	if len(e.tagFamilies[t.familyIndex].tags[t.tagIndex].values) <= e.index {
		return nil, fmt.Errorf("element index %d out of range", e.index)
	}
	v := e.tagFamilies[t.familyIndex].tags[t.tagIndex].values[e.index]
	return bytes.Clone(v), nil
}
