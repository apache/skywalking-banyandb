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
	"time"

	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/api/common"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

var (
	errUnspecifiedIndexType = errors.New("Unspecified index type")
	rangeOpts               = index.RangeOpts{}
)

type filterFn func(item item) bool

type iterBuilder struct {
	indexFilter         index.Filter
	timeRange           *timestamp.TimeRange
	tableWrappers       []storage.TSTableWrapper[*tsTable]
	indexRuleForSorting *databasev1.IndexRule
	l                   *logger.Logger
	tagProjection       []pbv1.TagProjection
	seriesID            common.SeriesID
	order               modelv1.Sort
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
	}
}

func buildSeriesByIndex(s *iterBuilder) (series []*searcherIterator, err error) {
	timeFilter := func(item item) bool {
		valid := s.timeRange.Contains(item.Time())
		timeRange := s.timeRange
		s.l.Trace().
			Times("time_range", []time.Time{timeRange.Start, timeRange.End}).
			Bool("valid", valid).Msg("filter item by time range")
		return valid
	}
	for _, tw := range s.tableWrappers {
		indexFilter := func(item item) bool {
			if s.indexFilter == nil {
				return true
			}
			pl, err := s.indexFilter.Execute(func(ruleType databasev1.IndexRule_Type) (index.Searcher, error) {
				return tw.Table().Index().store, nil
			}, s.seriesID)
			if err != nil {
				return false
			}
			valid := pl.Contains(item.Time())
			s.l.Trace().Int("valid_item_num", pl.Len()).Bool("valid", valid).Msg("filter item by index")
			return valid
		}
		var inner index.FieldIterator
		var err error
		fieldKey := index.FieldKey{
			SeriesID:    s.seriesID,
			IndexRuleID: s.indexRuleForSorting.GetMetadata().GetId(),
			Analyzer:    s.indexRuleForSorting.GetAnalyzer(),
		}
		inner, err = tw.Table().Index().Iterator(fieldKey, rangeOpts, s.order)
		if err != nil {
			return nil, err
		}
		if inner != nil {
			series = append(series, newSearcherIterator(s.l, inner, tw.Table(),
				s.seriesID, indexFilter, timeFilter, s.tagProjection))
		}
	}
	return
}
