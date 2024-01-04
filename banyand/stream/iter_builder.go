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

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

var (
	errUnspecifiedIndexType = errors.New("Unspecified index type")
	rangeOpts               = index.RangeOpts{}
)

type filterFn func(item item) bool

type seekerBuilder struct {
	indexFilter         index.Filter
	seriesSpan          *seriesSpan
	indexRuleForSorting *databasev1.IndexRule
	l                   *logger.Logger
	tagProjection       []pbv1.TagProjection
	order               modelv1.Sort
}

func (s *seekerBuilder) enhance(indexFilter index.Filter,
	indexRuleForSorting *databasev1.IndexRule,
	order modelv1.Sort, tagProjection []pbv1.TagProjection,
) {
	s.indexFilter = indexFilter
	s.indexRuleForSorting = indexRuleForSorting
	s.order = order
	s.tagProjection = tagProjection
}

func (s *seekerBuilder) buildSeriesByIndex() (series []*searcherIterator, err error) {
	timeFilter := func(item item) bool {
		valid := s.seriesSpan.timeRange.Contains(item.Time())
		timeRange := s.seriesSpan.timeRange
		s.seriesSpan.l.Trace().
			Times("time_range", []time.Time{timeRange.Start, timeRange.End}).
			Bool("valid", valid).Msg("filter item by time range")
		return valid
	}
	for _, tw := range s.seriesSpan.tableWrappers {
		var inner index.FieldIterator
		var err error
		fieldKey := index.FieldKey{
			SeriesID:    s.seriesSpan.seriesID,
			IndexRuleID: s.indexRuleForSorting.GetMetadata().GetId(),
			Analyzer:    s.indexRuleForSorting.GetAnalyzer(),
		}
		switch s.indexRuleForSorting.GetType() {
		case databasev1.IndexRule_TYPE_TREE:
			inner, err = tw.Table().Index().Iterator(fieldKey, rangeOpts, s.order)
		case databasev1.IndexRule_TYPE_INVERTED:
			inner, err = tw.Table().Index().Iterator(fieldKey, rangeOpts, s.order)
		case databasev1.IndexRule_TYPE_UNSPECIFIED:
			return nil, errors.WithMessagef(errUnspecifiedIndexType, "index rule:%v", s.indexRuleForSorting)
		}
		if err != nil {
			return nil, err
		}
		if inner != nil {
			series = append(series, newSearcherIterator(s.seriesSpan.l, inner, tw.Table(),
				s.seriesSpan.seriesID, s.indexFilter, timeFilter, s.tagProjection))
		}
	}
	return
}
