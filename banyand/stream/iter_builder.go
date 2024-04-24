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
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/index"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

type filterFn func(itemID uint64) bool

func (s *stream) buildSeriesByIndex(tableWrappers []storage.TSTableWrapper[*tsTable],
	seriesList pbv1.SeriesList, sso pbv1.StreamSortOptions,
) (series []*searcherIterator, err error) {
	timeFilter := func(itemID uint64) bool {
		return sso.TimeRange.Contains(int64(itemID))
	}
	indexRuleForSorting := sso.Order.Index
	if len(indexRuleForSorting.Tags) != 1 {
		return nil, fmt.Errorf("only support one tag for sorting, but got %d", len(indexRuleForSorting.Tags))
	}
	sortedTag := indexRuleForSorting.Tags[0]
	tl := newTagLocation()
	for i := range sso.TagProjection {
		for j := range sso.TagProjection[i].Names {
			if sso.TagProjection[i].Names[j] == sortedTag {
				tl.familyIndex, tl.tagIndex = i, j
			}
		}
	}
	if !tl.valid() {
		return nil, fmt.Errorf("sorted tag %s not found in tag projection", sortedTag)
	}
	entityMap, tagSpecIndex, tagProjIndex, sidToIndex := s.genIndex(sso.TagProjection, seriesList)
	sids := seriesList.IDs()
	for _, tw := range tableWrappers {
		seriesFilter := make(map[common.SeriesID]filterFn)
		if sso.Filter != nil {
			for i := range sids {
				pl, errExe := sso.Filter.Execute(func(ruleType databasev1.IndexRule_Type) (index.Searcher, error) {
					return tw.Table().Index().store, nil
				}, sids[i])
				if errExe != nil {
					return nil, err
				}

				seriesFilter[sids[i]] = func(itemID uint64) bool {
					if pl == nil {
						return true
					}
					return pl.Contains(itemID)
				}
			}
		}

		var inner index.FieldIterator
		fieldKey := index.FieldKey{
			IndexRuleID: indexRuleForSorting.GetMetadata().GetId(),
			Analyzer:    indexRuleForSorting.GetAnalyzer(),
		}
		inner, err = tw.Table().Index().Sort(sids, fieldKey, sso.Order.Sort, sso.MaxElementSize)
		if err != nil {
			return nil, err
		}

		if inner != nil {
			series = append(series, newSearcherIterator(s.l, inner, tw.Table(),
				seriesFilter, timeFilter, sso.TagProjection, tl,
				tagSpecIndex, tagProjIndex, sidToIndex, seriesList, entityMap))
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
