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
	"fmt"

	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/iter/sort"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

func (s *stream) buildItersByIndex(tableWrappers []storage.TSTableWrapper[*tsTable],
	seriesList pbv1.SeriesList, sqo pbv1.StreamQueryOptions,
) (iters []sort.Iterator[*index.ItemRef], tl *tagLocation, err error) {
	indexRuleForSorting := sqo.Order.Index
	if len(indexRuleForSorting.Tags) != 1 {
		return nil, nil, fmt.Errorf("only support one tag for sorting, but got %d", len(indexRuleForSorting.Tags))
	}
	sortedTag := indexRuleForSorting.Tags[0]
	tl = newTagLocation()
	for i := range sqo.TagProjection {
		for j := range sqo.TagProjection[i].Names {
			if sqo.TagProjection[i].Names[j] == sortedTag {
				tl.familyIndex, tl.tagIndex = i, j
			}
		}
	}
	if !tl.valid() {
		return nil, nil, fmt.Errorf("sorted tag %s not found in tag projection", sortedTag)
	}
	sids := seriesList.IDs()
	for _, tw := range tableWrappers {
		var iter index.FieldIterator[*index.ItemRef]
		fieldKey := index.FieldKey{
			IndexRuleID: indexRuleForSorting.GetMetadata().GetId(),
			Analyzer:    indexRuleForSorting.GetAnalyzer(),
		}
		iter, err = tw.Table().Index().Sort(sids, fieldKey, sqo.Order.Sort, sqo.MaxElementSize)
		if err != nil {
			return nil, nil, err
		}
		iters = append(iters, iter)
	}
	return
}

type tagLocation struct {
	familyIndex int
	tagIndex    int
}

func newTagLocation() *tagLocation {
	return &tagLocation{
		familyIndex: -1,
		tagIndex:    -1,
	}
}

func (t tagLocation) valid() bool {
	return t.familyIndex != -1 && t.tagIndex != -1
}
