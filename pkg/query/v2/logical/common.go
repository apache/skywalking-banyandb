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

package logical

import (
	"bytes"

	modelv2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v2"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/pkg/query/v2/executor"
)

type (
	seekerBuilder func(builder tsdb.SeekerBuilder)
	comparator    func(a, b tsdb.Item) bool
)

func createTimestampComparator(sortDirection modelv2.QueryOrder_Sort) comparator {
	if sortDirection == modelv2.QueryOrder_SORT_ASC {
		return func(a, b tsdb.Item) bool {
			return a.Time() < b.Time()
		}
	}

	return func(a, b tsdb.Item) bool {
		return a.Time() > b.Time()
	}
}

func createComparator(sortDirection modelv2.QueryOrder_Sort) comparator {
	return func(a, b tsdb.Item) bool {
		comp := bytes.Compare(a.SortedField(), b.SortedField())
		if sortDirection == modelv2.QueryOrder_SORT_ASC {
			return comp == -1
		}
		return comp == 1
	}
}

// projectItem parses the item within the ExecutionContext.
// projectionFieldRefs must be prepared before calling this method, projectionFieldRefs should be a list of
// tag list where the inner list must exist in the same tag family.
// Strict order can be guaranteed in the result.
func projectItem(ec executor.ExecutionContext, item tsdb.Item, projectionFieldRefs [][]*FieldRef) ([]*modelv2.TagFamily, error) {
	tagFamily := make([]*modelv2.TagFamily, len(projectionFieldRefs))
	for i, refs := range projectionFieldRefs {
		tags := make([]*modelv2.Tag, len(refs))
		familyName := refs[0].tag.GetFamilyName()
		parsedTagFamily, err := ec.ParseTagFamily(familyName, item)
		if err != nil {
			return nil, err
		}
		for j, ref := range refs {
			tags[j] = parsedTagFamily.GetTags()[ref.Spec.TagIdx]
		}

		tagFamily[i] = &modelv2.TagFamily{
			Name: familyName,
			Tags: tags,
		}
	}

	return tagFamily, nil
}

// executeForShard fetches elements from series within a single shard. A list of series must be prepared in advanced
// with the help of Entity. The result is a list of element set, where the order of inner list is kept
// as what the users specify in the seekerBuilder.
// This method is used by the underlying tableScan and indexScan plans.
func executeForShard(series tsdb.SeriesList, timeRange tsdb.TimeRange,
	builders ...seekerBuilder) ([]tsdb.Iterator, error) {
	var itersInShard []tsdb.Iterator
	for _, seriesFound := range series {
		itersInSeries, err := func() ([]tsdb.Iterator, error) {
			sp, errInner := seriesFound.Span(timeRange)
			defer func(sp tsdb.SeriesSpan) {
				_ = sp.Close()
			}(sp)
			if errInner != nil {
				return nil, errInner
			}
			b := sp.SeekerBuilder()
			for _, builder := range builders {
				builder(b)
			}
			seeker, errInner := b.Build()
			if errInner != nil {
				return nil, errInner
			}
			iters, errInner := seeker.Seek()
			if errInner != nil {
				return nil, errInner
			}
			return iters, nil
		}()
		if err != nil {
			return nil, err
		}
		if len(itersInSeries) > 0 {
			itersInShard = append(itersInShard, itersInSeries...)
		}
	}
	return itersInShard, nil
}
