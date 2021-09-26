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
	"time"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/types/known/timestamppb"

	modelv2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v2"
	streamv2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v2"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/pkg/query/v2/executor"
)

type (
	seekerBuilder func(builder tsdb.SeekerBuilder)
	comparator    func(a, b *streamv2.Element) bool
)

func createTimestampComparator(sort modelv2.QueryOrder_Sort) comparator {
	if sort == modelv2.QueryOrder_SORT_ASC {
		return func(a, b *streamv2.Element) bool {
			return a.GetTimestamp().AsTime().Before(b.GetTimestamp().AsTime())
		}
	}

	return func(a, b *streamv2.Element) bool {
		return a.GetTimestamp().AsTime().After(b.GetTimestamp().AsTime())
	}
}

func createMultiTagsComparator(fieldRefs []*FieldRef, sortDirection modelv2.QueryOrder_Sort) comparator {
	tagFamilyIdx, tagIdx := fieldRefs[0].Spec.TagFamilyIdx, fieldRefs[0].Spec.TagIdx
	return func(a, b *streamv2.Element) bool {
		iTag, jTag := a.GetTagFamilies()[tagFamilyIdx].GetTags()[tagIdx], b.GetTagFamilies()[tagFamilyIdx].GetTags()[tagIdx]
		lField, _ := getRawTagValue(iTag)
		rField, _ := getRawTagValue(jTag)
		comp := bytes.Compare(lField, rField)
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
func executeForShard(ec executor.ExecutionContext, series tsdb.SeriesList, timeRange tsdb.TimeRange,
	projectionFieldRefs [][]*FieldRef, builders ...seekerBuilder) ([][]*streamv2.Element, error) {
	var elementsInShard [][]*streamv2.Element
	for _, seriesFound := range series {
		elementsInSeries, err := func() ([]*streamv2.Element, error) {
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
			iter, errInner := seeker.Seek()
			if errInner != nil {
				return nil, errInner
			}
			var elems []*streamv2.Element
			for _, iterator := range iter {
				for iterator.Next() {
					item := iterator.Val()
					tagFamilies, errInner := projectItem(ec, item, projectionFieldRefs)
					if errInner != nil {
						return nil, errors.WithStack(errInner)
					}
					elementID, errInner := ec.ParseElementID(item)
					if errInner != nil {
						return nil, errors.WithStack(errInner)
					}
					elems = append(elems, &streamv2.Element{
						ElementId:   elementID,
						Timestamp:   timestamppb.New(time.Unix(0, int64(item.Time()))),
						TagFamilies: tagFamilies,
					})
				}
				_ = iterator.Close()
			}

			return elems, nil
		}()
		if err != nil {
			return nil, err
		}
		if len(elementsInSeries) > 0 {
			elementsInShard = append(elementsInShard, elementsInSeries)
		}
	}
	return elementsInShard, nil
}

func mergeSort(input [][]*streamv2.Element, c comparator) []*streamv2.Element {
	// if input is nil, return empty list
	if input == nil {
		return []*streamv2.Element{}
	}

	var result = input[0]
	// if only one list, return the first ordered list
	if len(input) == 1 {
		return result
	}

	for i := 1; i < len(input); i++ {
		result = merge(result, input[i], c)
	}

	return result
}

func merge(left, right []*streamv2.Element, c comparator) []*streamv2.Element {
	size, i, j := len(left)+len(right), 0, 0
	slice := make([]*streamv2.Element, size)

	for k := 0; k < size; k++ {
		if i > len(left)-1 && j <= len(right)-1 {
			slice[k] = right[j]
			j++
		} else if j > len(right)-1 && i <= len(left)-1 {
			slice[k] = left[i]
			i++
		} else if c(left[i], right[j]) {
			slice[k] = left[i]
			i++
		} else {
			slice[k] = right[j]
			j++
		}
	}

	return slice
}
