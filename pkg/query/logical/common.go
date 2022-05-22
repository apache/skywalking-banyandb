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

	"github.com/pkg/errors"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

var (
	ErrTagNotDefined              = errors.New("tag is not defined")
	ErrFieldNotDefined            = errors.New("field is not defined")
	ErrInvalidConditionType       = errors.New("invalid pair type")
	ErrIncompatibleQueryCondition = errors.New("incompatible query condition type")
	ErrIndexNotDefined            = errors.New("index is not define for the tag")
	ErrMultipleGlobalIndexes      = errors.New("multiple global indexes are not supported")
)

var ErrInvalidData = errors.New("data is invalid")

type (
	seekerBuilder func(builder tsdb.SeekerBuilder)
	comparator    func(a, b tsdb.Item) bool
)

func createComparator(sortDirection modelv1.Sort) comparator {
	return func(a, b tsdb.Item) bool {
		comp := bytes.Compare(a.SortedField(), b.SortedField())
		if sortDirection == modelv1.Sort_SORT_DESC {
			return comp == 1
		}
		return comp == -1
	}
}

// projectItem parses the item within the StreamExecutionContext.
// projectionFieldRefs must be prepared before calling this method, projectionFieldRefs should be a list of
// tag list where the inner list must exist in the same tag family.
// Strict order can be guaranteed in the result.
func projectItem(ec executor.ExecutionContext, item tsdb.Item, projectionFieldRefs [][]*TagRef) ([]*modelv1.TagFamily, error) {
	tagFamily := make([]*modelv1.TagFamily, len(projectionFieldRefs))
	for i, refs := range projectionFieldRefs {
		if len(refs) == 0 {
			continue
		}
		tags := make([]*modelv1.Tag, len(refs))
		familyName := refs[0].tag.GetFamilyName()
		parsedTagFamily, err := ec.ParseTagFamily(familyName, item)
		if err != nil {
			return nil, err
		}
		if len(refs) > len(parsedTagFamily.Tags) {
			return nil, errors.Wrapf(ErrInvalidData,
				"the number of tags %d in %s is less then expected %d",
				len(parsedTagFamily.Tags), familyName, len(refs))
		}
		for j, ref := range refs {
			tags[j] = parsedTagFamily.GetTags()[ref.Spec.TagIdx]
		}

		tagFamily[i] = &modelv1.TagFamily{
			Name: familyName,
			Tags: tags,
		}
	}

	return tagFamily, nil
}

// executeForShard fetches elements from series within a single shard. A list of series must be prepared in advanced
// with the help of Entity. The result is a list of element set, where the order of inner list is kept
// as what the users specify in the seekerBuilder.
// This method is used by the underlying tableScan and localIndexScan plans.
func executeForShard(series tsdb.SeriesList, timeRange timestamp.TimeRange,
	builders ...seekerBuilder) ([]tsdb.Iterator, error) {
	var itersInShard []tsdb.Iterator
	for _, seriesFound := range series {
		itersInSeries, err := func() ([]tsdb.Iterator, error) {
			sp, errInner := seriesFound.Span(timeRange)
			defer func(sp tsdb.SeriesSpan) {
				if sp != nil {
					_ = sp.Close()
				}
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
