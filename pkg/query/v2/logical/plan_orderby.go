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
	"fmt"
	"sort"

	"github.com/pkg/errors"

	databasev2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v2"
	modelv2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v2"
	streamv2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v2"
	"github.com/apache/skywalking-banyandb/pkg/convert"
)

type unresolvedOrderBy struct {
	sort                modelv2.QueryOrder_Sort
	targetIndexRuleName string
}

func (u *unresolvedOrderBy) Analyze(s Schema) (*orderBy, error) {
	if u == nil {
		// return a default orderBy sub-plan
		return &orderBy{
			sort: modelv2.QueryOrder_SORT_DESC,
		}, nil
	}

	if u.targetIndexRuleName == "" {
		return &orderBy{
			sort: u.sort,
		}, nil
	}

	defined, indexRule := s.IndexRuleDefined(u.targetIndexRuleName)
	if !defined {
		return nil, errors.Wrap(ErrIndexNotDefined, u.targetIndexRuleName)
	}

	refs, err := s.CreateRef(NewTags("", indexRule.GetTags()...))

	if err != nil {
		return nil, ErrFieldNotDefined
	}

	projFieldSpecs, err := s.Proj(refs...).CreateRef(NewTags("", indexRule.GetTags()...))

	if err != nil {
		return nil, ErrFieldNotDefined
	}

	return &orderBy{
		sort:      u.sort,
		index:     indexRule,
		fieldRefs: projFieldSpecs[0],
	}, nil
}

type orderBy struct {
	// orderByIndex describes the indexRule used to sort the elements/
	// It can be null since by default we may sort by created-time.
	index *databasev2.IndexRule
	// while orderBySort describes the sort direction
	sort modelv2.QueryOrder_Sort
	// TODO: support multiple tags. Currently only the first member will be used for sorting.
	fieldRefs []*FieldRef
}

func (o *orderBy) Equal(other interface{}) bool {
	if otherOrderBy, ok := other.(*orderBy); ok {
		if o == nil && otherOrderBy == nil {
			return true
		}
		if o != nil && otherOrderBy == nil || o == nil && otherOrderBy != nil {
			return false
		}
		return o.sort == otherOrderBy.sort &&
			o.index.GetMetadata().GetName() == otherOrderBy.index.GetMetadata().GetName()
	} else {
		return false
	}
}

func (o *orderBy) String() string {
	return fmt.Sprintf("OrderBy: %v, sort=%s", o.index.GetTags(), o.sort.String())
}

func OrderBy(indexRuleName string, sort modelv2.QueryOrder_Sort) *unresolvedOrderBy {
	return &unresolvedOrderBy{
		sort:                sort,
		targetIndexRuleName: indexRuleName,
	}
}

func getRawTagValue(typedPair *modelv2.Tag) ([]byte, error) {
	switch v := typedPair.GetValue().Value.(type) {
	case *modelv2.TagValue_Str:
		return []byte(v.Str.GetValue()), nil
	case *modelv2.TagValue_Int:
		return convert.Int64ToBytes(v.Int.GetValue()), nil
	default:
		return nil, errors.New("unsupported data types")
	}
}

// SortedByIndex is used to test whether the given entities are sorted by the sortDirection
// The given entities MUST satisfy both the positive check and the negative check for the reversed direction
func SortedByIndex(elements []*streamv2.Element, tagFamilyIdx, tagIdx int, sortDirection modelv2.QueryOrder_Sort) bool {
	if modelv2.QueryOrder_SORT_UNSPECIFIED == sortDirection {
		return true
	}
	return sort.SliceIsSorted(elements, sortByIndex(elements, tagFamilyIdx, tagIdx, sortDirection)) &&
		!sort.SliceIsSorted(elements, sortByIndex(elements, tagFamilyIdx, tagIdx, reverseSortDirection(sortDirection)))
}

func SortedByTimestamp(elements []*streamv2.Element, sortDirection modelv2.QueryOrder_Sort) bool {
	if modelv2.QueryOrder_SORT_UNSPECIFIED == sortDirection {
		return true
	}
	return sort.SliceIsSorted(elements, sortByTimestamp(elements, sortDirection)) &&
		!sort.SliceIsSorted(elements, sortByTimestamp(elements, reverseSortDirection(sortDirection)))
}

func reverseSortDirection(sort modelv2.QueryOrder_Sort) modelv2.QueryOrder_Sort {
	if sort == modelv2.QueryOrder_SORT_DESC {
		return modelv2.QueryOrder_SORT_ASC
	}
	return modelv2.QueryOrder_SORT_DESC
}

func sortByIndex(entities []*streamv2.Element, tagFamilyIdx, tagIdx int, sortDirection modelv2.QueryOrder_Sort) func(i, j int) bool {
	return func(i, j int) bool {
		iPair := entities[i].GetTagFamilies()[tagFamilyIdx].GetTags()[tagIdx]
		jPair := entities[j].GetTagFamilies()[tagFamilyIdx].GetTags()[tagIdx]
		lField, _ := getRawTagValue(iPair)
		rField, _ := getRawTagValue(jPair)
		comp := bytes.Compare(lField, rField)
		if sortDirection == modelv2.QueryOrder_SORT_ASC {
			return comp == -1
		}
		return comp == 1
	}
}

func sortByTimestamp(entities []*streamv2.Element, sortDirection modelv2.QueryOrder_Sort) func(i, j int) bool {
	return func(i, j int) bool {
		iPair := entities[i].GetTimestamp().AsTime().UnixNano()
		jPair := entities[j].GetTimestamp().AsTime().UnixNano()
		if sortDirection == modelv2.QueryOrder_SORT_ASC {
			return iPair < jPair
		}
		return iPair > jPair
	}
}
