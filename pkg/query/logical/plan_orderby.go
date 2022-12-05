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
	"fmt"

	"github.com/pkg/errors"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

// UnresolvedOrderBy is unanalyzed sorting expr.
type UnresolvedOrderBy struct {
	targetIndexRuleName string
	sort                modelv1.Sort
}

// Analyze the UnresolvedOrderBy to get OrderBy result.
func (u *UnresolvedOrderBy) Analyze(s Schema) (*OrderBy, error) {
	if u == nil {
		// return a default orderBy sub-plan
		return &OrderBy{
			Sort: modelv1.Sort_SORT_UNSPECIFIED,
		}, nil
	}

	if u.targetIndexRuleName == "" {
		return &OrderBy{
			Sort: u.sort,
		}, nil
	}

	defined, indexRule := s.IndexRuleDefined(u.targetIndexRuleName)
	if !defined {
		return nil, errors.Wrap(errIndexNotDefined, u.targetIndexRuleName)
	}

	projFieldSpecs, err := s.CreateTagRef(NewTags("", indexRule.GetTags()...))
	if err != nil {
		return nil, errTagNotDefined
	}

	return &OrderBy{
		Sort:      u.sort,
		Index:     indexRule,
		fieldRefs: projFieldSpecs[0],
	}, nil
}

// OrderBy is the sorting operator.
type OrderBy struct {
	Index     *databasev1.IndexRule
	fieldRefs []*TagRef
	Sort      modelv1.Sort
}

// Equal reports whether o and other has the same sorting order and name.
func (o *OrderBy) Equal(other interface{}) bool {
	if otherOrderBy, ok := other.(*OrderBy); ok {
		if o == nil && otherOrderBy == nil {
			return true
		}
		if o != nil && otherOrderBy == nil || o == nil && otherOrderBy != nil {
			return false
		}
		return o.Sort == otherOrderBy.Sort &&
			o.Index.GetMetadata().GetName() == otherOrderBy.Index.GetMetadata().GetName()
	}

	return false
}

// Strings shows the string represent.
func (o *OrderBy) String() string {
	return fmt.Sprintf("OrderBy: %v, sort=%s", o.Index.GetTags(), o.Sort.String())
}

// NewOrderBy return a sorting expr.
func NewOrderBy(indexRuleName string, sort modelv1.Sort) *UnresolvedOrderBy {
	return &UnresolvedOrderBy{
		sort:                sort,
		targetIndexRuleName: indexRuleName,
	}
}
