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

package tsdb

import (
	"github.com/pkg/errors"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/index"
)

var errUnsupportedIndexRule = errors.New("the index rule is not supported")

func (s *seekerBuilder) Filter(predicator index.Filter) SeekerBuilder {
	s.predicator = predicator
	return s
}

func (s *seekerBuilder) buildIndexFilter(block blockDelegate) (filterFn, error) {
	if s.predicator == nil {
		return nil, nil
	}
	allItemIDs, err := s.predicator.Execute(func(typ databasev1.IndexRule_Type) (index.Searcher, error) {
		switch typ {
		case databasev1.IndexRule_TYPE_INVERTED:
			return block.invertedIndexReader(), nil
		case databasev1.IndexRule_TYPE_TREE:
			return block.lsmIndexReader(), nil
		default:
			return nil, errUnsupportedIndexRule
		}
	}, s.seriesSpan.seriesID)
	if err != nil {
		return nil, err
	}
	return func(item Item) bool {
		valid := allItemIDs.Contains(uint64(item.ID()))
		s.seriesSpan.l.Trace().Int("valid_item_num", allItemIDs.Len()).Bool("valid", valid).Msg("filter item by index")
		return valid
	}, nil
}

type filterFn func(item Item) bool
