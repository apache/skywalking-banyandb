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

package aggregation

import (
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
)

// aggregationResultIterator is an iterator that returns a single aggregated result.
type aggregationResultIterator struct {
	dataPoint *measurev1.DataPoint
	consumed  bool
}

// NewAggregationResultIterator creates a new iterator for aggregation results.
func NewAggregationResultIterator(dataPoint *measurev1.DataPoint) executor.MIterator {
	return &aggregationResultIterator{
		dataPoint: dataPoint,
		consumed:  false,
	}
}

// Next moves to the next element in the iteration.
func (s *aggregationResultIterator) Next() bool {
	if s.consumed {
		return false
	}
	s.consumed = true
	return true
}

// Current returns the current data points in the iteration.
func (s *aggregationResultIterator) Current() []*measurev1.DataPoint {
	if s.consumed {
		return []*measurev1.DataPoint{s.dataPoint}
	}
	return nil
}

// Close closes the iterator and releases any resources.
func (s *aggregationResultIterator) Close() error {
	return nil
}
