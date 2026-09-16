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

package measure

import (
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
)

type sortableDataPoints struct {
	iter        executor.MIterator
	current     *comparableDataPoint
	sortTagSpec logical.TagSpec
	sortByTime  bool
}

func (s *sortableDataPoints) Next() bool {
	if !s.iter.Next() {
		return false
	}

	dp := s.iter.Current()[0]
	var err error
	s.current, err = newComparableElement(dp, s.sortByTime, s.sortTagSpec)
	return err == nil
}

func (s *sortableDataPoints) Val() *comparableDataPoint {
	return s.current
}

func (s *sortableDataPoints) Close() error {
	return s.iter.Close()
}
