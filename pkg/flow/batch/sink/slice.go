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

package sink

import (
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	batchApi "github.com/apache/skywalking-banyandb/pkg/flow/batch/api"
	"github.com/apache/skywalking-banyandb/pkg/iter"
)

var _ batchApi.Sink[tsdb.Item] = (*ItemSlice)(nil)

type ItemSlice struct {
	items []tsdb.Item
}

func (s *ItemSlice) Drain(iter iter.Iterator[tsdb.Item]) error {
	for {
		item, hasNext := iter.Next()
		if !hasNext {
			return nil
		}
		s.items = append(s.items, item)
	}
}

func (s *ItemSlice) Val() []tsdb.Item {
	return s.items
}

func NewItemSlice() *ItemSlice {
	return &ItemSlice{}
}
