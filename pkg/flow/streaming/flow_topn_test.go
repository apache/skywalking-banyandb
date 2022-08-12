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

package streaming

import (
	"testing"

	"github.com/emirpasic/gods/maps/treemap"
	"github.com/emirpasic/gods/utils"
	"github.com/stretchr/testify/assert"
)

type data []interface{}

func row(args ...interface{}) data {
	return args
}

func TestFlow_TopN_Aggregator(t *testing.T) {
	assert := assert.New(t)
	topN := &topNAggregator{
		topNum:    3,
		cacheSize: 5,
		treeMap:   treemap.NewWith(utils.Int64Comparator),
		sortKeyExtractor: func(elem interface{}) int64 {
			return int64(elem.(data)[1].(int))
		},
	}

	topN.Add([]interface{}{row("e2e-service-provider", 10000), row("e2e-service-consumer", 9900)})
	topN.Add([]interface{}{row("e2e-service-provider", 9800), row("e2e-service-consumer", 9700)})
	topN.Add([]interface{}{row("e2e-service-provider", 9700), row("e2e-service-consumer", 9600)})
	topN.Add([]interface{}{row("e2e-service-provider", 9800), row("e2e-service-consumer", 9500)})

	result := topN.GetResult()
	assert.Len(result, 3)
	assert.Equal([]*Tuple2{
		{int64(9500), row("e2e-service-consumer", 9500)},
		{int64(9600), row("e2e-service-consumer", 9600)},
		{int64(9700), row("e2e-service-consumer", 9700)},
	}, result)
}
