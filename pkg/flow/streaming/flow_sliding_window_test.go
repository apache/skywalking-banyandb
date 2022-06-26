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
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"github.com/apache/skywalking-banyandb/pkg/flow/api"
	"github.com/apache/skywalking-banyandb/pkg/flow/streaming/sink"
)

func Test_SlidingWindow_NoOutput(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	aggrFunc := api.NewMockAggregateFunction(ctrl)

	assert := assert.New(t)
	slidingWindows := NewSlidingTimeWindows(time.Minute*1, time.Second*15)
	slidingWindows.Aggregate(aggrFunc)
	assert.NoError(slidingWindows.Setup(context.TODO()))
	slidingWindows.Exec(sink.NewSlice())
	baseTs := time.Now()
	aggrFunc.EXPECT().Add(gomock.Any()).MaxTimes(0)
	// add a single
	input := []api.StreamRecord{
		api.NewStreamRecord(1, baseTs.UnixMilli()),
	}
	for _, r := range input {
		slidingWindows.In() <- r
	}
}

func Test_SlidingWindow_Trigger_Once(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	aggrFunc := api.NewMockAggregateFunction(ctrl)

	assert := assert.New(t)
	slidingWindows := NewSlidingTimeWindows(time.Minute*1, time.Second*15)
	slidingWindows.Aggregate(aggrFunc)
	assert.NoError(slidingWindows.Setup(context.TODO()))
	snk := sink.NewSlice()
	assert.NoError(snk.Setup(context.TODO()))
	slidingWindows.Exec(snk)
	baseTs := time.Now()
	aggrFunc.EXPECT().
		Add(gomock.Eq([]interface{}{1})).
		Times(1)

	aggrFunc.EXPECT().
		GetResult().
		Times(1).Return(1)

	// add a single
	input := []api.StreamRecord{
		api.NewStreamRecord(1, baseTs.UnixMilli()),
		api.NewStreamRecord(2, baseTs.Add(time.Minute*1).UnixMilli()),
	}
	for _, r := range input {
		slidingWindows.In() <- r
	}
	assert.NoError(Await().AtMost(10 * time.Second).Until(func() bool {
		if len(snk.Value()) > 0 {
			return assert.Len(snk.Value(), 1)
		}
		return false
	}))
}
