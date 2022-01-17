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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
)

func Test_ParseTag_And_ParseField(t *testing.T) {
	s, deferFunc := setup(t)
	defer deferFunc()
	baseTime := writeData(t, "query_data.json", s)
	r := require.New(t)
	shard, err := s.Shard(0)
	r.NoError(err)
	series, err := shard.Series().Get(tsdb.Entity{tsdb.Entry("1")})
	r.NoError(err)
	seriesSpan, err := series.Span(tsdb.NewInclusiveTimeRangeDuration(baseTime, 1*time.Hour))
	defer func(seriesSpan tsdb.SeriesSpan) {
		_ = seriesSpan.Close()
	}(seriesSpan)
	r.NoError(err)
	seeker, err := seriesSpan.SeekerBuilder().OrderByTime(modelv1.Sort_SORT_DESC).Build()
	r.NoError(err)
	iter, err := seeker.Seek()
	r.NoError(err)
	at := assert.New(t)
	r.Equal(1, len(iter))
	{
		defer func(iterator tsdb.Iterator) {
			_ = iterator.Close()
		}(iter[0])
		i := 0
		expectedFields := [][]int64{{150, 300, 5}, {200, 50, 4}, {100, 100, 1}}
		for ; iter[0].Next(); i++ {
			item := iter[0].Val()
			tagFamily, err := s.ParseTagFamily("default", item)
			r.NoError(err)
			at.Equal(2, len(tagFamily.Tags))
			summation, err := s.ParseField("summation", item)
			r.NoError(err)
			at.Equal(expectedFields[i][0], summation.GetValue().GetInt().Value)
			count, err := s.ParseField("count", item)
			r.NoError(err)
			at.Equal(expectedFields[i][1], count.GetValue().GetInt().Value)
			value, err := s.ParseField("value", item)
			r.NoError(err)
			at.Equal(expectedFields[i][2], value.GetValue().GetInt().Value)
		}
	}
}
