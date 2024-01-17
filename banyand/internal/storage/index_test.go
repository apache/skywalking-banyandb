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

package storage

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
)

var testSeriesPool pbv1.SeriesPool

func TestSeriesIndex_Primary(t *testing.T) {
	ctx := context.Background()
	path, fn := setUp(require.New(t))
	si, err := newSeriesIndex(ctx, path, 0)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, si.Close())
		fn()
	}()
	var docs index.Documents
	for i := 0; i < 100; i++ {
		series := testSeriesPool.Generate()
		series.Subject = "service_instance_latency"
		series.EntityValues = []*modelv1.TagValue{
			{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: fmt.Sprintf("svc_%d", i)}}},
			{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: fmt.Sprintf("svc_%d_instance_%d", i, i)}}},
		}

		// Initialize test data
		if err = series.Marshal(); err != nil {
			t.Fatalf("Failed to marshal series: %v", err)
		}
		require.True(t, series.ID > 0)
		doc := index.Document{
			DocID:        uint64(series.ID),
			EntityValues: make([]byte, len(series.Buffer)),
		}
		copy(doc.EntityValues, series.Buffer)
		docs = append(docs, doc)
		testSeriesPool.Release(series)
	}
	require.NoError(t, si.Write(docs))
	// Restart the index
	require.NoError(t, si.Close())
	si, err = newSeriesIndex(ctx, path, 0)
	require.NoError(t, err)
	tests := []struct {
		name         string
		subject      string
		entityValues []*modelv1.TagValue
		expected     []*modelv1.TagValue
	}{
		{
			name:    "Search",
			subject: "service_instance_latency",
			entityValues: []*modelv1.TagValue{
				{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "svc_1"}}},
				{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "svc_1_instance_1"}}},
			},
			expected: []*modelv1.TagValue{
				{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "svc_1"}}},
				{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "svc_1_instance_1"}}},
			},
		},
		{
			name:    "Prefix",
			subject: "service_instance_latency",
			entityValues: []*modelv1.TagValue{
				{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "svc_1"}}},
				pbv1.AnyTagValue,
			},
			expected: []*modelv1.TagValue{
				{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "svc_1"}}},
				{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "svc_1_instance_1"}}},
			},
		},
		{
			name:    "Wildcard",
			subject: "service_instance_latency",
			entityValues: []*modelv1.TagValue{
				pbv1.AnyTagValue,
				{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "svc_1_instance_1"}}},
			},
			expected: []*modelv1.TagValue{
				{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "svc_1"}}},
				{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "svc_1_instance_1"}}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			seriesQuery := testSeriesPool.Generate()
			defer testSeriesPool.Release(seriesQuery)
			seriesQuery.Subject = tt.subject
			seriesQuery.EntityValues = tt.entityValues
			sl, err := si.searchPrimary(ctx, seriesQuery)
			require.NoError(t, err)
			require.Equal(t, 1, len(sl))
			assert.Equal(t, tt.subject, sl[0].Subject)
			assert.Equal(t, tt.expected[0].GetStr().GetValue(), sl[0].EntityValues[0].GetStr().GetValue())
			assert.Equal(t, tt.expected[1].GetStr().GetValue(), sl[0].EntityValues[1].GetStr().GetValue())
			assert.True(t, sl[0].ID > 0)
		})
	}
}

func setUp(t *require.Assertions) (tempDir string, deferFunc func()) {
	t.NoError(logger.Init(logger.Logging{
		Env:   "dev",
		Level: flags.LogLevel,
	}))
	tempDir, deferFunc = test.Space(t)
	return tempDir, deferFunc
}
