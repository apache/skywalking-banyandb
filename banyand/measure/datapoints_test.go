// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package measure

import (
	"sort"
	"testing"

	"github.com/apache/skywalking-banyandb/api/common"
)

func TestSorting(t *testing.T) {
	dp := &dataPoints{
		seriesIDs:   []common.SeriesID{3, 1, 1, 2},
		timestamps:  []int64{300, 100, 100, 200},
		versions:    []int64{3, 1, 3, 2},
		tagFamilies: [][]nameValues{{}, {}, {}, {}},
		fields:      []nameValues{{}, {}, {}, {}},
	}

	sort.Sort(dp)

	expectedSeriesIDs := []common.SeriesID{1, 1, 2, 3}
	expectedTimestamps := []int64{100, 100, 200, 300}
	expectedVersions := []int64{3, 1, 2, 3}

	for i := range dp.seriesIDs {
		if dp.seriesIDs[i] != expectedSeriesIDs[i] {
			t.Errorf("Expected seriesID at index %d to be %d, got %d", i, expectedSeriesIDs[i], dp.seriesIDs[i])
		}
		if dp.timestamps[i] != expectedTimestamps[i] {
			t.Errorf("Expected timestamp at index %d to be %d, got %d", i, expectedTimestamps[i], dp.timestamps[i])
		}
		if dp.versions[i] != expectedVersions[i] {
			t.Errorf("Expected version at index %d to be %d, got %d", i, expectedVersions[i], dp.versions[i])
		}
	}

	dp.skip(1)

	expectedSeriesIDs = []common.SeriesID{1, 2, 3}
	expectedTimestamps = []int64{100, 200, 300}
	expectedVersions = []int64{3, 2, 3}

	for i := range dp.seriesIDs {
		if dp.seriesIDs[i] != expectedSeriesIDs[i] {
			t.Errorf("Expected seriesID at index %d to be %d, got %d", i, expectedSeriesIDs[i], dp.seriesIDs[i])
		}
		if dp.timestamps[i] != expectedTimestamps[i] {
			t.Errorf("Expected timestamp at index %d to be %d, got %d", i, expectedTimestamps[i], dp.timestamps[i])
		}
		if dp.versions[i] != expectedVersions[i] {
			t.Errorf("Expected version at index %d to be %d, got %d", i, expectedVersions[i], dp.versions[i])
		}
	}
}
