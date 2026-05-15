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
	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
)

// fakeMeasureQueryResult yields a hand-built sequence of MeasureResults.
// Mirrors the shape produced by banyand/measure/query.go's queryResult.Pull.
type fakeMeasureQueryResult struct {
	seq        []*model.MeasureResult
	idx        int
	releaseCnt int
}

func (f *fakeMeasureQueryResult) Pull() *model.MeasureResult {
	if f.idx >= len(f.seq) {
		return nil
	}
	r := f.seq[f.idx]
	f.idx++
	return r
}

func (f *fakeMeasureQueryResult) Release() {
	f.releaseCnt++
}

// mkResult constructs a minimal MeasureResult with parallel arrays sized to
// match ts. Versions and ShardIDs are zero-valued; tags and fields are empty.
func mkResult(sid common.SeriesID, ts ...int64) *model.MeasureResult {
	return &model.MeasureResult{
		SID:        sid,
		Timestamps: ts,
		Versions:   make([]int64, len(ts)),
		ShardIDs:   make([]common.ShardID, len(ts)),
	}
}

// mkResultErr constructs a MeasureResult carrying a storage error.
// MeasureQueryResult.Pull may return such results to signal pull-time failures.
func mkResultErr(err error) *model.MeasureResult {
	return &model.MeasureResult{Error: err}
}
