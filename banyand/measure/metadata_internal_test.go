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

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
)

// nilIndexDBSegment is a stub Segment whose IndexDB() returns an untyped
// nil. The embedded interface lets us satisfy the Segment[*tsTable, option]
// signature without implementing every method (only IndexDB is exercised
// by collectSeriesIndexInfo).
type nilIndexDBSegment struct {
	storage.Segment[*tsTable, option]
}

func (nilIndexDBSegment) IndexDB() storage.IndexDB { return nil }

// TestSchemaRepo_CollectSeriesIndexInfo_NilIndexDB locks in the contract
// that collectSeriesIndexInfo returns an empty SeriesIndexInfo (no panic)
// when the segment is in the cold-tier residual state where its underlying
// series index has been torn down by performCleanup.
func TestSchemaRepo_CollectSeriesIndexInfo_NilIndexDB(t *testing.T) {
	sr := &schemaRepo{}
	info := sr.collectSeriesIndexInfo(nilIndexDBSegment{})
	require.NotNil(t, info)
	require.Equal(t, int64(0), info.DataCount)
	require.Equal(t, int64(0), info.DataSizeBytes)
}
