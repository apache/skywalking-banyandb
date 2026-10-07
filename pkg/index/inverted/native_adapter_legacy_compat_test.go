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

package inverted

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/metrics"
	"github.com/apache/skywalking-banyandb/pkg/index/native"
	"github.com/apache/skywalking-banyandb/pkg/index/nativeadapter"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

type adapterCompatLease struct{}

func (adapterCompatLease) Validate() error           { return nil }
func (adapterCompatLease) ValidatePath(string) error { return nil }

// TestLegacyStoreReadsNativeAdapterTimestamps writes element documents through
// the native adapter and reads the persisted directory back with the legacy
// store, the rollback oracle: a timestamp-bounded term query must find exactly
// the document inside the bound, with its timestamp.
func TestLegacyStoreReadsNativeAdapterTimestamps(t *testing.T) {
	path := t.TempDir()
	owner, err := native.NewOwner(native.OwnerOptions{Lease: adapterCompatLease{}, Path: path})
	require.NoError(t, err)
	adapter := &nativeadapter.Adapter{Owner: owner}
	field := index.NewStringField(index.FieldKey{IndexRuleID: 5, SeriesID: 1}, "ok")
	require.NoError(t, adapter.Batch(context.Background(), index.Batch{Documents: []index.Document{
		{DocID: 1, Timestamp: 200, Fields: []index.Field{field}},
		{DocID: 2, Timestamp: 100, Fields: []index.Field{field}},
	}}))
	require.NoError(t, owner.Close())

	legacy, err := NewStore(StoreOpts{
		Path:    path,
		Logger:  logger.GetLogger("test"),
		Metrics: metrics.NewMetrics(observability.BypassRegistry.With(observability.RootScope)),
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, legacy.Close()) }()
	timeRange := index.NewIntRangeOpts(50, 150, true, true)
	bounded := index.NewStringField(index.FieldKey{IndexRuleID: 5, SeriesID: 1, TimeRange: &timeRange}, "ok")
	documents, timestamps, err := legacy.MatchTerms(bounded)
	require.NoError(t, err)
	require.Equal(t, 1, documents.Len(), "exactly one document lies inside the bound")
	require.Equal(t, []uint64{100}, timestamps.ToSlice())
}
