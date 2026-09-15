// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for additional
// information regarding copyright ownership. The ASF licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package inverted

import (
	"context"
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	querypkg "github.com/apache/skywalking-banyandb/pkg/query"
)

func TestSearchLargeLimitUsesScanBudget(t *testing.T) {
	store, openErr := NewStore(StoreOpts{Path: t.TempDir(), Logger: logger.GetLogger("scan-budget-test")})
	require.NoError(t, openErr)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	payloadKey := index.FieldKey{TagName: "payload"}
	require.NoError(t, store.InsertSeriesBatch(index.Batch{Documents: []index.Document{
		{EntityValues: []byte("first"), Fields: []index.Field{field(payloadKey, []byte(strings.Repeat("a", 4096)), false)}},
		{EntityValues: []byte("second"), Fields: []index.Field{field(payloadKey, []byte(strings.Repeat("b", 4096)), false)}},
	}}))
	searchQuery, buildErr := store.BuildQuery([]index.SeriesMatcher{
		{Type: index.SeriesMatcherTypeExact, Match: []byte("first")},
		{Type: index.SeriesMatcherTypeExact, Match: []byte("second")},
	}, nil, nil)
	require.NoError(t, buildErr)
	budget := protector.NewQueryBudget(nil)
	for _, limit := range []int{math.MaxInt32, math.MaxUint32} {
		t.Run(stringLimitName(limit), func(t *testing.T) {
			ctx, release, admitErr := budget.AdmitScanContext(context.Background())
			require.NoError(t, admitErr)
			result, searchErr := store.Search(ctx, []index.FieldKey{payloadKey}, searchQuery, limit)
			release()
			require.NoError(t, searchErr)
			require.Len(t, result, 2)
			require.Zero(t, budget.Reserved())
		})
	}
	t.Run("byte exhaustion returns no partial result", func(t *testing.T) {
		ctx, release, admitErr := budget.AdmitScanContext(context.Background())
		require.NoError(t, admitErr)
		defer release()
		lease, found := querypkg.BudgetLeaseFromContext(ctx)
		require.True(t, found)
		require.NoError(t, querypkg.Charge(ctx, lease.Limit()-lease.Used()-2048))
		result, searchErr := store.Search(ctx, []index.FieldKey{payloadKey}, searchQuery, math.MaxInt32)
		require.ErrorIs(t, searchErr, protector.ErrQueryResourceExhausted)
		require.Nil(t, result)
	})
	t.Run("result ceiling returns no partial result", func(t *testing.T) {
		ctx, release, admitErr := budget.AdmitScanContext(context.Background())
		require.NoError(t, admitErr)
		defer release()
		// Consume count credits only; do not allocate a large fixture.
		for count := 0; count < 99999; count++ {
			require.NoError(t, querypkg.ChargeResult(ctx, 0))
		}
		result, searchErr := store.Search(ctx, []index.FieldKey{payloadKey}, searchQuery, math.MaxInt32)
		require.ErrorIs(t, searchErr, protector.ErrQueryResourceExhausted)
		require.Nil(t, result)
	})
}

func stringLimitName(limit int) string {
	if limit == math.MaxInt32 {
		return "signed maximum"
	}
	return "unsigned maximum"
}
