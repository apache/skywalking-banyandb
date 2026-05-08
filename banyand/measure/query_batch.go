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
	"context"
	"fmt"

	"github.com/apache/skywalking-banyandb/pkg/query/model"
	vmeasure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"
)

// Compile-time assertions that both result types satisfy the columnar
// interface alongside MeasureQueryResult.
var (
	_ model.MeasureBatchResult = (*queryResult)(nil)
	_ model.MeasureBatchResult = (*indexSortResult)(nil)
)

// PullBatch implements model.MeasureBatchResult for queryResult.
//
// G5b "dual-emit" wrapper: the implementation calls Pull() to obtain the
// next *model.MeasureResult and converts it to a *model.MeasureBatch using
// vmeasure.BuildMeasureBatchFromResult. Pull()'s row-path output stays
// byte-identical for legacy consumers; PullBatch is purely additive.
//
// Mixing Pull() and PullBatch() on the same queryResult is undefined —
// each call advances the same underlying cursors. Callers must pick one.
//
// Sticky-error contract: if Pull() returns a *MeasureResult with
// non-nil Error, PullBatch returns (nil, that error) so future calls
// continue to surface the same error (Pull() is itself sticky here).
func (qr *queryResult) PullBatch(_ context.Context) (*model.MeasureBatch, error) {
	if qr.batchSchema == nil {
		return nil, fmt.Errorf("queryResult.PullBatch: batchSchema not initialized; " +
			"the underlying query did not record a vectorized BatchSchema (likely a schema-build error at Query time)")
	}
	r := qr.Pull()
	if r == nil {
		return nil, nil
	}
	if r.Error != nil {
		return nil, r.Error
	}
	return vmeasure.BuildMeasureBatchFromResult(r, qr.batchSchema)
}

// PullBatch implements model.MeasureBatchResult for indexSortResult.
//
// indexSortResult.Pull() yields single-row MeasureResults; the conversion
// shape is identical to queryResult.PullBatch — single-row batches that
// the vectorized adapter accumulates into larger pipeline batches.
func (iqr *indexSortResult) PullBatch(_ context.Context) (*model.MeasureBatch, error) {
	if iqr.batchSchema == nil {
		return nil, fmt.Errorf("indexSortResult.PullBatch: batchSchema not initialized; " +
			"the underlying query did not record a vectorized BatchSchema (likely a schema-build error at Query time)")
	}
	r := iqr.Pull()
	if r == nil {
		return nil, nil
	}
	if r.Error != nil {
		return nil, r.Error
	}
	return vmeasure.BuildMeasureBatchFromResult(r, iqr.batchSchema)
}
