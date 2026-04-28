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

package run

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/panicdiag"
)

func TestGo_RecoveryCapturesBreadcrumbsAddedInFn(t *testing.T) {
	t.Helper()

	reported := make(chan *panicdiag.PanicRecord, 1)
	panicdiag.SetDefaultReporter(func(_ context.Context, result panicdiag.RecoveryResult) {
		select {
		case reported <- result.Record:
		default:
		}
	})
	t.Cleanup(func() { panicdiag.SetDefaultReporter(nil) })

	Go(context.Background(), "test", logger.GetLogger("test"), func(ctx context.Context) {
		panicdiag.WithBreadcrumb(ctx, "stage-A", "component", nil)
		panicdiag.WithBreadcrumb(ctx, "stage-B", "component", nil)
		panic("boom")
	})

	select {
	case capturedRecord := <-reported:
		require.NotNil(t, capturedRecord)
		require.Len(t, capturedRecord.Breadcrumbs, 2)
		require.Equal(t, "stage-A", capturedRecord.Breadcrumbs[0].Stage)
		require.Equal(t, "stage-B", capturedRecord.Breadcrumbs[1].Stage)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for panic recovery")
	}
}
