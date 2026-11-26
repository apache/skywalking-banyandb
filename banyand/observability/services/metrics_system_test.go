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

package services

import (
	"context"
	"net/http"
	"net/http/httptest"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestCollectCPUExposesLimitCores asserts the cpu_limit_cores gauge equals GOMAXPROCS,
// which is set to 1 so that it differs from runtime.NumCPU() on a multi-CPU host.
func TestCollectCPUExposesLimitCores(t *testing.T) {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(1))
	svc := newTestPromService(t, true)
	require.NoError(t, svc.PreRun(context.Background()))

	svc.initMetrics()
	collectCPU()

	rec := httptest.NewRecorder()
	svc.PrometheusHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), "banyandb_system_cpu_limit_cores 1\n")
}
