// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain a
// copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package grpc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	bydbqlv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/bydbql/v1"
	"github.com/apache/skywalking-banyandb/banyand/liaison/pkg/auth"
)

const bydbQLObservabilityPolicy = `
users:
  - username: "reader"
    password: "reader-secret"
rbac:
  enabled: true
  bindings:
    - principal: "reader"
      role: "reader"
      groups: ["*"]
`

func TestBydbQLQuery_ParseErrorRecordsAdmissionDecision(t *testing.T) {
	t.Helper()
	snapshot, snapshotErr := auth.CompileSnapshot(1, []byte(bydbQLObservabilityPolicy))
	require.NoError(t, snapshotErr)

	factory, metricSet := newRecordingMetrics(t)
	service := &bydbQLService{metrics: metricSet, cache: newPreparedCache(16, 1<<20, metricSet)}
	ctx := ContextWithSnapshot(context.Background(), snapshot)
	_, queryErr := service.Query(ctx, &bydbqlv1.QueryRequest{Query: "NOT A QUERY"})
	require.Equal(t, codes.InvalidArgument, status.Code(queryErr))

	decisionCalls := factory.counter("rbac_decisions_total").snapshot()
	require.Len(t, decisionCalls, 1, "an admitted ByDBQL call must record one decision even when parsing fails")
	assert.Equal(t, []string{"allow", "data:read", "banyandb.bydbql.v1.BydbQLService/Query", "granted"}, decisionCalls[0].labels)
}
