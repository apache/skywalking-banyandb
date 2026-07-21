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

package pub

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	grpc_health_v1 "google.golang.org/grpc/health/grpc_health_v1"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// The periodic prober itself has long been implemented and tested in pkg/grpchelper; what
// was missing is that PreRun never passed an interval, so for the queue client the prober
// simply never started. The active set was then only ever corrected by a request failing on
// a dead node, which is why a node that disappeared stayed routable until some query paid
// the full timeout to discover it.
//
// These tests therefore pin the WIRING, not the mechanism. Both fail before the fix: the
// default assertion because no flag existed, and the eviction assertion because a zero
// interval leaves the prober unstarted, so the node stays active forever.
func TestFlagSetSuppliesNonZeroHealthCheckInterval(t *testing.T) {
	p := New(nil, databasev1.Role_ROLE_DATA).(*pub)
	p.FlagSet() // DurationVar writes the default through immediately

	assert.NotZero(t, p.healthCheckInterval,
		"a zero interval silently disables the prober, which is the bug this flag exists to prevent")
	assert.Equal(t, defaultHealthCheckInterval, p.healthCheckInterval)
}

func TestPreRunStartsPeriodicHealthCheck(t *testing.T) {
	addr := getAddress()
	healthSrv, stopServer := setupWithStatus(addr, modelv1.Status_STATUS_SUCCEED)
	defer stopServer()

	p := New(nil, databasev1.Role_ROLE_DATA).(*pub)
	p.log = logger.GetLogger("queue-client")
	p.FlagSet()
	p.healthCheckInterval = 100 * time.Millisecond // keep the test short
	require.NoError(t, p.PreRun(context.Background()))
	defer p.connMgr.GracefulStop()

	p.OnAddOrUpdate(getDataNode("node1", addr))
	require.Eventually(t, func() bool { return p.connMgr.ActiveCount() == 1 },
		10*time.Second, 20*time.Millisecond, "the healthy node should be admitted")

	// The node stops serving. Nothing publishes to it, so only a background prober can
	// notice — which is exactly what a zero interval would fail to do.
	healthSrv.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)

	assert.Eventually(t, func() bool { return p.connMgr.ActiveCount() == 0 },
		10*time.Second, 20*time.Millisecond,
		"the prober must evict the unreachable node without any request being sent to it")
}
