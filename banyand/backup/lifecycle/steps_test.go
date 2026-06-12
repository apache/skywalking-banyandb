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

package lifecycle

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
)

// TestParseGroup_RejectsMissingIntervals verifies that parseGroup fails fast
// with a descriptive error when required IntervalRule fields are nil, rather
// than panicking later inside the migration log. parseGroup validates these
// fields before touching metadata.Repo or the cluster state manager, so
// passing nil for those parameters is safe in this test - if a future change
// reorders parseGroup so the deps run first, the tests will panic clearly
// and that contract should be revisited.
func TestParseGroup_RejectsMissingIntervals(t *testing.T) {
	makeGroup := func(mutate func(ro *commonv1.ResourceOpts)) *commonv1.Group {
		ro := &commonv1.ResourceOpts{
			ShardNum:        1,
			SegmentInterval: dayInterval(1),
			Ttl:             dayInterval(7),
			Stages: []*commonv1.LifecycleStage{
				stage(stageWarm, selectorWarm, 7, 7),
				stage(stageCold, selectorCold, 15, 30),
			},
		}
		mutate(ro)
		return &commonv1.Group{
			Metadata:     &commonv1.Metadata{Name: "test-group"},
			ResourceOpts: ro,
		}
	}

	cases := []struct {
		name    string
		mutate  func(*commonv1.ResourceOpts)
		errFrag string
	}{
		{
			name:    "top-level ttl missing",
			mutate:  func(ro *commonv1.ResourceOpts) { ro.Ttl = nil },
			errFrag: "group test-group: missing ttl",
		},
		{
			name:    "top-level segment_interval missing",
			mutate:  func(ro *commonv1.ResourceOpts) { ro.SegmentInterval = nil },
			errFrag: "group test-group: missing segment_interval",
		},
		{
			name:    "stage segment_interval missing",
			mutate:  func(ro *commonv1.ResourceOpts) { ro.Stages[0].SegmentInterval = nil },
			errFrag: "group test-group stage warm: missing segment_interval",
		},
		{
			name:    "stage ttl missing",
			mutate:  func(ro *commonv1.ResourceOpts) { ro.Stages[0].Ttl = nil },
			errFrag: "group test-group stage warm: missing ttl",
		},
		{
			name:    "next stage segment_interval missing",
			mutate:  func(ro *commonv1.ResourceOpts) { ro.Stages[1].SegmentInterval = nil },
			errFrag: "group test-group stage cold: missing segment_interval",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			g := makeGroup(c.mutate)
			_, err := parseGroup(g, map[string]string{"type": "warm"}, nil, nil, nil, nil, nil, nil)
			require.Error(t, err)
			assert.Contains(t, err.Error(), c.errFrag)
		})
	}
}

// TestResolveSelfIdentity exercises the post-fix pod-name primary lookup
// against representative registry shapes. The first three cases prove
// the bug fix: a host-portion match on the registry's GrpcAddress
// (whether loopback, IP, or FQDN) resolves to the co-located data
// node's Metadata.Name. The last two cases prove the no-match and
// empty-input guards.
func TestResolveSelfIdentity(t *testing.T) {
	nodes := []*databasev1.Node{
		// Production-bug repro: lifecycle's --grpc-addr is
		// 127.0.0.1:17912, but the data pod registered its
		// GrpcAddress as the headless-service DNS form. The new
		// algorithm must match on the host portion (data-hot-0).
		{Metadata: &commonv1.Metadata{Name: "data-hot-0:17912"}, GrpcAddress: "data-hot-0.data-hot-headless.ns:17912", Labels: map[string]string{"type": "hot"}},

		// Loopback-registered case: --grpc-addr "127.0.0.1:17912"
		// matches the loopback form via isLoopbackHost equivalence.
		{Metadata: &commonv1.Metadata{Name: "data-warm-0:17912"}, GrpcAddress: "127.0.0.1:17912", Labels: map[string]string{"type": "warm"}},

		// IP-only registered case (NodeHostProvider=ip): must NOT
		// match unless the selfPodHost is itself an IP, so we
		// expect an empty result here. The first case in this
		// table covers the production path; this one documents
		// the known limitation of the post-fix algorithm.
		{Metadata: &commonv1.Metadata{Name: "data-cold-0:17912"}, GrpcAddress: "10.116.3.84:17912", Labels: map[string]string{"type": "cold"}},
	}

	node, tier, ok := resolveSelfIdentity("data-hot-0", nodes)
	assert.True(t, ok, "DNS-form GrpcAddress must match by host portion (the production-bug case)")
	assert.Equal(t, "data-hot-0:17912", node)
	assert.Equal(t, "hot", tier)

	node, tier, ok = resolveSelfIdentity("127.0.0.1", nodes)
	assert.True(t, ok, "loopback GrpcAddress matches a loopback selfPodHost")
	assert.Equal(t, "data-warm-0:17912", node)
	assert.Equal(t, "warm", tier)

	// selfPodHost="10.116.3.84" matches the IP-form entry.
	node, tier, ok = resolveSelfIdentity("10.116.3.84", nodes)
	assert.True(t, ok, "IP-form GrpcAddress matches an IP selfPodHost")
	assert.Equal(t, "data-cold-0:17912", node)
	assert.Equal(t, "cold", tier)

	// selfPodHost="data-warm-1" is not in the registry: no match.
	_, _, ok = resolveSelfIdentity("data-warm-1", nodes)
	assert.False(t, ok, "selfPodHost not in registry must return ok=false (no wildcard)")

	// Empty selfPodHost: no match.
	_, _, ok = resolveSelfIdentity("", nodes)
	assert.False(t, ok, "empty selfPodHost must return ok=false (no panic)")

	// grpcAddrEqual: post-fix also accepts host-portion exact match.
	assert.True(t, grpcAddrEqual("data-x.headless:17912", "data-x:17912"),
		"same port and same host (after SplitHostPort) must match")
	assert.True(t, grpcAddrEqual("127.0.0.1:17912", "127.0.0.1:17912"),
		"exact match still works")
	assert.True(t, grpcAddrEqual("localhost:17912", "127.0.0.1:17912"),
		"loopback-vs-loopback still works")
	assert.False(t, grpcAddrEqual("data-x:17912", "data-y:17912"),
		"different hosts with same port must not match")
}

// TestSelfPodHostnamePrecedence checks that selfPodHostname() prefers
// POD_NAME (the K8s downward API value) and falls back to os.Hostname()
// when POD_NAME is unset.
func TestSelfPodHostnamePrecedence(t *testing.T) {
	t.Setenv("POD_NAME", "my-pod")
	assert.Equal(t, "my-pod", selfPodHostname(),
		"POD_NAME takes precedence (K8s downward API)")

	t.Setenv("POD_NAME", "")
	// Without POD_NAME, the function falls back to os.Hostname().
	// The test host name is non-empty on Linux, so the result is
	// either the test runner's hostname or "" if uname fails.
	// Either way it must not panic and must not return "my-pod".
	got := selfPodHostname()
	assert.NotEqual(t, "my-pod", got,
		"empty POD_NAME must fall back to os.Hostname(), not return the previous value")
}
