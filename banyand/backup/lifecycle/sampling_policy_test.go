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
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/pipeline"
	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk"
)

func TestSourceSamplingChainSelection(t *testing.T) {
	hot := &commonv1.Plugin{Name: "hot"}
	warm := &commonv1.Plugin{Name: "warm"}
	config := &commonv1.TracePipelineConfig{
		Enabled:     true,
		Plugins:     []*commonv1.Plugin{{Name: "top-level"}},
		SchemaNames: []string{"selected"},
		Stages: []*commonv1.StageRule{
			{
				Stage:   "hot",
				Plugins: []*commonv1.Plugin{hot},
			},
			{
				Stage:   "warm",
				Plugins: []*commonv1.Plugin{warm},
			},
		},
	}
	selected, err := sourceSamplingChain(config, "hot", "selected")
	require.NoError(t, err)
	require.Equal(t, []*commonv1.Plugin{hot}, selected, "migration must select source chain even without MERGE/FINALIZE enablement")
	selected, err = sourceSamplingChain(config, "warm", "selected")
	require.NoError(t, err)
	require.Equal(t, []*commonv1.Plugin{warm}, selected)
	for _, test := range []struct{ source, name string }{{"hot", "unmatched"}, {"cold", "selected"}} {
		selected, err = sourceSamplingChain(config, test.source, test.name)
		require.NoError(t, err)
		require.Empty(t, selected)
	}
	config.Enabled = false
	selected, err = sourceSamplingChain(config, "", "selected")
	require.NoError(t, err)
	require.Empty(t, selected)
}

func TestSourceSamplingChainRejectsAmbiguityAndInvalidSelector(t *testing.T) {
	config := &commonv1.TracePipelineConfig{Enabled: true, Stages: []*commonv1.StageRule{{Stage: "hot"}, {Stage: "hot"}}}
	_, err := sourceSamplingChain(config, "hot", "trace")
	require.ErrorContains(t, err, "multiple sampling rules")
	_, err = sourceSamplingChain(config, "", "trace")
	require.ErrorContains(t, err, "identity is missing")
	config.SchemaNameRegex = "["
	_, err = sourceSamplingChain(config, "hot", "trace")
	require.ErrorContains(t, err, "invalid sampling schema selector")
	config.SchemaNameRegex = "^trace$"
	config.Stages = config.Stages[:1]
	selected, err := sourceSamplingChain(config, "hot", "trace")
	require.NoError(t, err)
	require.Empty(t, selected, "empty stage chain preserves legacy migration")
}

func TestSamplingHostFlagsValidate(t *testing.T) {
	service := &lifecycleService{orphanPolicyStr: "archive", samplingHost: samplingHostConfig{Enabled: true, Timeout: time.Second, CircuitBreakN: 3}}
	require.ErrorContains(t, service.Validate(), "trusted-dir is required")
	service.samplingHost.TrustedDirectory = t.TempDir()
	require.NoError(t, service.Validate())
	service.samplingHost.Timeout = 0
	require.ErrorContains(t, service.Validate(), "timeout must be positive")
	service.samplingHost.Timeout = time.Second
	service.samplingHost.CircuitBreakN = 0
	require.ErrorContains(t, service.Validate(), "circuit-break must be positive")
}

type statefulShardSampler struct {
	closed chan struct{}
	calls  int
	closes atomic.Int64
}

func (*statefulShardSampler) Kind() sdk.Kind          { return sdk.KindSampler }
func (*statefulShardSampler) Project() sdk.Projection { return sdk.Projection{} }
func (s *statefulShardSampler) Decide(*sdk.TraceBatch) (sdk.Verdict, error) {
	s.calls++
	return sdk.Verdict{Keep: []bool{s.calls%2 == 1}}, nil
}
func (s *statefulShardSampler) Close() error { s.closes.Add(1); close(s.closed); return nil }

func TestSamplingShardSessionsRestartNativeState(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "stateful.so")
	require.NoError(t, os.WriteFile(path, []byte("stateful-native-binary"), 0o600))
	digest, err := samplingFileDigest(path)
	require.NoError(t, err)
	preflight := &statefulShardSampler{closed: make(chan struct{})}
	policy := &pinnedSamplingPolicy{
		plugins: []*commonv1.Plugin{{
			Name: "stateful",
			Kind: &commonv1.Plugin_Sampler{Sampler: &commonv1.SamplerPlugin{
				Path:       "stateful.so",
				AbiVersion: uint32(sdk.ABIVersion),
			}},
		}},

		binaryDigests: []string{digest}, samplers: []sdk.Sampler{preflight}, host: samplingHostConfig{TrustedDirectory: directory, CircuitBreakN: 3},
	}
	var constructed []*statefulShardSampler
	newLoader := func() *pipeline.Loader {
		loader := pipeline.NewLoader()
		loader.OpenSampler = func(string, string, []byte) (sdk.Sampler, error) {
			sampler := &statefulShardSampler{closed: make(chan struct{})}
			constructed = append(constructed, sampler)
			return sampler, nil
		}
		return loader
	}
	batch := &sdk.TraceBatch{Traces: []sdk.TraceBlock{{TraceID: "trace"}}}
	for shard := 0; shard < 3; shard++ {
		child, loadErr := policy.newShardSessionWithLoader("group", newLoader)
		require.NoError(t, loadErr)
		first, executeErr := child.session.Execute(batch, time.Second)
		require.NoError(t, executeErr)
		require.Equal(t, []bool{true}, first.Keep, "new shard and restarted shard must begin at identical native state")
		second, executeErr := child.session.Execute(batch, time.Second)
		require.NoError(t, executeErr)
		require.Equal(t, []bool{false}, second.Keep, "sampler state must persist within one shard")
		child.close()
		child.close()
		select {
		case <-constructed[shard].closed:
		case <-time.After(time.Second):
			t.Fatal("idle shard sampler was not closed")
		}
		require.Equal(t, int64(1), constructed[shard].closes.Load())
	}
	require.Len(t, constructed, 3)
	require.NotSame(t, constructed[0], constructed[1])
	require.Zero(t, preflight.calls, "preflight instance must never influence shard decision state")
	require.Zero(t, preflight.closes.Load())
	policy.close()
	policy.close()
	require.Equal(t, int64(1), preflight.closes.Load())
}
