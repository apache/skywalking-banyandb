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

package stream

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	vstream "github.com/apache/skywalking-banyandb/pkg/query/vectorized/stream"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

func parseVectorizedFlags(t *testing.T, args ...string) vstream.VectorizedConfig {
	t.Helper()
	cfg := vstream.VectorizedConfig{}
	flagS := run.NewFlagSet("stream-vec-test")
	bindVectorizedFlags(flagS, &cfg)
	require.NoError(t, flagS.Parse(args))
	return cfg
}

func TestVectorizedEnabledFalseAbortsStartup(t *testing.T) {
	cfg := parseVectorizedFlags(t, "--stream-vectorized-enabled=false")
	err := cfg.Validate()
	require.Error(t, err, "--stream-vectorized-enabled=false must abort startup, not silently run the vec path")
	require.Contains(t, err.Error(), "apache/skywalking#13998")

	// Both node roles must surface it from their own Validate, which is what run.Group
	// calls before PreRun — otherwise the flag would be accepted and quietly ignored.
	standaloneSvc := &standalone{root: t.TempDir(), option: option{vectorized: cfg}}
	standaloneSvc.retentionConfig.Cooldown = time.Second
	require.ErrorContains(t, standaloneSvc.Validate(), "apache/skywalking#13998")
	liaisonSvc := &liaison{root: t.TempDir(), option: option{vectorized: cfg}}
	require.ErrorContains(t, liaisonSvc.Validate(), "apache/skywalking#13998")
}

func TestVectorizedEnabledTrueAndDefaultAreAccepted(t *testing.T) {
	require.NoError(t, parseVectorizedFlags(t, "--stream-vectorized-enabled=true").Validate())
	require.NoError(t, parseVectorizedFlags(t).Validate())
}
