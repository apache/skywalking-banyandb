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

package pipeline

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk"
)

func TestLoaderRejectsChangedMappedBinary(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "sampler.so")
	require.NoError(t, os.WriteFile(path, []byte("binary-one"), 0o600))
	loader := NewLoader()
	calls := 0
	loader.OpenSampler = func(string, string, []byte) (sdk.Sampler, error) { calls++; return &testSampler{}, nil }
	config := &commonv1.SamplerPlugin{Path: "sampler.so", AbiVersion: uint32(sdk.ABIVersion)}
	_, err := loader.LoadSamplerPlugin(config, directory, "group", nil)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, []byte("binary-two"), 0o600))
	_, err = loader.LoadSamplerPlugin(config, directory, "group", nil)
	require.ErrorContains(t, err, "changed at previously loaded path")
	require.Equal(t, 1, calls, "a cache hit cannot mask replacement of mapped code")
	fresh := NewLoader()
	fresh.OpenSampler = loader.OpenSampler
	_, err = fresh.LoadSamplerPlugin(config, directory, "group", nil)
	require.ErrorContains(t, err, "changed at previously loaded path", "new policy sessions share process-wide mapped identities")
}

func TestLoaderRejectsReplacementDuringConstruction(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "sampler.so")
	require.NoError(t, os.WriteFile(path, []byte("binary-one"), 0o600))
	sampler := &testSampler{closed: make(chan struct{})}
	loader := NewLoader()
	loader.OpenSampler = func(string, string, []byte) (sdk.Sampler, error) {
		if err := os.WriteFile(path, []byte("binary-two"), 0o600); err != nil {
			return nil, err
		}
		return sampler, nil
	}
	_, err := loader.LoadSamplerPlugin(&commonv1.SamplerPlugin{Path: "sampler.so", AbiVersion: uint32(sdk.ABIVersion)}, directory, "group", nil)
	require.ErrorContains(t, err, "changed while loading")
	select {
	case <-sampler.closed:
	default:
		t.Fatal("discarded instance was not closed")
	}
}
