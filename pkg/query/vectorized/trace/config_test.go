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

package trace

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	require.False(t, cfg.Enabled)
	require.Equal(t, vectorized.DefaultBatchSize, cfg.BatchSize)
	require.Equal(t, 256, cfg.QueryMemoryMiB)
	require.NoError(t, cfg.Validate())
}

func TestVectorizedConfigValidate(t *testing.T) {
	require.Error(t, VectorizedConfig{BatchSize: 0, QueryMemoryMiB: 256}.Validate())
	require.Error(t, VectorizedConfig{BatchSize: 1, QueryMemoryMiB: 0}.Validate())
	require.NoError(t, VectorizedConfig{BatchSize: 1, QueryMemoryMiB: 1}.Validate())
}
