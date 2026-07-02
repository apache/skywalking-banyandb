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
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	itersort "github.com/apache/skywalking-banyandb/pkg/iter/sort"
)

func TestBuildStaticPhase1CarriesOnlyLimitedRows(t *testing.T) {
	plan, err := BuildStaticPhase1([]string{"trace-a", "trace-b", "trace-c"}, map[string]int64{"trace-c": 3}, 2, 10)
	require.NoError(t, err)
	require.NoError(t, plan.Pipeline.Init(context.Background()))

	batch, err := plan.Pipeline.Next(context.Background())
	require.NoError(t, err)
	require.NotNil(t, batch)
	require.Equal(t, []uint16{0, 1}, batch.Selection)
	require.Equal(t, []string{"trace-a", "trace-b"}, plan.Carry.Order())
	require.Equal(t, map[int64][]string{0: {"trace-a", "trace-b"}}, plan.Carry.TraceIDsByPart())

	done, err := plan.Pipeline.Next(context.Background())
	require.NoError(t, err)
	require.Nil(t, done)
	require.NoError(t, plan.Pipeline.Close())
}

func TestBuildMergePhase1DecodesLimitsAndCarries(t *testing.T) {
	iter := newCountingIter([]*MergeItem{
		NewMergeItem(1, 10, 100, traceIDPayload("trace-a")),
		NewMergeItem(2, 20, 200, traceIDPayload("trace-b")),
	})
	plan, err := BuildMergePhase1([]itersort.Iterator[*MergeItem]{iter}, false, 1, 10)
	require.NoError(t, err)
	require.NoError(t, plan.Pipeline.Init(context.Background()))

	batch, err := plan.Pipeline.Next(context.Background())
	require.NoError(t, err)
	require.NotNil(t, batch)
	require.Equal(t, []uint16{0}, batch.Selection)
	require.Equal(t, []string{"trace-a"}, plan.Carry.Order())
	require.Equal(t, map[int64][]string{100: {"trace-a"}}, plan.Carry.TraceIDsByPart())
	require.Equal(t, map[string]int64{"trace-a": 1}, plan.Carry.Keys())

	done, err := plan.Pipeline.Next(context.Background())
	require.NoError(t, err)
	require.Nil(t, done)
	require.NoError(t, plan.Pipeline.Close())
}

func TestBuildStaticPhase1ZeroLimitIsUnbounded(t *testing.T) {
	plan, err := BuildStaticPhase1([]string{"trace-a", "trace-b"}, nil, 0, 10)
	require.NoError(t, err)
	require.NoError(t, plan.Pipeline.Init(context.Background()))
	batch, err := plan.Pipeline.Next(context.Background())
	require.NoError(t, err)
	require.NotNil(t, batch)
	require.Equal(t, []uint16{0, 1}, batch.Selection)
	require.Equal(t, []string{"trace-a", "trace-b"}, plan.Carry.Order())
	require.NoError(t, plan.Pipeline.Close())
}
