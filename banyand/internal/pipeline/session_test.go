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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk"
)

type testSampler struct {
	decide     func(*sdk.TraceBatch) (sdk.Verdict, error)
	closed     chan struct{}
	projection sdk.Projection
}

func (s *testSampler) Kind() sdk.Kind                                    { return sdk.KindSampler }
func (s *testSampler) Project() sdk.Projection                           { return s.projection }
func (s *testSampler) Decide(batch *sdk.TraceBatch) (sdk.Verdict, error) { return s.decide(batch) }
func (s *testSampler) Close() error {
	if s.closed != nil {
		close(s.closed)
	}
	return nil
}

func TestSessionChainAndProjection(t *testing.T) {
	first := &testSampler{
		projection: sdk.Projection{
			Tags:    []string{"a"},
			SpanIDs: true,
		},
		decide: func(*sdk.TraceBatch) (sdk.Verdict,
			error,
		) {
			return sdk.Verdict{Keep: []bool{
					false,
					true,
				}},
				nil
		},
	}
	failed := &testSampler{projection: sdk.Projection{Tags: []string{"a", "b"}, Spans: true}, decide: func(*sdk.TraceBatch) (sdk.Verdict, error) { panic("broken link") }}
	session := NewSession([]sdk.Sampler{first, failed}, 2)
	defer session.Close()
	require.Equal(t, sdk.Projection{Tags: []string{"a", "b"}, SpanIDs: true, Spans: true}, session.Projection())
	verdict, err := session.Execute(&sdk.TraceBatch{Traces: make([]sdk.TraceBlock, 2)}, time.Second)
	require.NoError(t, err)
	require.Equal(t, []bool{false, true}, verdict.Keep)
}

func TestSessionTimeoutOwnsBuffersAndDefersClose(t *testing.T) {
	release := make(chan struct{})
	entered := make(chan struct{})
	sampler := &testSampler{closed: make(chan struct{}), decide: func(batch *sdk.TraceBatch) (sdk.Verdict, error) {
		close(entered)
		<-release
		return sdk.Verdict{Keep: make([]bool, len(batch.Traces))}, nil
	}}
	session := NewSession([]sdk.Sampler{sampler}, 2)
	session.SetExecutionSlots(make(chan struct{}, 1))
	verdict, reusable, err := session.ExecuteInto(&sdk.TraceBatch{Traces: make([]sdk.TraceBlock, 1)}, 10*time.Millisecond, nil, nil)
	require.EqualError(t, err, "timeout")
	require.False(t, reusable)
	require.Equal(t, []bool{true}, verdict.Keep)
	<-entered
	_, reusable, err = session.ExecuteInto(&sdk.TraceBatch{Traces: make([]sdk.TraceBlock, 1)}, time.Second, nil, nil)
	require.EqualError(t, err, "circuit_open")
	require.True(t, reusable)
	session.ClosePluginsWhenIdle()
	select {
	case <-sampler.closed:
		t.Fatal("closed a running native instance")
	default:
	}
	close(release)
	select {
	case <-sampler.closed:
	case <-time.After(time.Second):
		t.Fatal("instance never closed")
	}
}

func TestSessionRecoversAfterTimedOutWorkerReturns(t *testing.T) {
	release := make(chan struct{})
	calls := 0
	sampler := &testSampler{decide: func(*sdk.TraceBatch) (sdk.Verdict, error) {
		calls++
		if calls == 1 {
			<-release
		}
		return sdk.Verdict{Keep: []bool{false}}, nil
	}}
	session := NewSession([]sdk.Sampler{sampler}, 3)
	defer session.Close()
	batch := &sdk.TraceBatch{Traces: make([]sdk.TraceBlock, 1)}
	_, reusable, err := session.ExecuteInto(batch, 10*time.Millisecond, nil, nil)
	require.EqualError(t, err, "timeout")
	require.False(t, reusable)
	close(release)
	<-session.done
	verdict, err := session.Execute(batch, time.Second)
	require.NoError(t, err)
	require.Equal(t, []bool{false}, verdict.Keep)
	open, timeouts := session.State()
	require.False(t, open)
	require.Zero(t, timeouts)
}

func TestSessionClosesRepeatedInstanceOnce(t *testing.T) {
	sampler := &testSampler{closed: make(chan struct{}), decide: func(*sdk.TraceBatch) (sdk.Verdict, error) { return sdk.Verdict{}, nil }}
	session := NewSession([]sdk.Sampler{sampler, sampler}, 3)
	session.ClosePluginsWhenIdle()
	session.ClosePluginsWhenIdle()
	select {
	case <-sampler.closed:
	case <-time.After(time.Second):
		t.Fatal("owned instance not closed")
	}
}

type projectionPanicSampler struct{ testSampler }

func (*projectionPanicSampler) Project() sdk.Projection { panic("invalid projection") }
func TestSessionCheckedRejectsProjectionPanic(t *testing.T) {
	session, err := NewSessionChecked([]sdk.Sampler{&projectionPanicSampler{}}, 3)
	require.Nil(t, session)
	require.ErrorContains(t, err, "sampler projection panic")
}
