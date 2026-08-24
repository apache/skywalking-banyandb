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

package measure

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	"github.com/apache/skywalking-banyandb/pkg/flow"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

type fakeTopNProcessor struct {
	schema *databasev1.TopNAggregation
	closed *bool
}

func (f fakeTopNProcessor) In() chan<- flow.StreamRecord            { return nil }
func (f fakeTopNProcessor) Setup(context.Context) error             { return nil }
func (f fakeTopNProcessor) Teardown(context.Context) error          { return nil }
func (f fakeTopNProcessor) Close() error                            { *f.closed = true; return nil }
func (f fakeTopNProcessor) Write(flow.StreamRecord)                 {}
func (f fakeTopNProcessor) TopNSchema() *databasev1.TopNAggregation { return f.schema }

type blockingTopNProcessor struct {
	writeStarted chan struct{}
	closeStarted chan struct{}
	releaseWrite chan struct{}
	writeMu      sync.Mutex
}

func (b *blockingTopNProcessor) In() chan<- flow.StreamRecord            { return nil }
func (b *blockingTopNProcessor) Setup(context.Context) error             { return nil }
func (b *blockingTopNProcessor) Teardown(context.Context) error          { return nil }
func (b *blockingTopNProcessor) TopNSchema() *databasev1.TopNAggregation { return nil }

func (b *blockingTopNProcessor) Write(flow.StreamRecord) {
	b.writeMu.Lock()
	defer b.writeMu.Unlock()
	close(b.writeStarted)
	<-b.releaseWrite
}

func (b *blockingTopNProcessor) Close() error {
	close(b.closeStarted)
	b.writeMu.Lock()
	b.writeMu.Unlock()
	return nil
}

func agg(name string) *databasev1.TopNAggregation {
	return &databasev1.TopNAggregation{Metadata: &commonv1.Metadata{Group: "g", Name: name}}
}

// TestTopNManager_UnregisterKeepsSiblings asserts that removing one top-n aggregation
// closes only its own processor and leaves the sibling aggregation on the same source
// measure running -- deleting one previously tore down every sibling.
func TestTopNManager_UnregisterKeepsSiblings(t *testing.T) {
	aggA, aggB := agg("a"), agg("b")
	var closedA, closedB bool
	mgr := &topNProcessorManager{
		registeredTasks: []*databasev1.TopNAggregation{aggA, aggB},
		processorList: []topNProcessor{
			fakeTopNProcessor{schema: aggA, closed: &closedA},
			fakeTopNProcessor{schema: aggB, closed: &closedB},
		},
	}

	empty := mgr.unregister(aggA)
	require.False(t, empty, "sibling b is still registered")
	require.True(t, closedA, "a's processor must be closed")
	require.False(t, closedB, "b's processor must stay alive")
	require.Len(t, mgr.registeredTasks, 1)
	require.Equal(t, "b", mgr.registeredTasks[0].GetMetadata().GetName())
	require.Len(t, mgr.processorList, 1)

	empty = mgr.unregister(aggB)
	require.True(t, empty, "no aggregation left after removing b")
	require.True(t, closedB, "b's processor closed on the last removal")
	require.Empty(t, mgr.processorList)
}

// TestRemoveTopNAggregation_DropsManagerOnlyWhenEmpty asserts the schemaRepo-level
// wrapper keeps the manager (and its siblings) alive until the last aggregation on a
// source measure is removed, at which point the manager is closed and dropped from the map.
func TestRemoveTopNAggregation_DropsManagerOnlyWhenEmpty(t *testing.T) {
	l := logger.GetLogger("test")
	sr := &schemaRepo{l: l}
	source := &commonv1.Metadata{Group: "g", Name: "m"}
	aggA, aggB := agg("a"), agg("b")
	var closedA, closedB bool
	mgr := &topNProcessorManager{
		l:               l,
		registeredTasks: []*databasev1.TopNAggregation{aggA, aggB},
		processorList: []topNProcessor{
			fakeTopNProcessor{schema: aggA, closed: &closedA},
			fakeTopNProcessor{schema: aggB, closed: &closedB},
		},
	}
	sr.topNProcessorMap.Store(getKey(source), mgr)

	sr.removeTopNAggregation(source, aggA)
	_, ok := sr.topNProcessorMap.Load(getKey(source))
	require.True(t, ok, "manager kept while sibling b remains")
	require.True(t, closedA)
	require.False(t, closedB)

	sr.removeTopNAggregation(source, aggB)
	_, ok = sr.topNProcessorMap.Load(getKey(source))
	require.False(t, ok, "manager dropped from the map after the last aggregation is removed")
	require.True(t, closedB)
}

// TestTopNManager_CloseDoesNotHoldManagerLockWhileWriterDrains ensures a blocked
// processor write cannot prevent the manager from transitioning to closed.
func TestTopNManager_CloseDoesNotHoldManagerLockWhileWriterDrains(t *testing.T) {
	processor := &blockingTopNProcessor{
		writeStarted: make(chan struct{}),
		closeStarted: make(chan struct{}),
		releaseWrite: make(chan struct{}),
	}
	manager := &topNProcessorManager{
		m:             &databasev1.Measure{},
		processorList: []topNProcessor{processor},
	}
	request := &measurev1.InternalWriteRequest{
		Request: &measurev1.WriteRequest{DataPoint: &measurev1.DataPointValue{}},
	}
	writeDone := make(chan struct{})
	go func() {
		manager.onMeasureWrite(context.Background(), 0, 0, request, manager.m)
		close(writeDone)
	}()
	require.Eventually(t, func() bool {
		select {
		case <-processor.writeStarted:
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond)

	closeErr := make(chan error, 1)
	go func() {
		closeErr <- manager.Close()
	}()
	require.Eventually(t, func() bool {
		select {
		case <-processor.closeStarted:
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond)

	locked := make(chan struct{})
	go func() {
		manager.Lock()
		manager.Unlock()
		close(locked)
	}()
	require.Eventually(t, func() bool {
		select {
		case <-locked:
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond)

	close(processor.releaseWrite)
	require.Eventually(t, func() bool {
		select {
		case <-writeDone:
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond)
	require.Eventually(t, func() bool {
		select {
		case closeResult := <-closeErr:
			require.NoError(t, closeResult)
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond)
}
