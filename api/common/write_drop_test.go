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

package common

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/apache/skywalking-banyandb/pkg/meter"
)

// heldGroups holds the groups in the set.
type heldGroups map[string]bool

func (h heldGroups) LoadGroup(name string) (struct{}, bool) { return struct{}{}, h[name] }

func TestReasonOfFindsTheTagAnywhereInTheChain(t *testing.T) {
	cause := errors.New("group g not found")
	tagged := NewRowDropError(DropReasonGroupNotFound, fmt.Errorf("cannot load tsdb for group g: %w", cause))
	wrapped := fmt.Errorf("stream g/sw: %w", tagged)

	require.Equal(t, DropReasonGroupNotFound, ReasonOf(tagged))
	require.Equal(t, DropReasonGroupNotFound, ReasonOf(wrapped), "a caller may wrap the tagged error")
	require.Equal(t, "cannot load tsdb for group g: group g not found", tagged.Error(), "the tag leaves the message as it was")
	require.ErrorIs(t, wrapped, cause, "the tag keeps the cause reachable")
	require.Equal(t, DropReasonInvalidElement, ReasonOf(errors.New("has no tag family")), "an untagged error is an invalid element")
}

func TestSkipRowCountsWithTheGroupOnlyWhenTheNodeHoldsIt(t *testing.T) {
	counter := meter.NewMockCounter(gomock.NewController(t))
	counter.EXPECT().Inc(float64(1), "g", string(DropReasonInvalidTimestamp)).Times(2)
	counter.EXPECT().Inc(float64(1), "", string(DropReasonGroupNotFound)).Times(1)
	counter.EXPECT().Inc(float64(1), "", string(DropReasonDecode)).Times(1)
	counter.EXPECT().Inc(float64(1), "", string(DropReasonInvalidType)).Times(1)
	d := NewWriteDrops[struct{}](counter, heldGroups{"g": true})
	ctx := context.Background()

	d.SkipRow(ctx, 0, "g", DropReasonInvalidTimestamp, errors.New("invalid timestamp"))
	d.SkipRow(ctx, 1, "g", DropReasonInvalidTimestamp, errors.New("invalid timestamp"))
	d.SkipRow(ctx, 2, "any-name-a-writer-sends", DropReasonGroupNotFound, errors.New("group not found"))
	d.SkipRow(ctx, 3, "", DropReasonDecode, errors.New("cannot decode"))
	d.SkipMessage(ctx, DropReasonInvalidType)
}

func TestSkipRowRecordsInsteadOfCountingForACallerThatRecords(t *testing.T) {
	counter := meter.NewMockCounter(gomock.NewController(t)) // no call is expected
	d := NewWriteDrops[struct{}](counter, heldGroups{"g": true})
	recorder := NewWriteRecorder(time.Time{})
	ctx := WithWriteRecorder(context.Background(), recorder)

	d.SkipRow(ctx, 4, "g", DropReasonInvalidTimestamp, errors.New("invalid timestamp"))
	d.SkipMessage(ctx, DropReasonInvalidType)
	failures := recorder.RowFailures()
	require.Len(t, failures, 1, "the caller is told which row failed and may write it again, so the row is not counted")
	require.Equal(t, 4, failures[0].Row)
	require.ErrorContains(t, failures[0].Err, "invalid timestamp")
}

func TestNilWriteDropsStillRecords(t *testing.T) {
	require.Nil(t, NewWriteDrops[struct{}](nil, heldGroups{}), "a listener built without metrics has no counter")
	var d *WriteDrops
	recorder := NewWriteRecorder(time.Time{})
	require.NotPanics(t, func() {
		d.SkipRow(context.Background(), 0, "g", DropReasonDecode, errors.New("cannot decode"))
		d.SkipMessage(context.Background(), DropReasonInvalidType)
		d.SkipRow(WithWriteRecorder(context.Background(), recorder), 1, "g", DropReasonDecode, errors.New("cannot decode"))
	})
	require.Len(t, recorder.RowFailures(), 1, "a caller that records is told about the row whatever the metrics")
}
